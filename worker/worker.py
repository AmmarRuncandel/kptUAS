"""
worker/worker.py
================
Entry point Worker Node.

Komponen b – Parallel Processing: ProcessPoolExecutor multicore.
Komponen c – Distributed Processing: koneksi TCP ke Master, kirim/terima pesan.
Komponen d – Load Balancing: pull-based (REQUEST_TASK saat ada slot kosong).
Komponen e – Fault Tolerance: heartbeat periodik + auto-reconnect + crash simulation.

Jalankan:
    python worker/worker.py --master-host 192.168.1.1 --worker-id w01

Simulasi crash:
    python worker/worker.py --master-host ... --crash-after 5
    python worker/worker.py --master-host ... --crash-at 30
"""

from __future__ import annotations
import argparse
import concurrent.futures
import json
import logging
import os
import socket
import sys
import threading
import time
from typing import Dict, Optional, Tuple

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from common.protocol import send_message, recv_message, MsgType
from common.tasks import Task, TaskResult
from common.utils import setup_logging, format_bytes
from common.discovery import discover_master
from worker.executor import WorkerExecutor
from worker.monitor import ResourceMonitor

logger = logging.getLogger("worker")

DEFAULTS = {
    "master_host":        "auto",
    "master_port":        9000,
    "num_procs":          None,          # default: os.cpu_count()
    "heartbeat_interval": 2.0,
    "reconnect_delay":    3.0,
    "max_reconnect":      10,
    "passcode":           None,
    "log_level":          "INFO",
    "throttle":           0.0,
    "crash_after":        None,
    "crash_at":           None,
}


class WorkerNode:
    """
    Node Worker yang terhubung ke Master, mengeksekusi task kompresi secara paralel.
    """

    def __init__(self, cfg: dict) -> None:
        self.master_host = cfg["master_host"]
        self.master_port = cfg["master_port"]
        self.worker_id   = cfg.get("worker_id") or f"w-{socket.gethostname()}"
        self.passcode    = cfg.get("passcode")
        self.transfer_mode = "socket"  # diperbarui saat REGISTER_ACK

        self.executor = WorkerExecutor(
            num_procs   = cfg.get("num_procs"),
            worker_id   = self.worker_id,
            throttle_pct= cfg.get("throttle", 0.0),
        )
        self.monitor = ResourceMonitor(worker_id=self.worker_id)

        self._cfg  = cfg
        self._sock: Optional[socket.socket] = None
        self._running = False
        self._tasks_done = 0

        # Simulasi crash (Komponen e)
        self._crash_after = cfg.get("crash_after")  # crash setelah N task
        self._crash_at    = cfg.get("crash_at")      # crash setelah N detik
        self._start_time  = 0.0

        # Futures yang sedang berjalan: task_id → (Future, Task, raw_data)
        self._pending: Dict[str, Tuple] = {}
        self._pending_lock = threading.Lock()
        self._send_lock    = threading.Lock()

    def _send(self, msg_type: str, payload: dict,
              binary_data: Optional[bytes] = None) -> None:
        """Kirim pesan ke socket dengan lock thread-safe."""
        with self._send_lock:
            if self._sock:
                send_message(self._sock, msg_type, payload, binary_data=binary_data)

    # ------------------------------------------------------------------
    #  Koneksi
    # ------------------------------------------------------------------

    def connect(self) -> bool:
        """Hubungkan ke Master dengan dukungan Auto-Discovery dan backoff."""
        if self.master_host == "auto":
            if self.passcode:
                logger.info(f"Mencari Master dengan sandi '{self.passcode}' di LAN (UDP Broadcast)...")
            else:
                logger.info("Mencari Master secara otomatis di jaringan lokal (UDP Broadcast)...")
            found = discover_master(timeout=3.5, passcode=self.passcode)
            if found:
                self.master_host, self.master_port = found
                logger.info(f"-> Berhasil menemukan Master di {self.master_host}:{self.master_port}")
            else:
                if self.passcode:
                    logger.warning(
                        f"Master dengan sandi '{self.passcode}' belum merespons broadcast. "
                        f"Mencoba fallback ke 127.0.0.1:{self.master_port}..."
                    )
                else:
                    logger.warning("Master tidak merespons broadcast. Mencoba fallback ke 127.0.0.1...")
                self.master_host = "127.0.0.1"

        delay = self._cfg.get("reconnect_delay", 3.0)
        max_tries = self._cfg.get("max_reconnect", 10)
        for attempt in range(1, max_tries + 1):
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(10.0)
                sock.connect((self.master_host, self.master_port))
                sock.settimeout(None)
                self._sock = sock
                logger.info(f"Terhubung ke Master {self.master_host}:{self.master_port}")
                return True
            except (OSError, ConnectionRefusedError) as e:
                # Jika sebelumnya gagal konek pada host auto, coba discover ulang
                if self._cfg.get("master_host") == "auto" and attempt % 2 == 0:
                    found = discover_master(timeout=2.0, passcode=self.passcode)
                    if found:
                        self.master_host, self.master_port = found
                logger.warning(
                    f"Gagal konek ke {self.master_host}:{self.master_port} (percobaan {attempt}/{max_tries}): {e}. "
                    f"Coba lagi dalam {delay:.0f}s..."
                )
                time.sleep(delay)
                delay = min(delay * 1.5, 30.0)
        return False

    def register(self) -> bool:
        """Kirim REGISTER ke Master dengan verifikasi sandi."""
        try:
            import os as _os
            num_cores = _os.cpu_count() or 1
            try:
                import psutil
                ram_gb = psutil.virtual_memory().total / (1024 ** 3)
            except ImportError:
                ram_gb = 0.0

            self._send(MsgType.REGISTER, {
                "worker_id": self.worker_id,
                "hostname":  socket.gethostname(),
                "ip":        socket.gethostbyname(socket.gethostname()),
                "num_cores": num_cores,
                "ram_gb":    round(ram_gb, 2),
                "num_procs": self.executor.num_procs,
                "passcode":  self.passcode,
            })
            msg, _ = recv_message(self._sock)
            if msg.get("type") == MsgType.REGISTER_ACK:
                self.transfer_mode = msg.get("transfer", "socket")
                logger.info(
                    f"Terdaftar sebagai {self.worker_id} "
                    f"[transfer={self.transfer_mode}]"
                )
                return True
            elif msg.get("type") == MsgType.ERROR:
                err_msg = msg.get("message", "Akses ditolak")
                logger.error(f"❌ Registrasi DITOLAK oleh Master: {err_msg}")
                return False
            return False
        except Exception as e:
            logger.error(f"Gagal register: {e}")
            return False

    # ------------------------------------------------------------------
    #  Loop utama
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Loop utama Worker."""
        self._running    = True
        self._start_time = time.time()

        self.executor.start()
        self.monitor.start()

        # Thread heartbeat
        hb_thread = threading.Thread(
            target=self._heartbeat_loop, daemon=True, name="heartbeat"
        )
        hb_thread.start()

        # Thread penerima hasil dari pool proses
        collector_thread = threading.Thread(
            target=self._collect_results_loop, daemon=True, name="result-collector"
        )
        collector_thread.start()

        # Loop request task
        try:
            self._request_task_loop()
        except KeyboardInterrupt:
            logger.info("Worker dihentikan oleh pengguna")
        finally:
            self._running = False
            self.monitor.stop()
            self.executor.stop()
            if self._sock:
                try:
                    self._sock.close()
                except OSError:
                    pass

    def _request_task_loop(self) -> None:
        """
        Komponen d – pull-based: minta task ke Master setiap ada slot kosong.
        """
        no_task_wait = 0.5  # detik tunggu jika tidak ada task
        while self._running:
            # Cek simulasi crash
            self._check_crash()

            # Minta task jika ada slot kosong
            if self.executor.available_slots > 0:
                try:
                    self._send(MsgType.REQUEST_TASK, {
                        "worker_id":   self.worker_id,
                        "active_tasks": self.executor.active_count,
                    })

                    # Terima pesan, lewati HEARTBEAT_ACK jika ada
                    msg, binary = None, None
                    while self._running:
                        msg, binary = recv_message(self._sock)
                        if msg.get("type") == MsgType.HEARTBEAT_ACK:
                            continue
                        break

                    if msg is None:
                        break

                    self._handle_message(msg, binary)

                    if msg.get("type") == MsgType.NO_MORE_TASKS:
                        if not msg.get("wait"):
                            # Semua selesai
                            break
                        time.sleep(no_task_wait)
                    elif msg.get("type") == MsgType.SHUTDOWN:
                        logger.info("Menerima SHUTDOWN dari Master")
                        break

                except ConnectionError as e:
                    logger.error(f"Koneksi terputus: {e}")
                    break
                except Exception as e:
                    logger.error(f"Error request task: {e}", exc_info=True)
                    break
            else:
                time.sleep(0.1)

        # Tunggu semua task yang sedang berjalan selesai
        logger.info("Menunggu task yang masih berjalan selesai...")
        with self._pending_lock:
            futures = dict(self._pending)
        for tid, (fut, task, raw) in futures.items():
            try:
                result, packed = self.executor.collect_result(task, raw, fut, timeout=300)
                self._send_result(result, packed)
            except Exception as e:
                logger.error(f"Task {tid[:8]} gagal saat shutdown: {e}")

    def _handle_message(self, msg: dict, binary: Optional[bytes]) -> None:
        """Tangani pesan dari Master."""
        mtype = msg.get("type")
        if mtype == MsgType.TASK_ASSIGN:
            self._handle_task_assign(msg, binary)
        elif mtype == MsgType.HEARTBEAT_ACK:
            pass
        elif mtype == MsgType.SHUTDOWN:
            self._running = False
        elif mtype == MsgType.ERROR:
            logger.error(f"Master melaporkan error: {msg.get('message')}")

    def _handle_task_assign(self, msg: dict, binary: Optional[bytes]) -> None:
        """Komponen b: Terima task dan submit ke ProcessPoolExecutor."""
        task_dict = msg.get("task", {})
        task = Task.from_dict(task_dict)

        if self.transfer_mode == "socket":
            raw_data = binary or b""
        else:
            # shared_fs: baca dari disk
            try:
                with open(task.file_path, "rb") as f:
                    f.seek(task.offset)
                    raw_data = f.read(task.length)
            except OSError as e:
                logger.error(f"Gagal baca file {task.file_path}: {e}")
                return

        logger.debug(
            f"← TASK {task.task_id[:8]} ({format_bytes(task.length)}) "
            f"algo={task.algorithm}"
        )

        # Submit ke pool proses (Komponen b – CPU-bound paralel)
        future = self.executor.submit(task, raw_data)
        with self._pending_lock:
            self._pending[task.task_id] = (future, task, raw_data)

    def _collect_results_loop(self) -> None:
        """Thread yang mengumpulkan hasil dari futures yang sudah selesai."""
        while self._running:
            with self._pending_lock:
                done_ids = [
                    tid for tid, (fut, _, _) in self._pending.items()
                    if fut.done()
                ]
            for tid in done_ids:
                with self._pending_lock:
                    item = self._pending.pop(tid, None)
                if item:
                    fut, task, raw = item
                    result, packed = self.executor.collect_result(task, raw, fut)
                    self._send_result(result, packed)
                    self._tasks_done += 1

                    # Simulasi crash setelah N task
                    if (self._crash_after is not None and
                            self._tasks_done >= self._crash_after):
                        logger.warning(
                            f"[CRASH-SIM] Worker {self.worker_id} crash "
                            f"setelah {self._tasks_done} task!"
                        )
                        os._exit(1)
            time.sleep(0.1)

    def _send_result(self, result: TaskResult, packed_bytes: bytes) -> None:
        """Kirim TASK_RESULT ke Master."""
        try:
            self._send(
                MsgType.TASK_RESULT,
                {"result": result.to_dict()},
                binary_data=packed_bytes if result.status == "DONE" else None,
            )
        except (ConnectionError, OSError) as e:
            logger.error(f"Gagal kirim hasil task {result.task_id[:8]}: {e}")

    def _heartbeat_loop(self) -> None:
        """Komponen e: kirim HEARTBEAT tiap interval."""
        interval = self._cfg.get("heartbeat_interval", 2.0)
        while self._running:
            time.sleep(interval)
            stats = self.monitor.get_current()
            try:
                self._send(MsgType.HEARTBEAT, {
                    "worker_id":   self.worker_id,
                    "cpu_pct":     stats["cpu_pct"],
                    "mem_pct":     stats["mem_pct"],
                    "active_tasks": self.executor.active_count,
                })
            except (ConnectionError, OSError):
                break

    def _check_crash(self) -> None:
        """Simulasi crash berdasarkan --crash-at (detik)."""
        if self._crash_at is not None:
            elapsed = time.time() - self._start_time
            if elapsed >= self._crash_at:
                logger.warning(
                    f"[CRASH-SIM] Worker {self.worker_id} crash "
                    f"setelah {elapsed:.1f}s!"
                )
                os._exit(1)


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Worker Node — Distributed Data Compression")
    p.add_argument("--master-host",  default=DEFAULTS["master_host"])
    p.add_argument("--master-port",  type=int, default=DEFAULTS["master_port"])
    p.add_argument("--worker-id",    default=None,
                   help="ID unik Worker (default: w-<hostname>)")
    p.add_argument("--procs",        type=int, default=None,
                   help="Jumlah proses (default: jumlah CPU core)")
    p.add_argument("--throttle",     type=float, default=0.0,
                   help="Simulasi Worker lambat 0–100 persen")
    p.add_argument("--crash-after",  type=int, default=None,
                   help="Crash setelah N task selesai (simulasi fault)")
    p.add_argument("--crash-at",     type=float, default=None,
                   help="Crash setelah N detik (simulasi fault)")
    p.add_argument("--passcode", "--auth-key", default=None, dest="passcode",
                   help="Sandi Master untuk bergabung ke cluster tertentu")
    p.add_argument("--heartbeat",    type=float, default=DEFAULTS["heartbeat_interval"])
    p.add_argument("--log-level",    default=DEFAULTS["log_level"])
    p.add_argument("--config",       default=None)
    return p.parse_args()


def main() -> None:
    if sys.platform == "win32":
        import multiprocessing
        multiprocessing.freeze_support()

    args = parse_args()

    cfg = dict(DEFAULTS)
    if args.config and os.path.isfile(args.config):
        with open(args.config) as f:
            cfg.update(json.load(f))

    worker_id = cfg.get("worker_id") or f"w-{socket.gethostname()}"
    setup_logging("worker", cfg["log_level"],
                  log_file=f"results/logs/worker-{worker_id}.jsonl")

    logger.info(f"=== WORKER {worker_id} DIMULAI ===")
    logger.info(f"Master: {cfg['master_host']}:{cfg['master_port']}")
    logger.info(f"Procs : {cfg.get('num_procs') or os.cpu_count()}")

    MAX_AUTH_ATTEMPTS = 3
    node: Optional[WorkerNode] = None

    for attempt in range(1, MAX_AUTH_ATTEMPTS + 1):
        if args.passcode is not None:
            cfg["passcode"] = args.passcode
        else:
            print("\n" + "="*56)
            print(f"🔐  KONEKSI WORKER KE MASTER (Percobaan {attempt}/{MAX_AUTH_ATTEMPTS})")
            print("="*56)
            try:
                user_pass = input("👉 Masukkan sandi Master: ").strip()
                cfg["passcode"] = user_pass if user_pass else None
            except (EOFError, KeyboardInterrupt):
                print("\n[!] Dibatalkan oleh pengguna.")
                sys.exit(0)
            print("="*56 + "\n")

            # Reset host dan port ke nilai awal jika percobaan sebelumnya fallback
            cfg["master_host"] = args.master_host
            cfg["master_port"] = args.master_port

        if cfg.get("passcode"):
            logger.info(f"Mencoba autentikasi dengan sandi Master: {cfg['passcode']}")

        node = WorkerNode(cfg)
        connected = node.connect()
        if not connected:
            if args.passcode is None and attempt < MAX_AUTH_ATTEMPTS:
                print(f"⚠️  Master dengan sandi tersebut tidak ditemukan / tidak merespons. Sisa percobaan: {MAX_AUTH_ATTEMPTS - attempt}x\n")
                continue
            elif args.passcode is None:
                print(f"\n❌ Anda telah salah memasukkan sandi sebanyak {MAX_AUTH_ATTEMPTS} kali.")
                print("⛔ Program dihentikan demi keamanan. Silakan jalankan 'python worker.py' kembali.\n")
                sys.exit(1)
            else:
                logger.error("Tidak bisa terhubung ke Master. Keluar.")
                sys.exit(1)

        registered = node.register()
        if registered:
            break
        else:
            if node._sock:
                try:
                    node._sock.close()
                except OSError:
                    pass
            if args.passcode is None and attempt < MAX_AUTH_ATTEMPTS:
                print(f"❌ Sandi salah atau ditolak oleh Master! Sisa percobaan: {MAX_AUTH_ATTEMPTS - attempt}x\n")
                continue
            elif args.passcode is None:
                print(f"\n❌ Anda telah salah memasukkan sandi sebanyak {MAX_AUTH_ATTEMPTS} kali.")
                print("⛔ Program dihentikan demi keamanan. Silakan jalankan 'python worker.py' kembali.\n")
                sys.exit(1)
            else:
                logger.error("Gagal register ke Master. Keluar.")
                sys.exit(1)

    if not node:
        sys.exit(1)

    node.run()

    # Simpan time-series monitoring
    ts_path = f"results/logs/monitor-{worker_id}.json"
    os.makedirs(os.path.dirname(ts_path), exist_ok=True)
    import json as _json
    with open(ts_path, "w") as f:
        _json.dump(node.monitor.get_time_series(), f)

    logger.info(f"=== WORKER {worker_id} SELESAI (task={node._tasks_done}) ===")


if __name__ == "__main__":
    main()
