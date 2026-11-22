"""
master/master.py
================
Entry point Master Node.

Komponen c – Distributed Processing: Server TCP multi-client (thread per Worker).
Komponen d – Dynamic Load Balancing: Scheduler pull-based.
Komponen e – Fault Tolerance: Heartbeat monitor + requeue.

Jalankan:
    python master/master.py --input-dir data/sample --output-dir results/output

Dashboard teks ringkas diperbarui tiap 2 detik di terminal.
"""

from __future__ import annotations
import argparse
import json
import os
import socket
import sys
import threading
import time
import logging
from typing import Dict, Optional

# Tambahkan root project ke sys.path agar import common/* bisa berjalan
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from common.protocol import send_message, recv_message, MsgType
from common.tasks import decompose_directory, Task, TaskStatus
from common.utils import setup_logging, ensure_dir, format_bytes, format_duration, sha256_file_range
from master.scheduler import Scheduler, WorkerInfo
from master.fault_monitor import FaultMonitor
from master.collector import ResultCollector
from common.tasks import TaskResult

logger = logging.getLogger("master")


# ---------------------------------------------------------------------------
# Konfigurasi default
# ---------------------------------------------------------------------------

DEFAULTS = {
    "host":               "0.0.0.0",
    "port":               9000,
    "algorithm":          "lzma",
    "level":              6,
    "chunk_size_mb":      32,
    "max_retries":        3,
    "heartbeat_timeout":  10.0,
    "heartbeat_interval": 2.0,
    "mode":               "dynamic",
    "transfer_mode":      "socket",
    "log_level":          "INFO",
}


# ---------------------------------------------------------------------------
# Handler per koneksi Worker
# ---------------------------------------------------------------------------

class WorkerHandler(threading.Thread):
    """
    Thread yang menangani satu koneksi Worker.
    Komponen c: komunikasi TCP + distribusi task.
    """

    def __init__(self, conn: socket.socket, addr,
                 master: "MasterServer") -> None:
        super().__init__(daemon=True, name=f"worker-handler-{addr}")
        self.conn   = conn
        self.addr   = addr
        self.master = master
        self.worker_id: Optional[str] = None

    def run(self) -> None:
        try:
            self._handle()
        except (ConnectionError, OSError) as e:
            if self.worker_id:
                logger.warning(f"Koneksi Worker {self.worker_id} terputus: {e}")
                self._on_disconnect()
        except Exception as e:
            logger.error(f"Error handler Worker {self.worker_id}: {e}", exc_info=True)
            self._on_disconnect()
        finally:
            try:
                self.conn.close()
            except OSError:
                pass

    def _handle(self) -> None:
        """Loop pesan utama."""
        while True:
            try:
                msg, binary = recv_message(self.conn)
            except ConnectionError:
                break

            mtype = msg.get("type")

            if mtype == MsgType.REGISTER:
                self._handle_register(msg)

            elif mtype == MsgType.REQUEST_TASK:
                self._handle_request_task(msg)

            elif mtype == MsgType.TASK_RESULT:
                self._handle_task_result(msg, binary)

            elif mtype == MsgType.HEARTBEAT:
                self._handle_heartbeat(msg)

            elif mtype == MsgType.ERROR:
                logger.error(f"Worker {self.worker_id} melaporkan error: {msg.get('message')}")

            else:
                logger.warning(f"Tipe pesan tidak dikenal dari {self.addr}: {mtype}")

    # ------------------------------------------------------------------ #
    #  Handler per tipe pesan                                              #
    # ------------------------------------------------------------------ #

    def _handle_register(self, msg: dict) -> None:
        """Komponen c: REGISTER — daftarkan Worker."""
        self.worker_id = msg["worker_id"]
        winfo = WorkerInfo(
            worker_id = self.worker_id,
            hostname  = msg.get("hostname", "unknown"),
            ip        = msg.get("ip", str(self.addr[0])),
            num_cores = msg.get("num_cores", 1),
            ram_gb    = msg.get("ram_gb", 0.0),
            num_procs = msg.get("num_procs", 1),
        )
        with self.master._registry_lock:
            self.master.worker_registry[self.worker_id] = winfo
            self.master.worker_sockets[self.worker_id]  = self.conn

        logger.info(
            f"Worker TERDAFTAR: {self.worker_id} @ {winfo.ip} "
            f"(cores={winfo.num_cores}, procs={winfo.num_procs})"
        )
        send_message(self.conn, MsgType.REGISTER_ACK, {
            "worker_id": self.worker_id,
            "mode":      self.master.mode,
            "transfer":  self.master.transfer_mode,
        })

    def _handle_request_task(self, msg: dict) -> None:
        """Komponen d: REQUEST_TASK — kirim task berikutnya."""
        if not self.worker_id:
            send_message(self.conn, MsgType.ERROR, {"message": "Belum register"})
            return

        winfo = self.master.worker_registry.get(self.worker_id)
        if not winfo:
            send_message(self.conn, MsgType.ERROR, {"message": "Worker tidak dikenal"})
            return

        # Update jumlah slot kosong dari pesan REQUEST_TASK
        winfo.active_tasks = msg.get("active_tasks", 0)

        task = self.master.scheduler.get_task(winfo)

        if task is None:
            if self.master.scheduler.is_all_done():
                send_message(self.conn, MsgType.NO_MORE_TASKS, {})
            else:
                # Masih ada task sedang diproses; Worker tunggu sebentar
                send_message(self.conn, MsgType.NO_MORE_TASKS, {"wait": True})
            return

        # Kirim task
        payload = {
            "task":          task.to_dict(),
            "transfer_mode": self.master.transfer_mode,
        }

        if self.master.transfer_mode == "socket":
            # Baca chunk dari disk dan kirim sebagai binary
            try:
                data = self._read_chunk(task)
            except OSError as e:
                logger.error(f"Gagal baca file {task.file_path}: {e}")
                self.master.scheduler.requeue_worker_tasks(self.worker_id)
                send_message(self.conn, MsgType.ERROR, {"message": str(e)})
                return
            send_message(self.conn, MsgType.TASK_ASSIGN, payload,
                         binary_data=data)
            logger.debug(
                f"→ TASK {task.task_id[:8]} ke {self.worker_id} "
                f"({format_bytes(task.length)})"
            )
        else:
            # shared_fs: hanya kirim metadata
            send_message(self.conn, MsgType.TASK_ASSIGN, payload)

    def _read_chunk(self, task: Task) -> bytes:
        """Baca data chunk dari file."""
        with open(task.file_path, "rb") as f:
            f.seek(task.offset)
            return f.read(task.length)

    def _handle_task_result(self, msg: dict, binary: Optional[bytes]) -> None:
        """Komponen c + e: TASK_RESULT — terima dan simpan hasil."""
        result_dict = msg.get("result", {})
        result = TaskResult(
            task_id           = result_dict["task_id"],
            original_size     = result_dict["original_size"],
            compressed_size   = result_dict["compressed_size"],
            ratio             = result_dict["ratio"],
            duration          = result_dict["duration"],
            worker_id         = result_dict["worker_id"],
            sha256_original   = result_dict["sha256_original"],
            sha256_compressed = result_dict.get("sha256_compressed", ""),
            status            = result_dict["status"],
            error             = result_dict.get("error"),
            algorithm         = result_dict.get("algorithm"),
            level             = result_dict.get("level"),
        )

        winfo = self.master.worker_registry.get(self.worker_id)

        if result.status == "DONE":
            # Simpan hasil
            compressed_data = binary or b""
            is_new = self.master.collector.collect(result, compressed_data)

            if is_new:
                self.master.scheduler.mark_done(result.task_id)

                # Update statistik Worker
                if winfo:
                    winfo.tasks_done      += 1
                    winfo.bytes_processed += result.original_size
                    winfo.record_task_done(result.original_size, result.duration)

                # Catat recovery jika task ini adalah hasil requeue
                self.master.fault_monitor.record_recovery(result.task_id)

                self.master._retry_count_total += result_dict.get("retried", 0)

                logger.debug(
                    f"← RESULT {result.task_id[:8]} dari {self.worker_id} "
                    f"ratio={result.ratio:.2f} dur={result.duration:.2f}s"
                )
        else:
            logger.warning(
                f"Task {result.task_id[:8]} GAGAL di {self.worker_id}: "
                f"{result.error}"
            )
            # Requeue task ini saja
            with self.master.scheduler._lock:
                task = self.master.scheduler._all_tasks.get(result.task_id)
                if task and task.is_retryable(self.master.scheduler.max_retries):
                    task.status          = TaskStatus.PENDING
                    task.assigned_worker = None
                    self.master.scheduler._queue.appendleft(task)
                    self.master.scheduler._assigned.pop(result.task_id, None)

    def _handle_heartbeat(self, msg: dict) -> None:
        """Komponen e: HEARTBEAT — perbarui status Worker."""
        if not self.worker_id:
            return
        winfo = self.master.worker_registry.get(self.worker_id)
        if winfo:
            winfo.update_heartbeat(
                cpu_pct     = msg.get("cpu_pct", 0.0),
                mem_pct     = msg.get("mem_pct", 0.0),
                active_tasks= msg.get("active_tasks", 0),
            )
            self.master.fault_monitor.notify_heartbeat(self.worker_id)
        send_message(self.conn, MsgType.HEARTBEAT_ACK, {})

    def _on_disconnect(self) -> None:
        """Tangani putus koneksi — requeue task Worker ini."""
        if self.worker_id:
            winfo = self.master.worker_registry.get(self.worker_id)
            if winfo:
                winfo.alive = False
            requeued = self.master.scheduler.requeue_worker_tasks(self.worker_id)
            logger.warning(
                f"Worker {self.worker_id} disconnect: {requeued} task di-requeue"
            )


# ---------------------------------------------------------------------------
# MasterServer
# ---------------------------------------------------------------------------

class MasterServer:
    """
    Server TCP utama yang menerima koneksi dari Worker.
    Komponen c: multi-client dengan thread per koneksi.
    """

    def __init__(self, cfg: dict) -> None:
        self.host          = cfg["host"]
        self.port          = cfg["port"]
        self.mode          = cfg["mode"]
        self.transfer_mode = cfg["transfer_mode"]
        self.output_dir    = cfg["output_dir"]
        self.max_retries   = cfg["max_retries"]

        self.worker_registry: Dict[str, WorkerInfo] = {}
        self.worker_sockets:  Dict[str, socket.socket] = {}
        self._registry_lock   = threading.Lock()
        self._retry_count_total = 0

        # Akan diinisialisasi setelah decompose
        self.scheduler:     Optional[Scheduler]       = None
        self.fault_monitor: Optional[FaultMonitor]    = None
        self.collector:     Optional[ResultCollector] = None

        self._running = False
        self._server_sock: Optional[socket.socket] = None

    def prepare(self, tasks, all_tasks_dict) -> None:
        """Inisialisasi scheduler, collector, fault monitor."""
        self.scheduler = Scheduler(
            tasks,
            mode        = self.mode,
            max_retries = self.max_retries,
        )
        self.collector = ResultCollector(self.output_dir, all_tasks_dict)
        self.fault_monitor = FaultMonitor(
            worker_registry    = self.worker_registry,
            scheduler          = self.scheduler,
            heartbeat_timeout  = DEFAULTS["heartbeat_timeout"],
            check_interval     = DEFAULTS["heartbeat_interval"],
            on_worker_dead     = self._on_worker_dead,
        )

    def _on_worker_dead(self, worker_id: str) -> None:
        """Callback dari FaultMonitor saat Worker DEAD."""
        logger.error(f"[FAULT-TOLERANCE] Worker {worker_id} dinyatakan DEAD → task di-requeue")

    def serve(self) -> None:
        """Loop terima koneksi Worker."""
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind((self.host, self.port))
        self._server_sock.listen(32)
        self._running = True
        logger.info(f"Master mendengarkan di {self.host}:{self.port} [mode={self.mode}]")
        self._server_sock.settimeout(1.0)
        while self._running:
            try:
                conn, addr = self._server_sock.accept()
                handler = WorkerHandler(conn, addr, self)
                handler.start()
                logger.info(f"Koneksi baru dari {addr}")
            except socket.timeout:
                continue
            except OSError:
                break

    def stop(self) -> None:
        self._running = False
        if self._server_sock:
            try:
                self._server_sock.close()
            except OSError:
                pass

    def broadcast_shutdown(self) -> None:
        """Kirim SHUTDOWN ke semua Worker."""
        with self._registry_lock:
            sockets = dict(self.worker_sockets)
        for wid, sock in sockets.items():
            try:
                send_message(sock, MsgType.SHUTDOWN, {})
                logger.info(f"SHUTDOWN dikirim ke {wid}")
            except OSError:
                pass

    def run_dashboard(self, interval: float = 2.0) -> None:
        """Dashboard teks ringkas di terminal (untuk video demo)."""
        while not (self.scheduler and self.scheduler.is_all_done()):
            if self.scheduler and self.collector:
                prog = self.collector.get_progress()
                sched = self.scheduler.get_summary()
                workers_alive = sum(
                    1 for w in self.worker_registry.values() if w.alive
                )
                print(
                    f"\r[MASTER] Worker: {workers_alive} aktif | "
                    f"Task: {prog['done']}/{prog['total']} ({prog['pct']}%) | "
                    f"Throughput: {prog['throughput_mbs']:.1f} MB/s | "
                    f"Pending: {sched['pending']} | "
                    f"Assigned: {sched['assigned']}    ",
                    end="", flush=True
                )
            time.sleep(interval)
        print()


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Master Node — Distributed Data Compression")
    p.add_argument("--host",         default=DEFAULTS["host"])
    p.add_argument("--port",         type=int, default=DEFAULTS["port"])
    p.add_argument("--input-dir",    required=True, help="Direktori file yang akan dikompresi")
    p.add_argument("--output-dir",   default="results/output")
    p.add_argument("--algorithm",    default=DEFAULTS["algorithm"],
                   choices=["lzma","bz2","gzip","zlib","zstd"])
    p.add_argument("--level",        type=int, default=DEFAULTS["level"])
    p.add_argument("--chunk-size",   type=int, default=DEFAULTS["chunk_size_mb"],
                   help="Ukuran chunk dalam MB (default: 32)")
    p.add_argument("--mode",         default=DEFAULTS["mode"],
                   choices=["static","dynamic"])
    p.add_argument("--transfer",     default=DEFAULTS["transfer_mode"],
                   choices=["socket","shared_fs"])
    p.add_argument("--max-retries",  type=int, default=DEFAULTS["max_retries"])
    p.add_argument("--log-level",    default=DEFAULTS["log_level"])
    p.add_argument("--config",       default=None, help="Path file config JSON")
    return p.parse_args()


def load_config(args) -> dict:
    """Gabung argparse + file config JSON (argparse menang jika konflik)."""
    cfg = dict(DEFAULTS)
    if args.config and os.path.isfile(args.config):
        with open(args.config) as f:
            file_cfg = json.load(f)
        cfg.update(file_cfg)
    # Override dari argparse
    cfg["host"]          = args.host
    cfg["port"]          = args.port
    cfg["input_dir"]     = args.input_dir
    cfg["output_dir"]    = args.output_dir
    cfg["algorithm"]     = args.algorithm
    cfg["level"]         = args.level
    cfg["chunk_size"]    = args.chunk_size * 1024 * 1024
    cfg["mode"]          = args.mode
    cfg["transfer_mode"] = args.transfer
    cfg["max_retries"]   = args.max_retries
    cfg["log_level"]     = args.log_level
    return cfg


def main() -> None:
    if sys.platform == "win32":
        import multiprocessing
        multiprocessing.freeze_support()

    args = parse_args()
    cfg  = load_config(args)

    setup_logging("master", cfg["log_level"],
                  log_file=os.path.join(cfg["output_dir"], "master.jsonl"))

    logger.info("=== MASTER NODE DIMULAI ===")
    logger.info(f"Input  : {cfg['input_dir']}")
    logger.info(f"Output : {cfg['output_dir']}")
    logger.info(f"Algo   : {cfg['algorithm']} level={cfg['level']}")
    logger.info(f"Mode   : {cfg['mode']}")

    # --- Komponen a: Task Decomposition ---
    logger.info("Mendekomposisi file menjadi task...")
    tasks = decompose_directory(
        cfg["input_dir"], cfg["chunk_size"],
        cfg["algorithm"], cfg["level"]
    )
    logger.info(f"Total task: {len(tasks)}")
    all_tasks_dict = {t.task_id: t for t in tasks}

    if not tasks:
        logger.error("Tidak ada file ditemukan di input_dir. Keluar.")
        sys.exit(1)

    ensure_dir(cfg["output_dir"])

    # --- Siapkan server ---
    server = MasterServer(cfg)
    server.prepare(tasks, all_tasks_dict)

    # Mulai thread server TCP
    server_thread = threading.Thread(target=server.serve, daemon=True, name="tcp-server")
    server_thread.start()

    # Mulai FaultMonitor
    server.fault_monitor.start()

    # Dashboard di thread terpisah
    dash_thread = threading.Thread(target=server.run_dashboard, daemon=True, name="dashboard")
    dash_thread.start()

    # Tunggu semua task selesai
    start_ts = time.time()
    logger.info("Menunggu task selesai... (Ctrl+C untuk berhenti)")
    try:
        while not server.scheduler.is_all_done():
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Dihentikan oleh pengguna")

    elapsed = time.time() - start_ts

    # Hentikan semua
    server.broadcast_shutdown()
    server.fault_monitor.stop()
    server.stop()

    # Ringkasan akhir
    summary = server.collector.get_final_summary(
        server.worker_registry, server._retry_count_total
    )
    summary["elapsed_seconds"] = round(elapsed, 3)
    summary["elapsed"]         = format_duration(elapsed)

    summary_path = os.path.join(cfg["output_dir"], "summary.json")
    server.collector.save_summary(summary, summary_path)

    # Recovery stats
    recovery = server.fault_monitor.get_recovery_stats()
    logger.info(f"Recovery Time stats: {recovery}")

    # Imbalance metrics
    imbalance = server.scheduler.get_imbalance_metrics()
    logger.info(f"Imbalance (CV={imbalance['cv']}, max/min={imbalance['max_min_ratio']})")

    print("\n" + "="*60)
    print("RINGKASAN AKHIR MASTER")
    print("="*60)
    for k, v in summary.items():
        if k != "worker_summary":
            print(f"  {k:<25}: {v}")
    print("  Worker breakdown:")
    for wid, ws in summary.get("worker_summary", {}).items():
        print(f"    {wid}: tasks={ws['tasks_done']} bytes={ws['bytes']} thr={ws['throughput']} MB/s")
    print("="*60)
    logger.info("=== MASTER SELESAI ===")


if __name__ == "__main__":
    main()
