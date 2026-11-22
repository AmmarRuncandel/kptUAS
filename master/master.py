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
from common.discovery import MasterBeacon

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
    "expected_workers":   None,
    "passcode":           None,
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
        """Komponen c: REGISTER — daftarkan Worker dengan verifikasi sandi."""
        # Verifikasi sandi Master (jika passcode dikonfigurasi)
        if self.master.passcode:
            worker_passcode = msg.get("passcode") or msg.get("auth_key")
            if worker_passcode != self.master.passcode:
                logger.warning(
                    f"Koneksi Worker dari {self.addr} DITOLAK: Sandi salah "
                    f"(diterima: '{worker_passcode}', dibutuhkan: '{self.master.passcode}')"
                )
                send_message(self.conn, MsgType.ERROR, {
                    "code": "AUTH_FAILED",
                    "message": "Sandi Master salah! Akses ditolak oleh Master.",
                })
                return

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

        # Daftarkan worker ke scheduler
        if self.master.scheduler:
            self.master.scheduler.register_worker(winfo)
            # Jika mode static dengan target expected_workers, lakukan distribute_static saat kuota terpenuhi
            if self.master.mode == "static" and self.master.expected_workers:
                with self.master._registry_lock:
                    if len(self.master.worker_registry) >= self.master.expected_workers:
                        self.master.scheduler.distribute_static(list(self.master.worker_registry.keys()))

        logger.info(
            f"Worker TERDAFTAR: {self.worker_id} @ {winfo.ip} "
            f"(cores={winfo.num_cores}, procs={winfo.num_procs})"
        )
        send_message(self.conn, MsgType.REGISTER_ACK, {
            "worker_id": self.worker_id,
            "mode":      self.master.mode,
            "transfer":  self.master.transfer_mode,
        })
        # Jika Master masih di lobi persiapan, cetak realtime tabel Worker
        if not self.master.job_started:
            self.master.print_worker_table()

    def _handle_request_task(self, msg: dict) -> None:
        """Komponen d: REQUEST_TASK — kirim task berikutnya."""
        if not self.worker_id:
            send_message(self.conn, MsgType.ERROR, {"message": "Belum register"})
            return

        winfo = self.master.worker_registry.get(self.worker_id)
        if not winfo:
            send_message(self.conn, MsgType.ERROR, {"message": "Worker tidak dikenal"})
            return

        # Jika Master belum memulai pekerjaan (masih menunggu di lobi), minta Worker standby
        if not self.master.job_started:
            send_message(self.conn, MsgType.NO_MORE_TASKS, {"wait": True})
            return

        # Pada mode static, jika expected_workers ditentukan, tunggu sampai semua worker terhubung
        if self.master.mode == "static" and self.master.expected_workers:
            with self.master._registry_lock:
                if len(self.master.worker_registry) < self.master.expected_workers:
                    send_message(self.conn, MsgType.NO_MORE_TASKS, {"wait": True})
                    return

        # Update jumlah slot kosong dari pesan REQUEST_TASK
        winfo.active_tasks = msg.get("active_tasks", 0)

        task = self.master.scheduler.get_task(winfo)

        if task is None:
            # Berikan wait: True agar Worker tetap standby hidup di jaringan (siap batch job berikutnya)
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
            compressed_data = binary or b""

            # Verifikasi integritas container format & hash data
            is_corrupt = False
            try:
                from common.compression import unpack_chunk_result
                chunk_header = unpack_chunk_result(compressed_data)
                if chunk_header.get("sha256_original") != result.sha256_original:
                    is_corrupt = True
            except Exception:
                is_corrupt = True

            if is_corrupt:
                logger.error(
                    f"⚠️ INTEGRITAS GAGAL: Chunk task {result.task_id[:8]} dari Worker {self.worker_id} CORRUPT! "
                    f"Hash tidak cocok. Menolak hasil & me-requeue task."
                )
                if winfo:
                    winfo.record_task_corrupt()
                with self.master.scheduler._lock:
                    task = self.master.scheduler._all_tasks.get(result.task_id)
                    if task and task.is_retryable(self.master.scheduler.max_retries):
                        task.status          = TaskStatus.PENDING
                        task.assigned_worker = None
                        self.master.scheduler._queue.appendleft(task)
                        self.master.scheduler._assigned.pop(result.task_id, None)
                return

            # Simpan hasil jika valid
            is_new = self.master.collector.collect(result, compressed_data)

            if is_new:
                self.master.scheduler.mark_done(result.task_id)

                # Update statistik Worker (record_task_done otomatis meng-update tasks_done dan bytes_processed)
                if winfo:
                    winfo.record_task_done(result.original_size, result.duration)

                # Catat recovery jika task ini adalah hasil requeue
                rec_time = self.master.fault_monitor.record_recovery(result.task_id)
                if rec_time is not None:
                    logger.info(
                        f"[RECOVERY] Task {result.task_id[:8]} berhasil pulih di {self.worker_id} "
                        f"dalam {rec_time:.2f} detik"
                    )

                logger.debug(
                    f"← RESULT {result.task_id[:8]} dari {self.worker_id} "
                    f"ratio={result.ratio:.2f} dur={result.duration:.2f}s"
                )
        else:
            logger.warning(
                f"Task {result.task_id[:8]} GAGAL di {self.worker_id}: "
                f"{result.error}"
            )
            if winfo:
                winfo.record_task_failed()
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
            # Catat kegagalan worker untuk perhitungan Recovery Time sebelum requeue
            if self.master.fault_monitor:
                self.master.fault_monitor.record_worker_failure(self.worker_id)
            requeued = self.master.scheduler.requeue_worker_tasks(self.worker_id)
            requeued_count = len(requeued) if isinstance(requeued, list) else requeued
            logger.warning(
                f"Worker {self.worker_id} disconnect: {requeued_count} task di-requeue"
            )
            if not self.master.job_started:
                self.master.print_worker_table()


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
        self.expected_workers = cfg.get("expected_workers")
        self.passcode      = cfg.get("passcode")
        self.job_started   = cfg.get("job_started", True)

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
        self.beacon: Optional[MasterBeacon]        = None

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

        # Mulai siaran UDP Auto-Discovery beacon dengan passcode
        self.beacon = MasterBeacon(tcp_port=self.port, passcode=self.passcode)
        self.beacon.start_beacon()

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
        if self.beacon:
            self.beacon.stop_beacon()
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

    def print_worker_table(self) -> None:
        """Tampilkan daftar Worker yang sudah terhubung secara realtime di lobi."""
        with self._registry_lock:
            workers = [w for w in self.worker_registry.values() if w.alive]

        print("\n" + "="*76)
        print(f"👥 [LOBBY WORKER] {len(workers)} Node Terhubung & Standby")
        print("="*76)
        print(f"{'No':<3} | {'Worker ID':<22} | {'IP Address':<16} | {'CPU Cores':<10} | {'RAM (GB)':<8}")
        print("-"*76)
        for idx, w in enumerate(workers, 1):
            print(f"{idx:<3} | {w.worker_id:<22} | {w.ip:<16} | {w.num_cores:<10} | {w.ram_gb:<8.1f}")
        print("="*76)
        if not self.job_started:
            print("👉 Tekan [ENTER] jika semua Worker sudah terhubung untuk MULAI KOMPRESI!")
            print("="*76 + "\n")

    def prepare_new_job(self, tasks, all_tasks_dict) -> None:
        """Siapkan batch pekerjaan kompresi baru tanpa memutus koneksi Worker."""
        self.job_started = False
        self.scheduler = Scheduler(
            tasks,
            mode        = self.mode,
            max_retries = self.max_retries,
        )
        self.collector = ResultCollector(self.output_dir, all_tasks_dict)
        if self.fault_monitor:
            self.fault_monitor.scheduler = self.scheduler

        # Reset statistik per-job dan daftarkan kembali worker yang aktif ke scheduler baru
        with self._registry_lock:
            for winfo in self.worker_registry.values():
                winfo.reset_job_stats()
                if winfo.alive:
                    self.scheduler.register_worker(winfo)

    def print_detailed_report(self, summary: dict, job_index: int = 1, folder_path: str = "") -> None:
        """Tampilkan rekap analisis komprehensif hasil pekerjaan dan integritas Worker."""
        print("\n" + "="*84)
        print(f"📊  LAPORAN FEEDBACK & REKAP HASIL PEKERJAAN #{job_index}")
        print("="*84)
        print(f"📁 Folder Sumber       : {folder_path} ({summary.get('total_files')} file)")
        print(f"💾 Folder Penyimpanan  : {self.output_dir}/")
        print(f"⚡ Algoritma Kompresi  : {summary.get('algorithm', 'lzma').upper()}")
        print(f"⏱️ Waktu Eksekusi      : {summary.get('elapsed')}")
        print(f"📦 Total Ukuran Asli   : {summary.get('total_original')}")
        print(f"🗜️ Total Terkompresi   : {summary.get('total_compressed')}")
        print(f"📉 Rasio Kompresi      : {summary.get('ratio')}x (Hemat: {summary.get('saved_pct', 0)}%)")
        print(f"🚀 Throughput Cluster  : {summary.get('throughput_mbs')} MB/s")
        print(f"🔁 Task Retry / Pulih  : {summary.get('retry_count')} kali")
        print(f"🛡️ Status Integritas   : {summary.get('cluster_integrity')}")
        print("-"*84)
        print("👥 ANALISIS KINERJA & KESEHATAN PER WORKER:")
        print("-"*84)
        print(f"{'Worker ID':<20} | {'IP Address':<15} | {'Task (Ok/Fail)':<15} | {'Beban Data':<16} | {'Throughput':<12} | {'Integritas'}")
        print("-"*84)
        for wid, ws in summary.get("worker_summary", {}).items():
            task_str = f"{ws.get('tasks_done', 0)} / {ws.get('tasks_failed', 0)}"
            data_str = f"{ws.get('bytes_fmt', '0 B')} ({ws.get('pct_workload', 0)}%)"
            thr_str  = f"{ws.get('throughput', 0):.2f} MB/s"
            integ    = ws.get('integrity', '100% VALID ✅')
            print(f"{wid:<20} | {ws.get('ip', '-'):<15} | {task_str:<15} | {data_str:<16} | {thr_str:<12} | {integ}")
        print("="*84)
        summary_path = os.path.join(self.output_dir, f"summary_job_{job_index}.json")
        print(f"📄 Rekap lengkap tersimpan di: {summary_path}")
        print("="*84 + "\n")

    def run_dashboard(self, interval: float = 2.0) -> None:
        """Dashboard teks ringkas di terminal (untuk video demo)."""
        # Tunggu sampai pekerjaan benar-benar dimulai oleh Master
        while not self.job_started and self._running:
            time.sleep(0.5)

        while not (self.scheduler and self.scheduler.is_all_done()) and self._running:
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
    p.add_argument("--input-dir",    default=None,
                   help="Direktori file yang akan dikompresi (default: pilih interaktif atau data/sample)")
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
    p.add_argument("--expected-workers", type=int, default=None,
                   help="Jumlah Worker yang ditunggu sebelum mendistribusikan task (mode static)")
    p.add_argument("--passcode", "--auth-key", default=None, dest="passcode",
                   help="Sandi verifikasi Master untuk mengunci Worker ke cluster ini")
    p.add_argument("--no-auth", action="store_true", default=False,
                   help="Nonaktifkan verifikasi sandi")
    p.add_argument("--interactive", action="store_true", default=False,
                   help="Paksa mode interaktif (meminta input sandi, folder, dan lobi)")
    p.add_argument("--no-wait", action="store_true", default=False,
                   help="Langsung mulai kompresi tanpa menunggu konfirmasi Enter di lobi")
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
    cfg["expected_workers"] = args.expected_workers
    cfg["passcode"]      = args.passcode
    cfg["no_auth"]       = args.no_auth
    cfg["interactive"]   = args.interactive
    cfg["no_wait"]       = args.no_wait
    cfg["max_retries"]   = args.max_retries
    cfg["log_level"]     = args.log_level
    return cfg


def main() -> None:
    if sys.platform == "win32":
        import multiprocessing
        multiprocessing.freeze_support()

    args = parse_args()
    cfg  = load_config(args)

    is_interactive = (sys.stdin.isatty() or args.interactive) and not cfg.get("no_wait")

    # ------------------------------------------------------------------ #
    # LANGKAH 1: Pengaturan Keamanan Sandi Cluster Master
    # ------------------------------------------------------------------ #
    if cfg.get("no_auth"):
        cfg["passcode"] = None
    elif not cfg.get("passcode"):
        if is_interactive:
            print("\n" + "="*68)
            print("🔐  LANGKAH 1: PENGATURAN KEAMANAN CLUSTER MASTER")
            print("="*68)
            print("Masukkan sandi cluster agar Worker hanya terhubung ke Master ini.")
            try:
                user_pass = input("👉 Masukkan sandi Master [Tekan Enter untuk PIN acak]: ").strip()
            except (EOFError, KeyboardInterrupt):
                user_pass = ""
            if user_pass:
                cfg["passcode"] = user_pass
            else:
                import random
                cfg["passcode"] = str(random.randint(100000, 999999))
        else:
            import random
            cfg["passcode"] = str(random.randint(100000, 999999))

    setup_logging("master", cfg["log_level"],
                  log_file=os.path.join(cfg["output_dir"], "master.jsonl"))

    logger.info("=== MASTER NODE DIMULAI ===")
    if cfg.get("passcode"):
        logger.info(f"Sandi Keamanan Master: {cfg['passcode']}")
    logger.info(f"Output : {cfg['output_dir']}")
    logger.info(f"Algo   : {cfg['algorithm']} level={cfg['level']}")
    logger.info(f"Mode   : {cfg['mode']}")

    ensure_dir(cfg["output_dir"])

    # ------------------------------------------------------------------ #
    # LANGKAH 3: Inisialisasi Server TCP & UDP Auto-Discovery Beacon
    # ------------------------------------------------------------------ #
    # Server TCP dan discovery berjalan terus melayani batch pekerjaan (Multi-Job)
    cfg["job_started"] = not is_interactive
    server = MasterServer(cfg)

    server_thread = threading.Thread(target=server.serve, daemon=True, name="tcp-server")
    server_thread.start()

    dash_thread_started = False
    job_index = 1

    while True:
        # ------------------------------------------------------------------ #
        # LANGKAH 2: Pemilihan Folder yang Akan Dikompresi (Validasi Otomatis)
        # ------------------------------------------------------------------ #
        if not cfg.get("input_dir"):
            if is_interactive:
                while True:
                    print("\n" + "="*68)
                    print(f"📁  LANGKAH 2: PILIH FOLDER UNTUK PEKERJAAN #{job_index}")
                    print("="*68)
                    print("Masukkan path direktori folder file yang ingin dikompres.")
                    print("Contoh: D:\\FileKuliah\\Dataset atau data/sample")
                    print("[Tekan ENTER langsung untuk menggunakan sampel bawaan: data/sample]")
                    try:
                        folder_input = input("👉 Path folder: ").strip()
                    except (EOFError, KeyboardInterrupt):
                        folder_input = "data/sample"

                    if not folder_input:
                        folder_input = "data/sample"

                    # Bersihkan tanda kutip jika user copy-paste path ber-kutip di Windows
                    folder_input = folder_input.strip('"').strip("'")

                    if not os.path.exists(folder_input):
                        print(f"\n❌ ERROR: Folder '{folder_input}' TIDAK DITEMUKAN!")
                        print("   Pastikan path folder benar dan coba lagi.\n")
                        continue
                    if not os.path.isdir(folder_input):
                        print(f"\n❌ ERROR: '{folder_input}' adalah file, bukan folder direktori!")
                        print("   Silakan masukkan path direktori folder.\n")
                        continue

                    # Cek isi folder
                    entries = [e for e in os.listdir(folder_input) if os.path.isfile(os.path.join(folder_input, e))]
                    if not entries:
                        print(f"\n⚠️  PERINGATAN: Folder '{folder_input}' KOSONG (tidak berisi file)! Silakan masukkan folder lain.\n")
                        continue

                    cfg["input_dir"] = folder_input
                    print(f"✅ Folder valid dipilih: '{folder_input}' ({len(entries)} file terdeteksi).\n")
                    break
            else:
                cfg["input_dir"] = "data/sample"

        # Pemilihan Level Kompresi secara interaktif
        if is_interactive:
            while True:
                print("="*68)
                print("⚙️   PILIH TINGKAT KOMPRESI (COMPRESSION LEVEL 1 - 9)")
                print("="*68)
                print("  [1] Cepat (Fast)     - Kecepatan tinggi, hemat CPU, file agak besar")
                print("  [6] Standar (Normal) - Keseimbangan optimal rasio & kecepatan [Default]")
                print("  [9] Ultra (Maksimal) - Kompresi paling rapat, hemat storage, butuh waktu lebih")
                print("="*68)
                try:
                    level_input = input("👉 Pilih Level [1-9] (Tekan ENTER untuk default: 6): ").strip()
                except (EOFError, KeyboardInterrupt):
                    level_input = "6"

                if not level_input:
                    cfg["level"] = 6
                    break
                elif level_input.isdigit() and 1 <= int(level_input) <= 9:
                    cfg["level"] = int(level_input)
                    break
                else:
                    print("❌ Pilihan tidak valid! Masukkan angka antara 1 sampai 9.\n")
                    continue
            print(f"✅ Tingkat kompresi dipilih: Level {cfg['level']}\n")

        # --- Komponen a: Task Decomposition ---
        if cfg["input_dir"] == "data/sample" and (not os.path.isdir("data/sample") or not os.listdir("data/sample")):
            logger.info("Membuat data sintetis sampel otomatis di data/sample...")
            try:
                from data.generate_synthetic import generate
                generate(cfg["input_dir"], num_small=20, num_large=2)
            except Exception as e:
                logger.warning(f"Gagal generate data otomatis: {e}")

        logger.info(f"Mendekomposisi file dari '{cfg['input_dir']}'...")
        tasks = decompose_directory(
            cfg["input_dir"], cfg["chunk_size"],
            cfg["algorithm"], cfg["level"]
        )
        logger.info(f"Total task untuk Job #{job_index}: {len(tasks)}")
        all_tasks_dict = {t.task_id: t for t in tasks}

        if not tasks:
            logger.error(f"Tidak ada file yang dapat dikompresi di '{cfg['input_dir']}'.")
            if is_interactive:
                cfg["input_dir"] = None
                continue
            else:
                sys.exit(1)

        total_orig_bytes = sum(t.length for t in tasks)

        # Siapkan scheduler dan collector untuk batch ini
        if job_index == 1:
            server.prepare(tasks, all_tasks_dict)
            server.fault_monitor.start()
        else:
            server.prepare_new_job(tasks, all_tasks_dict)

        if not dash_thread_started:
            dash_thread = threading.Thread(target=server.run_dashboard, daemon=True, name="dashboard")
            dash_thread.start()
            dash_thread_started = True

        # Tampilkan Banner Lobi Siap Koneksi
        print("\n" + "="*76)
        print(f"🚀 MASTER DISTRIBUTED COMPRESSION (PEKERJAAN #{job_index} - WAITING ROOM)")
        print("="*76)
        if cfg.get("passcode"):
            print(f"🔑 SANDI MASTER    : {cfg['passcode']}")
        else:
            print("🔓 MODE KEAMANAN   : Bebas Akses (Tanpa Sandi)")
        print(f"📁 FOLDER TARGET   : {cfg['input_dir']} ({len(tasks)} task, {format_bytes(total_orig_bytes)})")
        print(f"⚡ ALGORITMA       : {cfg['algorithm'].upper()} (Level {cfg['level']})")
        print(f"🌐 STATUS          : Menunggu konfirmasi Master untuk memulai...")
        print("="*76)
        print("👉 Worker yang sudah terhubung akan tetap standby secara otomatis.")
        print("👉 Tekan [ENTER] jika Worker sudah siap untuk MULAI KOMPRESI!")
        print("="*76 + "\n")

        # Tampilkan tabel worker yang sudah ada
        server.print_worker_table()

        # Tunggu konfirmasi Master untuk mulai jika sesi interaktif
        if is_interactive:
            while True:
                try:
                    input()
                except (EOFError, KeyboardInterrupt):
                    print("\n[!] Dibatalkan oleh pengguna.")
                    server.broadcast_shutdown()
                    server.stop()
                    sys.exit(0)

                with server._registry_lock:
                    active_workers = [w for w in server.worker_registry.values() if w.alive]

                if len(active_workers) == 0:
                    print("⚠️  Belum ada Worker yang terhubung! Tunggu minimal 1 Worker masuk...")
                    continue

                server.job_started = True
                print("\n" + "="*76)
                print(f"🚀 MEMULAI PEKERJAAN #{job_index}! Kompresi didistribusikan ke {len(active_workers)} Worker...")
                print("="*76 + "\n")
                break
        else:
            server.job_started = True

        # Tunggu semua task selesai
        start_ts = time.time()
        logger.info(f"Menunggu task Pekerjaan #{job_index} selesai...")
        try:
            while not server.scheduler.is_all_done():
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Dihentikan oleh pengguna")
            break

        elapsed = time.time() - start_ts

        # Recovery stats & Imbalance metrics
        recovery = server.fault_monitor.get_recovery_stats() if server.fault_monitor else {}
        imbalance = server.scheduler.get_imbalance_metrics()

        # Ringkasan akhir per pekerjaan
        summary = server.collector.get_final_summary(
            server.worker_registry, server.scheduler.total_retries
        )
        summary["elapsed_seconds"] = round(elapsed, 3)
        summary["elapsed"]         = format_duration(elapsed)
        summary["recovery_stats"]   = recovery
        summary["imbalance"]       = imbalance
        summary["algorithm"]       = cfg["algorithm"]
        summary["job_index"]       = job_index
        summary["folder_path"]     = cfg["input_dir"]

        # Simpan summary per pekerjaan dan summary global terbaru
        job_summary_path = os.path.join(cfg["output_dir"], f"summary_job_{job_index}.json")
        server.collector.save_summary(summary, job_summary_path)
        global_summary_path = os.path.join(cfg["output_dir"], "summary.json")
        server.collector.save_summary(summary, global_summary_path)

        # Tampilkan Laporan Feedback & Rekap Lengkap
        server.print_detailed_report(summary, job_index, cfg["input_dir"])

        # Tanya apakah ada pekerjaan lagi jika interaktif
        if is_interactive:
            print("="*84)
            print(f"🔄  PEKERJAAN #{job_index} TELAH SELESAI DENGAN SUKSES!")
            print("="*84)
            try:
                tanya = input("👉 Apakah ada folder/pekerjaan lain yang ingin dikompresi lagi? [y/N]: ").strip().lower()
            except (EOFError, KeyboardInterrupt):
                tanya = "n"

            if tanya in ("y", "yes"):
                print("\n[OK] Menyiapkan pekerjaan baru. Semua Worker tetap standby terhubung di jaringan...")
                job_index += 1
                cfg["input_dir"] = None  # Reset agar meminta input folder baru di Langkah 2
                continue
            else:
                print("\n✨ Seluruh pekerjaan selesai. Mengirim sinyal SHUTDOWN ke semua Worker...")
                server.broadcast_shutdown()
                break
        else:
            server.broadcast_shutdown()
            break

    # Hentikan server dan monitor setelah seluruh batch pekerjaan selesai
    if server.fault_monitor:
        server.fault_monitor.stop()
    server.stop()
    logger.info("=== MASTER SELESAI ===")


if __name__ == "__main__":
    main()
