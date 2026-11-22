"""
master/fault_monitor.py
=======================
Komponen e – Fault Tolerance: Monitor heartbeat Worker, deteksi kegagalan,
requeue task, dan pengukuran Recovery Time.

Alur:
  1. Worker mengirim HEARTBEAT tiap heartbeat_interval detik.
  2. FaultMonitor berjalan di thread terpisah, cek tiap interval.
  3. Jika Worker tidak heartbeat > heartbeat_timeout → tandai DEAD.
  4. Panggil scheduler.requeue_worker_tasks(worker_id).
  5. Catat waktu deteksi dan waktu task selesai di Worker lain → Recovery Time.
"""

from __future__ import annotations
import threading
import time
import logging
from typing import Dict, Optional, Callable, Any

logger = logging.getLogger(__name__)


class FaultMonitor:
    """
    Monitor heartbeat Worker secara periodik.
    Thread-safe.
    """

    def __init__(self,
                 worker_registry: Dict[str, Any],     # worker_id → WorkerInfo
                 scheduler,                            # Scheduler instance
                 heartbeat_timeout: float = 10.0,     # detik
                 check_interval: float   = 2.0,       # detik
                 on_worker_dead: Optional[Callable[[str], None]] = None) -> None:
        """
        Args:
            worker_registry:   dict yang diisi oleh MasterServer.
            scheduler:         Scheduler instance.
            heartbeat_timeout: batas waktu tanpa heartbeat → DEAD.
            check_interval:    seberapa sering memonitor (detik).
            on_worker_dead:    callback dipanggil saat Worker dinyatakan DEAD.
        """
        self._registry        = worker_registry
        self._scheduler       = scheduler
        self._timeout         = heartbeat_timeout
        self._interval        = check_interval
        self._on_dead         = on_worker_dead
        self._lock            = threading.Lock()
        self._running         = False
        self._thread: Optional[threading.Thread] = None

        # Recovery Time tracking (Komponen e)
        # task_id → timestamp saat Worker pembawa task ini dinyatakan DEAD
        self._failure_ts:  Dict[str, float] = {}
        # task_id → waktu selesai di Worker lain
        self._recovery_ts: Dict[str, float] = {}

    def start(self) -> None:
        """Mulai thread monitor."""
        self._running = True
        self._thread  = threading.Thread(target=self._run, daemon=True,
                                         name="fault-monitor")
        self._thread.start()
        logger.info("FaultMonitor dimulai")

    def stop(self) -> None:
        """Hentikan thread monitor."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("FaultMonitor dihentikan")

    def _run(self) -> None:
        """Loop utama: cek heartbeat tiap interval."""
        while self._running:
            time.sleep(self._interval)
            self._check_workers()

    def _check_workers(self) -> None:
        """Periksa semua Worker; tandai yang tidak heartbeat sebagai DEAD."""
        now = time.time()
        with self._lock:
            dead_workers = []
            for wid, winfo in self._registry.items():
                if not winfo.alive:
                    continue
                elapsed = now - winfo.last_heartbeat
                if elapsed > self._timeout:
                    winfo.alive = False
                    dead_workers.append(wid)

        for wid in dead_workers:
            self._handle_dead_worker(wid, now)

    def _handle_dead_worker(self, worker_id: str, detected_at: float) -> None:
        """Tangani Worker yang terdeteksi mati."""
        logger.error(
            f"Worker {worker_id} DEAD (tidak ada heartbeat > {self._timeout:.0f}s)",
            extra={"worker_id": worker_id, "event": "worker_dead",
                   "timestamp": detected_at}
        )

        # Catat timestamp deteksi untuk tiap task yang dipegang SEBELUM di-requeue
        # (untuk pengukuran Recovery Time)
        assigned_tasks = self._get_assigned_tasks_of_worker(worker_id)
        with self._lock:
            for tid in assigned_tasks:
                if tid not in self._failure_ts:
                    self._failure_ts[tid] = detected_at

        # Requeue task milik Worker DEAD
        requeued = self._scheduler.requeue_worker_tasks(worker_id)
        requeued_count = len(requeued) if isinstance(requeued, list) else requeued

        logger.info(
            f"Worker {worker_id}: {requeued_count} task di-requeue",
            extra={"worker_id": worker_id, "event": "requeue",
                   "requeued": requeued_count}
        )

        if self._on_dead:
            self._on_dead(worker_id)

    def record_worker_failure(self, worker_id: str, detected_at: Optional[float] = None) -> None:
        """
        Catat waktu kegagalan Worker (misal saat koneksi TCP putus seketika).
        Harus dipanggil sebelum task di-requeue agar Recovery Time dapat dihitung.
        """
        ts = detected_at or time.time()
        assigned_tasks = self._get_assigned_tasks_of_worker(worker_id)
        with self._lock:
            for tid in assigned_tasks:
                if tid not in self._failure_ts:
                    self._failure_ts[tid] = ts

    def _get_assigned_tasks_of_worker(self, worker_id: str) -> list:
        """Ambil daftar task_id yang sebelumnya dipegang Worker ini."""
        if hasattr(self._scheduler, "get_assigned_task_ids_of_worker"):
            return self._scheduler.get_assigned_task_ids_of_worker(worker_id)
        summary = self._scheduler.get_summary()
        return []

    def record_recovery(self, task_id: str) -> Optional[float]:
        """
        Catat task_id selesai di Worker lain.
        Hitung Recovery Time = completed_at - failure_detected_at.

        Returns:
            Recovery Time dalam detik, atau None jika tidak tercatat.
        """
        completed_at = time.time()
        with self._lock:
            failure_ts = self._failure_ts.get(task_id)
            if failure_ts is None:
                return None
            recovery_time = completed_at - failure_ts
            self._recovery_ts[task_id] = completed_at
            logger.info(
                f"Recovery task {task_id[:8]}: {recovery_time:.2f}s",
                extra={"task_id": task_id, "event": "recovery",
                       "recovery_time": recovery_time}
            )
            return recovery_time

    def get_recovery_stats(self) -> dict:
        """Ringkasan statistik Recovery Time."""
        with self._lock:
            times = []
            for tid, ft in self._failure_ts.items():
                rt = self._recovery_ts.get(tid)
                if rt is not None:
                    times.append(rt - ft)
            if not times:
                return {"count": 0, "mean": 0.0, "max": 0.0, "min": 0.0}
            return {
                "count": len(times),
                "mean":  round(sum(times) / len(times), 3),
                "max":   round(max(times), 3),
                "min":   round(min(times), 3),
                "all":   [round(t, 3) for t in times],
            }

    def notify_heartbeat(self, worker_id: str) -> None:
        """
        Dipanggil saat heartbeat diterima dari Worker.
        Memperbarui status 'alive' dan mencegah false-positive.
        """
        with self._lock:
            worker = self._registry.get(worker_id)
            if worker:
                # Jika Worker kembali setelah DEAD → biarkan register ulang
                pass  # update dilakukan di WorkerInfo.update_heartbeat()
