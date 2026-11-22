"""
master/scheduler.py
===================
Komponen d – Dynamic Load Balancing: Antrean task + distribusi dinamis.

Dua mode:
  - static:  seluruh task dibagi rata/round-robin di awal ke Worker.
  - dynamic: pull-based; Worker minta task saat slot kosong; Master
             mempertimbangkan kapasitas Worker.

Mekanisme dynamic:
  - Pull-based work queue.
  - Prefetch sebanding kapasitas Worker (jumlah core + throughput terukur).
  - Menahan task besar untuk Worker yang terlalu sibuk.
  - LPT (Longest-Processing-Time-first): task terbesar diprioritaskan.
"""

from __future__ import annotations
import threading
import time
import logging
from collections import defaultdict, deque
from typing import Optional, List, Dict, Deque

from common.tasks import Task, TaskStatus

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helper RequeuedTasks untuk kompatibilitas backward (list & int)
# ---------------------------------------------------------------------------

class RequeuedTasks(list):
    """
    List of task_id yang di-requeue.
    Mendukung perbandingan dengan int (misal: requeued == 3 atau len(requeued) == 3)
    agar kompatibel dengan kode lama dan unit test.
    """
    def __eq__(self, other):
        if isinstance(other, int):
            return len(self) == other
        return super().__eq__(other)

    def __int__(self):
        return len(self)


# ---------------------------------------------------------------------------
# WorkerInfo: rekam kapasitas dan status tiap Worker
# ---------------------------------------------------------------------------

class WorkerInfo:
    """Informasi dan statistik per Worker."""

    def __init__(self, worker_id: str, hostname: str, ip: str,
                 num_cores: int, ram_gb: float, num_procs: int) -> None:
        self.worker_id  = worker_id
        self.hostname   = hostname
        self.ip         = ip
        self.num_cores  = num_cores
        self.ram_gb     = ram_gb
        self.num_procs  = num_procs
        self.alive      = True
        self.last_heartbeat: float = time.time()

        # Statistik beban (Komponen d)
        self.cpu_pct:       float = 0.0
        self.mem_pct:       float = 0.0
        self.active_tasks:  int   = 0
        self.throughput_mbs: float = 0.0  # MB/detik terukur
        self.bytes_processed: int  = 0
        self.tasks_done:    int   = 0
        self.tasks_failed:  int   = 0
        self.corrupt_tasks: int   = 0
        self.job_bytes:     int   = 0
        self.job_tasks:     int   = 0
        self.job_failed:    int   = 0
        self.job_corrupt:   int   = 0
        self.tasks_assigned: int  = 0
        self.active_seconds: float = 0.0
        self.idle_seconds:  float  = 0.0
        self._last_active_ts: float = time.time()

    @property
    def capacity(self) -> int:
        """Jumlah slot task yang bisa ditampung (berdasarkan num_procs)."""
        return max(1, self.num_procs)

    @property
    def available_slots(self) -> int:
        """Slot kosong = capacity - active_tasks."""
        return max(0, self.capacity - self.active_tasks)

    @property
    def is_busy(self) -> bool:
        """Worker dianggap sangat sibuk jika CPU > 90%."""
        return self.cpu_pct > 90.0

    def update_heartbeat(self, cpu_pct: float, mem_pct: float,
                         active_tasks: int) -> None:
        self.last_heartbeat = time.time()
        self.cpu_pct       = cpu_pct
        self.mem_pct       = mem_pct
        self.active_tasks  = active_tasks
        self.alive         = True

    def reset_job_stats(self) -> None:
        """Reset statistik khusus untuk pekerjaan (job) baru."""
        self.job_bytes   = 0
        self.job_tasks   = 0
        self.job_failed  = 0
        self.job_corrupt = 0

    def record_task_done(self, bytes_processed: int, duration: float) -> None:
        self.bytes_processed += bytes_processed
        self.tasks_done      += 1
        self.job_bytes       += bytes_processed
        self.job_tasks       += 1
        self.active_seconds  += duration
        if duration > 0:
            mb = bytes_processed / (1024 * 1024)
            # Moving average throughput
            if self.throughput_mbs == 0:
                self.throughput_mbs = mb / duration
            else:
                self.throughput_mbs = 0.7 * self.throughput_mbs + 0.3 * (mb / duration)

    def record_task_failed(self) -> None:
        self.tasks_failed += 1
        self.job_failed   += 1

    def record_task_corrupt(self) -> None:
        self.corrupt_tasks += 1
        self.tasks_failed  += 1
        self.job_corrupt   += 1
        self.job_failed    += 1


# ---------------------------------------------------------------------------
# Scheduler (Komponen d)
# ---------------------------------------------------------------------------

class Scheduler:
    """
    Mengelola antrean task dan distribusi ke Worker.
    Thread-safe (semua state dilindungi lock).
    """

    def __init__(self, tasks: List[Task], mode: str = "dynamic",
                 max_retries: int = 3) -> None:
        """
        Args:
            tasks:       daftar Task hasil decompose.
            mode:        "static" atau "dynamic".
            max_retries: batas percobaan ulang per task.
        """
        self.mode        = mode
        self.max_retries = max_retries
        self._lock       = threading.Lock()

        # Antrean task per mode
        if mode == "dynamic":
            # LPT: urutkan dari terbesar ke terkecil
            sorted_tasks = sorted(tasks, key=lambda t: t.length, reverse=True)
            self._queue: Deque[Task] = deque(sorted_tasks)
        else:
            self._queue = deque(tasks)

        # State
        self._all_tasks:      Dict[str, Task]       = {t.task_id: t for t in tasks}
        self._assigned:       Dict[str, Task]        = {}  # task_id → Task
        self._failed_tasks:   List[Task]              = []
        self._completed_count: int                   = 0
        self._total_count:    int                    = len(tasks)

        # Statistik distribusi task per Worker
        self._worker_task_counts: Dict[str, int]    = defaultdict(int)
        self._worker_bytes:       Dict[str, int]    = defaultdict(int)

        # Static mode: pre-assign round-robin
        self._static_assignments: Dict[str, Deque[Task]] = {}
        self._registered_worker_ids: List[str] = []

        logger.info(f"Scheduler mode={mode}, total_tasks={len(tasks)}")

    def register_worker(self, worker_info: WorkerInfo) -> None:
        """Daftarkan Worker ke scheduler untuk mode static."""
        with self._lock:
            if worker_info.worker_id not in self._registered_worker_ids:
                self._registered_worker_ids.append(worker_info.worker_id)
            if self.mode == "static":
                if worker_info.worker_id not in self._static_assignments:
                    self._static_assignments[worker_info.worker_id] = deque()

    def distribute_static(self, worker_ids: List[str]) -> None:
        """
        Mode static: bagikan semua task ke Worker secara round-robin.
        Dipanggil sekali setelah semua Worker terdaftar atau secara dinamis.
        """
        with self._lock:
            if self.mode != "static":
                return
            for wid in worker_ids:
                if wid not in self._static_assignments:
                    self._static_assignments[wid] = deque()
            tasks_list = list(self._queue)
            self._queue.clear()
            for i, task in enumerate(tasks_list):
                wid = worker_ids[i % len(worker_ids)]
                self._static_assignments[wid].append(task)
            total = sum(len(q) for q in self._static_assignments.values())
            logger.info(f"Static distribution: {total} tasks ke {len(worker_ids)} Worker")

    def get_assigned_task_ids_of_worker(self, worker_id: str) -> List[str]:
        """Ambil daftar task_id yang sedang dipegang oleh Worker ini."""
        with self._lock:
            return [t.task_id for t in self._assigned.values()
                    if t.assigned_worker == worker_id]

    @property
    def total_retries(self) -> int:
        """Total percobaan ulang yang terjadi pada seluruh task."""
        with self._lock:
            return sum(max(0, t.attempts - 1) for t in self._all_tasks.values())

    def get_task(self, worker_info: WorkerInfo) -> Optional[Task]:
        """
        Ambil task berikutnya untuk Worker.
        Mode dynamic: pilih dari antrean global, pertimbangkan kapasitas.
        Mode static:  ambil dari antrean personal Worker.

        Returns:
            Task atau None jika tidak ada.
        """
        with self._lock:
            if self.mode == "static":
                return self._get_task_static(worker_info)
            else:
                return self._get_task_dynamic(worker_info)

    def _get_task_static(self, worker_info: WorkerInfo) -> Optional[Task]:
        # Jika antrean static worker belum diisi tapi masih ada task global di _queue,
        # otomatis bagikan ke worker yang sudah terdaftar
        if self._queue:
            workers = self._registered_worker_ids or [worker_info.worker_id]
            for wid in workers:
                if wid not in self._static_assignments:
                    self._static_assignments[wid] = deque()
            tasks_list = list(self._queue)
            self._queue.clear()
            for i, task in enumerate(tasks_list):
                wid = workers[i % len(workers)]
                self._static_assignments[wid].append(task)
            logger.info(f"Auto static distribution: {len(tasks_list)} tasks ke {len(workers)} worker")

        q = self._static_assignments.get(worker_info.worker_id, deque())
        if not q:
            return None
        task = q.popleft()
        self._mark_assigned(task, worker_info.worker_id)
        return task

    def _get_task_dynamic(self, worker_info: WorkerInfo) -> Optional[Task]:
        if not self._queue:
            return None
        # Jika Worker sangat sibuk, hanya berikan task kecil
        if worker_info.is_busy and len(self._queue) > 1:
            # Cari task terkecil yang ada
            small_task = min(self._queue, key=lambda t: t.length)
            self._queue.remove(small_task)
            self._mark_assigned(small_task, worker_info.worker_id)
            return small_task
        # Normal: ambil task terbesar (LPT sudah terurut di queue)
        task = self._queue.popleft()
        self._mark_assigned(task, worker_info.worker_id)
        return task

    def _mark_assigned(self, task: Task, worker_id: str) -> None:
        """Internal: tandai task sebagai ASSIGNED."""
        task.status          = TaskStatus.ASSIGNED
        task.attempts       += 1
        task.assigned_worker = worker_id
        task.assigned_at     = time.time()
        self._assigned[task.task_id] = task
        self._worker_task_counts[worker_id] += 1
        self._worker_bytes[worker_id]       += task.length

    def mark_done(self, task_id: str, bytes_done: int = 0) -> None:
        """Tandai task selesai berhasil."""
        with self._lock:
            task = self._assigned.pop(task_id, None)
            if task:
                task.status       = TaskStatus.DONE
                task.completed_at = time.time()
                self._completed_count += 1
                self._all_tasks[task_id] = task
                logger.debug(f"Task {task_id[:8]} DONE (total: {self._completed_count}/{self._total_count})")

    def requeue_worker_tasks(self, worker_id: str) -> RequeuedTasks:
        """
        Komponen e – Fault Tolerance: kembalikan semua task milik Worker DEAD ke antrean.

        Returns:
            RequeuedTasks (list of task_id) yang juga bernilai integer jumlah task
            agar kompatibel dengan perbandingan int (contoh: requeued == 3).
        """
        with self._lock:
            to_requeue = [t for t in self._assigned.values()
                          if t.assigned_worker == worker_id]
            requeued_ids = []
            for task in to_requeue:
                del self._assigned[task.task_id]
                if task.attempts >= self.max_retries:
                    # Melebihi batas retry → tandai FAILED
                    task.status = TaskStatus.FAILED
                    self._failed_tasks.append(task)
                    self._total_count -= 1  # kurangi target
                    logger.warning(
                        f"Task {task.task_id[:8]} FAILED (melebihi max_retries={self.max_retries})")
                else:
                    # Requeue ke depan antrean (prioritaskan recovery)
                    task.status          = TaskStatus.PENDING
                    task.assigned_worker = None
                    self._queue.appendleft(task)
                    requeued_ids.append(task.task_id)
            if requeued_ids:
                logger.info(f"Requeue {len(requeued_ids)} task dari Worker {worker_id}")
            return RequeuedTasks(requeued_ids)

    def is_all_done(self) -> bool:
        """Semua task selesai atau gagal?"""
        with self._lock:
            return (self._completed_count + len(self._failed_tasks)) >= self._total_count

    def has_pending(self) -> bool:
        """Masih ada task yang menunggu atau dikerjakan?"""
        with self._lock:
            return bool(self._queue) or bool(self._assigned)

    def get_summary(self) -> dict:
        """Ringkasan progres scheduler."""
        with self._lock:
            return {
                "total":           self._total_count,
                "completed":       self._completed_count,
                "pending":         len(self._queue),
                "assigned":        len(self._assigned),
                "failed":          len(self._failed_tasks),
                "worker_tasks":    dict(self._worker_task_counts),
                "worker_bytes":    dict(self._worker_bytes),
            }

    def get_imbalance_metrics(self) -> dict:
        """
        Komponen d: Hitung metrik ketidakseimbangan beban.
        Koefisien variasi (CV) dan rasio max/min task per Worker.
        """
        with self._lock:
            counts = list(self._worker_task_counts.values())
            if len(counts) < 2:
                return {"cv": 0.0, "max_min_ratio": 1.0}
            mean = sum(counts) / len(counts)
            var  = sum((c - mean) ** 2 for c in counts) / len(counts)
            std  = var ** 0.5
            cv   = std / mean if mean > 0 else 0.0
            ratio = max(counts) / max(min(counts), 1)
            return {"cv": round(cv, 4), "max_min_ratio": round(ratio, 4)}
