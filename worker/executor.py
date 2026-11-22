"""
worker/executor.py
==================
Komponen b – Parallel Processing: ProcessPoolExecutor multicore untuk kompresi.

Gunakan proses (bukan thread) agar terhindar dari GIL Python.
Tiap Worker dapat menampung N task sekaligus (N = jumlah proses/core).
"""

from __future__ import annotations
import concurrent.futures
import hashlib
import logging
import os
import time
from typing import Optional, Tuple

from common.compression import compress_chunk_task, compress_data, pack_chunk_result
from common.tasks import Task, TaskResult
from common.utils import sha256_bytes

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Fungsi top-level untuk ProcessPoolExecutor (harus bisa di-pickle)
# ---------------------------------------------------------------------------

def _execute_compression_job(
    raw_data:    bytes,
    algorithm:   str,
    level:       int,
    chunk_index: int,
    chunk_count: int,
) -> Tuple[bytes, dict]:
    """
    Fungsi yang dijalankan di proses terpisah (CPU-bound).
    Harus top-level agar bisa di-pickle di Windows.

    Returns:
        (packed_container_bytes, stats_dict)
    """
    return compress_chunk_task(raw_data, algorithm, level, chunk_index, chunk_count)


# ---------------------------------------------------------------------------
# Executor kelas
# ---------------------------------------------------------------------------

class WorkerExecutor:
    """
    Mengelola pool proses untuk kompresi paralel di Worker.
    Komponen b: ProcessPoolExecutor dengan max_workers = num_procs.
    """

    def __init__(self, num_procs: Optional[int] = None,
                 worker_id: str = "worker",
                 throttle_pct: float = 0.0) -> None:
        """
        Args:
            num_procs:    jumlah proses (default: os.cpu_count()).
            worker_id:    ID Worker untuk logging.
            throttle_pct: 0–100; simulasikan Worker lambat (Komponen d/e).
                          sleep proporsional sebelum submit task ke pool.
        """
        self.num_procs   = num_procs or os.cpu_count() or 1
        self.worker_id   = worker_id
        self.throttle    = throttle_pct / 100.0  # rasio 0–1
        self._pool: Optional[concurrent.futures.ProcessPoolExecutor] = None
        self._active_futures: dict = {}  # task_id → Future

    def start(self) -> None:
        """Buat ProcessPoolExecutor."""
        self._pool = concurrent.futures.ProcessPoolExecutor(
            max_workers=self.num_procs
        )
        logger.info(
            f"[{self.worker_id}] ProcessPoolExecutor dimulai "
            f"(procs={self.num_procs}, throttle={self.throttle*100:.0f}%)"
        )

    def stop(self) -> None:
        """Shutdown pool dengan aman."""
        if self._pool:
            self._pool.shutdown(wait=True)
            self._pool = None
        logger.info(f"[{self.worker_id}] ProcessPoolExecutor dihentikan")

    @property
    def active_count(self) -> int:
        """Jumlah task yang sedang diproses."""
        return sum(1 for f in self._active_futures.values() if not f.done())

    @property
    def available_slots(self) -> int:
        """Slot kosong di pool."""
        return max(0, self.num_procs - self.active_count)

    def submit(self, task: Task, raw_data: bytes) -> "concurrent.futures.Future":
        """
        Submit satu task kompresi ke pool proses.

        Args:
            task:     Task yang akan dikerjakan.
            raw_data: data mentah chunk.

        Returns:
            Future yang bisa di-await.
        """
        if self.throttle > 0:
            # Simulasi Worker lambat (Komponen d – load balancing test)
            sleep_time = task.length / (1024 * 1024) * self.throttle * 2
            time.sleep(sleep_time)

        future = self._pool.submit(
            _execute_compression_job,
            raw_data,
            task.algorithm,
            task.level,
            task.chunk_index,
            task.chunk_count,
        )
        self._active_futures[task.task_id] = future
        return future

    def collect_result(self, task: Task, raw_data: bytes,
                       future: "concurrent.futures.Future",
                       timeout: float = 300.0) -> TaskResult:
        """
        Tunggu hasil future dan bungkus menjadi TaskResult.

        Args:
            task:    Task asli.
            raw_data: data mentah (untuk SHA-256 original).
            future:  Future dari submit().
            timeout: batas waktu tunggu (detik).

        Returns:
            TaskResult.
        """
        try:
            packed_bytes, stats = future.result(timeout=timeout)
            self._active_futures.pop(task.task_id, None)

            return TaskResult(
                task_id           = task.task_id,
                original_size     = stats["original_size"],
                compressed_size   = stats["compressed_size"],
                ratio             = stats["ratio"],
                duration          = stats["duration"],
                worker_id         = self.worker_id,
                sha256_original   = stats["sha256_original"],
                sha256_compressed = stats["sha256_compressed"],
                status            = "DONE",
                algorithm         = task.algorithm,
                level             = task.level,
            ), packed_bytes

        except concurrent.futures.TimeoutError:
            self._active_futures.pop(task.task_id, None)
            return TaskResult(
                task_id=task.task_id, original_size=0, compressed_size=0,
                ratio=0.0, duration=0.0, worker_id=self.worker_id,
                sha256_original="", sha256_compressed="",
                status="FAILED", error="Timeout",
                algorithm=task.algorithm, level=task.level,
            ), b""

        except Exception as e:
            self._active_futures.pop(task.task_id, None)
            logger.error(f"[{self.worker_id}] Task {task.task_id[:8]} gagal: {e}")
            return TaskResult(
                task_id=task.task_id, original_size=0, compressed_size=0,
                ratio=0.0, duration=0.0, worker_id=self.worker_id,
                sha256_original="", sha256_compressed="",
                status="FAILED", error=str(e),
                algorithm=task.algorithm, level=task.level,
            ), b""
