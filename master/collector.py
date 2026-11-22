"""
master/collector.py
===================
Komponen c – Distributed Processing: Penerimaan hasil task, penulisan output
atomik, rekonstruksi file multi-chunk, dan ringkasan akhir.
"""

from __future__ import annotations
import os
import threading
import logging
import json
import time
from typing import Dict, List, Optional, Tuple
from collections import defaultdict

from common.tasks import Task, TaskResult
from common.compression import unpack_chunk_result
from common.utils import atomic_write, ensure_dir, format_bytes, format_duration

logger = logging.getLogger(__name__)


class ResultCollector:
    """
    Menerima dan menyimpan hasil kompresi dari Worker.
    Thread-safe (lock pada semua state bersama).
    """

    def __init__(self, output_dir: str, all_tasks: Dict[str, Task]) -> None:
        """
        Args:
            output_dir: direktori output Master.
            all_tasks:  dict task_id → Task (semua task).
        """
        self._output_dir = output_dir
        self._all_tasks  = all_tasks
        self._lock       = threading.Lock()
        ensure_dir(output_dir)

        # Penyimpanan hasil per file (untuk rekonstruksi multi-chunk)
        # file_path → {chunk_index → (packed_bytes, result)}
        self._chunks: Dict[str, Dict[int, Tuple[bytes, TaskResult]]] = \
            defaultdict(dict)

        # Hasil per task
        self._results: Dict[str, TaskResult] = {}

        # Statistik
        self._total_original:   int   = 0
        self._total_compressed: int   = 0
        self._total_duration:   float = 0.0
        self._start_time:       float = time.time()

        # Idempotency: set task_id yang sudah diterima (Komponen e)
        self._received_ids: set = set()

    def collect(self, result: TaskResult, compressed_data: bytes) -> bool:
        """
        Simpan satu hasil task.
        Idempoten: hasil duplikat (dari Worker yang kembali) diabaikan.

        Args:
            result:          TaskResult dari Worker.
            compressed_data: bytes terkompresi (sudah dalam container format).

        Returns:
            True jika hasil baru; False jika duplikat (diabaikan).
        """
        with self._lock:
            if result.task_id in self._received_ids:
                logger.debug(f"Duplikat task {result.task_id[:8]} diabaikan")
                return False
            self._received_ids.add(result.task_id)
            self._results[result.task_id] = result
            self._total_original   += result.original_size
            self._total_compressed += result.compressed_size
            self._total_duration   += result.duration

        # Tulis chunk ke disk
        task = self._all_tasks.get(result.task_id)
        if task:
            self._save_chunk(task, compressed_data, result)

        return True

    def _save_chunk(self, task: Task, compressed_data: bytes,
                    result: TaskResult) -> None:
        """Simpan satu chunk ke disk; rekonstruksi file jika semua chunk ada."""
        file_key   = task.file_path
        chunk_idx  = task.chunk_index
        chunk_count = task.chunk_count

        with self._lock:
            self._chunks[file_key][chunk_idx] = (compressed_data, result)
            is_complete = len(self._chunks[file_key]) == chunk_count

        if is_complete:
            self._reconstruct_output(file_key, chunk_count)

    def _reconstruct_output(self, file_path: str, chunk_count: int) -> None:
        """
        Gabung semua chunk menjadi satu file output terkompresi.
        Format output: satu file .compressed berisi semua container format chunk
        secara berurutan berdasarkan chunk_index (untuk rekonstruksi sempurna).
        """
        with self._lock:
            chunks_dict = dict(self._chunks[file_path])

        # Susun chunk secara berurutan berdasarkan chunk_index yang tersimpan di container
        # (bukan hanya dari key dict yang mungkin di-insert tidak berurutan)
        ordered_items = sorted(chunks_dict.items(), key=lambda kv: kv[0])
        ordered = [item[1][0] for item in ordered_items]  # (packed_bytes, result)
        combined = b"".join(ordered)

        # Tentukan path output (sejajar struktur sumber)
        rel = os.path.relpath(file_path, start=os.path.dirname(file_path))
        fname = os.path.basename(file_path) + ".compressed"
        out_path = os.path.join(self._output_dir, fname)

        atomic_write(out_path, combined)
        logger.info(
            f"File selesai: {os.path.basename(file_path)} "
            f"({chunk_count} chunk) → {out_path}"
        )

    def get_progress(self) -> dict:
        """Progres saat ini."""
        with self._lock:
            done = len(self._received_ids)
            total = len(self._all_tasks)
            elapsed = time.time() - self._start_time
            throughput = (self._total_original / (1024**2)) / max(elapsed, 0.001)
            return {
                "done":          done,
                "total":         total,
                "pct":           round(100 * done / max(total, 1), 1),
                "elapsed":       round(elapsed, 2),
                "throughput_mbs": round(throughput, 2),
                "total_original": self._total_original,
                "total_compressed": self._total_compressed,
            }

    def get_final_summary(self, worker_registry: dict,
                          retry_count: int) -> dict:
        """
        Ringkasan akhir setelah semua task selesai.
        Dicetak oleh Master dan disimpan ke file JSON.
        """
        with self._lock:
            elapsed   = time.time() - self._start_time
            orig      = self._total_original
            comp      = self._total_compressed
            ratio     = orig / max(comp, 1)
            workers   = {wid: {
                "tasks_done":   wi.tasks_done,
                "bytes":        wi.bytes_processed,
                "throughput":   round(wi.throughput_mbs, 2),
            } for wid, wi in worker_registry.items()}

            return {
                "total_files":       len(self._chunks),
                "total_original":    format_bytes(orig),
                "total_compressed":  format_bytes(comp),
                "ratio":             round(ratio, 4),
                "elapsed":           format_duration(elapsed),
                "elapsed_seconds":   round(elapsed, 3),
                "throughput_mbs":    round((orig / (1024**2)) / max(elapsed, 0.001), 2),
                "worker_summary":    workers,
                "retry_count":       retry_count,
                "failed_tasks":      0,
            }

    def save_summary(self, summary: dict, path: str) -> None:
        """Simpan ringkasan ke file JSON."""
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        logger.info(f"Ringkasan disimpan ke {path}")
