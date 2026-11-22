"""
benchmark/sequential.py
=======================
Komponen f – Performance Evaluation: Skenario a — Sequential baseline (T₁).
1 proses, 1 komputer. Dipakai sebagai T₁ untuk menghitung Speedup = T₁ / Tₚ.
"""

from __future__ import annotations
import os
import sys
import time
import json
import hashlib
import argparse
import logging

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from common.tasks import decompose_directory
from common.compression import compress_chunk_task
from common.utils import setup_logging, format_bytes, format_duration

logger = logging.getLogger("sequential")


def run_sequential(input_dir: str, algorithm: str = "lzma",
                   level: int = 6, chunk_size_mb: int = 32) -> dict:
    """
    Jalankan kompresi secara sekuensial (1 proses).

    Returns:
        dict hasil: execution_time, throughput, ratio, dsb.
    """
    chunk_size = chunk_size_mb * 1024 * 1024
    tasks = decompose_directory(input_dir, chunk_size, algorithm, level)
    if not tasks:
        raise ValueError(f"Tidak ada file di {input_dir}")

    logger.info(f"Sequential: {len(tasks)} task, algo={algorithm}, level={level}")

    total_original   = 0
    total_compressed = 0
    t0 = time.perf_counter()

    for task in tasks:
        with open(task.file_path, "rb") as f:
            f.seek(task.offset)
            raw = f.read(task.length)

        packed, stats = compress_chunk_task(
            raw, task.algorithm, task.level,
            task.chunk_index, task.chunk_count
        )
        total_original   += stats["original_size"]
        total_compressed += stats["compressed_size"]

    elapsed = time.perf_counter() - t0

    return {
        "mode":            "sequential",
        "num_processes":   1,
        "num_workers":     1,
        "tasks":           len(tasks),
        "execution_time":  round(elapsed, 4),
        "total_original":  total_original,
        "total_compressed": total_compressed,
        "ratio":           round(total_original / max(total_compressed, 1), 4),
        "throughput_mbs":  round((total_original / (1024**2)) / max(elapsed, 0.001), 3),
        "throughput_fps":  round(len(tasks) / max(elapsed, 0.001), 3),
        "algorithm":       algorithm,
        "level":           level,
    }


def main() -> None:
    p = argparse.ArgumentParser(description="Sequential benchmark (baseline T₁)")
    p.add_argument("--input-dir",  required=True)
    p.add_argument("--algorithm",  default="lzma")
    p.add_argument("--level",      type=int, default=6)
    p.add_argument("--chunk-size", type=int, default=32)
    p.add_argument("--repeats",    type=int, default=3)
    p.add_argument("--output",     default="results/benchmark_sequential.json")
    args = p.parse_args()

    setup_logging("sequential")
    results = []
    for i in range(args.repeats):
        logger.info(f"Run {i+1}/{args.repeats}...")
        r = run_sequential(args.input_dir, args.algorithm,
                           args.level, args.chunk_size)
        r["run"] = i + 1
        results.append(r)
        logger.info(
            f"  waktu={r['execution_time']:.3f}s "
            f"throughput={r['throughput_mbs']:.2f} MB/s "
            f"ratio={r['ratio']:.4f}"
        )

    times = [r["execution_time"] for r in results]
    mean  = sum(times) / len(times)
    std   = (sum((t - mean)**2 for t in times) / len(times)) ** 0.5

    output = {
        "runs":    results,
        "mean_time": round(mean, 4),
        "std_time":  round(std, 4),
        "T1":        round(mean, 4),   # T₁ untuk Speedup
    }
    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(output, f, indent=2)
    logger.info(f"Hasil disimpan ke {args.output}")
    logger.info(f"T₁ (rata-rata) = {mean:.4f}s ± {std:.4f}s")


if __name__ == "__main__":
    main()
