"""
benchmark/local_parallel.py
============================
Komponen f – Performance Evaluation: Skenario b — Local Parallel.
Multiprocessing di 1 komputer; variasi jumlah proses: 1, 2, 4, … hingga jumlah core.
"""

from __future__ import annotations
import os
import sys
import time
import json
import argparse
import logging
import concurrent.futures
import multiprocessing

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from common.tasks import decompose_directory
from common.compression import compress_chunk_task
from common.utils import setup_logging

logger = logging.getLogger("local_parallel")


def _compress_one(args_tuple):
    """Top-level untuk ProcessPoolExecutor (harus bisa di-pickle)."""
    file_path, offset, length, algorithm, level, chunk_index, chunk_count = args_tuple
    with open(file_path, "rb") as f:
        f.seek(offset)
        raw = f.read(length)
    return compress_chunk_task(raw, algorithm, level, chunk_index, chunk_count)


def run_local_parallel(input_dir: str, num_procs: int,
                       algorithm: str = "lzma", level: int = 6,
                       chunk_size_mb: int = 32) -> dict:
    """
    Kompresi paralel di 1 komputer dengan num_procs proses.

    Returns:
        dict hasil.
    """
    chunk_size = chunk_size_mb * 1024 * 1024
    tasks      = decompose_directory(input_dir, chunk_size, algorithm, level)
    if not tasks:
        raise ValueError(f"Tidak ada file di {input_dir}")

    args_list = [
        (t.file_path, t.offset, t.length, t.algorithm, t.level,
         t.chunk_index, t.chunk_count)
        for t in tasks
    ]

    logger.info(f"LocalParallel: {len(tasks)} task, procs={num_procs}")
    total_original   = 0
    total_compressed = 0

    t0 = time.perf_counter()
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_procs) as pool:
        futures = {pool.submit(_compress_one, a): a for a in args_list}
        for fut in concurrent.futures.as_completed(futures):
            try:
                _, stats = fut.result()
                total_original   += stats["original_size"]
                total_compressed += stats["compressed_size"]
            except Exception as e:
                logger.error(f"Task gagal: {e}")
    elapsed = time.perf_counter() - t0

    return {
        "mode":            "local_parallel",
        "num_processes":   num_procs,
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
    if sys.platform == "win32":
        multiprocessing.freeze_support()

    p = argparse.ArgumentParser(description="Local Parallel benchmark (Skenario b)")
    p.add_argument("--input-dir",  required=True)
    p.add_argument("--algorithm",  default="lzma")
    p.add_argument("--level",      type=int, default=6)
    p.add_argument("--chunk-size", type=int, default=32)
    p.add_argument("--repeats",    type=int, default=3)
    p.add_argument("--output",     default="results/benchmark_local_parallel.json")
    p.add_argument("--t1",         type=float, default=None,
                   help="T₁ dari sequential benchmark (untuk hitung Speedup)")
    args = p.parse_args()

    setup_logging("local_parallel")

    max_procs = os.cpu_count() or 1
    # Variasi: 1, 2, 4, 8, ... hingga max_procs
    proc_variants = []
    n = 1
    while n <= max_procs:
        proc_variants.append(n)
        n *= 2
    if max_procs not in proc_variants:
        proc_variants.append(max_procs)

    all_results = []
    for np in proc_variants:
        runs = []
        for i in range(args.repeats):
            logger.info(f"procs={np} run {i+1}/{args.repeats}...")
            r = run_local_parallel(
                args.input_dir, np, args.algorithm, args.level, args.chunk_size
            )
            r["run"] = i + 1
            runs.append(r)

        times = [r["execution_time"] for r in runs]
        mean  = sum(times) / len(times)
        std   = (sum((t - mean)**2 for t in times) / len(times)) ** 0.5
        speedup   = round(args.t1 / mean, 4) if args.t1 else None
        efficiency = round(speedup / np, 4) if speedup else None

        entry = {
            "num_procs":  np,
            "runs":       runs,
            "mean_time":  round(mean, 4),
            "std_time":   round(std, 4),
            "speedup":    speedup,
            "efficiency": efficiency,
        }
        all_results.append(entry)
        logger.info(
            f"  procs={np}: t={mean:.3f}s±{std:.3f}s "
            f"speedup={speedup} efficiency={efficiency}"
        )

    output = {"scenario": "local_parallel", "T1": args.t1, "variants": all_results}
    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(output, f, indent=2)
    logger.info(f"Hasil disimpan ke {args.output}")


if __name__ == "__main__":
    main()
