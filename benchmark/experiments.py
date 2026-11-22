"""
benchmark/experiments.py
========================
Komponen f – Performance Evaluation: Orkestrasi seluruh skenario pengujian.
Semua skenario diulang minimal 3 kali, hasilnya disimpan ke CSV dan JSON.

Skenario:
  a. Sequential        – baseline T₁
  b. Local Parallel    – variasi proses 1,2,4,… core
  c. Distributed       – Master + Worker (konfigurasi tetap)
  d. Load Balancing    – static vs dynamic (dengan Worker lambat)
  e. Fault Tolerance   – matikan Worker, ukur recovery
  f. Scalability       – variasi 1,2,4,6,9 Worker

Semua skenario b dijalankan in-process di komputer ini.
Skenario c,d,e,f membutuhkan sistem terdistribusi yang sudah berjalan
(jalankan run_distributed.py di klaster 10 node).
"""

from __future__ import annotations
import argparse
import csv
import json
import logging
import os
import sys
import time

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from common.utils import setup_logging
from benchmark.sequential    import run_sequential
from benchmark.local_parallel import run_local_parallel

logger = logging.getLogger("experiments")


def save_csv(rows: list, path: str) -> None:
    """Simpan daftar dict ke CSV."""
    if not rows:
        return
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


def run_scenario_a(input_dir: str, algorithm: str, level: int,
                   chunk_size: int, repeats: int) -> dict:
    """Skenario a: Sequential baseline."""
    logger.info("=== SKENARIO A: Sequential ===")
    rows = []
    for i in range(repeats):
        r = run_sequential(input_dir, algorithm, level, chunk_size)
        r["run"] = i + 1
        r["scenario"] = "a_sequential"
        rows.append(r)
        logger.info(f"  Run {i+1}: {r['execution_time']:.4f}s")

    times = [r["execution_time"] for r in rows]
    mean = sum(times) / len(times)
    std  = (sum((t - mean)**2 for t in times) / len(times)) ** 0.5
    result = {"scenario": "a", "rows": rows, "mean": mean, "std": std, "T1": mean}
    logger.info(f"Skenario A selesai: T1={mean:.4f}s ± {std:.4f}s")
    return result


def run_scenario_b(input_dir: str, algorithm: str, level: int,
                   chunk_size: int, repeats: int, t1: float) -> dict:
    """Skenario b: Local Parallel."""
    logger.info("=== SKENARIO B: Local Parallel ===")
    max_procs = os.cpu_count() or 1
    variants, n = [], 1
    while n <= max_procs:
        variants.append(n); n *= 2
    if max_procs not in variants:
        variants.append(max_procs)

    rows = []
    for np in variants:
        for i in range(repeats):
            r = run_local_parallel(input_dir, np, algorithm, level, chunk_size)
            r.update({"run": i+1, "scenario": "b_local_parallel",
                       "num_procs": np,
                       "speedup": round(t1 / max(r["execution_time"], 0.001), 4),
                       "efficiency": round((t1 / max(r["execution_time"], 0.001)) / np, 4)})
            rows.append(r)
            logger.info(f"  procs={np} run {i+1}: {r['execution_time']:.4f}s "
                        f"speedup={r['speedup']}")

    return {"scenario": "b", "rows": rows, "T1": t1}


def run_all_local(args) -> None:
    """Jalankan skenario a dan b (bisa dijalankan lokal tanpa cluster)."""
    setup_logging("experiments", "INFO")
    results = {}

    # Skenario a
    res_a = run_scenario_a(
        args.input_dir, args.algorithm, args.level,
        args.chunk_size, args.repeats
    )
    results["a"] = res_a
    save_csv(res_a["rows"], "results/raw/scenario_a.csv")

    # Skenario b
    res_b = run_scenario_b(
        args.input_dir, args.algorithm, args.level,
        args.chunk_size, args.repeats, res_a["T1"]
    )
    results["b"] = res_b
    save_csv(res_b["rows"], "results/raw/scenario_b.csv")

    # Simpan semua ke JSON
    out_path = "results/experiments_local.json"
    os.makedirs("results/raw", exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    logger.info(f"Semua hasil lokal disimpan ke {out_path}")

    print("\n" + "="*50)
    print("RINGKASAN EKSPERIMEN LOKAL")
    print("="*50)
    print(f"T₁ (sequential)  : {res_a['mean']:.4f}s ± {res_a['std']:.4f}s")
    b_rows = res_b["rows"]
    print(f"Local Parallel   :")
    for np in sorted(set(r["num_procs"] for r in b_rows)):
        sub = [r for r in b_rows if r["num_procs"] == np]
        t_avg = sum(r["execution_time"] for r in sub) / len(sub)
        sp = round(res_a["T1"] / t_avg, 3)
        ef = round(sp / np, 3)
        print(f"  procs={np:2d}: t={t_avg:.3f}s  speedup={sp}  efficiency={ef}")


def parse_args():
    p = argparse.ArgumentParser(description="Jalankan semua skenario eksperimen lokal")
    p.add_argument("--input-dir",  required=True)
    p.add_argument("--algorithm",  default="lzma")
    p.add_argument("--level",      type=int, default=6)
    p.add_argument("--chunk-size", type=int, default=32, help="MB")
    p.add_argument("--repeats",    type=int, default=3)
    return p.parse_args()


if __name__ == "__main__":
    import multiprocessing
    multiprocessing.freeze_support()
    run_all_local(parse_args())
