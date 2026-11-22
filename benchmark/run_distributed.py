"""
benchmark/run_distributed.py
=============================
Komponen f – Orkestrasi pengujian distributed (Skenario c, d, e, f).
Jalankan setelah Master dan Worker sudah berjalan di klaster 10 node.

Skrip ini mengumpulkan hasil summary.json dari Master, membaca log Worker,
lalu menulis file eksperimen lengkap untuk dianalisis oleh analyze.py.
"""

from __future__ import annotations
import argparse
import json
import logging
import os
import sys
import time

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from common.utils import setup_logging

logger = logging.getLogger("run_distributed")


def collect_distributed_results(summary_path: str, log_dir: str,
                                 t1: float, num_workers: int,
                                 scenario: str, mode: str = "dynamic") -> dict:
    """
    Baca ringkasan dari Master dan log Worker.

    Args:
        summary_path: path ke results/output/summary.json
        log_dir:      direktori log Worker (monitor-*.json)
        t1:           T₁ dari sequential
        num_workers:  jumlah Worker yang dipakai
        scenario:     label skenario (c/d/e/f)
        mode:         "static" atau "dynamic"

    Returns:
        dict hasil untuk disimpan ke experiments_distributed.json
    """
    if not os.path.isfile(summary_path):
        raise FileNotFoundError(f"summary.json tidak ditemukan: {summary_path}")

    with open(summary_path) as f:
        summary = json.load(f)

    exec_time = summary.get("elapsed_seconds", 0)
    speedup   = round(t1 / max(exec_time, 1e-6), 4) if t1 > 0 else None
    efficiency = round(speedup / num_workers, 4) if speedup else None

    return {
        "scenario":        scenario,
        "mode":            mode,
        "num_workers":     num_workers,
        "execution_time":  exec_time,
        "speedup":         speedup,
        "efficiency":      efficiency,
        "throughput_mbs":  summary.get("throughput_mbs"),
        "ratio":           summary.get("ratio"),
        "worker_summary":  summary.get("worker_summary", {}),
        "worker_tasks":    {
            wid: ws.get("tasks_done", 0)
            for wid, ws in summary.get("worker_summary", {}).items()
        },
        "retry_count":     summary.get("retry_count", 0),
        "timestamp":       time.time(),
    }


def save_experiment_entry(entry: dict, output_path: str = "results/experiments_distributed.json") -> None:
    """Tambahkan satu entri hasil ke file JSON kumulatif."""
    existing = []
    if os.path.isfile(output_path):
        with open(output_path) as f:
            try:
                existing = json.load(f)
                if not isinstance(existing, list):
                    existing = [existing]
            except json.JSONDecodeError:
                existing = []

    existing.append(entry)
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(existing, f, indent=2)
    logger.info(f"Entri disimpan ke {output_path}")


def build_full_experiment(input_path: str, t1: float) -> None:
    """
    Bangun file experiments_full.json dari semua hasil yang sudah dikumpulkan.
    Dipakai oleh analyze.py untuk membuat grafik skenario f (scalability).
    """
    if not os.path.isfile(input_path):
        logger.error(f"{input_path} tidak ditemukan")
        return

    with open(input_path) as f:
        entries = json.load(f)

    # Kelompokkan per skenario
    result = {"T1": t1, "f": []}
    for entry in entries:
        sc = entry.get("scenario", "")
        if sc == "f":
            result["f"].append(entry)
        elif sc == "d":
            mode = entry.get("mode", "dynamic")
            result.setdefault("d", {})[mode] = entry
        elif sc == "e":
            result.setdefault("e", []).append(entry)

    out = "results/experiments_full.json"
    with open(out, "w") as f:
        json.dump(result, f, indent=2)
    logger.info(f"experiments_full.json disimpan ke {out}")


def parse_args():
    p = argparse.ArgumentParser(description="Kumpulkan dan olah hasil distributed")
    p.add_argument("--summary",    required=True, help="Path summary.json dari Master")
    p.add_argument("--log-dir",    default="results/logs")
    p.add_argument("--t1",        type=float, required=True, help="T₁ (dari sequential)")
    p.add_argument("--num-workers", type=int, required=True)
    p.add_argument("--scenario",   default="f", choices=["c","d","e","f"])
    p.add_argument("--mode",       default="dynamic", choices=["static","dynamic"])
    p.add_argument("--output",     default="results/experiments_distributed.json")
    return p.parse_args()


if __name__ == "__main__":
    setup_logging("run_distributed")
    args = parse_args()
    entry = collect_distributed_results(
        args.summary, args.log_dir, args.t1,
        args.num_workers, args.scenario, args.mode
    )
    save_experiment_entry(entry, args.output)
    logger.info(json.dumps(entry, indent=2))
