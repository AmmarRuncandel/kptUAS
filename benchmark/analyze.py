"""
benchmark/analyze.py
====================
Komponen f – Performance Evaluation: Hitung metrik + buat grafik dan tabel.

Metrik:
  - Execution Time (detik, wall-clock)
  - Speedup = T₁ / Tₚ
  - Efficiency = Speedup / p
  - Throughput (MB/s dan file/s)
  - CPU/Memory Utilization (rata-rata dan puncak)
  - Recovery Time
  - Compression Ratio

Output:
  - results/grafik/*.png (semua grafik)
  - results/tabel_*.csv (semua tabel agregat)
"""

from __future__ import annotations
import argparse
import json
import logging
import os
import sys
import csv
import statistics
from typing import List, Dict, Any

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from common.utils import setup_logging

logger = logging.getLogger("analyze")

# Coba import matplotlib dan pandas (opsional di Worker; wajib di Master/analisis)
try:
    import matplotlib
    matplotlib.use("Agg")  # non-interactive backend
    import matplotlib.pyplot as plt
    import matplotlib.ticker as ticker
    HAS_MPL = True
except ImportError:
    HAS_MPL = False
    logger.warning("matplotlib tidak tersedia; grafik dilewati")

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False


GRAFIK_DIR = "results/grafik"
TABEL_DIR  = "results"


def ensure_dirs():
    os.makedirs(GRAFIK_DIR, exist_ok=True)
    os.makedirs(TABEL_DIR, exist_ok=True)


def save_csv(rows: List[Dict], filepath: str) -> None:
    """Simpan list of dict ke format CSV."""
    if not rows:
        return
    os.makedirs(os.path.dirname(os.path.abspath(filepath)), exist_ok=True)
    fieldnames = list(rows[0].keys())
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    logger.info(f"Tabel CSV disimpan: {filepath}")


# ---------------------------------------------------------------------------
# Fungsi bantu statistik
# ---------------------------------------------------------------------------

def agg(values: List[float]) -> Dict[str, float]:
    """Mean dan standar deviasi."""
    if not values:
        return {"mean": 0, "std": 0, "min": 0, "max": 0}
    mn = statistics.mean(values)
    sd = statistics.pstdev(values) if len(values) > 1 else 0.0
    return {"mean": round(mn, 4), "std": round(sd, 4),
            "min": round(min(values), 4), "max": round(max(values), 4)}


# ---------------------------------------------------------------------------
# Grafik: Execution Time vs Jumlah Proses/Worker
# ---------------------------------------------------------------------------

def plot_execution_time(data: List[Dict], t1: float, title: str,
                        xlabel: str, x_key: str, filename: str) -> None:
    if not HAS_MPL:
        return
    xs    = [d[x_key] for d in data]
    times = [d["mean_time"] for d in data]
    stds  = [d.get("std_time", 0) for d in data]

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.errorbar(xs, times, yerr=stds, fmt="o-", color="steelblue",
                capsize=5, label="Waktu Aktual")
    ax.axhline(t1, linestyle="--", color="gray", label=f"T₁ = {t1:.2f}s")
    ax.set_title(title, fontsize=13, fontweight="bold")
    ax.set_xlabel(xlabel, fontsize=11)
    ax.set_ylabel("Waktu Eksekusi (detik)", fontsize=11)
    ax.legend()
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    out = os.path.join(GRAFIK_DIR, filename)
    plt.savefig(out, dpi=150)
    plt.close()
    logger.info(f"Grafik disimpan: {out}")


def plot_speedup(data: List[Dict], xlabel: str, x_key: str,
                 filename: str) -> None:
    if not HAS_MPL:
        return
    xs       = [d[x_key] for d in data]
    speedups = [d.get("speedup", 0) or 0 for d in data]
    ideal    = xs  # Speedup ideal = p

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(xs, speedups, "o-", color="darkorange", label="Speedup Aktual")
    ax.plot(xs, ideal, "--", color="gray", label="Speedup Ideal (linear)")
    ax.set_title("Speedup vs Jumlah Proses/Worker", fontsize=13, fontweight="bold")
    ax.set_xlabel(xlabel, fontsize=11)
    ax.set_ylabel("Speedup = T₁ / Tₚ", fontsize=11)
    ax.legend()
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    out = os.path.join(GRAFIK_DIR, filename)
    plt.savefig(out, dpi=150)
    plt.close()


def plot_efficiency(data: List[Dict], xlabel: str, x_key: str,
                    filename: str) -> None:
    if not HAS_MPL:
        return
    xs  = [d[x_key] for d in data]
    eff = [d.get("efficiency", 0) or 0 for d in data]

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(xs, eff, "s-", color="green", label="Efisiensi")
    ax.axhline(1.0, linestyle="--", color="gray", label="Efisiensi Ideal = 1")
    ax.set_title("Efisiensi = Speedup / p", fontsize=13, fontweight="bold")
    ax.set_xlabel(xlabel, fontsize=11)
    ax.set_ylabel("Efisiensi", fontsize=11)
    ax.set_ylim(0, 1.3)
    ax.legend()
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    out = os.path.join(GRAFIK_DIR, filename)
    plt.savefig(out, dpi=150)
    plt.close()


def plot_throughput(data: List[Dict], xlabel: str, x_key: str,
                    filename: str) -> None:
    if not HAS_MPL:
        return
    xs  = [d[x_key] for d in data]
    thr = [d.get("mean_throughput", 0) or 0 for d in data]

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar(xs, thr, color="steelblue", width=0.6)
    ax.set_title("Throughput vs Jumlah Proses/Worker", fontsize=13, fontweight="bold")
    ax.set_xlabel(xlabel, fontsize=11)
    ax.set_ylabel("Throughput (MB/detik)", fontsize=11)
    ax.grid(True, alpha=0.3, axis="y")
    plt.tight_layout()
    out = os.path.join(GRAFIK_DIR, filename)
    plt.savefig(out, dpi=150)
    plt.close()


def plot_load_balance(static_data: Dict, dynamic_data: Dict,
                      filename: str = "load_balance_comparison.png") -> None:
    """Distribusi task per Worker: static vs dynamic."""
    if not HAS_MPL:
        return
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    for ax, d, title in zip(axes,
                             [static_data, dynamic_data],
                             ["Static Distribution", "Dynamic Distribution"]):
        wids   = list(d.get("worker_tasks", {}).keys())
        counts = list(d.get("worker_tasks", {}).values())
        ax.bar(range(len(wids)), counts, color="coral")
        ax.set_xticks(range(len(wids)))
        ax.set_xticklabels(wids, rotation=45, ha="right", fontsize=8)
        ax.set_title(title, fontweight="bold")
        ax.set_ylabel("Jumlah Task")
        ax.grid(True, alpha=0.3, axis="y")
    plt.suptitle("Distribusi Task per Worker: Static vs Dynamic",
                 fontsize=13, fontweight="bold")
    plt.tight_layout()
    out = os.path.join(GRAFIK_DIR, filename)
    plt.savefig(out, dpi=150)
    plt.close()


def plot_cpu_timeline(ts_data: List[Dict], worker_id: str,
                      filename: str) -> None:
    """Timeline CPU/memori per Worker."""
    if not HAS_MPL or not ts_data:
        return
    t0 = ts_data[0]["ts"]
    xs  = [s["ts"] - t0 for s in ts_data]
    cpu = [s["cpu_pct"] for s in ts_data]
    mem = [s["mem_pct"] for s in ts_data]

    fig, ax1 = plt.subplots(figsize=(10, 4))
    ax2 = ax1.twinx()
    ax1.plot(xs, cpu, color="steelblue", label="CPU %")
    ax2.plot(xs, mem, color="coral",     label="Memori %", linestyle="--")
    ax1.set_xlabel("Waktu (detik)", fontsize=11)
    ax1.set_ylabel("CPU (%)", color="steelblue")
    ax2.set_ylabel("Memori (%)", color="coral")
    ax1.set_title(f"Timeline CPU & Memori – {worker_id}", fontweight="bold")
    fig.legend(loc="upper right", bbox_to_anchor=(1, 1), bbox_transform=ax1.transAxes)
    plt.tight_layout()
    out = os.path.join(GRAFIK_DIR, filename)
    plt.savefig(out, dpi=150)
    plt.close()


def plot_fault_tolerance(events: List[Dict], filename: str = "fault_tolerance.png") -> None:
    """Timeline fault tolerance: saat Worker mati dan recovery."""
    if not HAS_MPL or not events:
        return
    fig, ax = plt.subplots(figsize=(10, 4))
    for ev in events:
        x_fail = ev.get("failure_time", 0)
        x_rec  = ev.get("recovery_time_abs", x_fail + ev.get("recovery_time", 0))
        ax.axvline(x_fail, color="red",   linestyle="--", label="Worker Mati" if ev == events[0] else "")
        ax.axvline(x_rec,  color="green", linestyle="--", label="Task Recovery" if ev == events[0] else "")
        ax.annotate(f"RT={ev.get('recovery_time',0):.1f}s",
                    xy=(x_rec, 0.5), fontsize=8, color="green")
    ax.set_title("Timeline Fault Tolerance – Deteksi & Recovery", fontweight="bold")
    ax.set_xlabel("Waktu (detik)", fontsize=11)
    ax.set_yticks([])
    ax.legend()
    plt.tight_layout()
    out = os.path.join(GRAFIK_DIR, filename)
    plt.savefig(out, dpi=150)
    plt.close()


# ---------------------------------------------------------------------------
# Analisis utama dari file JSON hasil eksperimen
# ---------------------------------------------------------------------------

def analyze(experiments_path: str) -> None:
    """Muat hasil eksperimen, hitung metrik, buat grafik dan tabel CSV."""
    ensure_dirs()
    if not os.path.isfile(experiments_path):
        logger.error(f"File tidak ditemukan: {experiments_path}")
        return

    with open(experiments_path) as f:
        data = json.load(f)

    t1 = data.get("a", {}).get("T1") or data.get("T1", 1.0)
    logger.info(f"T₁ = {t1:.4f}s")

    # Skenario a: sequential
    if "a" in data:
        rows = data["a"]["rows"]
        logger.info(f"Skenario a: mean={data['a']['mean']:.4f}s std={data['a']['std']:.4f}s")

    # Skenario b: local parallel
    if "b" in data:
        rows_b = data["b"]["rows"]
        # Agregasi per num_procs
        procs_set = sorted(set(r["num_procs"] for r in rows_b))
        agg_b = []
        for np in procs_set:
            sub = [r for r in rows_b if r["num_procs"] == np]
            times = [r["execution_time"] for r in sub]
            thr   = [r["throughput_mbs"] for r in sub]
            mean_t = statistics.mean(times)
            sp  = round(t1 / max(mean_t, 1e-6), 4)
            ef  = round(sp / np, 4)
            agg_b.append({
                "num_procs":        np,
                "mean_time":        round(mean_t, 4),
                "std_time":         round(statistics.pstdev(times), 4),
                "speedup":          sp,
                "efficiency":       ef,
                "mean_throughput":  round(statistics.mean(thr), 3),
            })

        plot_execution_time(agg_b, t1,
                            "Waktu Eksekusi vs Jumlah Proses (Local Parallel)",
                            "Jumlah Proses", "num_procs",
                            "exec_time_local_parallel.png")
        plot_speedup(agg_b, "Jumlah Proses", "num_procs", "speedup_local_parallel.png")
        plot_efficiency(agg_b, "Jumlah Proses", "num_procs", "efficiency_local_parallel.png")
        plot_throughput(agg_b, "Jumlah Proses", "num_procs", "throughput_local_parallel.png")

        save_csv(agg_b, os.path.join(TABEL_DIR, "tabel_local_parallel.csv"))
        logger.info("Skenario b dianalisis")

    # Skenario c: distributed (dari file terpisah jika ada)
    _try_analyze_distributed(data, t1)

    logger.info("Analisis selesai. Semua grafik dan tabel tersimpan di results/")


def _try_analyze_distributed(data: dict, t1: float) -> None:
    """Analisis skenario c,d,e,f jika datanya tersedia."""
    # Skenario f: scalability
    if "f" in data:
        rows_f = data["f"]  # list of {num_workers, mean_time, std_time, ...}
        agg_f = []
        workers_set = sorted(set(r["num_workers"] for r in rows_f))
        for nw in workers_set:
            sub = [r for r in rows_f if r["num_workers"] == nw]
            times = [r["execution_time"] for r in sub]
            thr   = [r["throughput_mbs"] for r in sub]
            mt = statistics.mean(times)
            sp = round(t1 / max(mt, 1e-6), 4)
            ef = round(sp / nw, 4)
            agg_f.append({
                "num_workers":     nw,
                "mean_time":       round(mt, 4),
                "std_time":        round(statistics.pstdev(times) if len(times) > 1 else 0, 4),
                "speedup":         sp,
                "efficiency":      ef,
                "mean_throughput": round(statistics.mean(thr), 3),
            })

        plot_execution_time(agg_f, t1,
                            "Waktu Eksekusi vs Jumlah Worker (Scalability)",
                            "Jumlah Worker", "num_workers",
                            "exec_time_scalability.png")
        plot_speedup(agg_f, "Jumlah Worker", "num_workers", "speedup_scalability.png")
        plot_efficiency(agg_f, "Jumlah Worker", "num_workers", "efficiency_scalability.png")
        plot_throughput(agg_f, "Jumlah Worker", "num_workers", "throughput_scalability.png")
        save_csv(agg_f, os.path.join(TABEL_DIR, "tabel_scalability.csv"))

    # Skenario d: load balancing
    if "d" in data:
        lb = data["d"]
        if "static" in lb and "dynamic" in lb:
            plot_load_balance(lb["static"], lb["dynamic"])


def main() -> None:
    p = argparse.ArgumentParser(description="Analisis hasil eksperimen + buat grafik")
    p.add_argument("--input",   default="results/experiments_local.json")
    p.add_argument("--log-dir", default="results/logs")
    args = p.parse_args()

    setup_logging("analyze")
    analyze(args.input)

    # Plot timeline CPU/memori dari file log Worker jika ada
    if os.path.isdir(args.log_dir):
        for fname in os.listdir(args.log_dir):
            if fname.startswith("monitor-") and fname.endswith(".json"):
                wid = fname[len("monitor-"):-len(".json")]
                fpath = os.path.join(args.log_dir, fname)
                try:
                    with open(fpath) as f:
                        ts_data = json.load(f)
                    plot_cpu_timeline(ts_data, wid, f"cpu_timeline_{wid}.png")
                except Exception as e:
                    logger.warning(f"Gagal plot {fpath}: {e}")


if __name__ == "__main__":
    main()
