"""
data/generate_synthetic.py
==========================
Pembangkit data uji sintetis sebagai fallback jika dataset resmi tidak tersedia.
Menghasilkan campuran file teks, log, CSV dengan ukuran bervariasi.

Data ini dinyatakan sebagai DATA SINTETIS (bukan dataset resmi).
Lihat DATASET_SOURCES.md untuk sumber resmi.
"""

from __future__ import annotations
import argparse
import os
import random
import string
import sys
import logging

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from common.utils import setup_logging, format_bytes

logger = logging.getLogger("generate_synthetic")

LOREM = (
    "Lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod "
    "tempor incididunt ut labore et dolore magna aliqua Ut enim ad minim veniam "
    "quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo "
    "consequat Duis aute irure dolor in reprehenderit in voluptate velit esse "
    "cillum dolore eu fugiat nulla pariatur Excepteur sint occaecat cupidatat "
    "non proident sunt in culpa qui officia deserunt mollit anim id est laborum "
)


def _random_text(size_bytes: int) -> bytes:
    """Teks lorem ipsum berukuran size_bytes."""
    result = (LOREM * (size_bytes // len(LOREM) + 1))[:size_bytes]
    return result.encode("utf-8")


def _random_log(size_bytes: int) -> bytes:
    """Log file sintetis."""
    levels  = ["INFO", "WARNING", "ERROR", "DEBUG"]
    modules = ["scheduler", "collector", "worker", "master", "protocol"]
    lines   = []
    total   = 0
    while total < size_bytes:
        ts = f"2024-01-01 {random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}"
        line = (f"{ts} [{random.choice(levels):7s}] "
                f"[{random.choice(modules):12s}] "
                f"Message #{random.randint(1000, 99999)}: "
                f"{''.join(random.choices(string.ascii_letters + ' ', k=40))}\n")
        lines.append(line)
        total += len(line)
    return "".join(lines).encode("utf-8")[:size_bytes]


def _random_csv(size_bytes: int) -> bytes:
    """CSV sintetis dengan 10 kolom numerik."""
    header = "id,ts,val1,val2,val3,val4,val5,val6,val7,val8\n"
    rows   = [header]
    total  = len(header)
    i      = 0
    while total < size_bytes:
        row = (f"{i},{random.randint(10**9, 2*10**9)},"
               + ",".join(str(round(random.gauss(50, 15), 4)) for _ in range(8))
               + "\n")
        rows.append(row)
        total += len(row)
        i     += 1
    return "".join(rows).encode("utf-8")[:size_bytes]


FILE_GENERATORS = {
    "txt": _random_text,
    "log": _random_log,
    "csv": _random_csv,
}


def generate(output_dir: str, num_small: int = 100, num_large: int = 5,
             small_max_kb: int = 512, large_min_mb: int = 10,
             large_max_mb: int = 50) -> None:
    """
    Hasilkan file uji campuran kecil dan besar.

    Args:
        output_dir:   direktori output.
        num_small:    jumlah file kecil.
        num_large:    jumlah file besar.
        small_max_kb: ukuran maksimal file kecil (KB).
        large_min_mb: ukuran minimum file besar (MB).
        large_max_mb: ukuran maksimum file besar (MB).
    """
    os.makedirs(output_dir, exist_ok=True)
    extensions = list(FILE_GENERATORS.keys())

    logger.info(f"Membuat {num_small} file kecil + {num_large} file besar di {output_dir}...")

    total_bytes = 0
    count       = 0

    # File kecil
    for i in range(num_small):
        ext  = random.choice(extensions)
        size = random.randint(1024, small_max_kb * 1024)
        data = FILE_GENERATORS[ext](size)
        fname = f"small_{i:04d}.{ext}"
        path  = os.path.join(output_dir, fname)
        with open(path, "wb") as f:
            f.write(data)
        total_bytes += len(data)
        count       += 1

    # File besar
    for i in range(num_large):
        ext  = random.choice(extensions)
        size = random.randint(large_min_mb * 1024**2, large_max_mb * 1024**2)
        data = FILE_GENERATORS[ext](size)
        fname = f"large_{i:02d}.{ext}"
        path  = os.path.join(output_dir, fname)
        with open(path, "wb") as f:
            f.write(data)
        total_bytes += len(data)
        count       += 1

    logger.info(
        f"Selesai: {count} file, total {format_bytes(total_bytes)} "
        f"[DATA SINTETIS]"
    )


def main() -> None:
    p = argparse.ArgumentParser(description="Buat data uji sintetis")
    p.add_argument("--output-dir",   default="data/sample")
    p.add_argument("--num-small",    type=int, default=100)
    p.add_argument("--num-large",    type=int, default=5)
    p.add_argument("--small-max-kb", type=int, default=512)
    p.add_argument("--large-min-mb", type=int, default=10)
    p.add_argument("--large-max-mb", type=int, default=50)
    args = p.parse_args()

    setup_logging("generate_synthetic")
    logger.warning(
        "Data yang dibuat adalah DATA SINTETIS. "
        "Untuk pengujian formal, gunakan fetch_dataset.py untuk mendapatkan "
        "dataset dari sumber resmi (lihat DATASET_SOURCES.md)."
    )
    generate(
        args.output_dir, args.num_small, args.num_large,
        args.small_max_kb, args.large_min_mb, args.large_max_mb
    )


if __name__ == "__main__":
    main()
