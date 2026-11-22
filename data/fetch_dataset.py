"""
data/fetch_dataset.py
=====================
Unduh dataset dari sumber resmi untuk pengujian kompresi.

Dataset yang digunakan (semua open-source / public domain):
  1. Canterbury Corpus   – https://corpus.canterbury.ac.nz/
  2. Silesia Corpus      – https://sun.aei.polsl.pl/~sdeor/index.php?page=silesia
  3. Project Gutenberg   – https://www.gutenberg.org/

CATATAN: Jalankan script ini hanya jika koneksi internet tersedia.
Jika tidak ada koneksi, gunakan generate_synthetic.py sebagai fallback.

Lihat DATASET_SOURCES.md untuk detail lisensi dan sumber.
"""

from __future__ import annotations
import argparse
import hashlib
import logging
import os
import sys
import urllib.request
import urllib.error
import zipfile

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from common.utils import setup_logging, format_bytes

logger = logging.getLogger("fetch_dataset")

# ---------------------------------------------------------------------------
# Daftar dataset resmi (URL diverifikasi pada tanggal akses)
# ---------------------------------------------------------------------------

DATASETS = {
    "canterbury": {
        "name":    "Canterbury Corpus",
        "url":     "https://corpus.canterbury.ac.nz/resources/cantrbry.tar.gz",
        "license": "Public Domain",
        "note":    "Corpus klasik untuk benchmark kompresi",
    },
    "gutenberg_pg10": {
        "name":    "Project Gutenberg – The Bible (pg10)",
        "url":     "https://www.gutenberg.org/cache/epub/10/pg10.txt",
        "license": "Public Domain",
        "note":    "Teks besar untuk pengujian kompresi teks",
    },
    "gutenberg_pg2600": {
        "name":    "Project Gutenberg – War and Peace (pg2600)",
        "url":     "https://www.gutenberg.org/cache/epub/2600/pg2600.txt",
        "license": "Public Domain",
        "note":    "Novel besar – cocok untuk chunk besar",
    },
}


def download_file(url: str, dest_path: str, timeout: int = 30) -> bool:
    """
    Unduh file dari URL ke dest_path.
    Mengembalikan True jika berhasil.
    """
    os.makedirs(os.path.dirname(os.path.abspath(dest_path)), exist_ok=True)
    try:
        logger.info(f"Mengunduh: {url}")
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            total = int(resp.headers.get("Content-Length", 0))
            downloaded = 0
            with open(dest_path, "wb") as f:
                while True:
                    chunk = resp.read(65536)
                    if not chunk:
                        break
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total:
                        pct = 100 * downloaded / total
                        print(f"\r  {format_bytes(downloaded)} / {format_bytes(total)} "
                              f"({pct:.1f}%)", end="", flush=True)
        print()
        logger.info(f"Tersimpan ke {dest_path} ({format_bytes(downloaded)})")
        return True
    except urllib.error.URLError as e:
        logger.error(f"Gagal mengunduh {url}: {e}")
        return False


def fetch_all(output_dir: str, datasets: list = None) -> None:
    """Unduh semua dataset atau subset yang dipilih."""
    targets = datasets or list(DATASETS.keys())
    for key in targets:
        if key not in DATASETS:
            logger.warning(f"Dataset tidak dikenal: {key}")
            continue
        ds  = DATASETS[key]
        url = ds["url"]
        filename = os.path.basename(url.split("?")[0])
        dest = os.path.join(output_dir, filename)

        if os.path.isfile(dest):
            logger.info(f"Sudah ada: {dest} (lewati)")
            continue

        ok = download_file(url, dest)
        if ok and dest.endswith(".tar.gz"):
            import tarfile
            logger.info(f"Ekstrak {dest}...")
            with tarfile.open(dest, "r:gz") as tar:
                tar.extractall(path=output_dir)

    logger.info("Semua dataset selesai diunduh.")
    logger.info("Lihat data/DATASET_SOURCES.md untuk detail sumber dan lisensi.")


def main() -> None:
    p = argparse.ArgumentParser(description="Unduh dataset resmi untuk benchmark")
    p.add_argument("--output-dir",  default="data/sample")
    p.add_argument("--datasets",    nargs="*",
                   help="Pilih dataset: " + ", ".join(DATASETS.keys()))
    args = p.parse_args()

    setup_logging("fetch_dataset")
    logger.info("=== FETCH DATASET RESMI ===")
    fetch_all(args.output_dir, args.datasets)


if __name__ == "__main__":
    main()
