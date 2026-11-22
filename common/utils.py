"""
common/utils.py
===============
Utilitas bersama: logging berwarna, hashing SHA-256, timer, dan utilitas file.
"""

from __future__ import annotations
import hashlib
import logging
import os
import sys
import time
import json
import tempfile
from typing import Optional


# ---------------------------------------------------------------------------
# Logging berwarna + terstruktur (Komponen c, e)
# ---------------------------------------------------------------------------

RESET  = "\033[0m"
COLORS = {
    "DEBUG":    "\033[36m",   # cyan
    "INFO":     "\033[32m",   # hijau
    "WARNING":  "\033[33m",   # kuning
    "ERROR":    "\033[31m",   # merah
    "CRITICAL": "\033[35m",   # magenta
}


class ColoredFormatter(logging.Formatter):
    """Formatter logging dengan warna ANSI dan timestamp."""

    def format(self, record: logging.LogRecord) -> str:
        color = COLORS.get(record.levelname, "")
        fmt = (
            f"{color}[{record.levelname[0]}]{RESET} "
            f"%(asctime)s "
            f"[%(name)s] %(message)s"
        )
        formatter = logging.Formatter(fmt, datefmt="%H:%M:%S")
        return formatter.format(record)


class JSONLineHandler(logging.Handler):
    """
    Handler logging yang menulis JSON lines ke file.
    Dipakai untuk log terstruktur yang bisa diparse oleh analisis.
    """

    def __init__(self, filepath: str) -> None:
        super().__init__()
        self.filepath = filepath
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        self._file = open(filepath, "a", encoding="utf-8")

    def emit(self, record: logging.LogRecord) -> None:
        entry = {
            "timestamp": record.created,
            "level":     record.levelname,
            "logger":    record.name,
            "message":   record.getMessage(),
        }
        # Tambahkan field extra jika ada
        for key in ("worker_id", "task_id", "event", "duration", "recovery_time"):
            if hasattr(record, key):
                entry[key] = getattr(record, key)
        self._file.write(json.dumps(entry) + "\n")
        self._file.flush()

    def close(self) -> None:
        self._file.close()
        super().close()


def setup_logging(name: str, level: str = "INFO",
                  log_file: Optional[str] = None) -> logging.Logger:
    """
    Konfigurasi logger dengan output ke konsol (berwarna) dan
    opsional ke file JSON lines.

    Args:
        name:     nama logger (biasanya __name__ atau "master"/"worker").
        level:    string level logging.
        log_file: path file JSON lines (opsional).

    Returns:
        logger yang sudah dikonfigurasi.
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    if not logger.handlers:
        # Handler konsol dengan warna
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(ColoredFormatter())
        logger.addHandler(ch)

        # Handler file JSON (opsional)
        if log_file:
            fh = JSONLineHandler(log_file)
            logger.addHandler(fh)

    return logger


# ---------------------------------------------------------------------------
# Hashing SHA-256 (Komponen a, e, verifikasi)
# ---------------------------------------------------------------------------

def sha256_file(filepath: str, chunk_size: int = 65536) -> str:
    """Hitung SHA-256 dari seluruh file, efisien untuk file besar."""
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        while True:
            block = f.read(chunk_size)
            if not block:
                break
            h.update(block)
    return h.hexdigest()


def sha256_bytes(data: bytes) -> str:
    """Hitung SHA-256 dari bytes."""
    return hashlib.sha256(data).hexdigest()


def sha256_file_range(filepath: str, offset: int, length: int) -> str:
    """Hitung SHA-256 dari sebagian file (offset..offset+length)."""
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        f.seek(offset)
        remaining = length
        while remaining > 0:
            block = f.read(min(65536, remaining))
            if not block:
                break
            h.update(block)
            remaining -= len(block)
    return h.hexdigest()


# ---------------------------------------------------------------------------
# Timer konteks
# ---------------------------------------------------------------------------

class Timer:
    """Konteks timer sederhana."""

    def __init__(self) -> None:
        self.elapsed: float = 0.0
        self._start: Optional[float] = None

    def __enter__(self) -> "Timer":
        self._start = time.perf_counter()
        return self

    def __exit__(self, *_) -> None:
        self.elapsed = time.perf_counter() - (self._start or 0.0)

    @property
    def seconds(self) -> float:
        return self.elapsed


# ---------------------------------------------------------------------------
# Penulisan file atomik (Komponen c)
# ---------------------------------------------------------------------------

def atomic_write(filepath: str, data: bytes) -> None:
    """
    Tulis data ke file secara atomik: tulis ke tmp, lalu rename.
    Mencegah file rusak jika proses mati di tengah penulisan.
    """
    dirpath = os.path.dirname(os.path.abspath(filepath))
    os.makedirs(dirpath, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(dir=dirpath, prefix=".tmp_")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
        os.replace(tmp_path, filepath)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def read_file_range(filepath: str, offset: int, length: int) -> bytes:
    """Baca sebagian file dari offset dengan panjang length."""
    with open(filepath, "rb") as f:
        f.seek(offset)
        return f.read(length)


def ensure_dir(path: str) -> str:
    """Buat direktori jika belum ada. Kembalikan path."""
    os.makedirs(path, exist_ok=True)
    return path


def format_bytes(n: int) -> str:
    """Format bytes menjadi string yang mudah dibaca."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def format_duration(seconds: float) -> str:
    """Format durasi menjadi string yang mudah dibaca."""
    if seconds < 60:
        return f"{seconds:.2f}s"
    elif seconds < 3600:
        m = int(seconds // 60)
        s = seconds % 60
        return f"{m}m {s:.1f}s"
    else:
        h = int(seconds // 3600)
        m = int((seconds % 3600) // 60)
        return f"{h}h {m}m"
