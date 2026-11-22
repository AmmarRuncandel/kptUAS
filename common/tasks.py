"""
common/tasks.py
===============
Komponen a – Task Decomposition: Definisi Task, TaskResult, status, dan logika chunking.

Aturan pemecahan:
  - File kecil (< chunk_size, default 32 MB) → 1 task = 1 file (chunk_index=0, chunk_count=1).
  - File besar → dipecah jadi beberapa chunk berukuran chunk_size; tiap chunk = 1 task independen.
"""

from __future__ import annotations
import os
import uuid
import hashlib
import time
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Optional, List


# ---------------------------------------------------------------------------
# Status Task
# ---------------------------------------------------------------------------

class TaskStatus(str, Enum):
    PENDING  = "PENDING"    # belum dikirim ke Worker
    ASSIGNED = "ASSIGNED"   # sedang diproses Worker
    DONE     = "DONE"       # selesai berhasil
    FAILED   = "FAILED"     # gagal melebihi max_retries


# ---------------------------------------------------------------------------
# Dataclass Task (Komponen a)
# ---------------------------------------------------------------------------

@dataclass
class Task:
    """
    Satu unit pekerjaan kompresi.
    Berisi metadata lengkap untuk memungkinkan chunk besar maupun file kecil.
    """
    task_id:         str         # UUID unik per task
    file_path:       str         # path absolut file sumber
    chunk_index:     int         # indeks chunk (0-based)
    chunk_count:     int         # total chunk dari file ini
    offset:          int         # byte offset dalam file
    length:          int         # panjang data (bytes)
    algorithm:       str         # "lzma" | "bz2" | "gzip" | "zlib" | "zstd"
    level:           int         # level kompresi
    status:          str         = TaskStatus.PENDING
    attempts:        int         = 0
    assigned_worker: Optional[str] = None
    created_at:      float       = field(default_factory=time.time)
    assigned_at:     Optional[float] = None
    completed_at:    Optional[float] = None
    output_path:     Optional[str]   = None   # path file hasil di Master

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "Task":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})

    def is_retryable(self, max_retries: int) -> bool:
        return self.attempts < max_retries


# ---------------------------------------------------------------------------
# Dataclass TaskResult (Komponen a)
# ---------------------------------------------------------------------------

@dataclass
class TaskResult:
    """
    Hasil eksekusi satu task kompresi dari Worker.
    """
    task_id:          str
    original_size:    int
    compressed_size:  int
    ratio:            float          # original / compressed (> 1 berarti lebih kecil)
    duration:         float          # detik
    worker_id:        str
    sha256_original:  str            # hash SHA-256 data asli (untuk verifikasi)
    sha256_compressed: str           # hash SHA-256 data terkompresi
    status:           str            # "DONE" atau "FAILED"
    error:            Optional[str]  = None
    algorithm:        Optional[str]  = None
    level:            Optional[int]  = None

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "TaskResult":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


# ---------------------------------------------------------------------------
# Task Decomposition (Komponen a)
# ---------------------------------------------------------------------------

def decompose_directory(input_dir: str, chunk_size: int, algorithm: str,
                        level: int) -> List[Task]:
    """
    Pecah semua file dalam direktori menjadi daftar Task.

    Strategi:
    - File < chunk_size → 1 task (1 chunk).
    - File >= chunk_size → beberapa task; tiap task = 1 chunk.
    - Urutan task dicampur besar/kecil untuk memperjelas manfaat load balancing.

    Args:
        input_dir:  direktori berisi file yang akan dikompresi.
        chunk_size: ukuran maksimum tiap chunk (bytes).
        algorithm:  algoritma kompresi.
        level:      level kompresi.

    Returns:
        List[Task] yang sudah diacak urutan besar/kecil.
    """
    tasks: List[Task] = []

    if not os.path.isdir(input_dir):
        raise ValueError(f"Direktori tidak ditemukan: {input_dir}")

    for root, _, files in os.walk(input_dir):
        for fname in sorted(files):
            fpath = os.path.join(root, fname)
            try:
                fsize = os.path.getsize(fpath)
            except OSError:
                continue
            if fsize == 0:
                continue

            if fsize < chunk_size:
                # File kecil → 1 task
                tasks.append(Task(
                    task_id=str(uuid.uuid4()),
                    file_path=fpath,
                    chunk_index=0,
                    chunk_count=1,
                    offset=0,
                    length=fsize,
                    algorithm=algorithm,
                    level=level,
                ))
            else:
                # File besar → beberapa chunk
                chunk_count = (fsize + chunk_size - 1) // chunk_size
                for ci in range(chunk_count):
                    offset = ci * chunk_size
                    length = min(chunk_size, fsize - offset)
                    tasks.append(Task(
                        task_id=str(uuid.uuid4()),
                        file_path=fpath,
                        chunk_index=ci,
                        chunk_count=chunk_count,
                        offset=offset,
                        length=length,
                        algorithm=algorithm,
                        level=level,
                    ))

    # Campur urutan: file besar dan kecil bergantian (penting untuk load balancing)
    large = [t for t in tasks if t.length >= chunk_size // 2]
    small = [t for t in tasks if t.length < chunk_size // 2]
    mixed: List[Task] = []
    li, si = 0, 0
    while li < len(large) or si < len(small):
        if li < len(large):
            mixed.append(large[li]); li += 1
        if si < len(small):
            mixed.append(small[si]); si += 1
    return mixed


def decompose_file_list(file_paths: List[str], chunk_size: int, algorithm: str,
                        level: int) -> List[Task]:
    """
    Pecah daftar file (bukan direktori) menjadi Task.
    Wrapper untuk decompose_directory pada file individual.
    """
    tasks: List[Task] = []
    for fpath in file_paths:
        if not os.path.isfile(fpath):
            continue
        fsize = os.path.getsize(fpath)
        if fsize == 0:
            continue
        if fsize < chunk_size:
            tasks.append(Task(
                task_id=str(uuid.uuid4()),
                file_path=fpath, chunk_index=0, chunk_count=1,
                offset=0, length=fsize, algorithm=algorithm, level=level,
            ))
        else:
            chunk_count = (fsize + chunk_size - 1) // chunk_size
            for ci in range(chunk_count):
                offset = ci * chunk_size
                length = min(chunk_size, fsize - offset)
                tasks.append(Task(
                    task_id=str(uuid.uuid4()),
                    file_path=fpath, chunk_index=ci, chunk_count=chunk_count,
                    offset=offset, length=length, algorithm=algorithm, level=level,
                ))
    return tasks
