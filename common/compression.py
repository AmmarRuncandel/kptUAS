"""
common/compression.py
=====================
Komponen b – Parallel Processing: Fungsi kompres/dekompres CPU-bound.
Mendukung: lzma, bz2, gzip, zlib, zstd (opsional).

Container format untuk hasil chunk:
  [4B magic "DCMP"] [1B versi] [1B algo_code] [1B level] [1B reserved]
  [8B original_size] [8B compressed_size] [32B sha256_original]
  [32B sha256_compressed] [4B chunk_index] [4B chunk_count]
  [compressed_data...]

Format ini memungkinkan rekonstruksi sempurna dan verifikasi hash.
"""

from __future__ import annotations
import gzip
import bz2
import lzma
import zlib
import struct
import hashlib
import io
import time
from typing import Tuple, Optional

# ---------------------------------------------------------------------------
# Coba import zstandard (opsional)
# ---------------------------------------------------------------------------
try:
    import zstandard as zstd
    HAS_ZSTD = True
except ImportError:
    HAS_ZSTD = False


# ---------------------------------------------------------------------------
# Konstanta Container Format
# ---------------------------------------------------------------------------

MAGIC         = b"DCMP"          # 4 bytes
FORMAT_VERSION = 1               # 1 byte
HEADER_SIZE   = 4 + 1 + 1 + 1 + 1 + 8 + 8 + 32 + 32 + 4 + 4  # = 96 bytes

ALGO_CODES = {
    "lzma": 1,
    "bz2":  2,
    "gzip": 3,
    "zlib": 4,
    "zstd": 5,
}
CODE_TO_ALGO = {v: k for k, v in ALGO_CODES.items()}


# ---------------------------------------------------------------------------
# Fungsi Kompresi (Komponen b – CPU-bound)
# ---------------------------------------------------------------------------

def compress_data(data: bytes, algorithm: str, level: int) -> bytes:
    """
    Kompresi bytes dengan algoritma yang dipilih.
    Ini adalah operasi CPU-bound yang dijalankan di ProcessPoolExecutor.

    Args:
        data:      data mentah.
        algorithm: "lzma" | "bz2" | "gzip" | "zlib" | "zstd".
        level:     level kompresi (makna tergantung algoritma).

    Returns:
        bytes terkompresi.
    """
    if algorithm == "lzma":
        return lzma.compress(data, preset=level)
    elif algorithm == "bz2":
        return bz2.compress(data, compresslevel=level)
    elif algorithm == "gzip":
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb", compresslevel=level) as f:
            f.write(data)
        return buf.getvalue()
    elif algorithm == "zlib":
        return zlib.compress(data, level)
    elif algorithm == "zstd":
        if not HAS_ZSTD:
            # Fallback ke lzma jika zstd tidak tersedia
            return lzma.compress(data, preset=min(level, 9))
        cctx = zstd.ZstdCompressor(level=level)
        return cctx.compress(data)
    else:
        raise ValueError(f"Algoritma tidak dikenal: {algorithm}")


def decompress_data(data: bytes, algorithm: str) -> bytes:
    """
    Dekompresi bytes.

    Args:
        data:      data terkompresi.
        algorithm: algoritma yang dipakai saat kompresi.

    Returns:
        bytes asli.
    """
    if algorithm == "lzma":
        return lzma.decompress(data)
    elif algorithm == "bz2":
        return bz2.decompress(data)
    elif algorithm == "gzip":
        buf = io.BytesIO(data)
        with gzip.GzipFile(fileobj=buf, mode="rb") as f:
            return f.read()
    elif algorithm == "zlib":
        return zlib.decompress(data)
    elif algorithm == "zstd":
        if not HAS_ZSTD:
            return lzma.decompress(data)
        dctx = zstd.ZstdDecompressor()
        return dctx.decompress(data)
    else:
        raise ValueError(f"Algoritma tidak dikenal: {algorithm}")


# ---------------------------------------------------------------------------
# Container Format: pack / unpack (Komponen a + b)
# ---------------------------------------------------------------------------

def pack_chunk_result(original_data: bytes, compressed_data: bytes,
                      algorithm: str, level: int,
                      chunk_index: int, chunk_count: int) -> bytes:
    """
    Bungkus hasil kompresi dalam container format.

    Returns:
        bytes = header (96B) + compressed_data
    """
    algo_code = ALGO_CODES.get(algorithm, 0)
    sha256_orig = hashlib.sha256(original_data).digest()    # 32 bytes
    sha256_comp = hashlib.sha256(compressed_data).digest()  # 32 bytes

    header = struct.pack(
        ">4sBBBBQQ32s32sII",
        MAGIC,
        FORMAT_VERSION,
        algo_code,
        level & 0xFF,
        0,                      # reserved
        len(original_data),
        len(compressed_data),
        sha256_orig,
        sha256_comp,
        chunk_index,
        chunk_count,
    )
    assert len(header) == HEADER_SIZE, f"Header size error: {len(header)}"
    return header + compressed_data


def unpack_chunk_result(data: bytes) -> dict:
    """
    Baca container format dan kembalikan metadata + data terkompresi.

    Returns:
        dict dengan field: algo, level, original_size, compressed_size,
                           sha256_original, sha256_compressed,
                           chunk_index, chunk_count, compressed_data.
    """
    if len(data) < HEADER_SIZE:
        raise ValueError("Data terlalu pendek untuk container format")
    magic, ver, algo_code, level, _, orig_size, comp_size, sha256_orig, sha256_comp, ci, cc = \
        struct.unpack(">4sBBBBQQ32s32sII", data[:HEADER_SIZE])
    if magic != MAGIC:
        raise ValueError(f"Magic bytes salah: {magic!r}")
    # Ambil hanya compressed_size byte (bukan sisa seluruh data)
    comp_end = HEADER_SIZE + comp_size
    return {
        "version":          ver,
        "algorithm":        CODE_TO_ALGO.get(algo_code, "unknown"),
        "level":            level,
        "original_size":    orig_size,
        "compressed_size":  comp_size,
        "sha256_original":  sha256_orig.hex(),
        "sha256_compressed": sha256_comp.hex(),
        "chunk_index":      ci,
        "chunk_count":      cc,
        "compressed_data":  data[HEADER_SIZE:comp_end],
        "total_frame_size": comp_end,  # panjang frame keseluruhan untuk navigasi multi-chunk
    }


# ---------------------------------------------------------------------------
# Fungsi utama yang dijalankan di process pool (Komponen b)
# ---------------------------------------------------------------------------

def compress_chunk_task(raw_data: bytes, algorithm: str, level: int,
                        chunk_index: int, chunk_count: int) -> Tuple[bytes, dict]:
    """
    Entry point untuk ProcessPoolExecutor.
    Kompresi CPU-bound + pack container.

    Returns:
        (packed_bytes, stats_dict)
    """
    t0 = time.perf_counter()
    sha256_orig = hashlib.sha256(raw_data).hexdigest()
    compressed = compress_data(raw_data, algorithm, level)
    packed = pack_chunk_result(raw_data, compressed, algorithm, level,
                               chunk_index, chunk_count)
    duration = time.perf_counter() - t0

    stats = {
        "original_size":   len(raw_data),
        "compressed_size": len(compressed),
        "ratio":           len(raw_data) / max(len(compressed), 1),
        "duration":        duration,
        "sha256_original": sha256_orig,
        "sha256_compressed": hashlib.sha256(compressed).hexdigest(),
    }
    return packed, stats
