"""
benchmark/verify.py
===================
Verifikasi Kebenaran: Dekompresi semua hasil, bandingkan SHA-256 dengan file asli.
Harus 100% cocok di semua skenario, termasuk fault tolerance.
"""

from __future__ import annotations
import argparse
import hashlib
import logging
import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from common.compression import unpack_chunk_result, decompress_data
from common.utils import setup_logging, sha256_file_range, sha256_bytes

logger = logging.getLogger("verify")


def verify_output(output_dir: str, input_dir: str) -> dict:
    """
    Dekompresi semua file .compressed di output_dir, bandingkan SHA-256.

    Returns:
        dict: {"total": N, "passed": N, "failed": N, "errors": [...]}
    """
    total  = 0
    passed = 0
    failed = 0
    errors = []

    for fname in os.listdir(output_dir):
        if not fname.endswith(".compressed"):
            continue
        orig_name = fname[:-len(".compressed")]
        orig_path = os.path.join(input_dir, orig_name)
        comp_path = os.path.join(output_dir, fname)

        if not os.path.isfile(orig_path):
            logger.warning(f"File asli tidak ditemukan: {orig_path}")
            continue

        total += 1
        try:
            # Baca file terkompresi (mungkin berisi beberapa chunk)
            with open(comp_path, "rb") as f:
                raw_comp = f.read()

            from common.compression import HEADER_SIZE
            import struct as _struct

            # Parse semua chunk dari container format; simpan berdasarkan chunk_index
            chunks: dict = {}
            offset = 0
            while offset < len(raw_comp):
                if offset + HEADER_SIZE > len(raw_comp):
                    break
                info = unpack_chunk_result(raw_comp[offset:])
                comp_data = info["compressed_data"]
                comp_data_len = info["compressed_size"]
                orig_chunk = decompress_data(comp_data, info["algorithm"])

                # Verifikasi hash chunk
                h_chunk = sha256_bytes(orig_chunk)
                if h_chunk != info["sha256_original"]:
                    raise ValueError(
                        f"Hash chunk {info['chunk_index']} tidak cocok: "
                        f"expected={info['sha256_original'][:16]}… "
                        f"got={h_chunk[:16]}…"
                    )

                chunks[info["chunk_index"]] = orig_chunk
                # Pindah ke chunk berikutnya (header + compressed data sebenarnya)
                offset += HEADER_SIZE + comp_data_len

            # Rekonstruksi berurutan berdasarkan chunk_index
            reconstructed = bytearray()
            for ci in sorted(chunks.keys()):
                reconstructed.extend(chunks[ci])

            # Bandingkan dengan file asli
            orig_hash = _hash_bytes_of_file(orig_path)
            recon_hash = hashlib.sha256(bytes(reconstructed)).hexdigest()

            if orig_hash == recon_hash:
                passed += 1
                logger.info(f"✓ {orig_name}: SHA-256 COCOK")
            else:
                failed += 1
                msg = (f"✗ {orig_name}: SHA-256 TIDAK COCOK "
                       f"(expected={orig_hash[:16]}… got={recon_hash[:16]}…)")
                logger.error(msg)
                errors.append({"file": orig_name, "error": msg})

        except Exception as e:
            failed += 1
            msg = f"✗ {orig_name}: Error — {e}"
            logger.error(msg)
            errors.append({"file": orig_name, "error": str(e)})

    return {"total": total, "passed": passed, "failed": failed, "errors": errors}


def _hash_bytes_of_file(path: str) -> str:
    """SHA-256 seluruh file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            blk = f.read(65536)
            if not blk:
                break
            h.update(blk)
    return h.hexdigest()


def main() -> None:
    p = argparse.ArgumentParser(description="Verifikasi SHA-256 hasil kompresi")
    p.add_argument("--output-dir", required=True, help="Direktori output Master")
    p.add_argument("--input-dir",  required=True, help="Direktori file asli")
    args = p.parse_args()

    setup_logging("verify")
    logger.info("=== VERIFIKASI MULAI ===")
    result = verify_output(args.output_dir, args.input_dir)
    logger.info(
        f"Hasil: {result['passed']}/{result['total']} LULUS "
        f"| {result['failed']} GAGAL"
    )
    if result["failed"] > 0:
        logger.error("VERIFIKASI GAGAL — ada file yang tidak cocok!")
        for e in result["errors"]:
            logger.error(f"  {e['file']}: {e['error']}")
        sys.exit(1)
    else:
        logger.info("VERIFIKASI LULUS 100% ✓")
    print(f"\nTotal file  : {result['total']}")
    print(f"Lulus       : {result['passed']}")
    print(f"Gagal       : {result['failed']}")


if __name__ == "__main__":
    main()
