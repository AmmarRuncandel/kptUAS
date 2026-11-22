"""
tests/test_core.py
==================
Unit test untuk komponen inti:
  - Chunking (Task Decomposition)
  - Container format (pack/unpack)
  - Protokol framing
  - Requeue logic
  - Scheduler

Jalankan: python -m pytest tests/ -v
"""

import os
import sys
import struct
import tempfile
import threading
import socket
import json
import time

try:
    import pytest
    HAS_PYTEST = True
except ImportError:
    HAS_PYTEST = False
    class DummyPytest:
        def fixture(self, fn=None):
            return fn if fn else lambda f: f
        class mark:
            @staticmethod
            def parametrize(*args, **kwargs):
                return lambda f: f
    pytest = DummyPytest()

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from common.tasks import decompose_directory, Task, TaskStatus
from common.compression import (
    compress_data, decompress_data,
    pack_chunk_result, unpack_chunk_result,
    HEADER_SIZE, MAGIC,
)
from common.utils import sha256_bytes
from common.protocol import send_message, recv_message, MsgType
from master.scheduler import Scheduler, WorkerInfo


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_dir(tmp_path):
    """Buat direktori berisi beberapa file contoh."""
    # File kecil
    for i in range(5):
        (tmp_path / f"small_{i}.txt").write_bytes(b"Hello World " * 1000)
    # File lebih besar (2 MB)
    (tmp_path / "medium.txt").write_bytes(b"A" * 2 * 1024 * 1024)
    return str(tmp_path)


@pytest.fixture
def simple_tasks():
    """Buat beberapa Task dummy."""
    tasks = []
    for i in range(10):
        t = Task(
            task_id=f"task-{i:04d}",
            file_path=f"/tmp/file_{i}.txt",
            chunk_index=0, chunk_count=1,
            offset=0, length=(i + 1) * 1024,
            algorithm="lzma", level=6,
        )
        tasks.append(t)
    return tasks


# ---------------------------------------------------------------------------
# Test: Task Decomposition (Komponen a)
# ---------------------------------------------------------------------------

class TestTaskDecomposition:

    def test_small_files_one_task_each(self, sample_dir):
        """File kecil (< chunk_size) → tepat 1 task per file."""
        chunk_size = 32 * 1024 * 1024  # 32 MB
        tasks = decompose_directory(sample_dir, chunk_size, "lzma", 6)
        # Semua file < 32 MB → tiap file = 1 task
        file_counts = {}
        for t in tasks:
            file_counts[t.file_path] = file_counts.get(t.file_path, 0) + 1
        for fpath, count in file_counts.items():
            fsize = os.path.getsize(fpath)
            if fsize < chunk_size:
                assert count == 1, f"{fpath} seharusnya 1 task, dapat {count}"

    def test_large_file_multiple_chunks(self, tmp_path):
        """File besar → beberapa chunk, chunk_index berurutan dari 0."""
        large_file = tmp_path / "large.bin"
        large_file.write_bytes(b"X" * (5 * 1024 * 1024))  # 5 MB
        chunk_size = 2 * 1024 * 1024  # 2 MB

        tasks = decompose_directory(str(tmp_path), chunk_size, "bz2", 9)
        # 5 MB / 2 MB = 3 chunk (ceiling)
        assert len(tasks) == 3
        indices = sorted(t.chunk_index for t in tasks)
        assert indices == [0, 1, 2]

    def test_chunk_coverage_no_gap(self, tmp_path):
        """Pastikan semua byte file tercakup oleh chunk tanpa gap."""
        fsize = 7 * 1024 * 1024
        large_file = tmp_path / "gap_test.bin"
        large_file.write_bytes(b"G" * fsize)
        chunk_size = 3 * 1024 * 1024

        tasks = decompose_directory(str(tmp_path), chunk_size, "gzip", 6)
        tasks_sorted = sorted(tasks, key=lambda t: t.offset)
        for i, t in enumerate(tasks_sorted):
            if i == 0:
                assert t.offset == 0
            else:
                assert t.offset == tasks_sorted[i-1].offset + tasks_sorted[i-1].length
        total_len = sum(t.length for t in tasks)
        assert total_len == fsize

    def test_task_fields(self, sample_dir):
        """Semua Task memiliki field wajib yang terisi."""
        tasks = decompose_directory(sample_dir, 32*1024*1024, "lzma", 6)
        for t in tasks:
            assert t.task_id
            assert t.file_path
            assert t.chunk_count >= 1
            assert t.length > 0
            assert t.algorithm == "lzma"
            assert t.status == TaskStatus.PENDING

    def test_to_dict_from_dict_roundtrip(self):
        """Task.to_dict() dan Task.from_dict() harus idempoten."""
        t = Task(
            task_id="abc-123",
            file_path="/tmp/test.txt",
            chunk_index=1, chunk_count=3,
            offset=1024, length=2048,
            algorithm="bz2", level=9,
        )
        d = t.to_dict()
        t2 = Task.from_dict(d)
        assert t2.task_id == t.task_id
        assert t2.offset  == t.offset
        assert t2.length  == t.length
        assert t2.algorithm == t.algorithm


# ---------------------------------------------------------------------------
# Test: Container Format (Komponen a + b)
# ---------------------------------------------------------------------------

class TestContainerFormat:

    def test_pack_unpack_roundtrip(self):
        """pack_chunk_result + unpack_chunk_result harus menghasilkan data asli."""
        original = b"Test data " * 1000
        algo, level = "lzma", 6
        from common.compression import compress_data
        compressed = compress_data(original, algo, level)
        packed = pack_chunk_result(original, compressed, algo, level, 0, 1)

        info = unpack_chunk_result(packed)
        assert info["algorithm"]     == algo
        assert info["level"]         == level
        assert info["original_size"] == len(original)
        assert info["chunk_index"]   == 0
        assert info["chunk_count"]   == 1
        assert info["sha256_original"] == sha256_bytes(original)

    def test_magic_bytes_check(self):
        """Data dengan magic bytes salah harus raise ValueError."""
        bad_data = b"XXXX" + b"\x00" * 92  # magic salah
        with pytest.raises(ValueError, match="Magic"):
            unpack_chunk_result(bad_data)

    def test_header_size_constant(self):
        """HEADER_SIZE harus 96 byte."""
        assert HEADER_SIZE == 96

    @pytest.mark.parametrize("algo,level", [
        ("lzma", 6), ("bz2", 9), ("gzip", 6), ("zlib", 6)
    ])
    def test_all_algorithms(self, algo, level):
        """Semua algoritma harus bisa di-pack dan di-unpack."""
        data = b"Sample " * 500
        compressed = compress_data(data, algo, level)
        packed = pack_chunk_result(data, compressed, algo, level, 2, 5)
        info = unpack_chunk_result(packed)
        assert info["algorithm"] == algo
        assert info["chunk_index"] == 2


# ---------------------------------------------------------------------------
# Test: Kompresi + Dekompresi (Komponen b)
# ---------------------------------------------------------------------------

class TestCompression:

    @pytest.mark.parametrize("algo,level", [
        ("lzma", 6), ("bz2", 9), ("gzip", 6), ("zlib", 6)
    ])
    def test_compress_decompress_roundtrip(self, algo, level):
        """Dekompresi data terkompresi harus menghasilkan data asli."""
        original = b"Hello World " * 1000
        compressed = compress_data(original, algo, level)
        restored   = decompress_data(compressed, algo)
        assert restored == original

    def test_lzma_is_cpu_bound(self):
        """lzma seharusnya menghasilkan ukuran lebih kecil dari gzip untuk teks berulang."""
        data = b"A" * 100000
        lzma_size = len(compress_data(data, "lzma", 6))
        gzip_size = len(compress_data(data, "gzip", 6))
        # Keduanya sangat efisien untuk data berulang; lzma biasanya lebih kecil
        assert lzma_size <= gzip_size + 100  # toleransi kecil


# ---------------------------------------------------------------------------
# Test: Protokol Framing (Komponen c)
# ---------------------------------------------------------------------------

class TestProtocolFraming:

    def test_send_recv_json_only(self):
        """send_message + recv_message tanpa binary harus berjalan benar."""
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind(("127.0.0.1", 0))
        server_sock.listen(1)
        port = server_sock.getsockname()[1]

        def server_fn():
            conn, _ = server_sock.accept()
            msg, binary = recv_message(conn)
            conn.close()
            server_sock.close()
            assert msg["type"] == MsgType.HEARTBEAT
            assert msg["cpu_pct"] == 55.0
            assert binary is None

        t = threading.Thread(target=server_fn, daemon=True)
        t.start()
        time.sleep(0.1)

        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect(("127.0.0.1", port))
        send_message(client, MsgType.HEARTBEAT, {
            "worker_id": "test", "cpu_pct": 55.0, "mem_pct": 30.0, "active_tasks": 2
        })
        client.close()
        t.join(timeout=3)

    def test_send_recv_with_binary(self):
        """send_message + recv_message dengan binary data harus benar."""
        binary_payload = b"\x00\xFF\xAB\xCD" * 1000

        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind(("127.0.0.1", 0))
        server_sock.listen(1)
        port = server_sock.getsockname()[1]

        received = {}

        def server_fn():
            conn, _ = server_sock.accept()
            msg, binary = recv_message(conn)
            received["msg"]    = msg
            received["binary"] = binary
            conn.close()
            server_sock.close()

        t = threading.Thread(target=server_fn, daemon=True)
        t.start()
        time.sleep(0.1)

        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect(("127.0.0.1", port))
        send_message(client, MsgType.TASK_RESULT,
                     {"result": {"task_id": "x"}},
                     binary_data=binary_payload)
        client.close()
        t.join(timeout=3)

        assert received["binary"] == binary_payload
        assert received["msg"]["type"] == MsgType.TASK_RESULT


# ---------------------------------------------------------------------------
# Test: Scheduler – Requeue Logic (Komponen e)
# ---------------------------------------------------------------------------

class TestSchedulerRequeue:

    def test_requeue_pending_tasks(self, simple_tasks):
        """Task dari Worker DEAD harus kembali ke antrean."""
        sched = Scheduler(simple_tasks, mode="dynamic", max_retries=3)
        winfo = WorkerInfo("w01", "host1", "1.1.1.1", 4, 8.0, 4)

        # Ambil 3 task untuk w01
        taken = []
        for _ in range(3):
            t = sched.get_task(winfo)
            if t:
                taken.append(t)

        assert len(taken) == 3
        assert len(sched._assigned) == 3

        # Simulasi Worker DEAD: requeue
        requeued = sched.requeue_worker_tasks("w01")
        assert requeued == 3
        assert len(sched._assigned) == 0
        # Task kembali ke antrean
        assert any(t.task_id == taken[0].task_id
                   for t in list(sched._queue))

    def test_max_retries_exceeded(self, simple_tasks):
        """Task yang melebihi max_retries harus ditandai FAILED."""
        sched = Scheduler(simple_tasks[:2], mode="dynamic", max_retries=1)
        winfo = WorkerInfo("w01", "h", "1.1.1.1", 2, 4.0, 2)

        # Ambil semua task, tambah attempts secara manual
        t = sched.get_task(winfo)
        assert t is not None
        t.attempts = 1  # sudah mencapai max_retries

        requeued = sched.requeue_worker_tasks("w01")
        # Task dengan attempts >= max_retries → FAILED, tidak di-requeue
        assert requeued == 0
        assert len(sched._failed_tasks) == 1

    def test_static_distribution(self, simple_tasks):
        """Mode static: task dibagi merata antar Worker."""
        sched = Scheduler(simple_tasks, mode="static", max_retries=3)
        worker_ids = ["w01", "w02"]
        w01 = WorkerInfo("w01", "h1", "1.1.1.1", 4, 8.0, 4)
        w02 = WorkerInfo("w02", "h2", "1.1.1.2", 4, 8.0, 4)
        sched.register_worker(w01)
        sched.register_worker(w02)
        sched.distribute_static(worker_ids)

        q01 = sched._static_assignments.get("w01", [])
        q02 = sched._static_assignments.get("w02", [])
        total = len(q01) + len(q02)
        assert total == len(simple_tasks)
        # Perbedaan paling besar 1 task
        assert abs(len(q01) - len(q02)) <= 1

    def test_imbalance_metrics(self, simple_tasks):
        """Metrik imbalance harus valid (CV ≥ 0, ratio ≥ 1)."""
        sched = Scheduler(simple_tasks, mode="dynamic", max_retries=3)
        # Simulasi distribusi tidak merata
        sched._worker_task_counts["w01"] = 8
        sched._worker_task_counts["w02"] = 2

        metrics = sched.get_imbalance_metrics()
        assert metrics["cv"] >= 0
        assert metrics["max_min_ratio"] >= 1.0
        assert metrics["max_min_ratio"] == 4.0


# ---------------------------------------------------------------------------
# Test: Scheduler – LPT order (Komponen d)
# ---------------------------------------------------------------------------

class TestSchedulerLPT:

    def test_dynamic_lpt_order(self):
        """Dynamic mode harus memberikan task terbesar terlebih dahulu."""
        tasks = [
            Task("t1", "/f", 0, 1, 0, 100, "lzma", 6),
            Task("t2", "/f", 0, 1, 0, 1000, "lzma", 6),
            Task("t3", "/f", 0, 1, 0, 50, "lzma", 6),
        ]
        sched = Scheduler(tasks, mode="dynamic", max_retries=3)
        winfo = WorkerInfo("w01", "h", "1.1.1.1", 4, 8.0, 4)

        t_first = sched.get_task(winfo)
        assert t_first is not None
        assert t_first.length == 1000  # task terbesar duluan


if __name__ == "__main__":
    if HAS_PYTEST:
        pytest.main([__file__, "-v"])
    else:
        print("pytest tidak terpasang; menjalankan test runner alternatif (tests/run_tests.py):")
        import subprocess
        subprocess.run([sys.executable, os.path.join(os.path.dirname(__file__), "run_tests.py")])
