"""
tests/run_tests.py
==================
Runner unit test menggunakan Python standard library (unittest).
Dapat dijalankan tanpa dependensi tambahan di node mana pun:
    python tests/run_tests.py
"""

from __future__ import annotations
import os
import sys
import unittest
import tempfile
import threading
import socket
import time

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from common.tasks import decompose_directory, Task, TaskStatus
from common.compression import (
    compress_data, decompress_data,
    pack_chunk_result, unpack_chunk_result,
    HEADER_SIZE, MAGIC
)
from common.utils import sha256_bytes
from common.protocol import send_message, recv_message, MsgType
from master.scheduler import Scheduler, WorkerInfo


class TestCompressionAndContainer(unittest.TestCase):
    def test_compress_decompress_roundtrip(self):
        original = b"Antigravity Distributed Compression " * 500
        for algo, level in [("lzma", 6), ("bz2", 9), ("gzip", 6), ("zlib", 6)]:
            compressed = compress_data(original, algo, level)
            restored = decompress_data(compressed, algo)
            self.assertEqual(restored, original, f"Failed for {algo}")

    def test_pack_unpack_roundtrip(self):
        original = b"Chunk payload data " * 200
        algo, level = "lzma", 6
        compressed = compress_data(original, algo, level)
        packed = pack_chunk_result(original, compressed, algo, level, 1, 3)

        info = unpack_chunk_result(packed)
        self.assertEqual(info["algorithm"], algo)
        self.assertEqual(info["level"], level)
        self.assertEqual(info["original_size"], len(original))
        self.assertEqual(info["chunk_index"], 1)
        self.assertEqual(info["chunk_count"], 3)
        self.assertEqual(info["sha256_original"], sha256_bytes(original))
        self.assertEqual(len(info["compressed_data"]), info["compressed_size"])

    def test_magic_check(self):
        bad_data = b"FAIL" + b"\x00" * 92
        with self.assertRaises(ValueError):
            unpack_chunk_result(bad_data)


class TestTaskDecomposition(unittest.TestCase):
    def test_small_files_one_task(self):
        with tempfile.TemporaryDirectory() as td:
            for i in range(5):
                fpath = os.path.join(td, f"file_{i}.txt")
                with open(fpath, "wb") as f:
                    f.write(b"ABC" * 100)
            tasks = decompose_directory(td, 32 * 1024 * 1024, "lzma", 6)
            self.assertEqual(len(tasks), 5)
            for t in tasks:
                self.assertEqual(t.chunk_count, 1)
                self.assertEqual(t.chunk_index, 0)

    def test_large_file_chunking(self):
        with tempfile.TemporaryDirectory() as td:
            fpath = os.path.join(td, "large.bin")
            with open(fpath, "wb") as f:
                f.write(b"Z" * (5 * 1024 * 1024))  # 5 MB
            chunk_size = 2 * 1024 * 1024  # 2 MB chunks -> 3 chunks
            tasks = decompose_directory(td, chunk_size, "bz2", 9)
            self.assertEqual(len(tasks), 3)
            indices = sorted(t.chunk_index for t in tasks)
            self.assertEqual(indices, [0, 1, 2])
            total_len = sum(t.length for t in tasks)
            self.assertEqual(total_len, 5 * 1024 * 1024)


class TestProtocolFraming(unittest.TestCase):
    def test_tcp_send_recv(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("127.0.0.1", 0))
        server.listen(1)
        port = server.getsockname()[1]

        received = {}

        def srv():
            conn, _ = server.accept()
            msg, bin_data = recv_message(conn)
            received["msg"] = msg
            received["bin"] = bin_data
            conn.close()
            server.close()

        t = threading.Thread(target=srv, daemon=True)
        t.start()
        time.sleep(0.05)

        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect(("127.0.0.1", port))
        send_message(client, MsgType.TASK_ASSIGN, {"test": 123}, binary_data=b"HELLO-BYTES")
        client.close()
        t.join(timeout=2.0)

        self.assertEqual(received["msg"]["type"], MsgType.TASK_ASSIGN)
        self.assertEqual(received["msg"]["test"], 123)
        self.assertEqual(received["bin"], b"HELLO-BYTES")


class TestSchedulerAndFaultTolerance(unittest.TestCase):
    def test_scheduler_lpt_and_requeue(self):
        tasks = [
            Task("t1", "/f", 0, 1, 0, 500, "lzma", 6),
            Task("t2", "/f", 0, 1, 0, 5000, "lzma", 6),
            Task("t3", "/f", 0, 1, 0, 100, "lzma", 6),
        ]
        sched = Scheduler(tasks, mode="dynamic", max_retries=3)
        winfo = WorkerInfo("w01", "h", "1.1.1.1", 4, 8.0, 4)

        # LPT order: task 5000 first
        t_first = sched.get_task(winfo)
        self.assertIsNotNone(t_first)
        self.assertEqual(t_first.length, 5000)

        # Requeue
        requeued = sched.requeue_worker_tasks("w01")
        self.assertEqual(requeued, 1)
        self.assertEqual(len(sched._assigned), 0)

        # Max retries exceeded test
        t_first.attempts = 3
        t_first.assigned_worker = "w01"
        sched._assigned[t_first.task_id] = t_first
        requeued2 = sched.requeue_worker_tasks("w01")
        self.assertEqual(requeued2, 0)
        self.assertEqual(len(sched._failed_tasks), 1)

    def test_fault_monitor_recovery_time_recorded(self):
        from master.fault_monitor import FaultMonitor
        tasks = [
            Task("task-rec-1", "/file1", 0, 1, 0, 1024, "lzma", 6),
        ]
        sched = Scheduler(tasks, mode="dynamic", max_retries=3)
        winfo = WorkerInfo("w-crash", "host", "1.1.1.1", 2, 4.0, 2)
        worker_registry = {"w-crash": winfo}
        fm = FaultMonitor(worker_registry, sched, heartbeat_timeout=1.0, check_interval=0.1)

        # Worker mengambil task
        t = sched.get_task(winfo)
        self.assertIsNotNone(t)

        # Worker mati (TCP disconnect atau dead)
        fm.record_worker_failure("w-crash", detected_at=time.time() - 0.5)
        sched.requeue_worker_tasks("w-crash")

        # Task diambil worker baru dan selesai
        winfo2 = WorkerInfo("w-survive", "host2", "1.1.1.2", 2, 4.0, 2)
        t_recovered = sched.get_task(winfo2)
        self.assertEqual(t_recovered.task_id, "task-rec-1")

        # Catat recovery
        rt = fm.record_recovery("task-rec-1")
        self.assertIsNotNone(rt)
        self.assertGreater(rt, 0.4)
        stats = fm.get_recovery_stats()
        self.assertEqual(stats["count"], 1)
        self.assertGreater(stats["mean"], 0.4)

    def test_static_scheduler_auto_distribute(self):
        tasks = [
            Task(f"t-{i}", "/f", 0, 1, 0, 100, "lzma", 6)
            for i in range(4)
        ]
        sched = Scheduler(tasks, mode="static", max_retries=3)
        w01 = WorkerInfo("w01", "h1", "1.1.1.1", 2, 4.0, 2)
        w02 = WorkerInfo("w02", "h2", "1.1.1.2", 2, 4.0, 2)
        sched.register_worker(w01)
        sched.register_worker(w02)

        # Tanpa distribute_static manual, get_task harus tetap memberikan task
        t1 = sched.get_task(w01)
        self.assertIsNotNone(t1)
        t2 = sched.get_task(w02)
        self.assertIsNotNone(t2)

    def test_scheduler_total_retries(self):
        tasks = [
            Task("t1", "/f", 0, 1, 0, 100, "lzma", 6),
            Task("t2", "/f", 0, 1, 0, 200, "lzma", 6),
        ]
        sched = Scheduler(tasks, mode="dynamic", max_retries=3)
        w01 = WorkerInfo("w01", "h1", "1.1.1.1", 2, 4.0, 2)
        sched.get_task(w01)
        self.assertEqual(sched.total_retries, 0)

        # Requeue -> attempts bertambah pada saat get_task berikutnya
        sched.requeue_worker_tasks("w01")
        sched.get_task(w01)
        self.assertEqual(sched.total_retries, 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
