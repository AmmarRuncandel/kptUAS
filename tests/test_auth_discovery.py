"""
tests/test_auth_discovery.py
============================
Pengujian fitur autentikasi sandi (passcode) dan isolasi multi-Master di LAN.
"""

import os
import sys
import time
import socket
import threading
import unittest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from common.protocol import send_message, recv_message, MsgType
from common.discovery import MasterBeacon, discover_master
from master.master import MasterServer
from common.tasks import Task


class TestAuthAndMultiMaster(unittest.TestCase):

    def test_discovery_passcode_filtering(self):
        """Worker dengan passcode hanya mendeteksi beacon Master yang cocok."""
        beacon_a = MasterBeacon(tcp_port=9010, interval=0.1, passcode="ALPHA_123", discovery_port=9011)
        beacon_b = MasterBeacon(tcp_port=9020, interval=0.1, passcode="BETA_456", discovery_port=9012)

        beacon_a.start_beacon()
        beacon_b.start_beacon()
        time.sleep(0.2)

        try:
            # Cari Master dengan passcode BETA_456 pada port 9012
            found_b = discover_master(timeout=2.0, passcode="BETA_456", discovery_port=9012)
            self.assertIsNotNone(found_b, "Master B harus ditemukan")
            self.assertEqual(found_b[1], 9020, "Harus mengarah ke port Master B")

            # Cari Master dengan passcode ALPHA_123 pada port 9011
            found_a = discover_master(timeout=2.0, passcode="ALPHA_123", discovery_port=9011)
            self.assertIsNotNone(found_a, "Master A harus ditemukan")
            self.assertEqual(found_a[1], 9010, "Harus mengarah ke port Master A")

            # Cari Master dengan passcode salah
            found_none = discover_master(timeout=0.6, passcode="WRONG_PASS", discovery_port=9011)
            self.assertIsNone(found_none, "Passcode salah tidak boleh menemukan Master")

        finally:
            beacon_a.stop_beacon()
            beacon_b.stop_beacon()

    def test_tcp_registration_auth(self):
        """Uji autentikasi TCP: sandi benar diterima, sandi salah ditolak."""
        cfg = {
            "host": "127.0.0.1",
            "port": 9030,
            "mode": "dynamic",
            "transfer_mode": "socket",
            "output_dir": "results/test_auth",
            "max_retries": 3,
            "passcode": "SUPER_SECRET",
        }
        master = MasterServer(cfg)
        task = Task(
            task_id="t-auth-1", file_path="dummy",
            chunk_index=0, chunk_count=1, offset=0, length=100,
            algorithm="lzma", level=6
        )
        master.prepare([task], {task.task_id: task})

        server_thread = threading.Thread(target=master.serve, daemon=True)
        server_thread.start()
        time.sleep(0.3)

        try:
            # 1. Worker coba register dengan sandi SALAH
            sock_bad = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock_bad.connect(("127.0.0.1", 9030))
            send_message(sock_bad, MsgType.REGISTER, {
                "worker_id": "w-intruder",
                "passcode": "WRONG_SECRET",
            })
            resp, _ = recv_message(sock_bad)
            self.assertEqual(resp.get("type"), MsgType.ERROR)
            self.assertEqual(resp.get("code"), "AUTH_FAILED")
            sock_bad.close()

            # 2. Worker coba register dengan sandi BENAR
            sock_good = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock_good.connect(("127.0.0.1", 9030))
            send_message(sock_good, MsgType.REGISTER, {
                "worker_id": "w-authorized",
                "passcode": "SUPER_SECRET",
            })
            resp, _ = recv_message(sock_good)
            self.assertEqual(resp.get("type"), MsgType.REGISTER_ACK)
            self.assertEqual(resp.get("worker_id"), "w-authorized")
            sock_good.close()

        finally:
            master.stop()

    def test_simultaneous_two_masters_isolation(self):
        """Dua Master berjalan bersamaan; Worker hanya masuk ke Master yang sandinya sesuai."""
        cfg_m1 = {
            "host": "127.0.0.1",
            "port": 9041,
            "mode": "dynamic",
            "transfer_mode": "socket",
            "output_dir": "results/test_m1",
            "max_retries": 3,
            "passcode": "CLUSTER_ONE",
        }
        cfg_m2 = {
            "host": "127.0.0.1",
            "port": 9042,
            "mode": "dynamic",
            "transfer_mode": "socket",
            "output_dir": "results/test_m2",
            "max_retries": 3,
            "passcode": "CLUSTER_TWO",
        }

        m1 = MasterServer(cfg_m1)
        m2 = MasterServer(cfg_m2)

        t1 = Task(task_id="t1", file_path="dummy", chunk_index=0, chunk_count=1, offset=0, length=10, algorithm="lzma", level=6)
        t2 = Task(task_id="t2", file_path="dummy", chunk_index=0, chunk_count=1, offset=0, length=10, algorithm="lzma", level=6)

        m1.prepare([t1], {t1.task_id: t1})
        m2.prepare([t2], {t2.task_id: t2})

        threading.Thread(target=m1.serve, daemon=True).start()
        threading.Thread(target=m2.serve, daemon=True).start()
        time.sleep(0.3)

        try:
            # Worker 1 mencoba register ke Master 1 dengan CLUSTER_ONE -> Berhasil
            s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s1.connect(("127.0.0.1", 9041))
            send_message(s1, MsgType.REGISTER, {"worker_id": "w1", "passcode": "CLUSTER_ONE"})
            r1, _ = recv_message(s1)
            self.assertEqual(r1.get("type"), MsgType.REGISTER_ACK)
            self.assertIn("w1", m1.worker_registry)
            self.assertNotIn("w1", m2.worker_registry)

            # Worker 2 mencoba register ke Master 2 dengan CLUSTER_ONE (sandi salah untuk M2) -> Ditolak!
            s2_bad = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s2_bad.connect(("127.0.0.1", 9042))
            send_message(s2_bad, MsgType.REGISTER, {"worker_id": "w2", "passcode": "CLUSTER_ONE"})
            r2_bad, _ = recv_message(s2_bad)
            self.assertEqual(r2_bad.get("type"), MsgType.ERROR)
            self.assertNotIn("w2", m2.worker_registry)
            s2_bad.close()

            # Worker 2 register ke Master 2 dengan CLUSTER_TWO -> Berhasil
            s2_good = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s2_good.connect(("127.0.0.1", 9042))
            send_message(s2_good, MsgType.REGISTER, {"worker_id": "w2", "passcode": "CLUSTER_TWO"})
            r2_good, _ = recv_message(s2_good)
            self.assertEqual(r2_good.get("type"), MsgType.REGISTER_ACK)
            self.assertIn("w2", m2.worker_registry)
            self.assertNotIn("w2", m1.worker_registry)

            s1.close()
            s2_good.close()

        finally:
            m1.stop()
            m2.stop()


if __name__ == "__main__":
    unittest.main()
