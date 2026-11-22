"""
common/discovery.py
===================
Auto-discovery Master di jaringan lokal (LAN) menggunakan UDP Broadcast Beacon.
Worker tidak perlu mengetahui IP Master secara manual — Worker akan mencari
dan menemukan Master secara otomatis.
"""

from __future__ import annotations
import json
import logging
import socket
import threading
import time
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

DISCOVERY_PORT  = 9001
DISCOVERY_MAGIC = "DCMP_MASTER_BEACON_V1"
PROBE_MAGIC     = "DCMP_WORKER_PROBE_V1"


class MasterBeacon(threading.Thread):
    """
    Thread pada Master yang menyiarkan (broadcast) keberadaan Master
    ke seluruh jaringan lokal via UDP tiap interval detik.
    Menyertakan passcode untuk autentikasi cluster Master tertentu.
    """

    def __init__(self, tcp_port: int, interval: float = 1.0,
                 passcode: Optional[str] = None,
                 discovery_port: int = DISCOVERY_PORT) -> None:
        super().__init__(daemon=True, name="master-beacon")
        self.tcp_port = tcp_port
        self.interval = interval
        self.passcode = passcode or ""
        self.discovery_port = discovery_port
        self._running = False
        self._sock: Optional[socket.socket] = None

    def start_beacon(self) -> None:
        self._running = True
        self.start()

    def stop_beacon(self) -> None:
        self._running = False
        if self._sock:
            try:
                self._sock.close()
            except OSError:
                pass

    def run(self) -> None:
        try:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            # Bind agar bisa juga menerima probe dari worker
            try:
                self._sock.bind(("", self.discovery_port))
                self._sock.settimeout(0.5)
            except OSError:
                # Jika port sudah terpakai di mesin yang sama, tetap bisa broadcast
                self._sock.settimeout(0.5)

            payload = json.dumps({
                "magic":    DISCOVERY_MAGIC,
                "tcp_port": self.tcp_port,
                "hostname": socket.gethostname(),
                "passcode": self.passcode,
            }).encode("utf-8")

            logger.info(
                f"Auto-Discovery Beacon aktif di UDP port {self.discovery_port} "
                f"(Passcode: {'aktif' if self.passcode else 'tanpa sandi'})"
            )

            while self._running:
                # Kirim broadcast beacon ke jaringan
                try:
                    self._sock.sendto(payload, ("<broadcast>", self.discovery_port))
                    self._sock.sendto(payload, ("255.255.255.255", self.discovery_port))
                    self._sock.sendto(payload, ("127.0.0.1", self.discovery_port))
                except OSError:
                    pass

                # Periksa apakah ada probe dari worker yang masuk
                try:
                    data, addr = self._sock.recvfrom(1024)
                    if data:
                        info = json.loads(data.decode("utf-8"))
                        if info.get("magic") == PROBE_MAGIC:
                            # Balas langsung ke worker
                            self._sock.sendto(payload, addr)
                except (socket.timeout, OSError, json.JSONDecodeError):
                    pass

                time.sleep(self.interval)

        except Exception as e:
            logger.warning(f"MasterBeacon terhenti: {e}")
        finally:
            if self._sock:
                try:
                    self._sock.close()
                except OSError:
                    pass


def discover_master(timeout: float = 4.0, passcode: Optional[str] = None,
                    discovery_port: int = DISCOVERY_PORT) -> Optional[Tuple[str, int]]:
    """
    Dijalankan di Worker untuk mencari Master secara otomatis di LAN.

    Mengirim probe broadcast dan mendengarkan beacon Master.
    Jika `passcode` ditentukan, hanya menerima Master yang menyiarkan passcode yang cocok.

    Returns:
        (master_ip, master_tcp_port) atau None jika tidak ditemukan dalam batas waktu timeout.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    probe_payload = json.dumps({
        "magic":           PROBE_MAGIC,
        "hostname":        socket.gethostname(),
        "target_passcode": passcode or "",
    }).encode("utf-8")

    try:
        # Bind ke port sembarang
        sock.bind(("", 0))
        sock.settimeout(0.5)

        # Kirim probe ke jaringan
        for target in ("<broadcast>", "255.255.255.255", "127.0.0.1"):
            try:
                sock.sendto(probe_payload, (target, discovery_port))
            except OSError:
                pass

        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                data, addr = sock.recvfrom(2048)
                if not data:
                    continue
                info = json.loads(data.decode("utf-8"))
                if info.get("magic") == DISCOVERY_MAGIC:
                    beacon_passcode = info.get("passcode", "")
                    # Filter berdasarkan passcode jika Worker menentukan passcode
                    if passcode and beacon_passcode != passcode:
                        # Ini sinyal dari Master lain dengan sandi berbeda di LAN, abaikan!
                        continue

                    master_ip = addr[0]
                    # Jika broadcast diterima dari 0.0.0.0, ubah ke 127.0.0.1
                    if master_ip == "0.0.0.0":
                        master_ip = "127.0.0.1"
                    tcp_port = info.get("tcp_port", 9000)
                    logger.info(
                        f"Master ditemukan di {master_ip}:{tcp_port} via Auto-Discovery "
                        f"(Sandi: {'COCOK' if passcode else 'AUTO'})"
                    )
                    return master_ip, tcp_port
            except socket.timeout:
                # Kirim ulang probe tiap 0.5s
                for target in ("<broadcast>", "255.255.255.255", "127.0.0.1"):
                    try:
                        sock.sendto(probe_payload, (target, discovery_port))
                    except OSError:
                        pass
            except (OSError, json.JSONDecodeError):
                continue

    finally:
        try:
            sock.close()
        except OSError:
            pass

    return None
