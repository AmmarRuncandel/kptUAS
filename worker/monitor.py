"""
worker/monitor.py
=================
Komponen b + d: Laporan CPU dan memori via psutil.
Sampling tiap 1 detik, dikirim bersamaan heartbeat.
"""

from __future__ import annotations
import threading
import time
import logging
from typing import Optional, List, Dict

logger = logging.getLogger(__name__)

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False
    logger.warning("psutil tidak tersedia; monitoring CPU/memori dinonaktifkan")


class ResourceMonitor:
    """
    Monitor CPU% dan memori% secara periodik.
    Menyimpan time-series untuk analisis performa (Komponen f).
    """

    def __init__(self, interval: float = 1.0, worker_id: str = "worker") -> None:
        self.interval  = interval
        self.worker_id = worker_id
        self._running  = False
        self._thread: Optional[threading.Thread] = None
        self._lock     = threading.Lock()

        # Time-series data
        self._samples: List[Dict] = []

        # Nilai terkini
        self.cpu_pct: float = 0.0
        self.mem_pct: float = 0.0
        self.mem_mb:  float = 0.0

    def start(self) -> None:
        self._running = True
        self._thread  = threading.Thread(
            target=self._run, daemon=True, name=f"monitor-{self.worker_id}"
        )
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=3)

    def _run(self) -> None:
        while self._running:
            self._sample()
            time.sleep(self.interval)

    def _sample(self) -> None:
        ts = time.time()
        if HAS_PSUTIL:
            try:
                cpu = psutil.cpu_percent(interval=None)
                mem = psutil.virtual_memory()
                mem_pct = mem.percent
                mem_mb  = mem.used / (1024 * 1024)
            except Exception:
                cpu, mem_pct, mem_mb = 0.0, 0.0, 0.0
        else:
            cpu, mem_pct, mem_mb = 0.0, 0.0, 0.0

        with self._lock:
            self.cpu_pct = cpu
            self.mem_pct = mem_pct
            self.mem_mb  = mem_mb
            self._samples.append({
                "ts":      ts,
                "cpu_pct": cpu,
                "mem_pct": mem_pct,
                "mem_mb":  mem_mb,
            })

    def get_current(self) -> Dict:
        with self._lock:
            return {
                "cpu_pct": self.cpu_pct,
                "mem_pct": self.mem_pct,
                "mem_mb":  self.mem_mb,
            }

    def get_time_series(self) -> List[Dict]:
        """Kembalikan seluruh time-series (untuk analisis/grafik)."""
        with self._lock:
            return list(self._samples)

    def get_stats(self) -> Dict:
        """Statistik ringkas: rata-rata dan puncak CPU/memori."""
        with self._lock:
            if not self._samples:
                return {"cpu_avg": 0, "cpu_max": 0, "mem_avg": 0, "mem_max": 0}
            cpus = [s["cpu_pct"] for s in self._samples]
            mems = [s["mem_pct"] for s in self._samples]
            return {
                "cpu_avg": round(sum(cpus) / len(cpus), 2),
                "cpu_max": round(max(cpus), 2),
                "mem_avg": round(sum(mems) / len(mems), 2),
                "mem_max": round(max(mems), 2),
                "samples": len(self._samples),
            }
