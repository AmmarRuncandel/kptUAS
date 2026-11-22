#!/bin/bash
# kill_worker_demo.sh – Simulasi kegagalan Worker untuk demo fault tolerance
# Komponen e: membunuh Worker di tengah proses
# Penggunaan: bash kill_worker_demo.sh [WORKER_ID_SUBSTRING]

WORKER_SUBSTR="${1:-worker}"

echo "[FAULT-TOLERANCE DEMO] Mencari proses Worker: $WORKER_SUBSTR..."
PIDS=$(pgrep -f "worker.py.*${WORKER_SUBSTR}" 2>/dev/null)

if [ -z "$PIDS" ]; then
    echo "Tidak ada proses Worker ditemukan dengan substring: $WORKER_SUBSTR"
    echo "Daftar proses worker.py:"
    pgrep -af "worker.py"
    exit 1
fi

echo "PID ditemukan: $PIDS"
for PID in $PIDS; do
    echo "Mematikan PID $PID..."
    kill -9 "$PID"
    echo "  PID $PID dihentikan (SIGKILL)"
done

echo "Selesai. Master akan mendeteksi Worker DEAD dalam ${heartbeat_timeout:-10} detik."
