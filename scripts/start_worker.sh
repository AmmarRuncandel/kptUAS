#!/bin/bash
# start_worker.sh – Jalankan Worker Node di Linux/macOS
# Penggunaan: bash start_worker.sh <MASTER_HOST> [WORKER_ID] [PROCS]

MASTER_HOST="${1:-127.0.0.1}"
WORKER_ID="${2:-w-$(hostname)}"
PROCS="${3:-0}"
MASTER_PORT=9000

echo "================================================================"
echo " WORKER NODE – Distributed Data Compression"
echo "================================================================"
echo " Master : $MASTER_HOST:$MASTER_PORT"
echo " Worker : $WORKER_ID"
echo " Procs  : $PROCS (0 = auto)"
echo "================================================================"

cd "$(dirname "$0")/.."

ARGS="--master-host $MASTER_HOST --master-port $MASTER_PORT --worker-id $WORKER_ID --log-level INFO"
if [ "$PROCS" != "0" ]; then
    ARGS="$ARGS --procs $PROCS"
fi

python3 worker/worker.py $ARGS
