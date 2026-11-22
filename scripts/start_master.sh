#!/bin/bash
# start_master.sh – Jalankan Master Node di Linux/macOS
# Penggunaan: bash start_master.sh [input_dir] [mode]

INPUT_DIR="${1:-data/sample}"
MODE="${2:-dynamic}"
OUTPUT_DIR="results/output"
PORT=9000
ALGORITHM="lzma"
LEVEL=6
CHUNK_SIZE=32

echo "================================================================"
echo " MASTER NODE – Distributed Data Compression"
echo "================================================================"
echo " Input  : $INPUT_DIR"
echo " Output : $OUTPUT_DIR"
echo " Mode   : $MODE"
echo " Port   : $PORT"
echo "================================================================"

cd "$(dirname "$0")/.."
python3 master/master.py \
    --input-dir "$INPUT_DIR" \
    --output-dir "$OUTPUT_DIR" \
    --host 0.0.0.0 \
    --port "$PORT" \
    --algorithm "$ALGORITHM" \
    --level "$LEVEL" \
    --chunk-size "$CHUNK_SIZE" \
    --mode "$MODE" \
    --transfer socket \
    --log-level INFO
