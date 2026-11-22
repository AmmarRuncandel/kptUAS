# Distributed Data Compression – UAS Komputasi Paralel dan Sistem Terdistribusi

Sistem kompresi file terdistribusi menggunakan arsitektur **Master–Worker** dengan:
- Dekomposisi task otomatis (file kecil = 1 task; file besar = beberapa chunk)
- Kompresi paralel multicore di setiap Worker (`ProcessPoolExecutor`)
- Komunikasi TCP dengan framing length-prefixed
- Dynamic Load Balancing (pull-based + LPT)
- Fault Tolerance (heartbeat + requeue + idempotency)
- Benchmark lengkap: sequential, local parallel, distributed, scalability

---

## Prasyarat

- Python 3.10+
- Semua komputer dalam satu jaringan LAN
- Port 9000 terbuka di Master

```bash
pip install psutil matplotlib pandas
```

---

## Struktur Direktori

```
uas-distributed-compression/
├── common/          # protokol, task, kompresi, utilitas
├── master/          # server Master + scheduler + fault monitor + collector
├── worker/          # client Worker + executor + monitor
├── benchmark/       # skenario pengujian + analisis + verifikasi
├── data/            # dataset + generator sintetis
├── results/         # output otomatis (CSV, JSON, grafik)
├── docs/            # dokumentasi arsitektur + protokol + bahan laporan
└── scripts/         # skrip start/stop untuk Windows dan Linux
```

---

## Cara Menjalankan di 10 Node

### 1. Siapkan Data (di Master)

```bash
# Gunakan data sintetis (cepat, tidak butuh internet):
python data/generate_synthetic.py --output-dir data/sample --num-small 200 --num-large 10

# ATAU unduh dataset resmi (butuh internet):
python data/fetch_dataset.py --output-dir data/sample
```

### 2. Jalankan Master

**Windows:**
```bat
scripts\start_master.bat data\sample dynamic
```

**Linux:**
```bash
bash scripts/start_master.sh data/sample dynamic
```

**Atau langsung:**
```bash
python master/master.py --input-dir data/sample --output-dir results/output \
    --host 0.0.0.0 --port 9000 --algorithm lzma --level 6 \
    --mode dynamic --transfer socket
```

### 3. Jalankan Worker di 9 Komputer

Ganti `192.168.1.10` dengan IP Master yang sebenarnya.

**Windows (tiap komputer Worker):**
```bat
scripts\start_worker.bat 192.168.1.10 w01
```

**Linux:**
```bash
bash scripts/start_worker.sh 192.168.1.10 w01
```

**Atau langsung:**
```bash
python worker/worker.py --master-host 192.168.1.10 --master-port 9000 \
    --worker-id w01 --log-level INFO
```

### 4. Simulasi Fault Tolerance (untuk demo)

```bash
# Crash Worker setelah 5 task:
python worker/worker.py --master-host 192.168.1.10 --worker-id w05 --crash-after 5

# Crash Worker setelah 30 detik:
python worker/worker.py --master-host 192.168.1.10 --worker-id w05 --crash-at 30

# Matikan Worker secara manual:
bash scripts/kill_worker_demo.sh w05
```

### 5. Verifikasi Hasil

```bash
python benchmark/verify.py --output-dir results/output --input-dir data/sample
```

---

## Benchmark Lokal (Skenario a + b)

```bash
# Jalankan skenario sequential + local parallel (tidak butuh klaster):
python benchmark/experiments.py --input-dir data/sample --repeats 3

# Buat grafik dari hasil:
python benchmark/analyze.py --input results/experiments_local.json
```

## Benchmark Distributed (Skenario c-f)

Setelah Master + Worker selesai, kumpulkan hasil:
```bash
python benchmark/run_distributed.py \
    --summary results/output/summary.json \
    --t1 <nilai_T1> \
    --num-workers 9 \
    --scenario f \
    --mode dynamic
```

---

## Konfigurasi Node dan Jaringan

| Parameter | Nilai Default | Keterangan |
|-----------|--------------|-----------|
| Port      | 9000         | Port TCP Master |
| Algorithm | lzma         | Algoritma kompresi default |
| Level     | 6            | Level kompresi |
| Chunk Size | 32 MB       | Ukuran chunk file besar |
| Mode      | dynamic      | Load balancing mode |
| Heartbeat | 2 detik      | Interval kirim heartbeat |
| Timeout   | 10 detik     | Batas sebelum Worker dinyatakan DEAD |

**Tips Jaringan:**
- Gunakan kabel LAN dan switch yang sama untuk performa terbaik.
- Jika menggunakan Wi-Fi, catat kondisi sinyal dan bandingkan dengan kabel.
- Pastikan firewall mengizinkan port 9000 (lihat `docs/PROTOCOL.md`).
- Cek konektivitas: `Test-NetConnection -ComputerName <IP_MASTER> -Port 9000`

---

## Output Otomatis

```
results/
├── output/              # file .compressed (hasil kompresi)
│   └── summary.json     # ringkasan lengkap Master
├── grafik/              # PNG: speedup, efficiency, throughput, CPU timeline, ...
├── raw/                 # CSV mentah per skenario
├── tabel_*.csv          # tabel agregat
├── experiments_*.json   # data eksperimen lengkap
└── logs/
    ├── master.jsonl     # log terstruktur Master
    └── worker-*.jsonl   # log terstruktur tiap Worker
```

---

## Unit Test

```bash
pip install pytest
python -m pytest tests/ -v
```
