# Bahan Laporan – BAB III & IV

> Dokumen ini berisi bahan siap pakai untuk menyusun laporan proyek UAS.
> **PENTING:** Jangan mengarang angka hasil; semua angka berasal dari run nyata
> yang dijalankan di klaster 10 node.

---

# BAB III – Metodologi

## 3.1 Arsitektur Sistem

Sistem menggunakan arsitektur **Master–Worker** dengan spesifikasi:

- **1 Master Node**: bertanggung jawab atas dekomposisi task, penjadwalan,
  pengumpulan hasil, dan pemantauan kesehatan Worker.
- **9 Worker Node**: masing-masing menjalankan pool proses multicore untuk
  kompresi file secara paralel.
- Komunikasi melalui **TCP socket** dengan protokol length-prefixed (8-byte header).

> Lihat `docs/ARCHITECTURE.md` untuk diagram lengkap dan sequence diagram.

---

## 3.2 Diagram Topologi

*(Salin diagram Mermaid dari `docs/ARCHITECTURE.md` ke laporan.)*

**Tabel Konfigurasi Node** (isi sesuai kondisi lab):

| No | Hostname | IP Address | CPU (core) | RAM (GB) | OS | Peran |
|----|----------|-----------|-----------|---------|-----|-------|
| 1  |          |           |           |         |     | Master |
| 2  |          |           |           |         |     | Worker w01 |
| 3  |          |           |           |         |     | Worker w02 |
| 4  |          |           |           |         |     | Worker w03 |
| 5  |          |           |           |         |     | Worker w04 |
| 6  |          |           |           |         |     | Worker w05 |
| 7  |          |           |           |         |     | Worker w06 |
| 8  |          |           |           |         |     | Worker w07 |
| 9  |          |           |           |         |     | Worker w08 |
| 10 |          |           |           |         |     | Worker w09 |

---

## 3.3 Mekanisme Kerja

### a. Task Decomposition (`common/tasks.py`)
File dalam direktori input dipecah berdasarkan `chunk_size` (default 32 MB):
- File < 32 MB → 1 task (1 chunk).
- File ≥ 32 MB → dipecah menjadi beberapa chunk; tiap chunk = 1 task independen.
- Urutan task dicampur besar/kecil untuk memperlihatkan manfaat load balancing.

### b. Parallel Processing (`worker/executor.py`)
Setiap Worker menggunakan `ProcessPoolExecutor(max_workers=N)` di mana
N = jumlah CPU core. Kompresi (`lzma`, `bz2`, `gzip`, `zlib`) berjalan di
proses terpisah untuk menghindari GIL Python. Worker dapat memproses hingga
N task secara bersamaan.

**Alasan pemilihan LZMA sebagai default:** LZMA memberikan rasio kompresi
terbaik di antara algoritma yang tersedia di Python standard library, dan
memiliki beban CPU yang tinggi (CPU-bound), sehingga manfaat paralel paling
terlihat.

### c. Distributed Processing (`common/protocol.py`, `master/master.py`)
- Protokol TCP length-prefixed (8-byte header).
- Master multi-client: 1 thread per koneksi Worker.
- Mode transfer: `socket` (kirim bytes lewat jaringan) atau `shared_fs` (NFS/SMB).
- Semua akses state bersama dilindungi `threading.Lock()`.

### d. Dynamic Load Balancing (`master/scheduler.py`)
Dua mode:
- **Static**: seluruh task dibagi rata ke Worker secara round-robin di awal.
- **Dynamic**: pull-based; Worker minta task saat slot kosong; LPT (task
  terbesar diprioritaskan); Worker sangat sibuk (CPU > 90%) hanya mendapat task kecil.

### e. Fault Tolerance (`master/fault_monitor.py`)
- Worker kirim `HEARTBEAT` tiap 2 detik.
- Master tandai Worker DEAD jika tidak ada heartbeat > 10 detik.
- Task milik Worker DEAD dikembalikan ke antrean (requeue).
- Batas `max_retries=3`; melebihi batas → task FAILED.
- Idempotency: hasil duplikat dari Worker yang kembali hidup diabaikan.

### f. Performance Evaluation (`benchmark/`)
*(Lihat 3.4 untuk rancangan percobaan.)*

---

## 3.4 Rancangan Percobaan

| ID | Skenario | Variabel Bebas | Variabel Kontrol | Pengulangan |
|----|----------|----------------|-----------------|-------------|
| a  | Sequential | — | Dataset, algoritma | 3× |
| b  | Local Parallel | Jumlah proses (1,2,4,…,N) | Dataset, algoritma, 1 komputer | 3× |
| c  | Distributed | Konfigurasi tetap (9 Worker) | Dataset, algoritma | 3× |
| d  | Load Balancing | Mode (static vs dynamic) + Worker lambat | Dataset, jumlah Worker | 3× |
| e  | Fault Tolerance | Waktu crash Worker | Dataset, jumlah Worker | 3× |
| f  | Scalability | Jumlah Worker (1,2,4,6,9) | Dataset, algoritma | 3× |

**Variabel kontrol global:**
- Dataset: Canterbury Corpus + file sintetis (total ≥ 500 MB)
- Algoritma: lzma, level 6
- Chunk size: 32 MB

---

# BAB IV – Implementasi dan Hasil

## 4.1 Potongan Kode Kunci per Komponen

### Komponen a: Task Decomposition
**File:** `common/tasks.py` → fungsi `decompose_directory()`
```python
# Aturan chunking (baris ~60-90 di tasks.py):
if fsize < chunk_size:
    tasks.append(Task(..., chunk_index=0, chunk_count=1, ...))
else:
    chunk_count = (fsize + chunk_size - 1) // chunk_size
    for ci in range(chunk_count):
        offset = ci * chunk_size
        length = min(chunk_size, fsize - offset)
        tasks.append(Task(..., chunk_index=ci, ...))
```

### Komponen b: Parallel Processing
**File:** `worker/executor.py` → `WorkerExecutor.submit()`
```python
# CPU-bound kompresi di proses terpisah:
future = self._pool.submit(
    _execute_compression_job,  # top-level, bisa di-pickle
    raw_data, task.algorithm, task.level,
    task.chunk_index, task.chunk_count,
)
```

### Komponen c: Distributed Processing
**File:** `common/protocol.py` → `send_message()` / `recv_message()`
```python
# Framing TCP length-prefixed:
length_header = struct.pack(">Q", len(data))  # 8-byte big-endian
sock.sendall(length_header)
sock.sendall(data)
```

### Komponen d: Dynamic Load Balancing
**File:** `master/scheduler.py` → `_get_task_dynamic()`
```python
# LPT + Worker sibuk hanya dapat task kecil:
if worker_info.is_busy and len(self._queue) > 1:
    small_task = min(self._queue, key=lambda t: t.length)
    ...
else:
    task = self._queue.popleft()  # terbesar duluan (LPT order)
```

### Komponen e: Fault Tolerance
**File:** `master/fault_monitor.py` → `_check_workers()`
```python
# Deteksi DEAD:
if elapsed > self._timeout:
    winfo.alive = False
    self._handle_dead_worker(wid, now)
# Requeue:
requeued = self._scheduler.requeue_worker_tasks(worker_id)
```

### Komponen f: Performance Evaluation
**File:** `benchmark/analyze.py` → `analyze()`
Rumus metrik:
- **Speedup** = T₁ / Tₚ  (T₁ = waktu sequential, Tₚ = waktu dengan p proses/Worker)
- **Efficiency** = Speedup / p
- **Throughput** = total_bytes_original / elapsed_time (MB/s)

---

## 4.2 Template Tabel Hasil

*(Isi dengan data nyata dari run di klaster 10 node)*

### Tabel a: Sequential Baseline
| Run | Waktu (s) | Throughput (MB/s) | Rasio |
|-----|-----------|------------------|-------|
| 1   |           |                  |       |
| 2   |           |                  |       |
| 3   |           |                  |       |
| **Rata-rata ± Std** | | | |

**T₁ = _____ detik**

### Tabel b: Local Parallel
| Proses | Waktu (s) | Std | Speedup | Efficiency | Throughput |
|--------|-----------|-----|---------|------------|-----------|
| 1      |           |     |         |            |           |
| 2      |           |     |         |            |           |
| 4      |           |     |         |            |           |
| N (max)|           |     |         |            |           |

### Tabel f: Scalability (Distributed)
| Worker | Waktu (s) | Std | Speedup | Efficiency | Throughput |
|--------|-----------|-----|---------|------------|-----------|
| 1      |           |     |         |            |           |
| 2      |           |     |         |            |           |
| 4      |           |     |         |            |           |
| 6      |           |     |         |            |           |
| 9      |           |     |         |            |           |

### Tabel d: Load Balancing
| Mode | Waktu (s) | CV (imbalance) | Max/Min Task |
|------|-----------|----------------|-------------|
| Static |         |                |             |
| Dynamic |        |                |             |
| Dynamic + Worker Lambat 50% | | | |

### Tabel e: Fault Tolerance
| Percobaan | Worker Mati | Waktu Deteksi (s) | Recovery Time (s) | Task Selesai |
|-----------|-------------|------------------|------------------|-------------|
| 1         |             |                  |                  | ___/___     |
| 2         |             |                  |                  | ___/___     |
| 3         |             |                  |                  | ___/___     |

---

## 4.3 Kerangka Analisis

### Mengapa Speedup Tidak Linear? (Hukum Amdahl)
$$S(p) = \frac{1}{f + \frac{1-f}{p}}$$
di mana $f$ = fraksi sequential (overhead jaringan, I/O disk, koordinasi).

Faktor-faktor yang membatasi speedup:
1. **Overhead jaringan**: transfer chunk file lewat TCP menambah latensi.
2. **Bottleneck I/O Master**: Master harus membaca file dari disk untuk setiap task.
3. **Koordinasi**: lock pada antrean task + registry Worker.
4. **Serialisasi Python** (pickle) untuk transfer data ke ProcessPoolExecutor.

### Mengapa Dynamic lebih baik dari Static?
Dalam distribusi static, jika ada 1 Worker yang lambat (crash/CPU tinggi),
semua task-nya tetap di antrean personal → Worker lain menganggur.
Dalam distribusi dynamic, task dari Worker lambat otomatis diambil alih.

### Recovery Time
Recovery Time = waktu dari Worker terdeteksi DEAD sampai task-nya selesai
di Worker lain. Komponen: `heartbeat_timeout` (10s) + waktu eksekusi di Worker baru.

---

## 4.4 Skrip Demo Video (≤ 5 Menit)

```
[00:00–00:30] Tunjukkan 10 komputer dalam satu jaringan (kamera scan ruangan/lab)
[00:30–01:00] Jalankan Master: start_master.bat data\sample dynamic
              → tampilkan terminal Master (dashboard: Worker aktif, task total)
[01:00–01:30] Jalankan 9 Worker di 9 komputer berbeda:
              start_worker.bat 192.168.1.10 w01
              (tiap Worker tampil di layar masing-masing)
[01:30–02:30] Proses berjalan: perlihatkan dashboard Master (task selesai, throughput)
              Zoom ke beberapa Worker (CPU usage tinggi = multicore aktif)
[02:30–03:00] Fault Tolerance demo:
              Matikan 1 Worker: kill_worker_demo.bat
              → Master log: "Worker w05 DEAD → 3 task di-requeue"
              → Worker lain mengambil alih task
[03:00–03:30] Semua task selesai. Perlihatkan ringkasan akhir Master:
              total file, rasio, throughput, distribusi task per Worker
[03:30–04:00] Jalankan verifikasi: python benchmark/verify.py --output-dir results/output --input-dir data/sample
              → "VERIFIKASI LULUS 100% ✓"
[04:00–04:30] Tampilkan grafik hasil dari results/grafik/ (speedup, efisiensi, scalability)
[04:30–05:00] Penutup
```

---

## 4.5 Referensi Teori

1. Amdahl, G. M. (1967). *Validity of the Single-Processor Approach to Achieving
   Large Scale Computing Capabilities*. AFIPS Spring Joint Computer Conference.
2. Gustafson, J. L. (1988). *Reevaluating Amdahl's Law*. CACM 31(5):532-533.
3. Tanenbaum, A. S. & Van Steen, M. (2017). *Distributed Systems: Principles
   and Paradigms* (3rd ed.). Pearson.
4. Dean, J. & Ghemawat, S. (2008). *MapReduce: Simplified Data Processing on
   Large Clusters*. CACM 51(1):107-113.
5. Python Software Foundation. *concurrent.futures – Launching parallel tasks*.
   https://docs.python.org/3/library/concurrent.futures.html
6. Ziv, J. & Lempel, A. (1977). *A Universal Algorithm for Sequential Data
   Compression*. IEEE Transactions on Information Theory, 23(3):337-343.
7. Arnold, R. & Bell, T. (1997). *A Corpus for the Evaluation of Lossless
   Compression Algorithms*. DCC '97, IEEE.
