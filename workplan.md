# PERAN
Kamu adalah senior engineer sistem terdistribusi. Bangun proyek UAS mata kuliah
"Komputasi Paralel dan Sistem Terdistribusi" dengan tema nomor 16:
**Distributed Data Compression** (kompresi sejumlah besar file, pembagian file ke beberapa Worker),
menggunakan arsitektur **Master–Worker**.

# KONTEKS PENILAIAN (WAJIB DIPENUHI SEMUA)
Proyek dinilai dari: (1) source code, (2) laporan (BAB I–IV + kesimpulan), (3) video demo maks 5 menit
yang memperlihatkan minimal 10 node fisik (1 Master + 9 Worker, tersambung jaringan kabel/wireless).
Kode harus mencakup dan membuktikan 6 komponen berikut, masing-masing terpisah jelas di kode agar mudah
dijelaskan di laporan:
a. Task Decomposition
b. Parallel Processing (multiprocessing, memanfaatkan multicore CPU di Worker)
c. Distributed Processing (komunikasi jaringan, distribusi kerja, pengumpulan hasil)
d. Dynamic Load Balancing (distribusi dinamis berdasarkan kondisi dan kemampuan Worker)
e. Fault Tolerance (deteksi kegagalan Worker + retry/requeue/pemulihan)
f. Performance Evaluation (pengujian terkontrol dan analisis)

# TECH STACK
- Python 3.10+, utamakan **standard library** supaya mudah dipasang di 10 komputer:
  `socket`, `threading`, `multiprocessing`/`concurrent.futures.ProcessPoolExecutor`, `struct`, `json`,
  `hashlib`, `lzma`, `bz2`, `gzip`, `zlib`, `argparse`, `logging`.
- Dependensi eksternal hanya yang perlu: `psutil` (CPU/memori), `matplotlib` + `pandas` (grafik dan tabel,
  hanya di sisi Master/analisis). Boleh `zstandard` secara opsional (fallback otomatis jika tidak ada).
- Lintas-platform (Windows/Linux). Gunakan `if __name__ == "__main__":` dan
  `multiprocessing.freeze_support()` agar aman di Windows. Jangan pakai fork-only fitur.
- Semua parameter lewat `argparse` + file `config.yaml`/`config.json` (host, port, algoritma, level,
  ukuran chunk, timeout, jumlah proses, dsb.). Tanpa nilai hardcoded.

# STRUKTUR REPOSITORI
```
uas-distributed-compression/
├── README.md                # cara instal, konfigurasi, menjalankan 10 node, urutan demo
├── requirements.txt
├── config.example.json
├── common/
│   ├── protocol.py          # format pesan + framing TCP (length-prefixed), tipe pesan
│   ├── tasks.py             # definisi Task, TaskResult, status, chunking
│   ├── compression.py       # fungsi kompres/dekompres (gzip/bz2/lzma/zstd), container format
│   └── utils.py             # logging, hashing SHA-256, timer, util file
├── master/
│   ├── master.py            # entry point Master
│   ├── scheduler.py         # antrean task, dynamic load balancing
│   ├── fault_monitor.py     # heartbeat monitor, requeue, retry
│   └── collector.py         # penerimaan hasil, penulisan output atomik, ringkasan
├── worker/
│   ├── worker.py            # entry point Worker (connect, register, pull task, heartbeat)
│   ├── executor.py          # process pool multicore + eksekusi kompresi
│   └── monitor.py           # laporan CPU/memori via psutil
├── benchmark/
│   ├── sequential.py        # baseline 1 proses
│   ├── local_parallel.py    # multiprocessing di 1 komputer (variasi 1,2,4,.. proses)
│   ├── run_distributed.py   # orkestrasi percobaan distributed + scalability
│   ├── experiments.py       # skenario a–f, pengulangan, simpan CSV/JSON
│   ├── analyze.py           # hitung metrik + buat grafik dan tabel
│   └── verify.py            # dekompresi + bandingkan SHA-256 dengan file asli
├── data/
│   ├── fetch_dataset.py     # unduh dataset dari sumber resmi
│   ├── generate_synthetic.py# fallback pembangkit data uji (teks/log/CSV campuran)
│   └── DATASET_SOURCES.md   # sumber resmi + lisensi + tanggal akses (untuk laporan)
├── results/                 # CSV, JSON, grafik (png) dihasilkan otomatis
├── docs/
│   ├── ARCHITECTURE.md      # diagram arsitektur/topologi, sequence diagram (Mermaid)
│   ├── PROTOCOL.md          # spesifikasi pesan Master–Worker
│   └── REPORT_NOTES.md      # bahan ajar untuk BAB III dan IV (lihat bagian "OUTPUT UNTUK LAPORAN")
└── scripts/
    ├── start_master.(sh|bat)
    ├── start_worker.(sh|bat)
    └── kill_worker_demo.(sh|bat)
```

# SPESIFIKASI TEKNIS

## 1. Task Decomposition (komponen a)
- Input: satu direktori berisi banyak file (teks, log, CSV, gambar, dsb.).
- Aturan pemecahan:
  - File kecil (< `chunk_size`, default 32 MB) → 1 task = 1 file.
  - File besar → dipecah jadi beberapa chunk berukuran `chunk_size` (parameter), tiap chunk = 1 task
    independen (kompresi per chunk, tanpa dependensi antar chunk).
- `Task`: `task_id`, `file_path`, `chunk_index`, `chunk_count`, `offset`, `length`, `algorithm`, `level`,
  `status` (PENDING/ASSIGNED/DONE/FAILED), `attempts`, `assigned_worker`, `created_at`.
- `TaskResult`: `task_id`, `original_size`, `compressed_size`, `ratio`, `duration`, `worker_id`,
  `sha256_original`, `status`, `error`.
- Hasil chunk disimpan dalam **container format** sederhana (header + indeks chunk) agar file bisa
  direkonstruksi sempurna saat dekompresi dan diverifikasi.
- Gunakan urutan task yang membuat beban tidak seragam (campur file besar/kecil), karena ini penting
  untuk menunjukkan manfaat load balancing.

## 2. Parallel Processing di Worker (komponen b)
- Tiap Worker membuat `ProcessPoolExecutor(max_workers=N)`, `N` default `os.cpu_count()`,
  bisa diubah via argumen `--procs` (dipakai untuk pengujian local parallel dan scalability).
- Worker boleh menampung beberapa task sekaligus (jumlah slot = `N`), sehingga semua core terpakai.
- Kompresi harus **CPU-bound** agar manfaat paralel terlihat: default `lzma` preset 6 atau `bz2` level 9;
  sediakan opsi `gzip`/`zlib` sebagai pembanding. Dokumentasikan alasan pemilihan di ARCHITECTURE.md.
- Jangan memakai threading untuk kompresi (hindari batasan GIL); gunakan proses.

## 3. Distributed Processing (komponen c)
- Komunikasi: **TCP socket** dengan framing length-prefixed (4/8 byte header panjang, lalu payload).
  Pesan kontrol berformat JSON; payload data (bytes chunk/hasil kompresi) dikirim sebagai bagian biner
  terpisah (tidak di-base64). Spesifikasikan di PROTOCOL.md.
- Tipe pesan minimal: `REGISTER`, `REGISTER_ACK`, `REQUEST_TASK`, `TASK_ASSIGN`, `TASK_RESULT`,
  `HEARTBEAT`, `NO_MORE_TASKS`, `SHUTDOWN`, `ERROR`.
- `REGISTER` memuat: `worker_id`, hostname, IP, jumlah core, RAM, jumlah proses.
- Mode transfer data (konfigurasi `transfer_mode`):
  - `socket`: Master membaca chunk dan mengirim bytes ke Worker, Worker mengembalikan hasil kompresi
    ke Master (tidak bergantung folder bersama).
  - `shared_fs`: Master hanya mengirim path/offset/length, Worker membaca dari folder bersama
    (NFS/SMB) dan menulis output di sana. Siapkan keduanya, default `socket`.
- Master multi-client menggunakan thread per koneksi Worker (atau `selectors`); aman thread
  (lock pada antrean/status).
- Penulisan output Master atomik: tulis ke file sementara, lalu rename.
- Ringkasan akhir Master: total file, total ukuran asli, total hasil, ratio keseluruhan, waktu total,
  distribusi task per Worker, jumlah retry.

## 4. Dynamic Load Balancing (komponen d)
- Mekanisme utama: **pull-based work queue**. Worker mengirim `REQUEST_TASK` setiap ada slot kosong;
  Master memberi task berikutnya. Worker cepat/multicore otomatis menerima lebih banyak task.
- Pertimbangan kemampuan: Master mencatat kapasitas tiap Worker (core, throughput terukur
  MB/detik, beban CPU terakhir dari heartbeat) dan memakainya untuk:
  - mengatur jumlah task yang diberikan sekaligus (prefetch) sebanding kapasitas,
  - menahan task besar dari Worker yang sedang sangat sibuk,
  - (opsional) task terbesar didahulukan (Longest-Processing-Time-first) agar ekor eksekusi pendek.
- Sediakan **dua mode** yang bisa dipilih lewat konfigurasi untuk pembandingan wajib "sebelum vs sesudah":
  - `static`: seluruh task dibagi rata/round-robin di awal, tanpa menyesuaikan kondisi Worker.
  - `dynamic`: mekanisme di atas.
- Sediakan opsi `--throttle <persen>` / `--slow-factor` di Worker untuk mensimulasikan Worker yang
  lebih lambat (misalnya `sleep` proporsional atau membatasi jumlah proses), supaya efek load balancing
  jelas terlihat saat pengujian.
- Catat per Worker: jumlah task, byte yang diproses, waktu aktif, waktu idle; hitung ukuran
  ketidakseimbangan (misalnya koefisien variasi atau max/min ratio).

## 5. Fault Tolerance (komponen e)
- Worker mengirim `HEARTBEAT` tiap `heartbeat_interval` (default 2 detik) berisi CPU%, memori%, jumlah
  task aktif.
- Master menandai Worker **DEAD** jika tidak ada heartbeat lebih lama dari `heartbeat_timeout`
  (default 3–5× interval) atau koneksi TCP terputus.
- Task milik Worker DEAD otomatis dikembalikan ke antrean (**requeue**), dengan:
  - batas `max_retries` per task (default 3); bila terlampaui, tandai FAILED dan laporkan,
  - idempotensi: hasil duplikat dari Worker yang "bangkit kembali" diabaikan (cek `task_id` + status),
  - Worker yang kembali terhubung boleh register ulang dan menerima task lagi.
- Task yang gagal di tengah (exception, file rusak) dicatat dan di-retry, tanpa menghentikan seluruh sistem.
- Master tetap menyelesaikan seluruh pekerjaan selama masih ada minimal 1 Worker hidup.
- Fitur simulasi kegagalan untuk pengujian dan demo:
  - Worker: `--crash-after <n_task>` atau `--crash-at <detik>` (proses berhenti mendadak),
  - skrip `kill_worker_demo` yang mematikan Worker tertentu di tengah proses.
- **Recovery Time** diukur sebagai selisih waktu dari Worker terdeteksi gagal (atau saat crash
  diinjeksi) sampai task yang gagal selesai di Worker lain. Catat tiap kejadian dengan timestamp
  di log terstruktur (JSON lines).

## 6. Performance Evaluation (komponen f)
Implementasikan seluruh skenario wajib di `benchmark/experiments.py`, dijalankan otomatis,
diulang minimal **3 kali** per konfigurasi (laporkan rata-rata dan simpangan baku), dengan dataset
yang sama untuk semua skenario:

| ID | Skenario | Keterangan |
|----|----------|------------|
| a | Sequential | 1 proses, 1 komputer (baseline T₁) |
| b | Local Parallel | multiprocessing di 1 komputer, variasi proses 1, 2, 4, … hingga jumlah core |
| c | Distributed | Master + beberapa Worker (konfigurasi tetap) |
| d | Load Balancing | `static` vs `dynamic`, termasuk skenario ada Worker lambat |
| e | Fault Tolerance | matikan Worker di tengah proses, ukur recovery dan kelengkapan hasil |
| f | Scalability | 1, 2, 4, 6, dan 9 Worker |

Metrik (dihitung di `analyze.py`, rumus eksplisit di dokumentasi):
- **Execution Time** (detik, wall-clock dari task pertama dikirim sampai hasil terakhir terkumpul)
- **Speedup** = T₁ / Tₚ
- **Efficiency** = Speedup / p (p = jumlah Worker atau total proses; jelaskan definisi yang dipakai)
- **Throughput** (MB/detik dan file/detik)
- **CPU/Memory Utilization** (rata-rata dan puncak per node, dari `psutil`, disampling tiap 1 detik;
  simpan time series)
- **Recovery Time**
- Compression ratio (sebagai metrik pendukung)

Keluaran otomatis di `results/`: CSV mentah per run, CSV agregat, dan grafik PNG
(execution time vs jumlah Worker, speedup vs ideal, efficiency, throughput, distribusi task per Worker
static vs dynamic, timeline CPU/memori, timeline fault tolerance). Beri label sumbu, judul, dan legenda
yang jelas (bahasa Indonesia).

## 7. Verifikasi Kebenaran
- `verify.py`: dekompresi semua hasil, bandingkan SHA-256 dengan file asli. Hasilnya wajib 100% cocok
  di semua skenario, termasuk skenario fault tolerance.
- Sertakan unit test ringan (`pytest`) untuk: chunking, container format, protokol framing,
  requeue logic, dan scheduler.

## 8. Dataset
- Unduh dari **sumber resmi** lewat `data/fetch_dataset.py`, misalnya Silesia Corpus, Canterbury Corpus,
  korpus teks Project Gutenberg, atau dataset publik resmi lain. Jangan menebak URL: verifikasi
  URL resmi dan lisensi sebelum dipakai.
- Catat di `DATASET_SOURCES.md`: nama, URL resmi, lisensi, ukuran, tanggal akses, dan cara dipakai.
  (Ini dipersyaratkan PDF: sumber resmi semua aset pendukung.)
- Targetkan total beberapa GB dan ribuan file campuran (kecil + besar) supaya beban cukup untuk
  menunjukkan manfaat paralel. Jika perlu, `generate_synthetic.py` membuat data tambahan
  (dan dinyatakan sebagai data sintetis).

## 9. Operasional di 10 Node
- Satu perintah per node (`start_master`, `start_worker --master-host <IP>`), tanpa edit kode.
- Worker auto-reconnect dengan backoff jika Master belum siap.
- Master menampilkan dashboard teks ringkas di terminal (Worker aktif, task selesai/total,
  throughput, task per Worker) agar bagus dalam video demo.
- Dokumentasikan konfigurasi firewall/port, cara cek konektivitas, dan tips jaringan
  (disarankan kabel/switch yang sama; catat bila memakai Wi-Fi).
- Logging berwarna/terstruktur dengan timestamp dan ID Worker.

# KUALITAS KODE
- Type hints, docstring singkat, penanganan exception di semua I/O jaringan dan file.
- Tidak ada race condition: semua akses state bersama dilindungi lock.
- Modular, tiap komponen (a–f) mudah ditunjuk di kode untuk penjelasan di laporan.
- Jangan menambahkan fitur di luar yang diminta kecuali menunjang salah satu komponen a–f.

# OUTPUT UNTUK LAPORAN (docs/REPORT_NOTES.md)
Hasilkan bahan yang bisa langsung dipakai menyusun laporan:
- **BAB III**: diagram arsitektur/topologi (Mermaid), sequence diagram alur kerja
  (register → request task → proses → result → heartbeat → requeue), flowchart scheduler,
  tabel template konfigurasi node dan jaringan (kolom untuk diisi: hostname, IP, CPU, RAM, OS), dan
  rancangan percobaan (variabel, kontrol, jumlah pengulangan).
- **BAB IV**: daftar potongan kode kunci beserta lokasi file untuk tiap komponen a–f, serta template
  tabel hasil dan grafik yang dihasilkan `analyze.py` dengan **kerangka analisis** untuk tiap hasil
  (apa yang diamati, mengapa speedup tidak linear: overhead jaringan, bottleneck I/O, Amdahl's law,
  dan sebagainya). Jangan mengarang angka hasil; semua angka berasal dari run nyata.
- Rumus metrik dan referensi sumber teori (daftar bacaan yang perlu dikutip).
- Skrip demo video (urutan langkah ≤ 5 menit): tunjukkan 10 node, jalankan sistem, tunjukkan distribusi
  task, matikan satu Worker, tunjukkan recovery, tunjukkan ringkasan hasil dan verifikasi.

# CARA KERJA YANG DIHARAPKAN
1. Mulai dengan rencana singkat dan struktur proyek, lalu implementasikan bertahap:
   common → worker/executor → master/scheduler → fault monitor → benchmark → analisis → dokumentasi.
2. Setelah tiap tahap, jalankan tes lokal: 1 Master + beberapa Worker di `localhost` (port berbeda)
   untuk membuktikan alur dan fault tolerance sebelum diuji di 10 node fisik.
3. Pada akhir, berikan: ringkasan arsitektur, cara menjalankan, daftar skenario uji yang sudah
   tervalidasi, serta hal yang masih perlu dilakukan manual di 10 node fisik (pengisian tabel
   konfigurasi node, pengambilan data final, rekaman video).
4. Jika ada asumsi atau keputusan desain yang ambigu, pilih opsi paling sederhana yang tetap
   memenuhi poin a–f, lalu catat di ARCHITECTURE.md.