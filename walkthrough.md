# PANDUAN LENGKAP EKSEKUSI PENGUJIAN & PENGAMBILAN DATA (WALKTHROUGH)
## Distributed Data Compression System (Tugas Besar Komputasi Paralel & Terdistribusi)

Dokumen ini adalah panduan operasional langkah demi langkah untuk menyelesaikan seluruh kebutuhan pengujian, pengambilan tangkapan layar (*screenshot*) untuk laporan, perekaman video demo, serta perintah untuk mengekstrak seluruh grafik dan tabel analisis.

---

### DAFTAR ISI & RANGKUMAN TAHAPAN

| Langkah | Tipe Pengujian | Jumlah Node | Tujuan Skenario Tugas |
| :---: | :--- | :---: | :--- |
| **Langkah 1** | **Mandiri (1 Laptop)** | 1 Node | Skenario a (Sequential Baseline $T_1$) & Skenario b (Local Parallel) |
| **Langkah 2** | **Mandiri (1 Laptop)** | 1 Node (2 Tab) | Gladi Bersih Alur Master-Worker (Dry Run Simulasi) |
| **Langkah 3** | **Kelompok (10 Laptop)** | 2 Node | Skenario f (Scalability Run 1: Master + 1 Worker) |
| **Langkah 4** | **Kelompok (10 Laptop)** | 3 Node | Skenario f (Scalability Run 2: Master + 2 Worker) |
| **Langkah 5** | **Kelompok (10 Laptop)** | 5 Node | Skenario f (Scalability Run 3: Master + 4 Worker) |
| **Langkah 6** | **Kelompok (10 Laptop)** | 7 Node | Skenario f (Scalability Run 4: Master + 6 Worker) |
| **Langkah 7** | **Kelompok (10 Laptop)** | 10 Node | Skenario c (Distributed Baseline) & Skenario f (Scalability Run 5: 9 Worker) |
| **Langkah 8** | **Kelompok (10 Laptop)** | 10 Node | Skenario d (Load Balancing: Uji Static vs Dynamic) |
| **Langkah 9** | **Kelompok (10 Laptop)** | 10 Node | Skenario e (Fault Tolerance: Simulasi Node Mati di Tengah Jalan) |
| **Langkah 10**| **Mandiri (1 Laptop)** | 1 Node | Ekstrak Seluruh Grafik Final & Tabel CSV untuk Laporan |

---

## FASE 1: DIKERJAKAN SENDIRI DI 1 LAPTOP (MANDIRI)

### LANGKAH 1: Benchmark Sequential Baseline ($T_1$) & Local Multiprocessing
* **Tipe:** 1 Laptop Sendiri.
* **Kenapa harus ini:**
  Untuk mengukur *Speedup* ($S_p = \frac{T_1}{T_p}$), sistem wajib memiliki patokan waktu $T_1$ (waktu kompresi murni menggunakan 1 proses/core). Selain itu, langkah ini menguji performa multiprocessing lokal (1, 2, 4, 8 core) sesuai tuntutan **Skenario a dan Skenario b**.
* **Perintah Terminal:**
  ```powershell
  python benchmark/experiments.py --input-dir data/sample
  ```
  *(Program otomatis menjalankan 3x pengulangan untuk tiap variasi core lalu menghitung rata-rata, standar deviasi, dan efisiensi).*
* **Yang Harus Di-Screenshot untuk Laporan:**
  - Tangkapan layar tabel ringkasan di terminal: `RINGKASAN EKSPERIMEN LOKAL` (menampilkan $T_1$, waktu eksekusi per proses, speedup, dan efficiency).
* **Perintah Ekstrak Grafik & Tabel:**
  ```powershell
  python benchmark/analyze.py --input results/experiments_local.json
  ```
  **Hasil yang otomatis terbentuk:**
  - `results/grafik/exec_time_local_parallel.png`
  - `results/grafik/speedup_local_parallel.png`
  - `results/grafik/efficiency_local_parallel.png`
  - `results/grafik/throughput_local_parallel.png`
  - `results/tabel_local_parallel.csv` *(Tabel untuk Bab IV Laporan)*.

---

### LANGKAH 2: Gladi Bersih Master-Worker Interaktif (Dry Run)
* **Tipe:** 1 Laptop Sendiri (Buka 2 Jendela Terminal).
* **Kenapa harus ini:**
  Memastikan antarmuka baru (input sandi, pemilihan folder, pemilihan level kompresi, tabel lobby worker, dan rekap feedback report) berjalan lancar sebelum bertemu teman-teman.
* **Perintah Terminal:**
  1. **Terminal 1 (Master):**
     ```powershell
     python master.py
     ```
     - Masukkan sandi klaster (contoh: `kpt2026` atau tekan ENTER).
     - Masukkan path folder: `data/sample`
     - Pilih level kompresi: `6` (Normal)
     - Master akan masuk mode *LOBBY* menunggu worker.
  2. **Terminal 2 (Simulasi Worker):**
     ```powershell
     python worker.py
     ```
     - Masukkan sandi yang sama.
     - Worker langsung terhubung ke Master via localhost.
  3. Di Terminal 1 (Master), tekan **[ENTER]** untuk menjalankan kompresi.
* **Yang Harus Di-Screenshot:**
  - Tampilan *Waiting Lobby* saat worker terdeteksi (tabel spesifikasi CPU & RAM worker).
  - Tampilan *Feedback Report* setelah selesai (rekap integritas SHA-256 100% match).

---

## FASE 2: DIKERJAKAN BARENG TEMAN (KLASTER 10 LAPTOP)

> **Persiapan Sebelum Mulai:**
> 1. Pastikan **semua 10 laptop terhubung ke 1 jaringan Wi-Fi / Hotspot yang sama**.
> 2. Laptop Master menjalankan: `python master.py`.
> 3. Laptop 9 Teman (Worker) cukup menjalankan: `python worker.py`.
> 4. Begitu teman memasukkan sandi Master, sistem otomatis menemukan IP Master via UDP Broadcast.

---

### LANGKAH 3: Skenario f — Uji Skalabilitas 1 Worker
* **Tipe:** Berdua (1 Master + 1 Laptop Teman).
* **Kenapa harus ini:** Titik awal pengujian komputasi terdistribusi dengan 1 node komputasi tunggal.
* **Perintah:**
  - **Master:** `python master.py` (pilih `data/sample`, level `6`).
  - **Teman 1:** `python worker.py` (masukkan sandi).
  - Saat tabel lobby Master menampilkan **1 Worker**, tekan **[ENTER]**.
* **Yang Harus Di-Screenshot:**
  - Tabel rekapitulasi akhir di Master (catat waktu eksekusinya).
* **Kompilasi Data:**
  ```powershell
  python benchmark/run_distributed.py --summary results/output/summary.json --t1 1.25 --num-workers 1 --scenario f
  ```
  *(Catatan: Ganti `1.25` dengan nilai $T_1$ yang didapat dari Langkah 1).*

---

### LANGKAH 4: Skenario f — Uji Skalabilitas 2 Worker
* **Tipe:** Bertiga (1 Master + 2 Laptop Teman).
* **Kenapa harus ini:** Mengukur peningkatan performa saat beban kerja dibagi ke 2 laptop fisik.
* **Perintah:**
  - **Master:** `python master.py`
  - **Teman 1 & 2:** `python worker.py`
  - Di lobby Master terlihat **2 Worker** $\rightarrow$ tekan **[ENTER]**.
* **Yang Harus Di-Screenshot:**
  - Rekap pembagian tugas antara 2 worker.
* **Kompilasi Data:**
  ```powershell
  python benchmark/run_distributed.py --summary results/output/summary.json --t1 1.25 --num-workers 2 --scenario f
  ```

---

### LANGKAH 5: Skenario f — Uji Skalabilitas 4 Worker
* **Tipe:** Berlima (1 Master + 4 Laptop Teman).
* **Kenapa harus ini:** Mengukur skalabilitas klaster skala menengah.
* **Perintah:**
  - **Master:** `python master.py`
  - **Teman 1 s.d. 4:** `python worker.py`
  - Di lobby Master terlihat **4 Worker** $\rightarrow$ tekan **[ENTER]**.
* **Kompilasi Data:**
  ```powershell
  python benchmark/run_distributed.py --summary results/output/summary.json --t1 1.25 --num-workers 4 --scenario f
  ```

---

### LANGKAH 6: Skenario f — Uji Skalabilitas 6 Worker
* **Tipe:** Bertujuh (1 Master + 6 Laptop Teman).
* **Kenapa harus ini:** Mengamati grafik kurva Amdahl saat jumlah node mendekati kapasitas penuh.
* **Perintah:**
  - **Master:** `python master.py`
  - **Teman 1 s.d. 6:** `python worker.py`
  - Di lobby Master terlihat **6 Worker** $\rightarrow$ tekan **[ENTER]**.
* **Kompilasi Data:**
  ```powershell
  python benchmark/run_distributed.py --summary results/output/summary.json --t1 1.25 --num-workers 6 --scenario f
  ```

---

### LANGKAH 7: Skenario c & f — Uji Klaster Maksimal (Master + 9 Worker)
* **Tipe:** Klaster Lengkap (10 Laptop: 1 Master + 9 Laptop Teman).
* **Kenapa harus ini:**
  Ini adalah **puncak implementasi sistem terdistribusi (Skenario c & f titik akhir)** yang membuktikan sistem berjalan di 10 node fisik secara bersamaan.
* **Perintah:**
  - **Master:** `python master.py`
  - **9 Teman:** Semuanya jalankan `python worker.py`
  - Tunggu hingga lobby Master menampilkan penuh **9 Worker** $\rightarrow$ tekan **[ENTER]**.
* **Yang Wajib Di-Screenshot & Direkam Video:**
  - **Tangkapan Layar:** Tabel Lobby menampilkan 9 Worker lengkap dengan IP dan Core masing-masing.
  - **Tangkapan Layar:** Dashboard saat ke-9 worker aktif mengompres chunk secara paralel.
  - **Tangkapan Layar:** Rekapitulasi Akhir (Total Chunk, Throughput MB/s, Integritas 100% SHA-256).
* **Kompilasi Data:**
  ```powershell
  python benchmark/run_distributed.py --summary results/output/summary.json --t1 1.25 --num-workers 9 --scenario f
  python benchmark/run_distributed.py --summary results/output/summary.json --t1 1.25 --num-workers 9 --scenario c
  ```

---

### LANGKAH 8: Skenario d — Uji Load Balancing (Dynamic vs Static)
* **Tipe:** Klaster Lengkap (10 Laptop).
* **Kenapa harus ini:**
  Membuktikan keunggulan *Dynamic Task-Stealing* dibanding *Static Chunk Allocation*, terutama saat ada laptop teman yang spesifikasinya lebih lambat.
* **Pengujian:**
  1. Data **Dynamic Scheduling** sudah otomatis didapat dari Langkah 7 di atas!
  2. Sekarang uji **Static Scheduling (Pembagian rata di awal)**:
     - **Master:**
       ```powershell
       python master.py --mode static --expected-workers 9
       ```
     - **9 Teman:** Jalankan `python worker.py`
     - Tekan **[ENTER]** saat 9 worker siap.
* **Yang Harus Di-Screenshot:**
  - Tabel distribusi beban tugas per worker (pada Static, worker cepat akan menganggur menunggu worker lambat selesai).
* **Kompilasi Data:**
  ```powershell
  python benchmark/run_distributed.py --summary results/output/summary.json --t1 1.25 --num-workers 9 --scenario d --mode static
  ```

---

### LANGKAH 9: Skenario e — Uji Fault Tolerance (Simulasi Worker Crash / Putus Wi-Fi)
* **Tipe:** Klaster Lengkap (10 Laptop).
* **Kenapa harus ini:**
  Mendemonstrasikan ketahanan sistem (*Fault Tolerance*): ketika 1 node mati di tengah kompresi, Master tidak boleh crash, chunk dialihkan ke node lain (*re-queue*), dan hasil dekompresi tetap 100% utuh tanpa korupsi data.
* **Langkah Uji:**
  1. **Master:** `python master.py`
  2. **9 Teman:** Jalankan `python worker.py`
  3. Master tekan **[ENTER]**.
  4. Saat progres kompresi berjalan sekitar **30% - 50%**, minta **1 laptop teman sengaja mematikan Wi-Fi atau menutup jendela terminalnya**.
  5. Perhatikan terminal Master:
     - Master mendeteksi heartbeat hilang (*Worker Timeout*).
     - Master menandai status worker `CRASHED/OFFLINE`.
     - Master menarik kembali (*re-queue*) tugas yang sedang dikerjakan teman tersebut dan memberikannya ke laptop teman lain yang masih hidup.
     - Kompresi selesai dengan sukses!
* **Yang Wajib Di-Screenshot & Direkam Video:**
  - Log peringatan di Master: `Worker ... heartbeat timeout / disconnect`.
  - Notifikasi Re-queue chunk task.
  - Laporan pemulihan (*Recovery Time*) dan verifikasi bahwa file output tetap 100% valid.
* **Kompilasi Data:**
  ```powershell
  python benchmark/run_distributed.py --summary results/output/summary.json --t1 1.25 --num-workers 9 --scenario e
  ```

---

## FASE 3: EKSTRAK GRAFIK & TABEL FINAL UNTUK LAPORAN

### LANGKAH 10: Generate Seluruh Grafik & Tabel Klaster Terdistribusi
* **Tipe:** 1 Laptop Master.
* **Kenapa harus ini:**
  Mengubah seluruh data JSON hasil pengujian Langkah 3 sampai Langkah 9 menjadi grafik profesional (.PNG) beresolusi tinggi dan tabel (.CSV) untuk Bab IV Laporan.
* **Perintah Terminal:**
  ```powershell
  python benchmark/analyze.py --input results/experiments_full.json
  ```
* **Grafik yang Langsung Dihasilkan di Folder `results/grafik/`:**
  1. `exec_time_scalability.png` $\rightarrow$ Grafik Waktu Eksekusi vs Jumlah Worker (1, 2, 4, 6, 9).
  2. `speedup_scalability.png` $\rightarrow$ Grafik Peningkatan Kecepatan (Speedup) Klaster.
  3. `efficiency_scalability.png` $\rightarrow$ Grafik Efisiensi Komputasi Paralel.
  4. `throughput_scalability.png` $\rightarrow$ Grafik Kecepatan Kompresi (MB/s).
  5. `load_balance_comparison.png` $\rightarrow$ Grafik Komparasi Beban: Dynamic vs Static Scheduling.
  6. `fault_tolerance.png` $\rightarrow$ Grafik Timeline Pemulihan (*Recovery Time*) saat Node Crash.
* **Tabel yang Langsung Dihasilkan di Folder `results/`:**
  - `tabel_scalability.csv`
  - `tabel_fault_tolerance.csv`
  - `tabel_distributed_baseline.csv`

---

## TIPS PENGAMBILAN VIDEO DEMO (DURASI 10 - 15 MENIT)

Sesuai ketentuan penugasan di PDF:
1. **Menit 01 - 03 (Pengenalan & Arsitektur):** Jelaskan arsitektur Master-Worker, Socket TCP, Auto-discovery UDP, dan protokol kompresi chunk.
2. **Menit 04 - 08 (Demo Klaster 10 Node):** Tunjukkan layar Master saat 9 Worker terhubung di lobby, lalu jalankan kompresi. Sorot beberapa laptop teman yang sedang bekerja.
3. **Menit 09 - 11 (Demo Load Balancing & Fault Tolerance):** Tunjukkan perbandingan dynamic vs static dan peragakan 1 laptop teman dimatikan Wi-Fi-nya, tunjukkan Master otomatis memulihkan task.
4. **Menit 12 - 15 (Verifikasi Integritas & Grafik):** Tunjukkan verifikasi SHA-256 integrity 100% match dan tampilkan grafik hasil `analyze.py`.
