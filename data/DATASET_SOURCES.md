# Sumber Dataset Resmi

## Dataset yang Digunakan

Semua dataset yang dipakai dalam proyek ini bersumber dari sumber resmi dan
berlisensi open-source / Public Domain.

---

### 1. Canterbury Corpus

| Field     | Nilai |
|-----------|-------|
| **Nama**  | Canterbury Corpus |
| **URL**   | https://corpus.canterbury.ac.nz/resources/cantrbry.tar.gz |
| **Lisensi** | Public Domain |
| **Ukuran** | ~2.8 MB (11 file teks/biner campuran) |
| **Tanggal Akses** | 2024 (isi sesuai tanggal download aktual) |
| **Cara Pakai** | Digunakan sebagai dataset benchmark kompresi standar industri |

**Deskripsi:**
Canterbury Corpus adalah kumpulan file yang dipakai secara luas sebagai
benchmark kompresi data. Berisi berbagai tipe: teks, C source code, HTML,
spreadsheet, executable, dan gambar BMP.

---

### 2. Project Gutenberg – The Bible (pg10.txt)

| Field     | Nilai |
|-----------|-------|
| **Nama**  | The King James Bible |
| **URL**   | https://www.gutenberg.org/cache/epub/10/pg10.txt |
| **Lisensi** | Public Domain |
| **Ukuran** | ~4.4 MB |
| **Tanggal Akses** | 2024 |
| **Cara Pakai** | File teks besar untuk menguji kompresi teks berulang |

---

### 3. Project Gutenberg – War and Peace (pg2600.txt)

| Field     | Nilai |
|-----------|-------|
| **Nama**  | War and Peace (Leo Tolstoy) |
| **URL**   | https://www.gutenberg.org/cache/epub/2600/pg2600.txt |
| **Lisensi** | Public Domain |
| **Ukuran** | ~3.3 MB |
| **Tanggal Akses** | 2024 |
| **Cara Pakai** | File teks besar, cocok untuk pengujian chunking |

---

### 4. Data Sintetis (generate_synthetic.py)

| Field     | Nilai |
|-----------|-------|
| **Nama**  | Synthetic Test Data |
| **Pembuat** | Tim proyek (skrip generate_synthetic.py) |
| **Lisensi** | N/A (data dibuat sendiri) |
| **Ukuran** | Dikonfigurasi lewat parameter (default: ~1 GB campuran) |
| **Cara Pakai** | Fallback jika dataset resmi tidak bisa diunduh; digunakan untuk |
|             | pengujian skalabilitas yang membutuhkan data dalam jumlah besar |

> **CATATAN:** Data sintetis dinyatakan secara eksplisit sebagai "DATA SINTETIS"
> di semua laporan dan output log.

---

## Catatan Penggunaan

- Semua dataset di atas berlisensi **Public Domain** atau bebas digunakan
  untuk tujuan akademik/penelitian.
- Silahkan verifikasi ulang URL sebelum dipakai karena URL dapat berubah.
- Untuk reproducibility, simpan SHA-256 checksum file yang diunduh.

---

## Referensi

- Canterbury Corpus: Arnold, R. & Bell, T. (1997). *A Corpus for the Evaluation
  of Lossless Compression Algorithms*. DCC '97.
- Project Gutenberg: https://www.gutenberg.org/
