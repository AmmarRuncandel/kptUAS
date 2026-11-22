# Spesifikasi Protokol Master–Worker

## Gambaran Umum

Protokol komunikasi berbasis **TCP socket** dengan **framing length-prefixed**
(8-byte header, big-endian uint64). Pesan kontrol berformat JSON; payload data
biner dikirim sebagai frame terpisah (tidak di-base64).

---

## Format Kawat (Wire Format)

```
Tiap pesan terdiri dari satu atau dua frame:

Frame JSON (selalu ada):
  ┌──────────────────┬──────────────────────────┐
  │  8 byte          │  N byte                  │
  │  panjang JSON    │  JSON payload (UTF-8)    │
  │  (big-endian u64)│                          │
  └──────────────────┴──────────────────────────┘

Frame Binary (opsional, jika has_binary=true):
  ┌──────────────────┬──────────────────────────┐
  │  8 byte          │  M byte                  │
  │  panjang binary  │  data biner              │
  │  (big-endian u64)│                          │
  └──────────────────┴──────────────────────────┘
```

**Implementasi**: `common/protocol.py` → `send_message()`, `recv_message()`

---

## Tipe Pesan

### REGISTER (Worker → Master)
```json
{
  "type": "REGISTER",
  "worker_id": "w01",
  "hostname": "pc-lab-01",
  "ip": "192.168.1.11",
  "num_cores": 8,
  "ram_gb": 16.0,
  "num_procs": 8,
  "has_binary": false
}
```

### REGISTER_ACK (Master → Worker)
```json
{
  "type": "REGISTER_ACK",
  "worker_id": "w01",
  "mode": "dynamic",
  "transfer": "socket",
  "has_binary": false
}
```

### REQUEST_TASK (Worker → Master)
```json
{
  "type": "REQUEST_TASK",
  "worker_id": "w01",
  "active_tasks": 3,
  "has_binary": false
}
```

### TASK_ASSIGN (Master → Worker)
```json
{
  "type": "TASK_ASSIGN",
  "task": {
    "task_id": "uuid4-string",
    "file_path": "/data/sample/file.txt",
    "chunk_index": 0,
    "chunk_count": 3,
    "offset": 0,
    "length": 33554432,
    "algorithm": "lzma",
    "level": 6,
    "status": "ASSIGNED",
    "attempts": 1,
    "assigned_worker": "w01",
    "created_at": 1700000000.0
  },
  "transfer_mode": "socket",
  "has_binary": true,
  "binary_length": 33554432
}
```
*Frame binary menyusul: bytes mentah chunk file (length = task.length)*

### TASK_RESULT (Worker → Master)
```json
{
  "type": "TASK_RESULT",
  "result": {
    "task_id": "uuid4-string",
    "original_size": 33554432,
    "compressed_size": 12000000,
    "ratio": 2.796,
    "duration": 4.21,
    "worker_id": "w01",
    "sha256_original": "abc123...",
    "sha256_compressed": "def456...",
    "status": "DONE",
    "algorithm": "lzma",
    "level": 6
  },
  "has_binary": true,
  "binary_length": 12000096
}
```
*Frame binary menyusul: container format (header 96 byte + data terkompresi)*

### HEARTBEAT (Worker → Master)
```json
{
  "type": "HEARTBEAT",
  "worker_id": "w01",
  "cpu_pct": 87.5,
  "mem_pct": 42.1,
  "active_tasks": 4,
  "has_binary": false
}
```

### HEARTBEAT_ACK (Master → Worker)
```json
{
  "type": "HEARTBEAT_ACK",
  "has_binary": false
}
```

### NO_MORE_TASKS (Master → Worker)
```json
{
  "type": "NO_MORE_TASKS",
  "wait": false,
  "has_binary": false
}
```
*(wait=true: masih ada task dikerjakan Worker lain; Worker tunggu sebentar)*

### SHUTDOWN (Master → Worker)
```json
{
  "type": "SHUTDOWN",
  "has_binary": false
}
```

### ERROR (Bidirectional)
```json
{
  "type": "ERROR",
  "message": "Deskripsi error",
  "has_binary": false
}
```

---

## Container Format Hasil Kompresi

Setiap chunk yang dikembalikan oleh Worker dibungkus dalam container format
(didefinisikan di `common/compression.py`):

```
Offset | Ukuran | Isi
-------|--------|------
0      | 4B     | Magic bytes: "DCMP"
4      | 1B     | Versi format (= 1)
5      | 1B     | Kode algoritma (1=lzma, 2=bz2, 3=gzip, 4=zlib, 5=zstd)
6      | 1B     | Level kompresi
7      | 1B     | Reserved (= 0)
8      | 8B     | Ukuran data asli (big-endian uint64)
16     | 8B     | Ukuran data terkompresi (big-endian uint64)
24     | 32B    | SHA-256 data asli
56     | 32B    | SHA-256 data terkompresi
88     | 4B     | Chunk index (big-endian uint32)
92     | 4B     | Chunk count (big-endian uint32)
96     | N      | Data terkompresi
```

Total header: **96 byte**. Diikuti langsung oleh data terkompresi.

Untuk file yang terdiri dari beberapa chunk, container format setiap chunk
disimpan secara berurutan dalam satu file `.compressed`.
Rekonstruksi: baca tiap container secara berurutan, dekompres, gabung.

---

## Konfigurasi Port dan Firewall

| Item | Nilai Default |
|------|--------------|
| Port Master TCP | 9000 |
| Protokol | TCP |

**Windows (PowerShell)**:
```powershell
New-NetFirewallRule -DisplayName "DC-Master" -Direction Inbound -Protocol TCP -LocalPort 9000 -Action Allow
```

**Linux (ufw)**:
```bash
ufw allow 9000/tcp
```

**Cek konektivitas dari Worker ke Master**:
```bash
# Windows
Test-NetConnection -ComputerName 192.168.1.10 -Port 9000
# Linux
nc -zv 192.168.1.10 9000
```
