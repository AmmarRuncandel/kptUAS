# Arsitektur Sistem – Distributed Data Compression

## Gambaran Umum

Sistem ini menggunakan arsitektur **Master–Worker** untuk kompresi file terdistribusi:
- **1 Master Node**: memecah pekerjaan, mendistribusikan task, mengumpulkan hasil.
- **9 Worker Node**: menerima chunk file, mengompresi secara paralel (multicore), mengembalikan hasil.

---

## Diagram Topologi Jaringan

```mermaid
graph TD
    M["🖥️ Master Node\n(192.168.1.10:9000)\nScheduler + Collector\nFault Monitor"]
    W1["💻 Worker w01\n192.168.1.11"]
    W2["💻 Worker w02\n192.168.1.12"]
    W3["💻 Worker w03\n192.168.1.13"]
    W4["💻 Worker w04\n192.168.1.14"]
    W5["💻 Worker w05\n192.168.1.15"]
    W6["💻 Worker w06\n192.168.1.16"]
    W7["💻 Worker w07\n192.168.1.17"]
    W8["💻 Worker w08\n192.168.1.18"]
    W9["💻 Worker w09\n192.168.1.19"]
    SW["🔀 Network Switch\n(Kabel / LAN)"]

    M --- SW
    SW --- W1
    SW --- W2
    SW --- W3
    SW --- W4
    SW --- W5
    SW --- W6
    SW --- W7
    SW --- W8
    SW --- W9
```

---

## Arsitektur Komponen

```mermaid
graph LR
    subgraph MASTER["Master Node"]
        DEC["Task Decomposition\n(common/tasks.py)"]
        SCHED["Scheduler\n(master/scheduler.py)\nmode: static | dynamic"]
        FM["Fault Monitor\n(master/fault_monitor.py)\nHeartbeat + Requeue"]
        COLL["Result Collector\n(master/collector.py)\nAtomic Write + SHA-256"]
        TCP["TCP Server\n(master/master.py)\nThread per Worker"]
    end

    subgraph WORKER["Worker Node (×9)"]
        WC["TCP Client\n(worker/worker.py)\nAuto-reconnect"]
        EXEC["Process Pool Executor\n(worker/executor.py)\nmulticore CPU-bound"]
        MON["Resource Monitor\n(worker/monitor.py)\npsutil CPU/Mem"]
    end

    subgraph BENCH["Benchmark & Docs"]
        SEQ["sequential.py"]
        LP["local_parallel.py"]
        EXP["experiments.py"]
        ANA["analyze.py → grafik PNG"]
        VER["verify.py → SHA-256 check"]
    end

    DEC --> SCHED
    SCHED --> TCP
    TCP <-->|"TCP Socket\nJSON + Binary"| WC
    WC --> EXEC
    EXEC --> WC
    WC --> MON
    TCP --> COLL
    FM --> SCHED
```

---

## Sequence Diagram Alur Kerja

```mermaid
sequenceDiagram
    participant W as Worker
    participant M as Master

    W->>M: REGISTER {worker_id, cores, procs, RAM}
    M->>W: REGISTER_ACK {mode, transfer}

    loop Selagi ada task
        W->>M: REQUEST_TASK {active_tasks}
        M->>W: TASK_ASSIGN {task_dict} + [binary chunk]
        Note over W: compress_chunk_task()<br/>ProcessPoolExecutor<br/>(CPU-bound paralel)
        W->>M: TASK_RESULT {result_dict} + [compressed bytes]
        M->>M: Simpan atomik + update statistik
    end

    W->>M: HEARTBEAT {cpu%, mem%, active_tasks}  [tiap 2 detik]
    M->>W: HEARTBEAT_ACK

    Note over M: Jika Worker tidak heartbeat > 10s → DEAD
    M->>M: requeue_worker_tasks(worker_id)
    Note over M: Task dikembalikan ke antrean

    M->>W: NO_MORE_TASKS  [saat semua task selesai]
    M->>W: SHUTDOWN
```

---

## Flowchart Scheduler (Dynamic Load Balancing)

```mermaid
flowchart TD
    START([Worker kirim REQUEST_TASK]) --> CHECK_MODE{Mode?}

    CHECK_MODE -->|static| STATIC[Ambil dari antrean\npersonal Worker\nround-robin]
    CHECK_MODE -->|dynamic| CHECK_QUEUE{Antrean\nkosong?}

    CHECK_QUEUE -->|Ya| NO_TASK([Kirim NO_MORE_TASKS])
    CHECK_QUEUE -->|Tidak| CHECK_BUSY{Worker\nsangat sibuk?\nCPU > 90%}

    CHECK_BUSY -->|Ya| SMALL[Cari task terkecil\ndi antrean]
    CHECK_BUSY -->|Tidak| LPT[Ambil task terbesar\nLPT = kepala antrean]

    SMALL --> ASSIGN[Tandai ASSIGNED\nupdate statistik]
    LPT   --> ASSIGN
    STATIC --> ASSIGN

    ASSIGN --> SEND([Kirim TASK_ASSIGN\n+ binary chunk])
```

---

## Komponen Penilaian

| ID | Komponen | Implementasi |
|----|----------|-------------|
| a | Task Decomposition | `common/tasks.py` → `decompose_directory()` |
| b | Parallel Processing | `worker/executor.py` → `ProcessPoolExecutor` |
| c | Distributed Processing | `common/protocol.py`, `master/master.py`, `worker/worker.py` |
| d | Dynamic Load Balancing | `master/scheduler.py` → mode static vs dynamic + LPT |
| e | Fault Tolerance | `master/fault_monitor.py` → heartbeat + requeue + recovery time |
| f | Performance Evaluation | `benchmark/` → sequential, local_parallel, scalability |

---

## Keputusan Desain

| Aspek | Pilihan | Alasan |
|-------|---------|--------|
| Algoritma default | `lzma` preset 6 | CPU-bound terkuat; manfaat paralel paling terlihat |
| Framing TCP | 8-byte length prefix (big-endian uint64) | Mendukung payload hingga 2 GB; sederhana |
| Format transfer | Binary terpisah (bukan base64) | Efisiensi bandwidth; menghindari overhead encoding |
| Load balancing | Pull-based + LPT | Worker cepat otomatis menerima lebih banyak task |
| Idempotency | Cek task_id di set received_ids | Mencegah duplikat dari Worker yang kembali hidup |
| Penulisan output | atomic_write (tmp + rename) | Mencegah file rusak jika Master mati di tengah tulis |
| Pengujian Windows | `freeze_support()` + top-level function | Kompatibel dengan ProcessPoolExecutor di Windows |
