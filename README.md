# Pub-Sub Log Aggregator Terdistribusi

UAS Sistem Terdistribusi — Pub-Sub Log Aggregator dengan Idempotent Consumer,
Deduplication, dan Transaksi/Kontrol Konkurensi. Stack: **Python (FastAPI) +
Redis Streams (broker) + PostgreSQL (storage)**, dijalankan dengan **Docker Compose**.

> 🎥 **Link video demo:** [ISI DENGAN LINK YOUTUBE UNLISTED/PUBLIC KAMU DI SINI]
> 
> ⏱️ **Durasi video: minimal 25 menit**
> 
> Checklist konten video (WAJIB ditampilkan):
> - [ ] Arsitektur multi-service dan alasan desain
> - [ ] Proses build dan jalankan `docker compose up --build`
> - [ ] Publikasi 20.000 event dengan 30% duplikasi
> - [ ] Bukti idempotency: event duplikat hanya tersimpan sekali
> - [ ] Multi-worker (`--scale worker=3`) tanpa race condition
> - [ ] `GET /stats` dan `GET /events` menunjukkan konsistensi
> - [ ] Crash recovery: hapus container/data → jalankan ulang → data persisten
> - [ ] Metrik throughput, latency, dan hasil statistik final

## 1. Arsitektur

```
                 ┌──────────────┐        XADD         ┌──────────────┐
  publisher ───► │  aggregator  │ ──────────────────►  │ broker (redis)│
 (generate event,│ (FastAPI API)│   events_stream      │ Stream + grup │
  termasuk dup)   └──────┬───────┘                      │  consumer    │
                         │ UPDATE stats.received         └──────┬───────┘
                         ▼                                       │ XREADGROUP
                 ┌──────────────┐                                ▼
                 │   storage    │ ◄──── INSERT ON CONFLICT ─ ┌────────┐
                 │  (postgres)  │       DO NOTHING +         │ worker │ (bisa di-scale,
                 │ processed_   │       UPDATE stats         │ (consumer) │  banyak instance)
                 │ events, stats│       (1 transaksi atomik) └────────┘
                 └──────────────┘
```

**Kenapa dipisah jadi 4 service (bukan 1 monolith)?**
- `aggregator` = lapisan API (publish + query) — stateless, mudah di-scale horizontal.
- `worker` = consumer idempotent — dipisah dari API supaya bisa dijalankan
  **banyak instance sekaligus** (`--scale worker=3`) tanpa mengubah aggregator,
  inilah yang membuktikan ketahanan terhadap race condition (Bab 8–9).
- `broker` (Redis Stream + consumer group) = decoupling antara penerimaan event
  dan pemrosesannya → aggregator tetap responsif walau worker lambat/down sebentar.
- `storage` (Postgres) = satu-satunya sumber kebenaran (source of truth) yang
  persisten, dengan **unique constraint (topic, event_id)** sebagai inti dedup.

Semua service berada di Docker network `internal` (bridge lokal); satu-satunya
port yang diexpose ke host adalah `8080` (aggregator), hanya untuk keperluan
demo dari komputer lokal — bukan akses publik.

## 2. Model Event

```json
{
  "topic": "orders",
  "event_id": "evt-123",
  "timestamp": "2026-06-19T10:00:00Z",
  "source": "publisher-1",
  "payload": { "amount": 100 }
}
```

## 3. Endpoint API (aggregator, port 8080)

| Method | Path             | Keterangan                                                        |
|--------|------------------|--------------------------------------------------------------------|
| GET    | `/health`        | Readiness/liveness check (cek koneksi DB + Redis)                  |
| POST   | `/publish`       | Terima 1 event atau batch (list event); validasi skema otomatis    |
| GET    | `/events?topic=` | Daftar event UNIK yang sudah diproses (opsional filter per topic)  |
| GET    | `/stats`         | `received`, `unique_processed`, `duplicate_dropped`, `topics`, `uptime_seconds` |

Contoh:
```bash
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{"topic":"orders","event_id":"evt-1","timestamp":"2026-06-19T10:00:00Z","source":"manual","payload":{}}'

curl http://localhost:8080/events?topic=orders
curl http://localhost:8080/stats
```

## 4. Cara Build & Run

```bash
# Jalankan seluruh stack (storage, broker, aggregator, 1 worker, lalu publisher
# otomatis mengirim 20.000 event dengan 30% duplikasi)
docker compose up --build

# Untuk demo KONKURENSI (poin penting di rubrik): jalankan 3 worker sekaligus
docker compose up --build --scale worker=3

# Kirim event manual tambahan / jalankan publisher lagi kapan saja:
docker compose run --rm publisher

# Cek hasil:
curl http://localhost:8080/stats
curl http://localhost:8080/events
```

### Bukti persistensi (wajib ditunjukkan di video demo)
```bash
docker compose stop storage
docker compose rm -f storage
docker compose up -d storage         # container baru, volume pg_data tetap sama
curl http://localhost:8080/stats     # data tetap ada, tidak ter-reset ke 0
```

### Bukti idempotency/dedup terhadap restart
```bash
# kirim event yang sama dua kali, restart aggregator/worker, kirim lagi event yang sama
docker compose restart aggregator worker
curl -X POST http://localhost:8080/publish -d '...event yang sama...'
# /stats -> duplicate_dropped bertambah, BUKAN unique_processed
```

## 5. Lokasi Persistensi Data

| Data            | Mekanisme              | Volume         |
|-----------------|-------------------------|----------------|
| Tabel Postgres  | Named volume            | `pg_data`      |
| Redis (AOF)     | Named volume            | `broker_data`  |

Kedua volume didefinisikan di `docker-compose.yml` dan **tidak ikut terhapus**
saat container di-`rm`/recreate — hanya hilang jika volume dihapus eksplisit
(`docker compose down -v`).

## 6. Idempotency, Dedup & Transaksi (ringkasan teknis)

- Dedup dijamin oleh **unique constraint `(topic, event_id)`** pada tabel
  `processed_events` (lihat `storage/init.sql`).
- Insert dedup + update counter statistik dilakukan dalam **satu statement SQL
  atomik** (CTE `INSERT ... ON CONFLICT DO NOTHING` + `UPDATE stats`), lihat
  `worker/worker.py` — ini menghindari race condition maupun lost-update
  walaupun banyak worker berjalan paralel.
- Isolation level: **READ COMMITTED** (default Postgres) — cukup aman karena
  proteksi dedup dilakukan lewat unique constraint, bukan lewat snapshot read.
  Pembahasan lebih lengkap ada di `report.md` (T8 & T9).
- Delivery semantics: **at-least-once** (Redis consumer group + manual XACK
  setelah transaksi DB sukses) dikombinasikan dengan **idempotent consumer**
  di sisi worker → hasil akhir setara *effectively-exactly-once*.

## 7. Menjalankan Test

```bash
pip install -r tests/requirements.txt -r aggregator/requirements.txt

# Test unit (TANPA Docker, validasi skema)
pytest tests/unit -v

# Test integration (Docker HARUS sudah jalan: docker compose up --build)
pytest tests/integration -v

# Semua test
pytest
```

Total 20 test (7 unit + 13 integration), cakupan: validasi skema, dedup,
konkurensi (multi-thread publish event sama → hasil tetap satu baris),
konsistensi `/stats` & `/events`, dan stress kecil (batch 1000 event).

## 8. Asumsi & Keputusan Desain

- Ordering total tidak diperlukan: aggregator didesain idempotent berbasis
  key `(topic, event_id)`, bukan berbasis urutan kedatangan, sehingga event
  yang datang out-of-order tetap aman diproses.
- Kebijakan batch `/publish`: setiap item dalam batch divalidasi skema secara
  serentak oleh FastAPI (jika ada satu item invalid, seluruh request ditolak
  dengan 422 — fail fast di level HTTP). Setelah lolos validasi, setiap event
  di-enqueue ke Redis secara independen; status `queued`/`rejected` per item
  dikembalikan ke caller.
- Publisher mengirim **≥ 20.000 event dengan ≥ 30% duplikasi** secara konkuren
  (lihat `publisher/publisher.py`), memenuhi syarat performa minimum.

## 9. Struktur Repository

```
.
├── aggregator/        # FastAPI API (publish, events, stats, health)
├── worker/            # Consumer idempotent (dedup + transaksi)
├── publisher/         # Generator event (termasuk duplikat) untuk uji beban
├── storage/init.sql   # Skema database
├── tests/
│   ├── unit/          # Test skema, tanpa Docker
│   └── integration/   # Test end-to-end, butuh docker compose up
├── docker-compose.yml
├── pytest.ini
├── README.md
└── report.md          # Laporan teori (Bab 1-13) + analisis performa
```
