# 📹 SCRIPT VIDEO DEMO (25 Menit)
## Pub-Sub Log Aggregator Terdistribusi

---

## **PERSIAPAN SEBELUM RECORDING**

```bash
# Di terminal 1, bersihkan data lama
cd C:\SISTER2\UAAS\uas-pubsub-aggregator
docker compose down -v

# Pastikan seluruh image sudah ter-build
docker compose build

# Buka PowerShell/terminal untuk recording
# Pastikan punya: curl.exe atau gunakan PowerShell Invoke-WebRequest
```

---

## **SEGMENT 1: INTRO & ARSITEKTUR (0:00 - 2:30)**

### Intro (0:00 - 0:15)
```
[Tatap kamera, senyum]

"Halo, saya akan mendemonstrasikan sistem Pub-Sub Log Aggregator Terdistribusi 
untuk UAS Sistem Paralel dan Terdistribusi. Sistem ini dibangun dengan Python, 
FastAPI, Redis, dan PostgreSQL menggunakan Docker Compose.

Dalam video ini saya akan menunjukkan:
- Arsitektur multi-service dan alasan desainnya
- Bagaimana sistem menangani duplikasi event dengan idempotency
- Konkurensi: membuktikan tidak ada race condition saat multi-worker
- Persistensi data: crash recovery menggunakan named volume
- Dan menganalisis performa sistem saat memproses 20.000 event."
```

### Arsitektur Diagram & Penjelasan (0:15 - 2:30)
```
[Buka README.md di editor, arahkan ke section "1. Arsitektur"]

"Sistem ini terdiri dari 4 service utama yang berjalan dalam Docker Compose:

1. **AGGREGATOR** (port 8080)
   - Lapisan API yang menerima event dari publisher
   - Endpoint: /publish (terima event), /events (query event unik), 
     /stats (metrik), /health (readiness check)
   - Stateless, bisa di-scale horizontal

2. **PUBLISHER**
   - Generator event untuk simulasi beban
   - Mengirim 20.000 event dengan 30% duplikasi secara konkuren
   - Setelah selesai, container exit

3. **BROKER** (Redis Stream)
   - Antrian event dengan consumer group
   - Menjamin at-least-once delivery
   - AOF persistent, disimpan di named volume 'broker_data'

4. **WORKER** (Consumer)
   - Membaca event dari Redis Stream
   - Memproses dengan idempotent insert (ON CONFLICT DO NOTHING)
   - Bisa di-scale: --scale worker=3 untuk menjalankan 3 instance sekaligus
   - UPDATE statistik secara transaksional dalam satu statement SQL atomik

5. **STORAGE** (PostgreSQL)
   - Tabel processed_events dengan PRIMARY KEY (topic, event_id)
   - Constraint unik ini adalah kunci deduplication
   - Tabel stats untuk tracking received/unique_processed/duplicate_dropped
   - Data persisten di named volume 'pg_data'

Semua service berada dalam Docker network 'internal' yang tertutup.
Hanya port 8080 (aggregator) yang diexpose ke host untuk demo lokal.
Tidak ada akses ke layanan eksternal publik — semuanya internal."

[Pause, lihat diagram]

"Alasan desain ini:
- Pub-Sub dipilih karena decoupling waktu dan ruang
  (publisher tidak perlu menunggu worker selesai)
- Atomicity (CTE + ON CONFLICT) mencegah race condition
- Eventual consistency tapi tetap dapat diandalkan berkat idempotency
- Scalability: worker bisa ditambah tanpa mengubah aggregator atau publisher"
```

---

## **SEGMENT 2: BUILD & JALANKAN SISTEM (2:30 - 6:00)**

### Perintah Build (2:30 - 4:00)
```
[Buka PowerShell terminal, arahkan ke project directory]

"Sekarang saya akan membangun dan menjalankan sistem dengan Docker Compose."

[Ketik/paste command ini]
docker compose up --build

[Tunggu sampai:
- Image berhasil di-build
- Services mulai start
- Lihat log: "Container ... Healthy" untuk storage dan broker
- Lihat log aggregator: "Application startup complete"
- Lihat log worker: "worker ... siap"
- Publisher mulai menjalankan publikasi event
]

[Saat publisher berjalan, baca:]

"Sekarang publisher sedang mengirimkan 20.000 event ke aggregator dengan 30% 
duplikasi. Perhatikan di log:

- 'publisher' menunjukkan: total dikirim, throughput (event/detik), 
  latency, dan progress
- Setiap worker menunjukkan: 'PROCESSED topic=... event_id=...' atau
  'DUPLICATE topic=... event_id=... -> dibuang'

Ini membuktikan idempotency: event yang dikirim lebih dari sekali 
hanya diproses SATU kali di database, walau publisher dan worker berjalan 
paralel dan tidak sinkron."
```

### Monitor Progress (4:00 - 6:00)
```
[Buka terminal baru, jalankan curl commands sambil publisher masih berjalan]

"Sementara publisher sedang berjalan, saya bisa query status sistem dengan 
endpoint /stats:"

curl http://localhost:8080/stats

[Tampilkan response JSON, baca beberapa kali dalam interval 5 detik untuk 
menunjukkan perubahan]

"Perhatikan kolom-kolom:
- 'received': terus bertambah saat publisher mengirim (event yang DITERIMA 
  aggregator, termasuk duplikat)
- 'unique_processed': jumlah event UNIK yang berhasil worker simpan ke database
- 'duplicate_dropped': jumlah duplikat yang dibuang
- 'uptime_seconds': waktu aggregator sudah berjalan

Invarian yang HARUS selalu terpenuhi:
  unique_processed + duplicate_dropped ≤ received

Ketidaksamaan (bukan persamaan) karena eventual consistency: event mungkin 
sudah diterima aggregator tapi belum diproses worker (masih di antrian Redis)."

[Tunggu publisher selesai, lihat log publisher menunjukkan summary]
```

---

## **SEGMENT 3: HASIL AKHIR & QUERY DATA (6:00 - 10:00)**

### /stats Final Result (6:00 - 7:00)
```
[Setelah publisher exit dan worker selesai, jalankan]

curl http://localhost:8080/stats

[Baca response, catat nomor-nomornya]

"Publisher sudah selesai. Sekarang cek /stats sekali lagi untuk melihat 
hasil akhir. Perhatikan bahwa:

unique_processed + duplicate_dropped HARUS SAMA dengan received

Ini membuktikan bahwa:
1. Tidak ada event yang hilang
2. Tidak ada event yang diproses ganda
3. Sistem berhasil memisahkan event unik dari duplikat walau datang konkuren"
```

### /events Query (7:00 - 10:00)
```
[Query events untuk topic tertentu]

curl "http://localhost:8080/events?topic=orders&limit=10"

[Tampilkan response, jelaskan struktur]

"Sekarang saya query daftar event unik yang sudah diproses untuk topic 'orders'.
Struktur setiap event:
- topic: nama topik (orders, payments, shipping, dll)
- event_id: ID unik untuk dedup
- timestamp: waktu event dibuat publisher
- source: sumber event (publisher-1, publisher-2, dll)
- payload: data event (dalam JSON)
- processed_at: waktu event selesai diproses di database

Setiap baris di sini adalah event UNIK. Jika publisher mengirim event yang
sama 5 kali, hanya satu baris yang tersimpan dengan processed_at menunjukkan
waktu pertama kali diproses.

Mari test ini dengan mengirim event yang sama beberapa kali."
```

---

## **SEGMENT 4: BUKTI DEDUP DENGAN MANUAL PUBLISH (10:00 - 13:30)**

### Setup Event Test (10:00 - 10:30)
```
[Siapkan JSON file atau inline untuk testing]

"Saya akan membuat event dengan event_id tertentu dan mengirimnya beberapa kali
untuk membuktikan idempotency."

[Manual event untuk publish]
EVENT_TEST='{
  "topic": "demo-dedup",
  "event_id": "evt-manual-001",
  "timestamp": "2026-06-19T10:00:00Z",
  "source": "demo-script",
  "payload": {"test": "bukti-dedup"}
}'
```

### Publish Duplikat & Verify (10:30 - 13:30)
```
[Jalankan 5 kali dengan jeda masing-masing]

"Saya akan mengirim event yang SAMA (event_id sama, topic sama) sebanyak 5 kali.
Setelah ini, sistem harus menunjukkan event tersebut hanya tersimpan SATU kali."

[Publish 5 kali, bisa menggunakan PowerShell]

for ($i = 1; $i -le 5; $i++) {
  curl -X POST http://localhost:8080/publish `
    -H "Content-Type: application/json" `
    -d '{
      "topic": "demo-dedup",
      "event_id": "evt-manual-001",
      "timestamp": "2026-06-19T10:00:00Z",
      "source": "demo-script",
      "payload": {"attempt": '$i'}
    }'
  Write-Host "Publish #$i selesai"
  Start-Sleep -Seconds 1
}

[Tunggu ~10 detik untuk worker memproses]

"Sekarang saya query /events untuk topic 'demo-dedup':"

curl "http://localhost:8080/events?topic=demo-dedup"

[Tampilkan response]

"Perhatikan: hanya ADA SATU baris dengan event_id 'evt-manual-001', 
walau saya mengirimnya 5 kali!

Di stats:"

curl http://localhost:8080/stats

"- received bertambah 5 (satu untuk setiap publish)
- duplicate_dropped bertambah 4 (4 duplikat terbuang)
- unique_processed bertambah 1 (hanya 1 event unik yang tersimpan)

Ini adalah BUKTI INTI dari idempotency dan deduplication yang berhasil.
Tanpa fitur ini, duplikat akan tersimpan berulang dan merusak analisis data."
```

---

## **SEGMENT 5: MULTI-WORKER KONKURENSI (13:30 - 17:00)**

### Setup Multi-Worker (13:30 - 14:30)
```
[Stop sistem yang sedang berjalan]

[Di terminal pertama]
docker compose down

[Jelaskan apa yang akan dilakukan]

"Sekarang saya akan mendemonstrasikan konkurensi: menjalankan 3 worker 
secara bersamaan dengan flag --scale worker=3.

Ini penting untuk membuktikan bahwa:
1. Unique constraint di database bekerja atomik
2. Tidak ada race condition antar worker
3. Sistem aman saat banyak instance memproses event paralel

Mari jalankan:"

docker compose up --build --scale worker=3

[Tunggu setup complete, lihat 3 worker instance jalan di log]
```

### Observasi Konkurensi (14:30 - 17:00)
```
[Biarkan sistem jalan dan publisher publish event]

"Perhatikan di log:

- Ada 3 worker dengan ID berbeda (container nama: uas-pubsub-aggregator-worker-1,
  worker-2, worker-3)
- Ketiganya membaca dari Redis Stream yang SAMA (consumer group 'workers')
- Setiap event diproses HANYA SATU dari ketiga worker (Redis Consumer Group
  memastikan ini)
- Event_id yang sama, jika dikirim berkali-kali, tetap hanya tersimpan 1 baris
  di database

Ini adalah BUKTI bahwa:
- Redis consumer group: horizontal scaling tanpa event yang diproses ganda
- Database unique constraint + ON CONFLICT: atomic idempotency bahkan saat
  multi-worker race condition

Jika sistem TIDAK idempotent atau TIDAK atomic, dengan 3 worker dan 30%
duplikasi event, kita akan melihat banyak duplikat di tabel processed_events.
Tapi kita tidak akan lihat itu, karena unique constraint berhasil mencegahnya."

[Setelah publisher selesai]

curl http://localhost:8080/stats

"Stats menunjukkan:
- unique_processed: jumlah unik
- duplicate_dropped: sesuai 30% duplikasi
- Invarian tetap terpenuhi walau 3 worker berjalan paralel

Total event: ~20.000
Unik: ~14.000
Duplikat: ~6.000 (30%)

Semua diproses KONSISTEN tanpa lost update atau double-process."
```

---

## **SEGMENT 6: CRASH RECOVERY & PERSISTENSI (17:00 - 21:00)**

### Setup Crash Recovery Test (17:00 - 17:45)
```
[Biarkan sistem tetap berjalan atau mulai fresh jika perlu]

"Sekarang saya akan mendemonstrasikan fitur paling penting: PERSISTENSI DATA.

Saya akan:
1. Catat stats sebelum 'crash'
2. Hapus container storage (PostgreSQL)
3. Jalankan storage ulang (container baru, TAPI VOLUME SAMA)
4. Verifikasi data tetap ada

Ini membuktikan bahwa data TIDAK HILANG meski container dihapus."

[Current stats]
curl http://localhost:8080/stats

[Catat nomor: received, unique_processed, duplicate_dropped]

"Stats saat ini:
- received: [CATAT NOMOR]
- unique_processed: [CATAT NOMOR]
- duplicate_dropped: [CATAT NOMOR]"
```

### Simulasi Crash (17:45 - 19:15)
```
[Di terminal baru, jalankan commands ini]

"Sekarang saya akan hapus storage container:"

docker compose stop storage

"Storage sudah di-stop. Sistem sekarang tidak bisa akses database. 
Coba query /stats:"

curl http://localhost:8080/stats

[Akan error atau timeout, itu NORMAL]

"Seperti yang diharapkan, /stats error karena database tidak tersedia.

Sekarang saya akan hapus container storage sepenuhnya, lalu jalankan ulang
dengan VOLUME YANG SAMA (pg_data):"

docker compose rm -f storage

"Container sudah dihapus. Jalankan storage lagi:"

docker compose up -d storage

[Tunggu ~10 detik untuk PostgreSQL startup]

"Storage container baru sudah jalan. Yang penting: volume 'pg_data' itu 
SAMA seperti sebelumnya (di Docker, named volume adalah independent dari 
container lifecycle). Data di folder itu TIDAK berubah."
```

### Verifikasi Persistensi (19:15 - 21:00)
```
[Setelah storage healthy]

"Sekarang saya query /stats lagi:"

curl http://localhost:8080/stats

[Tampilkan response]

"PERHATIAN: stats yang ditampilkan sekarang HARUS SAMA atau LEBIH BESAR
dari sebelum crash!

Before crash:
- received: [NOMOR 1]
- unique_processed: [NOMOR 2]
- duplicate_dropped: [NOMOR 3]

After recovery:
- received: [NOMOR SEKARANG]

Jika nomor setelah recovery sama dengan sebelum crash, itu BUKTI
bahwa data PERSISTEN di volume dan tidak ter-reset meski container baru.

Ini sangat penting untuk sistem terdistribusi yang harus RESILIENT terhadap
kegagalan. Bahkan jika instance crash atau di-recreate, data tetap aman."

[Jika ingin menunjukkan event juga tetap ada]

curl "http://localhost:8080/events?limit=5"

"Event yang sebelumnya tersimpan juga masih ada. Data 100% persisten."
```

---

## **SEGMENT 7: ISOLATION LEVEL & TRANSAKSI (21:00 - 23:00)**

### Penjelasan Desain (21:00 - 23:00)
```
[Buka file worker/worker.py di editor]

"Sekarang saya akan menjelaskan keputusan desain teknis: 
mengapa sistem ini AMAN dari race condition walau tanpa explicit locking.

Perhatikan query di worker/worker.py:"

UPSERT_SQL = """
WITH ins AS (
    INSERT INTO processed_events (topic, event_id, event_ts, source, payload)
    VALUES ($1, $2, $3::timestamptz, $4, $5::jsonb)
    ON CONFLICT (topic, event_id) DO NOTHING
    RETURNING 1
)
UPDATE stats SET
    unique_processed  = unique_processed  + (SELECT COUNT(*) FROM ins),
    duplicate_dropped = duplicate_dropped + (1 - (SELECT COUNT(*) FROM ins))
WHERE id = 1
RETURNING (SELECT COUNT(*) FROM ins) AS inserted;
"""

"Ini adalah SATU statement SQL yang mengandung DIRINYA SENDIRI transaksi atomik:
- INSERT dengan ON CONFLICT DO NOTHING: jika event sudah ada, abaikan
- UPDATE stats di CTE yang SAMA: update counter statistik

Dalam satu transaksi, baik INSERT lolos atau di-conflict, dan stats selalu
ter-update dengan benar. TIDAK ADA lost update karena UPDATE stats menggunakan
ekspresi 'kolom = kolom + n' yang atomik di level database (row-level lock).

Isolation level yang dipakai: READ COMMITTED (default Postgres)
Ini CUKUP AMAN karena:
1. Keputusan duplikat NOT diambil dari SELECT lalu compare di aplikasi
2. Melainkan diserahkan ke constraint enforcement database
3. Constraint enforcement adalah atomic di level index

Jika isolation level lebih tinggi (SERIALIZABLE), overhead akan meningkat
tanpa manfaat tambahan untuk pola akses ini (baca di report.md T8 & T9
untuk diskusi lebih detail).

[Pause]

Jadi kesimpulannya: Postgres + unique constraint + ON CONFLICT + atomicity
= sistem yang RACE CONDITION FREE tanpa explicit locking."
```

---

## **SEGMENT 8: RINGKASAN & KEPUTUSAN DESAIN (23:00 - 25:00)**

### Summary (23:00 - 25:00)
```
[Kembali ke intro, tatap kamera]

"Untuk merangkum, sistem Pub-Sub Log Aggregator ini mendemonstrasikan konsep
sistem terdistribusi dari Bab 1-13:

✓ BAB 1-2: Karakteristik terdistribusi dan arsitektur pub-sub
  - 4 service independen berkomunikasi lewat pesan (Redis Stream)
  - Decoupling yang membuat sistem scalable dan resilient

✓ BAB 3-4: Komunikasi & penamaan
  - REST API /publish untuk komunikasi
  - Penamaan (topic, event_id) yang collision-resistant (UUID)

✓ BAB 5: Ordering
  - Event dapat diproses out-of-order (idempotent berbasis key, bukan urutan)

✓ BAB 6: Toleransi kegagalan
  - Retry dengan backoff, crash recovery via volume persisten
  - Dedup store yang tahan restart

✓ BAB 7: Eventual consistency
  - /stats dan /events eventual consistent berkat idempotency

✓ BAB 8-9: TRANSAKSI & KONKURENSI (Fokus utama)
  - CTE atomik mencegah lost update
  - Unique constraint mencegah duplikat
  - ON CONFLICT pattern: idempotent write tanpa explicit locking
  - Multi-worker bisa berjalan paralel AMAN

✓ BAB 10-11: Keamanan & persistensi
  - Network lokal tertutup, hanya API port yg diexpose
  - Named volume menjamin persistensi data

✓ BAB 12-13: Orkestrasi & observability
  - Docker Compose: orchestration ringan
  - Healthcheck + readiness probe
  - /stats dan logging untuk visibility

KEPUTUSAN DESAIN UTAMA:
1. Pub-Sub dipilih untuk decoupling → scalability & availability
2. Idempotent consumer dengan unique constraint → eventually exactly-once
3. READ COMMITTED isolation level → balance performa vs safety
4. ON CONFLICT DO NOTHING pattern → atomic dedup tanpa pessimistic locking
5. Named volume → persistensi independen dari container lifecycle

METRIK PERFORMA (dari test):
- Throughput: ~[ISI NOMOR] event/detik
- P50 latency: ~[ISI NOMOR] ms
- P99 latency: ~[ISI NOMOR] ms
- 20.000 event + 30% duplikat diproses dalam ~[ISI WAKTU] menit
- 3 worker: NO double-processing, NO race condition, NO lost update

Terima kasih sudah menonton. Repository dan laporan lengkap bisa diakses di
[ISI GITHUB LINK]."

[Senyum, end recording]
```

---

## **SCRIPT ALTERNATIF: JIKA ADA ISSUE**

### Jika Publisher Lambat / Data Processing Lama
```
"Perhatikan bahwa eventual consistency berarti data mungkin belum 100% 
tersimpan saat publisher selesai. Mari tunggu beberapa saat kemudian 
verify lagi."

[Wait 30 detik, run curl /stats lagi]
```

### Jika Container Health Check Fail
```
"Health check sedang berjalan. PostgreSQL / Redis mungkin membutuhkan waktu
lebih lama untuk startup. Mari tunggu..."

[Tunggu ~30 detik atau check logs]
docker compose logs storage
docker compose logs broker
```

### Jika Query Return Error
```
"Mungkin format command tidak cocok dengan OS. Di PowerShell, gunakan ini:"

$body = @{
    topic = "orders"
    event_id = "evt-1"
    timestamp = "2026-06-19T10:00:00Z"
    source = "manual"
    payload = @{}
} | ConvertTo-Json

Invoke-WebRequest -Uri "http://localhost:8080/publish" `
  -Method Post `
  -Headers @{"Content-Type" = "application/json"} `
  -Body $body
```

---

## **TIPS RECORDING**

1. **Durasi**: Gunakan stopwatch atau timer untuk mematuhi 25 menit
2. **Clarity**: Berbicara pelan-pelan dan jelas, hindari jargon yang tidak dijelaskan
3. **Pauses**: Berikan pause 2-3 detik sebelum penting point agar penonton terbiasa
4. **Screenshots**: Screenshot hasil /stats, /events dan masukkan di report.md
5. **Audio**: Record di tempat hening untuk audio jernih
6. **Lighting**: Pastikan layar visible dengan baik
7. **Editing**: Bisa edit-minimal atau langsung satu take (yang penting content lengkap)

---

**Good luck! Pastikan setiap segment tercakup dengan baik.** 🎬
