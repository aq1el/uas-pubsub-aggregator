# Laporan UAS Sistem Terdistribusi
## Pub-Sub Log Aggregator Terdistribusi dengan Idempotent Consumer, Deduplication, dan Transaksi/Kontrol Konkurensi

Nama: _\[ISI NAMA\]_
NIM: _\[ISI NIM\]_
Mata Kuliah: Sistem Paralel dan Terdistribusi

---

## 1. Ringkasan Sistem

Sistem ini adalah **pub-sub log aggregator** multi-service yang berjalan di
atas Docker Compose, terdiri dari empat layanan: `aggregator` (FastAPI, lapisan
API), `worker` (consumer idempotent), `broker` (Redis Stream), dan `storage`
(PostgreSQL). Publisher mengirim event — termasuk duplikat yang disengaja —
ke aggregator, yang kemudian meneruskannya ke Redis Stream. Satu atau banyak
instance `worker` membaca stream tersebut dan menyimpan event ke Postgres
secara **idempotent**, dijamin oleh *unique constraint* `(topic, event_id)`.
Detail arsitektur lengkap ada di `README.md`.

---

## 2. Bagian Teori (Bab 1–13)

### T1 (Bab 1) — Karakteristik Sistem Terdistribusi dan Trade-off Desain Pub-Sub Aggregator

Sistem terdistribusi dicirikan oleh komponen-komponen yang saling tidak
bergantung, terletak di berbagai komputer, dan hanya berkomunikasi lewat
pengiriman pesan tanpa berbagi memori secara langsung. Karakteristik ini langsung
melahirkan trade-off pada rancangan aggregator: tidak ada *global clock* atau
*shared state* antara `aggregator`, `worker`, dan `broker`, sehingga sistem
harus menerima kemungkinan pesan datang berulang, terlambat, atau tidak
berurutan. Trade-off utama yang diambil adalah memilih **konsistensi yang
lebih lemah (eventual consistency) demi ketersediaan dan skalabilitas**: alih-alih
memproses setiap event secara sinkron di dalam request HTTP (yang akan membuat
`aggregator` lambat dan rentan timeout saat beban tinggi), event diantrekan ke
Redis lalu diproses asinkron oleh `worker` yang dapat di-scale independen.
Konsekuensinya, klien tidak langsung tahu kapan persis event selesai diproses
(harus polling `/stats` atau `/events`). Trade-off kedua adalah penempatan
"kebenaran" di satu titik (Postgres) sementara komponen lain bersifat
stateless/replaceable — ini menyederhanakan recovery setelah crash, sejalan
dengan tujuan ketahanan kegagalan pada sistem terdistribusi.

*(Coulouris et al., 2012)*

### T2 (Bab 2) — Kapan Memilih Arsitektur Publish–Subscribe dibanding Client–Server?

Arsitektur client–server cocok ketika hubungan antar pihak bersifat *one-to-one*
dan sinkron: klien tahu persis server mana yang dituju dan menunggu balasan
langsung. Publish–subscribe lebih tepat ketika terdapat **decoupling waktu,
ruang, dan jumlah** antara penghasil dan pengonsumsi data — penerbit tidak
perlu tahu siapa atau berapa banyak subscriber yang akan mengonsumsi event,
dan subscriber bisa lambat/offline tanpa membuat publisher gagal. Pada kasus
log aggregator, jumlah publisher (banyak sumber log) dan jumlah consumer
(`worker` yang dapat di-scale) tidak fixed, serta volume event sangat tinggi
dan *bursty* (≥20.000 event dengan lonjakan duplikat) — karakteristik ini
sangat sesuai dengan pola pub-sub karena broker (Redis Stream) menyediakan
*buffering* alami yang menyerap lonjakan tanpa membuat publisher menunggu
proses penyimpanan selesai. Jika dipaksakan model client–server langsung
(publisher memanggil API simpan-ke-database secara sinkron), maka latensi
publisher akan naik linear terhadap beban worker, dan kegagalan storage akan
langsung memengaruhi publisher. Alasan teknis lain: pub-sub memudahkan
penambahan consumer baru (misalnya layanan analitik) tanpa mengubah kode
publisher sama sekali — ini selaras dengan prinsip *extensibility* sistem
terdistribusi.

*(Coulouris et al., 2012)*

### T3 (Bab 3) — At-least-once vs Exactly-once Delivery; Peran Idempotent Consumer

Komunikasi antar proses pada sistem terdistribusi tidak dapat menjamin
*exactly-once delivery* secara native, karena pengirim tidak pernah bisa
membedakan dengan pasti antara "pesan hilang" dan "balasan ack hilang" — pada
kedua kasus pengirim akan melakukan retry, yang berpotensi mengirim ulang
pesan yang sebenarnya sudah diterima. Karena itu, jaminan paling realistis
yang dapat diberikan oleh infrastruktur pengiriman adalah **at-least-once**
(pesan dijamin sampai, tapi mungkin lebih dari sekali). Pada sistem ini,
kombinasi Redis consumer group (XREADGROUP/XACK) memberikan semantik
at-least-once: jika worker crash sebelum ACK, pesan akan di-redeliver. Supaya
hasil akhirnya tetap setara *exactly-once* dari sudut pandang data tersimpan,
sistem menambahkan **idempotent consumer** di sisi `worker` — setiap pemrosesan
mencoba INSERT dengan *unique constraint* `(topic, event_id)` dan mengabaikan
pesan yang sudah pernah disimpan. Dengan pola ini, "exactly-once" yang
sebenarnya dicapai bukan di layer pengiriman, melainkan di layer pemrosesan
(*effectively-once processing*), sebuah pendekatan praktis yang jauh lebih
murah dibanding mencoba membangun protokol *exactly-once delivery* murni.

*(Coulouris et al., 2012)*

### T4 (Bab 4) — Skema Penamaan Topic dan Event_id (Unik, Collision-Resistant) untuk Dedup

Penamaan (*naming*) pada sistem terdistribusi harus memberikan identitas yang
tidak ambigu agar entitas dapat dirujuk secara konsisten oleh komponen yang
berbeda tanpa koordinasi terpusat. Pada sistem ini, identitas unik sebuah
event bukan `event_id` saja, melainkan **pasangan `(topic, event_id)`** —
desain ini sengaja dipilih karena `event_id` yang dibuat oleh publisher yang
berbeda (mis. dua microservice berbeda) bisa kebetulan sama jika hanya
mengandalkan counter lokal, tetapi kombinasi dengan `topic` (yang biasanya
mencerminkan domain/sumber data) memperkecil risiko *collision*. Untuk
`event_id` itu sendiri, sistem mengasumsikan publisher menghasilkan id yang
*collision-resistant*, misalnya UUIDv4 (dipakai pada `publisher/publisher.py`)
yang secara praktis hampir tidak pernah menghasilkan nilai yang sama dua kali
walau dibuat di proses/mesin yang berbeda, tanpa perlu otoritas penamaan
terpusat. Skema ini langsung dipetakan menjadi `PRIMARY KEY (topic, event_id)`
di tabel `processed_events`, sehingga mekanisme penamaan dan mekanisme dedup
menjadi satu kesatuan: nama yang sama secara definisi berarti event yang sama.

*(Coulouris et al., 2012)*

### T5 (Bab 5) — Ordering Praktis (Timestamp + Monotonic Counter); Batasan dan Dampaknya

Pada sistem terdistribusi, *clock* fisik di setiap node tidak bisa
disinkronkan secara sempurna, sehingga timestamp lokal saja tidak cukup untuk
menentukan urutan kausal antar event dari sumber berbeda. Sistem ini memakai
strategi praktis: setiap event membawa `timestamp` (ISO8601) yang dibuat
publisher sebagai *hint* urutan untuk keperluan tampilan/audit, namun **tidak
mengandalkannya untuk korektnes pemrosesan**. Pemrosesan dedup di `worker`
sengaja didesain *order-independent* — event yang datang lebih dulu maupun
lebih lambat tetap aman diproses karena kuncinya adalah identitas
`(topic, event_id)`, bukan urutan kedatangan. Keterbatasan dari pendekatan ini:
sistem tidak menjamin *total ordering* maupun *causal ordering* antar event
dari topic yang sama — dua event pada topic `orders` bisa tersimpan dengan
urutan `processed_at` yang berbeda dari urutan `timestamp` aslinya jika
diproses oleh worker berbeda dengan kecepatan berbeda. Dampaknya, sistem ini
**tidak cocok** untuk use case yang butuh urutan ketat (mis. state machine
yang transisinya bergantung urutan event), tapi cocok untuk log aggregator
karena setiap entri bersifat independen (Bab 14 buku ini membahas *logical
clock*/*vector clock* sebagai solusi penuh, namun di luar cakupan minimum
tugas ini).

*(Coulouris et al., 2012)*

### T6 (Bab 6) — Failure Modes dan Mitigasi (Retry, Backoff, Durable Dedup Store, Crash Recovery)

Beberapa *failure mode* yang relevan diidentifikasi dan dimitigasi sebagai
berikut. **Duplikasi pesan** (akibat retry publisher atau redelivery Redis)
dimitigasi dengan idempotent consumer + unique constraint (lihat T3 & T4).
**Crash worker di tengah pemrosesan**: karena ACK ke Redis baru dikirim
*setelah* transaksi database commit, pesan yang sedang diproses saat worker
crash akan tetap berada di *Pending Entries List* dan di-redeliver ke worker
lain/yang baru — *durable dedup store* (Postgres dengan volume persisten)
memastikan redelivery ini tidak menghasilkan baris ganda. **Crash/restart
seluruh service** (`docker compose restart` atau container di-*recreate*):
data di Postgres dan Redis (AOF) tetap ada karena disimpan di *named volume*,
sehingga proses dapat melanjutkan pekerjaan tanpa reprocessing event yang
sudah tersimpan. **Storage/broker belum siap saat service lain start**:
dimitigasi dengan `healthcheck` + `depends_on: condition: service_healthy`
pada `docker-compose.yml`, sehingga aggregator/worker menunggu storage dan
broker benar-benar siap sebelum menerima trafik (mencegah *cascading failure*
saat startup). Backoff sederhana diterapkan pada loop pembacaan Redis di
worker (sleep 2 detik) bila terjadi error koneksi, mencegah *retry storm*.

*(Coulouris et al., 2012)*

### T7 (Bab 7) — Eventual Consistency pada Aggregator; Peran Idempotency + Dedup

Karena pemrosesan event bersifat asinkron (lewat antrian), sistem ini secara
sadar mengadopsi **eventual consistency**: segera setelah `/publish` sukses,
counter `received` langsung bertambah, tetapi `unique_processed` dan data di
`/events` baru konsisten beberapa saat kemudian setelah `worker` selesai
memproses dari antrian. Klien yang membaca `/stats` tepat setelah `/publish`
mungkin melihat *state* sementara yang belum mencerminkan event terbaru —
namun sistem menjamin bahwa, tanpa ada pengiriman baru, state akan
**konvergen** ke nilai akhir yang benar begitu seluruh antrian selesai
diproses. Idempotency dan dedup memainkan peran krusial agar konvergensi ini
selalu menuju nilai yang benar walau ada duplikasi pengiriman: tanpa idempotent
consumer, pengiriman ulang (retry) yang lazim terjadi pada sistem asinkron
akan menyebabkan *state* akhir yang salah (event yang sama terhitung berkali-
kali). Dengan unique constraint di level database, berapa kali pun event
yang sama "lewat" antrian, hasil akhirnya identik dengan jika event itu hanya
dikirim sekali — inilah yang membuat *eventual consistency* di sistem ini
tetap dapat diandalkan meski tanpa koordinasi terpusat.

*(Coulouris et al., 2012)*

### T8 (Bab 8) — Desain Transaksi: ACID, Isolation Level, dan Strategi Menghindari Lost-Update

Setiap pemrosesan event di `worker` dibungkus dalam satu transaksi Postgres
yang memenuhi properti ACID: **Atomicity** — INSERT ke `processed_events` dan
UPDATE ke `stats` terjadi dalam satu statement SQL (CTE), sehingga keduanya
sukses atau keduanya batal bersama, tidak pernah setengah-setengah;
**Consistency** — *unique constraint* memastikan invarian "satu event hanya
satu baris" selalu terjaga; **Isolation** — transaksi worker lain tidak melihat
baris yang belum di-commit; **Durability** — begitu transaksi commit, data
tersimpan permanen di volume Postgres. Isolation level yang dipilih adalah
**READ COMMITTED** (default Postgres), bukan SERIALIZABLE. Alasannya: risiko
klasik yang biasanya memaksa pemilihan SERIALIZABLE — seperti *write skew*
atau keputusan berbasis pembacaan data lama (stale read) — tidak relevan di
sini karena keputusan "apakah event ini duplikat atau tidak" **tidak**
diambil lewat `SELECT` lalu `INSERT` terpisah (yang rentan race condition),
melainkan diserahkan langsung ke *constraint enforcement* Postgres lewat
`ON CONFLICT DO NOTHING`. Begitu pula untuk strategi menghindari *lost update*
pada counter statistik: bukan dengan pola "baca nilai, tambah di aplikasi,
tulis balik" (rentan lost-update di level apa pun isolasinya tanpa locking
eksplisit), melainkan dengan ekspresi `kolom = kolom + n` yang dieksekusi
atomik oleh row-level lock otomatis Postgres saat `UPDATE`. Hasilnya,
READ COMMITTED cukup aman dan lebih murah dibanding SERIALIZABLE yang akan
menambah overhead *retry-on-conflict* tanpa manfaat tambahan pada pola akses
ini.

*(Coulouris et al., 2012)*

### T9 (Bab 9) — Kontrol Konkurensi: Locking/Unique Constraints/Upsert; Idempotent Write Pattern

Kontrol konkurensi pada sistem ini sepenuhnya bertumpu pada **mekanisme
deteksi konflik milik database** (*constraint-based concurrency control*),
bukan *pessimistic locking* eksplisit (seperti `SELECT ... FOR UPDATE`)
maupun *optimistic locking* berbasis versi. Pola yang dipakai adalah
**idempotent write / upsert**: `INSERT ... ON CONFLICT (topic, event_id) DO
NOTHING`. Pola ini punya beberapa keunggulan dibanding locking eksplisit pada
kasus log aggregator: (1) tidak butuh round-trip tambahan untuk mengambil lock
sebelum insert, (2) Postgres menjamin secara internal bahwa pemeriksaan unique
constraint dan insert bersifat atomik di level *index*, sehingga dua worker
yang mencoba insert `(topic, event_id)` yang identik **secara bersamaan**
dijamin hanya satu yang berhasil — yang lain otomatis "DO NOTHING" tanpa
deadlock maupun *busy-wait*. Ini dibuktikan lewat test
`test_concurrent_duplicate_publishes_no_double_process` (10 thread mengirim
event_id yang sama secara konkuren) yang harus selalu menghasilkan tepat satu
baris tersimpan. Strategi ini dipilih dibanding *application-level locking*
(mis. lock berbasis Redis) karena lock terpusat seperti itu justru
menciptakan *single point of contention* baru yang membatasi skalabilitas
horizontal worker — bertentangan dengan tujuan sistem terdistribusi yang ingin
dicapai.

*(Coulouris et al., 2012)*

### T10 (Bab 10–13) — Orkestrasi Compose, Keamanan Jaringan Lokal, Persistensi, Observability

**Orkestrasi (Bab 12–13):** Docker Compose berperan sebagai *lightweight
orchestrator* yang mengatur urutan startup antar service lewat `depends_on`
dikombinasikan dengan `healthcheck` (readiness check), sehingga aggregator dan
worker tidak mulai menerima/memproses trafik sebelum Postgres dan Redis
benar-benar siap — pola ini meniru *readiness/liveness probe* yang lazim
ditemukan pada orchestrator skala lebih besar seperti Kubernetes.
**Keamanan jaringan (Bab 10–11):** seluruh service ditempatkan dalam satu
Docker network privat (`internal`); hanya port `8080` (aggregator) yang
diekspos ke host untuk keperluan demo lokal, sedangkan Postgres dan Redis
sama sekali tidak memiliki mapping port ke luar — keduanya hanya bisa dijangkau
oleh service lain di dalam network Compose yang sama, sehingga tidak ada
permukaan serangan dari luar (tidak ada akses ke layanan eksternal publik
maupun dari publik ke layanan internal). **Persistensi (Bab 10):** data
disimpan di *named volume* (`pg_data`, `broker_data`) yang siklus hidupnya
independen dari container — menghapus/membuat ulang container tidak menghapus
data, sejalan dengan prinsip sistem berkas terdistribusi yang memisahkan
data dari proses yang mengaksesnya. **Observability:** endpoint `/health`
(liveness/readiness) dan `/stats` (metrik operasional: jumlah diterima, unik,
duplikat, dan uptime), ditambah logging terstruktur di setiap service
(`PROCESSED`/`DUPLICATE` per event di worker) memberi visibilitas terhadap
perilaku sistem tanpa perlu masuk ke database secara manual.

*(Coulouris et al., 2012)*

---

## 3. Analisis Performa & Hasil Uji Konkurensi

> **Catatan untuk mahasiswa:** Angka di bawah ini adalah **template/contoh**.
> Jalankan sendiri `docker compose up --build` di komputer kamu (sandbox ini
> tidak punya Docker/akses jaringan untuk menjalankannya), lalu **ganti**
> nilai placeholder `[...]` dengan hasil asli dari output `publisher` dan
> `GET /stats`. Sertakan juga screenshot di laporan/video demo.

### 3.1 Skenario Uji
- Total event dikirim: 20.000 (`TOTAL_EVENTS=20000`)
- Target duplikasi: 30% (`DUPLICATE_RATE=0.3`)
- Ukuran batch: 200 event/request, konkurensi 8 request paralel
- Jumlah worker: dijalankan dua kali untuk perbandingan — 1 worker vs 3 worker
  (`docker compose up --scale worker=3`)

### 3.2 Hasil (isi dengan output nyata dari `publisher` dan `/stats`)

| Metrik                              | 1 Worker     | 3 Worker     |
|--------------------------------------|--------------|--------------|
| Total event diterima (`received`)   | `[...]`      | `[...]`      |
| Event unik tersimpan (`unique_processed`) | `[...]` | `[...]`      |
| Duplikat dibuang (`duplicate_dropped`) | `[...]`    | `[...]`      |
| Waktu publisher selesai mengirim (detik) | `[...]` | `[...]`      |
| Throughput pengiriman (event/detik) | `[...]`      | `[...]`      |
| Rata-rata latensi per batch (ms)    | `[...]`      | `[...]`      |
| Waktu sampai seluruh antrian habis diproses | `[...]` | `[...]` |

**Invarian yang harus selalu terpenuhi (verifikasi manual):**
`unique_processed + duplicate_dropped == received` (setelah seluruh antrian
selesai diproses / Redis Stream kosong).

### 3.3 Hasil Uji Konkurensi

Test `test_concurrent_duplicate_publishes_no_double_process` mengirim 10
request `/publish` dengan `event_id` **identik** secara konkuren (10 thread
bersamaan) ke aggregator yang sama. Hasil yang diharapkan dan **harus**
selalu konsisten: tabel `processed_events` berisi **tepat satu baris** untuk
`event_id` tersebut, walau ada hingga 10 percobaan insert yang bersaing —
membuktikan bahwa *unique constraint* + transaksi atomik di `worker/worker.py`
berhasil mencegah race condition tanpa locking eksplisit (lihat pembahasan T9).

`[ISI: tempelkan output pytest `-v` asli di sini sebagai bukti, mis. screenshot
atau salinan teks `PASSED`]`

---

## 4. Keterkaitan ke Bab 1–13

| Bab    | Topik                              | Implementasi terkait                                            |
|--------|--------------------------------------|-------------------------------------------------------------------|
| 1–2    | Karakteristik sistem terdistribusi, arsitektur pub-sub | Pemisahan 4 service, Redis Stream sebagai broker      |
| 3–4    | Komunikasi & penamaan                | REST API `/publish`, naming `(topic, event_id)`                  |
| 5      | Waktu & ordering                     | Field `timestamp`, pemrosesan order-independent                  |
| 6      | Toleransi kegagalan                  | Healthcheck, retry/backoff worker, durable dedup store            |
| 7      | Konsistensi & replikasi              | Eventual consistency antara `/publish` dan `/stats`               |
| 8–9    | Transaksi & kontrol konkurensi       | CTE atomik `INSERT ON CONFLICT` + `UPDATE stats`, READ COMMITTED   |
| 10–11  | Keamanan & penyimpanan terdistribusi | Docker network privat, named volume Postgres/Redis                |
| 12–13  | Sistem web & koordinasi              | FastAPI, `depends_on`/healthcheck sebagai orkestrasi               |

---

## 5. Referensi

Coulouris, G., Dollimore, J., Kindberg, T., & Blair, G. (2012). *Distributed systems: Concepts and design* (5th ed.). Pearson Education.
