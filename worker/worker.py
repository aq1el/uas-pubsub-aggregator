"""
Worker Service (Consumer)
=========================
Inilah jantung dari Bab 8-9 (Transactions & Concurrency Control) di tugas ini.

Setiap instance worker (bisa dijalankan BANYAK SEKALIGUS lewat
`docker compose up --scale worker=3`) membaca event dari Redis Stream
memakai Consumer Group, lalu mencoba menyimpannya ke Postgres.

Kunci idempotency & dedup ada pada SATU statement SQL atomik (lihat UPSERT_SQL):
    1. INSERT ... ON CONFLICT (topic, event_id) DO NOTHING
       -> unique constraint di tabel processed_events MENJAMIN di level database
          bahwa event yang sama tidak akan pernah tersimpan dua kali, BERAPA PUN
          banyaknya worker yang mencoba insert event itu di waktu yang sama
          (race condition ditangani oleh Postgres, bukan oleh kode Python).
    2. UPDATE stats ... dalam CTE yang SAMA dengan INSERT di atas
       -> "received/unique_processed/duplicate_dropped" diupdate dalam SATU
          round-trip transaksional, sehingga tidak ada lost-update meski banyak
          worker mengupdate baris stats yang sama secara bersamaan.

Isolation level yang dipakai: READ COMMITTED (default Postgres). Ini AMAN untuk
pola ini karena:
    - Konflik dedup diselesaikan oleh UNIQUE CONSTRAINT (bukan oleh pembacaan
      snapshot), jadi tidak butuh SERIALIZABLE untuk mencegah duplicate insert.
    - Update counter pakai ekspresi `kolom = kolom + n` yang dieksekusi atomik
      oleh Postgres per-baris (row-level lock otomatis saat UPDATE), bukan
      read-modify-write di level aplikasi -> aman dari lost update walau READ
      COMMITTED.
Lihat report.md bagian T8/T9 untuk diskusi trade-off lebih lengkap.
"""
import asyncio
import json
import logging
import os
import socket
from datetime import datetime

import asyncpg
import redis.asyncio as redis

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@storage:5432/db")
REDIS_URL = os.getenv("REDIS_URL", "redis://broker:6379/0")
STREAM_NAME = os.getenv("STREAM_NAME", "events_stream")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "workers")
CONSUMER_NAME = os.getenv("CONSUMER_NAME", socket.gethostname())
BATCH_SIZE = int(os.getenv("WORKER_BATCH_SIZE", "20"))
BLOCK_MS = int(os.getenv("WORKER_BLOCK_MS", "5000"))

logging.basicConfig(
    level=logging.INFO,
    format=f"%(asctime)s [%(levelname)s] [worker:{CONSUMER_NAME}] %(message)s",
)
logger = logging.getLogger("worker")

# CTE tunggal: insert dedup + update statistik, ATOMIK dalam satu statement.
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


async def ensure_group(r: redis.Redis) -> None:
    try:
        await r.xgroup_create(STREAM_NAME, CONSUMER_GROUP, id="0", mkstream=True)
    except Exception as e:  # noqa: BLE001
        if "BUSYGROUP" not in str(e):
            raise


async def process_message(pool: asyncpg.Pool, fields: dict) -> None:
    data = json.loads(fields["data"])
    # Parse ISO timestamp string ke datetime object
    ts = datetime.fromisoformat(data["timestamp"])
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                UPSERT_SQL,
                data["topic"],
                data["event_id"],
                ts,
                data["source"],
                json.dumps(data.get("payload", {})),
            )
    inserted = row["inserted"] if row else 0
    if inserted:
        logger.info(f"PROCESSED  topic={data['topic']} event_id={data['event_id']}")
    else:
        logger.info(f"DUPLICATE  topic={data['topic']} event_id={data['event_id']} -> dibuang")


async def main() -> None:
    r = redis.from_url(REDIS_URL, decode_responses=True)
    await ensure_group(r)
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    logger.info(f"worker '{CONSUMER_NAME}' siap, bergabung ke grup '{CONSUMER_GROUP}'")

    while True:
        try:
            resp = await r.xreadgroup(
                CONSUMER_GROUP,
                CONSUMER_NAME,
                {STREAM_NAME: ">"},
                count=BATCH_SIZE,
                block=BLOCK_MS,
            )
        except Exception as e:  # noqa: BLE001
            logger.error(f"gagal membaca dari redis: {e} (retry dalam 2s)")
            await asyncio.sleep(2)
            continue

        if not resp:
            continue  # timeout block, tidak ada pesan baru -> loop lagi

        for _, messages in resp:
            for msg_id, fields in messages:
                try:
                    await process_message(pool, fields)
                    # ACK HANYA setelah transaksi DB sukses commit.
                    # Jika proses_message gagal (crash/exception) SEBELUM ack,
                    # pesan tetap ada di Pending Entries List Redis dan akan
                    # di-redeliver -> aman karena proses dedup di DB idempotent
                    # (Bab 6: failure tolerance lewat retry + idempotent consumer).
                    await r.xack(STREAM_NAME, CONSUMER_GROUP, msg_id)
                except Exception as e:  # noqa: BLE001
                    logger.error(f"gagal memproses pesan {msg_id}: {e} (akan di-redeliver)")


if __name__ == "__main__":
    asyncio.run(main())
