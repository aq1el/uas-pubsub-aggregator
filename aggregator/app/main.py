"""
Aggregator Service
==================
Bertindak sebagai API gateway (Bab 1-2: arsitektur publish-subscribe & microservices):
- Menerima event lewat HTTP (POST /publish), memvalidasi skema.
- MENERBITKAN (publish) event ke Redis Stream -> ini bagian "pub" dari pub-sub.
- TIDAK memproses/menyimpan event ke Postgres secara langsung. Itu tugas
  service `worker` yang berjalan independen (bisa di-scale jadi banyak instance)
  supaya proses idempotency/dedup teruji untuk konkurensi (Bab 8-9).
- Endpoint GET /events & GET /stats membaca hasil yang SUDAH diproses worker
  dari Postgres (read-model).

Konsistensi yang dipakai: eventual consistency (Bab 7). Begitu /publish sukses,
event PASTI akan diproses (at-least-once, via Redis consumer group), tapi
mungkin tidak instan -- ada selang waktu sampai worker memprosesnya.
"""
import json
import logging
import time
from contextlib import asynccontextmanager
from typing import Union

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from . import config, db, redis_client
from .models import Event

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [aggregator] %(message)s")
logger = logging.getLogger("aggregator")

START_TIME = time.time()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Siapkan pool DB & pastikan Redis Stream + consumer group sudah ada
    # sebelum menerima traffic (readiness, Bab 12-13).
    await db.get_pool()
    r = redis_client.get_client()
    try:
        await r.xgroup_create(config.STREAM_NAME, config.CONSUMER_GROUP, id="0", mkstream=True)
        logger.info(f"consumer group '{config.CONSUMER_GROUP}' siap pada stream '{config.STREAM_NAME}'")
    except Exception as e:  # noqa: BLE001
        if "BUSYGROUP" not in str(e):
            logger.warning(f"gagal membuat consumer group: {e}")
    yield
    await db.close_pool()


app = FastAPI(title="Pub-Sub Log Aggregator", lifespan=lifespan)


@app.get("/health")
async def health():
    """Liveness + readiness check sederhana: pastikan Postgres & Redis bisa diakses."""
    try:
        pool = await db.get_pool()
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        r = redis_client.get_client()
        await r.ping()
        return {"status": "ok"}
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"service belum siap: {e}") from e


@app.post("/publish")
async def publish(body: Union[Event, list[Event]]):
    """Menerima single event ATAU batch (list of event). Setiap event divalidasi
    skema oleh Pydantic SEBELUM masuk fungsi ini (otomatis return 422 jika invalid).

    Kebijakan batch (lihat report.md bagian Transaksi & Konkurensi - 'Batch atomic'):
    setiap item dalam batch divalidasi & di-enqueue secara INDEPENDEN. Jika satu
    item gagal di-enqueue ke Redis (kasus jarang, mis. Redis down di tengah loop),
    item lain TETAP lanjut diproses -- pilihan ini diambil karena unit konsistensi
    yang sebenarnya penting bukan "batch HTTP request"-nya, melainkan setiap
    (topic, event_id) individual yang sudah dijamin idempotent oleh worker.
    Item yang gagal dikembalikan di field `rejected` agar publisher tahu harus retry.
    """
    events = body if isinstance(body, list) else [body]
    if not events:
        raise HTTPException(status_code=400, detail="batch tidak boleh kosong")

    r = redis_client.get_client()
    pool = await db.get_pool()

    queued = 0
    rejected: list[dict] = []
    for ev in events:
        try:
            await r.xadd(config.STREAM_NAME, {"data": ev.model_dump_json()})
            queued += 1
        except Exception as e:  # noqa: BLE001
            rejected.append({"event_id": ev.event_id, "topic": ev.topic, "error": str(e)})

    if queued > 0:
        # Update counter "received" dalam SATU statement transaksional supaya
        # aman dari lost-update walau banyak request /publish datang bersamaan.
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE stats SET received = received + $1 WHERE id = 1", queued
            )

    logger.info(f"publish: total={len(events)} queued={queued} rejected={len(rejected)}")
    return JSONResponse(
        status_code=202,
        content={"queued": queued, "rejected": rejected, "total": len(events)},
    )


@app.get("/events")
async def get_events(topic: str | None = Query(default=None), limit: int = Query(default=100, le=1000)):
    """Daftar event UNIK yang sudah selesai diproses (sudah lolos dedup di worker)."""
    pool = await db.get_pool()
    async with pool.acquire() as conn:
        if topic:
            rows = await conn.fetch(
                "SELECT topic, event_id, event_ts, source, payload, processed_at "
                "FROM processed_events WHERE topic = $1 "
                "ORDER BY processed_at DESC LIMIT $2",
                topic, limit,
            )
        else:
            rows = await conn.fetch(
                "SELECT topic, event_id, event_ts, source, payload, processed_at "
                "FROM processed_events ORDER BY processed_at DESC LIMIT $1",
                limit,
            )

    result = []
    for row in rows:
        d = dict(row)
        # asyncpg mengembalikan kolom JSONB sebagai string -> parse jadi dict asli
        if isinstance(d.get("payload"), str):
            d["payload"] = json.loads(d["payload"])
        d["event_ts"] = d["event_ts"].isoformat()
        d["processed_at"] = d["processed_at"].isoformat()
        result.append(d)
    return result


@app.get("/stats")
async def get_stats():
    """received: jumlah event yang DITERIMA aggregator (termasuk duplikat).
    unique_processed: jumlah event UNIK yang berhasil disimpan worker.
    duplicate_dropped: jumlah event yang dibuang karena sudah pernah diproses.
    Invarian yang HARUS selalu benar: unique_processed + duplicate_dropped <= received
    (bisa lebih kecil sesaat karena worker memproses async, eventual consistency).
    """
    pool = await db.get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT received, unique_processed, duplicate_dropped FROM stats WHERE id = 1"
        )
        topic_rows = await conn.fetch("SELECT DISTINCT topic FROM processed_events ORDER BY topic")

    return {
        "received": row["received"],
        "unique_processed": row["unique_processed"],
        "duplicate_dropped": row["duplicate_dropped"],
        "topics": [t["topic"] for t in topic_rows],
        "uptime_seconds": round(time.time() - START_TIME, 2),
    }
