"""
Publisher Service
=================
Men-generate event (termasuk DUPLIKAT yang disengaja, default 30%) dan mengirimnya
ke aggregator lewat POST /publish dalam bentuk batch, secara konkuren (banyak
request HTTP berjalan bersamaan) untuk mensimulasikan beban nyata.

Ini memenuhi syarat:
- "At-least-once delivery": event yang sama dikirim berkali-kali secara sengaja.
- "Performa minimum": >= 20.000 event, >= 30% duplikasi.
Output skrip ini (throughput, latency, jumlah unique/duplicate) dipakai sebagai
bahan laporan (report.md bagian Analisis Performa).
"""
import asyncio
import os
import random
import string
import time
import uuid
from datetime import datetime, timezone

import httpx

TARGET_URL = os.getenv("TARGET_URL", "http://aggregator:8080/publish")
TOTAL_EVENTS = int(os.getenv("TOTAL_EVENTS", "20000"))
DUPLICATE_RATE = float(os.getenv("DUPLICATE_RATE", "0.3"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "200"))
CONCURRENCY = int(os.getenv("CONCURRENCY", "8"))
TOPICS = ["orders", "payments", "shipping", "auth", "inventory"]


def make_event(event_id: str | None = None, topic: str | None = None) -> dict:
    return {
        "topic": topic or random.choice(TOPICS),
        "event_id": event_id or str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": f"publisher-{random.randint(1, 5)}",
        "payload": {
            "value": random.randint(1, 1000),
            "note": "".join(random.choices(string.ascii_lowercase, k=6)),
        },
    }


def build_event_pool(n_total: int, dup_rate: float) -> tuple[list[dict], int, int]:
    """Bangun daftar event: n_unique event unik + sisanya adalah DUPLIKAT EKSAK
    (topic & event_id sama persis dengan salah satu event unik) supaya benar-benar
    menguji jalur dedup, bukan cuma id mirip-mirip."""
    n_unique = max(1, int(n_total * (1 - dup_rate)))
    unique_events = [make_event() for _ in range(n_unique)]
    n_dup = n_total - n_unique
    duplicate_events = [dict(random.choice(unique_events)) for _ in range(n_dup)]

    all_events = unique_events + duplicate_events
    random.shuffle(all_events)
    return all_events, n_unique, n_dup


async def send_batch(client: httpx.AsyncClient, batch: list[dict], stats: dict) -> None:
    t0 = time.perf_counter()
    try:
        resp = await client.post(TARGET_URL, json=batch, timeout=30)
        resp.raise_for_status()
        stats["ok_batches"] += 1
    except Exception as e:  # noqa: BLE001
        stats["failed_batches"] += 1
        print(f"[publisher] batch error: {e}")
    stats["latencies"].append(time.perf_counter() - t0)


async def main() -> None:
    events, n_unique, n_dup = build_event_pool(TOTAL_EVENTS, DUPLICATE_RATE)
    print(f"[publisher] total={len(events)} unique={n_unique} duplicate={n_dup} "
          f"({n_dup / len(events):.1%} duplikasi)")

    batches = [events[i:i + BATCH_SIZE] for i in range(0, len(events), BATCH_SIZE)]
    stats = {"ok_batches": 0, "failed_batches": 0, "latencies": []}

    sem = asyncio.Semaphore(CONCURRENCY)

    async with httpx.AsyncClient() as client:
        async def worker(batch: list[dict]) -> None:
            async with sem:
                await send_batch(client, batch, stats)

        t_start = time.perf_counter()
        await asyncio.gather(*(worker(b) for b in batches))
        elapsed = time.perf_counter() - t_start

    avg_latency_ms = (sum(stats["latencies"]) / len(stats["latencies"]) * 1000) if stats["latencies"] else 0
    throughput = len(events) / elapsed if elapsed > 0 else 0

    print("=" * 60)
    print(f"[publisher] Selesai dalam        : {elapsed:.2f} detik")
    print(f"[publisher] Batch sukses / gagal  : {stats['ok_batches']} / {stats['failed_batches']}")
    print(f"[publisher] Rata-rata latensi/batch: {avg_latency_ms:.1f} ms")
    print(f"[publisher] Throughput            : {throughput:.1f} event/detik")
    print(f"[publisher] Cek hasil akhir di    : GET {TARGET_URL.replace('/publish', '/stats')}")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
