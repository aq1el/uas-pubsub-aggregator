"""
Integration test terhadap aggregator yang HARUS sudah berjalan lewat:
    docker compose up --build
sebelum file ini dijalankan:
    pytest tests/integration -v

Mencakup: idempotency/dedup, transaksi/konkurensi, validasi skema,
konsistensi GET /events & GET /stats, dan stress kecil (sesuai rubrik 'Tests').
"""
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import requests

from conftest import BASE_URL, wait_until


def _unique_event(topic: str = "orders") -> dict:
    return {
        "topic": topic,
        "event_id": f"evt-{uuid.uuid4()}",
        "timestamp": "2026-06-19T10:00:00Z",
        "source": "pytest",
        "payload": {"amount": 1},
    }


def test_health_endpoint_returns_ok():
    r = requests.get(f"{BASE_URL}/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_publish_single_event_is_accepted_and_queued():
    ev = _unique_event()
    r = requests.post(f"{BASE_URL}/publish", json=ev)
    assert r.status_code == 202
    body = r.json()
    assert body["queued"] == 1
    assert body["rejected"] == []


def test_publish_batch_events_all_queued():
    batch = [_unique_event() for _ in range(10)]
    r = requests.post(f"{BASE_URL}/publish", json=batch)
    assert r.status_code == 202
    assert r.json()["queued"] == 10


def test_invalid_event_schema_rejected_with_422():
    # 'topic' tidak ada -> harus ditolak oleh validasi Pydantic FastAPI
    r = requests.post(f"{BASE_URL}/publish", json={"event_id": "x", "timestamp": "2026-06-19T10:00:00Z", "source": "p"})
    assert r.status_code == 422


def test_invalid_timestamp_rejected_with_422():
    ev = _unique_event()
    ev["timestamp"] = "bukan-format-tanggal"
    r = requests.post(f"{BASE_URL}/publish", json=ev)
    assert r.status_code == 422


def test_duplicate_event_is_processed_only_once():
    ev = _unique_event(topic="dedup-test")
    # Kirim event yang SAMA 5 kali secara berurutan (simulasi at-least-once delivery)
    for _ in range(5):
        r = requests.post(f"{BASE_URL}/publish", json=ev)
        assert r.status_code == 202

    def processed_once():
        resp = requests.get(f"{BASE_URL}/events", params={"topic": "dedup-test"})
        matching = [e for e in resp.json() if e["event_id"] == ev["event_id"]]
        return len(matching) == 1

    assert wait_until(processed_once, timeout=15), "event duplikat seharusnya hanya tersimpan SATU kali"


def test_concurrent_duplicate_publishes_no_double_process():
    """Bukti inti Bab 8-9: kirim event_id SAMA dari banyak thread SEKALIGUS
    (simulasi race condition antar worker), pastikan hasil akhir tetap SATU baris
    di database -- tidak ada double-processing walau dikirim konkuren."""
    ev = _unique_event(topic="concurrency-test")

    def fire():
        return requests.post(f"{BASE_URL}/publish", json=ev).status_code

    with ThreadPoolExecutor(max_workers=10) as pool:
        results = list(pool.map(lambda _: fire(), range(10)))
    assert all(code == 202 for code in results)

    def exactly_one_row():
        resp = requests.get(f"{BASE_URL}/events", params={"topic": "concurrency-test"})
        matching = [e for e in resp.json() if e["event_id"] == ev["event_id"]]
        return len(matching) == 1

    assert wait_until(exactly_one_row, timeout=15), (
        "10 publish konkuren untuk event_id yang sama harus menghasilkan TEPAT SATU "
        "baris tersimpan (unique constraint + transaksi atomik di worker)"
    )


def test_stats_received_increases_after_publish():
    before = requests.get(f"{BASE_URL}/stats").json()["received"]
    requests.post(f"{BASE_URL}/publish", json=_unique_event())
    after = requests.get(f"{BASE_URL}/stats").json()["received"]
    assert after >= before + 1


def test_stats_duplicate_dropped_increases_after_resend():
    ev = _unique_event(topic="stats-dup-test")
    requests.post(f"{BASE_URL}/publish", json=ev)

    def event_processed():
        resp = requests.get(f"{BASE_URL}/events", params={"topic": "stats-dup-test"})
        return any(e["event_id"] == ev["event_id"] for e in resp.json())

    assert wait_until(event_processed, timeout=15)

    before = requests.get(f"{BASE_URL}/stats").json()["duplicate_dropped"]
    requests.post(f"{BASE_URL}/publish", json=ev)  # kirim ulang event yang sama

    def dup_counter_increased():
        return requests.get(f"{BASE_URL}/stats").json()["duplicate_dropped"] >= before + 1

    assert wait_until(dup_counter_increased, timeout=15)


def test_get_events_filtered_by_topic_only_returns_that_topic():
    topic = f"topic-{uuid.uuid4().hex[:8]}"
    ev = _unique_event(topic=topic)
    requests.post(f"{BASE_URL}/publish", json=ev)

    def topic_appears():
        resp = requests.get(f"{BASE_URL}/events", params={"topic": topic})
        return len(resp.json()) == 1

    assert wait_until(topic_appears, timeout=15)
    rows = requests.get(f"{BASE_URL}/events", params={"topic": topic}).json()
    assert all(r["topic"] == topic for r in rows)


def test_get_events_unknown_topic_returns_empty_list():
    r = requests.get(f"{BASE_URL}/events", params={"topic": "topic-tidak-pernah-ada-xyz"})
    assert r.status_code == 200
    assert r.json() == []


def test_batch_with_one_invalid_item_returns_422_for_whole_batch():
    # FastAPI memvalidasi SELURUH body sebagai list[Event] -> jika salah satu
    # item tidak valid, seluruh request ditolak di level skema (fail fast).
    batch = [_unique_event(), {"topic": "orders"}]  # item ke-2 tidak lengkap
    r = requests.post(f"{BASE_URL}/publish", json=batch)
    assert r.status_code == 422


def test_stress_batch_of_1000_events_completes_within_timeout():
    batch = [_unique_event(topic="stress-test") for _ in range(1000)]
    t0 = time.perf_counter()
    r = requests.post(f"{BASE_URL}/publish", json=batch, timeout=30)
    elapsed = time.perf_counter() - t0
    assert r.status_code == 202
    assert r.json()["queued"] == 1000
    assert elapsed < 10, f"publish 1000 event harus selesai < 10 detik, aktual {elapsed:.2f}s"
