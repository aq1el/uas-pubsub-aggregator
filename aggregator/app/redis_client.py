"""Klien Redis (async). Dipakai aggregator untuk MENERBITKAN event ke Stream
('broker' dalam arsitektur pub-sub) -- aggregator sendiri TIDAK mengonsumsi
event secara langsung; itu tugas service `worker` (separation of concerns)."""
import redis.asyncio as redis

from . import config

_client: redis.Redis | None = None


def get_client() -> redis.Redis:
    global _client
    if _client is None:
        _client = redis.from_url(config.REDIS_URL, decode_responses=True)
    return _client
