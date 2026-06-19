"""Connection pool ke PostgreSQL. Pool dibuat sekali (singleton) dan dipakai ulang
supaya tidak membuka-tutup koneksi baru untuk setiap request (penting untuk performa
saat menerima beban tinggi, lihat persyaratan >= 20.000 event)."""
import asyncpg

from . import config

_pool: asyncpg.Pool | None = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(config.DATABASE_URL, min_size=2, max_size=10)
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
