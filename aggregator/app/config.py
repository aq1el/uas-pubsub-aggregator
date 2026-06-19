"""Konfigurasi aggregator, dibaca dari environment variable (di-set lewat docker-compose.yml).
Tidak ada nilai rahasia yang di-hardcode -> memudahkan ganti environment tanpa rebuild image.
"""
import os

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@storage:5432/db")
REDIS_URL = os.getenv("REDIS_URL", "redis://broker:6379/0")

# Nama Redis Stream yang dipakai sebagai "topic channel" pub-sub internal.
STREAM_NAME = os.getenv("STREAM_NAME", "events_stream")
# Consumer group dipakai supaya satu message hanya diambil oleh SATU worker
# (Redis menjamin pembagian beban antar consumer dalam grup yang sama).
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "workers")
