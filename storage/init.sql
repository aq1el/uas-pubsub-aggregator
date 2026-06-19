-- init.sql
-- Dijalankan otomatis oleh image postgres:16-alpine SAAT VOLUME MASIH KOSONG
-- (lihat docker-entrypoint-initdb.d). Jika volume sudah ada isinya (mis. setelah
-- container di-recreate), file ini TIDAK dijalankan ulang -> data tetap persisten.

-- Tabel utama: setiap event yang SUDAH selesai diproses (unik per topic+event_id).
-- PRIMARY KEY (topic, event_id) = unique constraint yang menjadi inti mekanisme
-- idempotency & deduplication (Bab 8-9: Transactions & Concurrency Control).
CREATE TABLE IF NOT EXISTS processed_events (
    topic        TEXT NOT NULL,
    event_id     TEXT NOT NULL,
    event_ts     TIMESTAMPTZ NOT NULL,
    source       TEXT NOT NULL,
    payload      JSONB NOT NULL DEFAULT '{}'::jsonb,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (topic, event_id)
);

CREATE INDEX IF NOT EXISTS idx_processed_events_topic
    ON processed_events (topic);

CREATE INDEX IF NOT EXISTS idx_processed_events_processed_at
    ON processed_events (processed_at DESC);

-- Tabel statistik single-row (id selalu 1). Diupdate transaksional bersamaan
-- dengan insert dedup (lihat query UPSERT_SQL di worker/worker.py) supaya
-- bebas dari lost-update walau banyak worker jalan paralel.
CREATE TABLE IF NOT EXISTS stats (
    id                INTEGER PRIMARY KEY DEFAULT 1,
    received          BIGINT NOT NULL DEFAULT 0,
    unique_processed  BIGINT NOT NULL DEFAULT 0,
    duplicate_dropped BIGINT NOT NULL DEFAULT 0,
    started_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO stats (id, received, unique_processed, duplicate_dropped)
VALUES (1, 0, 0, 0)
ON CONFLICT (id) DO NOTHING;
