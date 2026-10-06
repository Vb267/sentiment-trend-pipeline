"""Local store for Phase 1 and Phase 2.

Phase 3 replaces this with ClickHouse (built for time-series aggregation at
scale). Until then, SQLite is enough to prove the pipeline end to end and
needs zero infrastructure. The dashboard (Streamlit for now, Grafana later)
reads from whatever this points at.

Idempotency: `id` is the primary key and writes are upserts, so replaying
the same message (e.g. after a consumer restart re-reads uncommitted
offsets) overwrites the same row instead of double-counting it.
"""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

from common.schema import ScoredPost

_SCHEMA = """
CREATE TABLE IF NOT EXISTS scored_posts (
    id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    author TEXT NOT NULL,
    text TEXT NOT NULL,
    created_at TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    url TEXT NOT NULL,
    sentiment_label TEXT NOT NULL,
    sentiment_score REAL NOT NULL,
    scored_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_scored_posts_created_at ON scored_posts (created_at);
"""


class SqliteStore:
    def __init__(self, path: str) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def upsert(self, post: ScoredPost) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO scored_posts
                    (id, source, author, text, created_at, fetched_at, url,
                     sentiment_label, sentiment_score, scored_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                ON CONFLICT(id) DO UPDATE SET
                    sentiment_label = excluded.sentiment_label,
                    sentiment_score = excluded.sentiment_score,
                    scored_at = excluded.scored_at
                """,
                (
                    post.id,
                    post.source,
                    post.author,
                    post.text,
                    post.created_at,
                    post.fetched_at,
                    post.url,
                    post.sentiment_label,
                    post.sentiment_score,
                ),
            )
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()
