"""ClickHouse schema + Quix Streams sinks for the processor.

Why sinks instead of inserting from inside `sdf.apply()`: ClickHouse wants
few large inserts, not one insert per message (each insert creates a data
part on disk, and thousands of tiny parts per minute will make it fall over).
Quix's `BatchingSink` buffers rows per partition and writes them in `flush()`,
which Quix calls at each checkpoint *before* committing Kafka offsets. So a
batch is either written and its offsets committed, or (on a crash) not
committed and re-read on restart: at-least-once delivery, with no
hand-rolled buffering.

At-least-once means a replay can insert the same row twice. Every table is a
ReplacingMergeTree keyed on the row's natural identity, so duplicates
collapse into one row when ClickHouse merges parts. Merges are eventual,
so queries that must be exact before a merge happens use `FINAL` (the
Grafana dashboards do).

The DDL lives here, not in a docker init script, so the processor creates
its own tables whether ClickHouse runs in compose or anywhere else.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from quixstreams.sinks import BatchingSink, SinkBatch

from common.config import Settings

logger = logging.getLogger(__name__)

_DDL = [
    """
    CREATE TABLE IF NOT EXISTS scored_posts (
        id String,
        source LowCardinality(String),
        author String,
        text String,
        created_at DateTime64(3, 'UTC'),
        fetched_at DateTime64(3, 'UTC'),
        processed_at DateTime64(3, 'UTC'),
        url String,
        sentiment_label LowCardinality(String),
        sentiment_score Float32,
        polarity Float32,
        INDEX idx_processed_at processed_at TYPE minmax GRANULARITY 1
    )
    ENGINE = ReplacingMergeTree(processed_at)
    ORDER BY (source, id)
    TTL toDateTime(processed_at) + INTERVAL 7 DAY
    """,
    """
    CREATE TABLE IF NOT EXISTS sentiment_windows (
        window_start DateTime('UTC'),
        window_end DateTime('UTC'),
        source LowCardinality(String),
        message_count UInt32,
        avg_polarity Float32,
        positive_count UInt32,
        negative_count UInt32,
        neutral_count UInt32,
        written_at DateTime64(3, 'UTC')
    )
    ENGINE = ReplacingMergeTree(written_at)
    ORDER BY (source, window_start)
    """,
    """
    CREATE TABLE IF NOT EXISTS term_windows (
        window_start DateTime('UTC'),
        window_end DateTime('UTC'),
        term String,
        mention_count UInt32,
        baseline_mean Float32,
        baseline_std Float32,
        baseline_windows UInt8,
        zscore Float32,
        is_trending Bool,
        written_at DateTime64(3, 'UTC')
    )
    ENGINE = ReplacingMergeTree(written_at)
    ORDER BY (window_start, term)
    """,
]

POST_COLUMNS = [
    "id", "source", "author", "text", "created_at", "fetched_at", "processed_at",
    "url", "sentiment_label", "sentiment_score", "polarity",
]
WINDOW_COLUMNS = [
    "window_start", "window_end", "source", "message_count", "avg_polarity",
    "positive_count", "negative_count", "neutral_count", "written_at",
]
TERM_COLUMNS = [
    "window_start", "window_end", "term", "mention_count", "baseline_mean",
    "baseline_std", "baseline_windows", "zscore", "is_trending", "written_at",
]


def connect(settings: Settings) -> Client:
    # Create the database first with a connection that doesn't select one,
    # since connecting with a non-existent `database=` fails.
    bootstrap = clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
    )
    bootstrap.command(f"CREATE DATABASE IF NOT EXISTS {settings.clickhouse_database}")
    bootstrap.close()

    client = clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_database,
    )
    for ddl in _DDL:
        client.command(ddl)
    return client


def parse_ts(value: str, fallback: datetime) -> datetime:
    """Parse a source-supplied ISO8601 timestamp, tolerating junk.

    `created_at` comes from the client that made the post, so it can be
    malformed or wildly wrong; one bad value shouldn't fail a whole batch.
    """
    try:
        ts = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return fallback
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    # DateTime64 can't hold pre-1900 values; treat them as junk too.
    return ts if ts.year >= 1900 else fallback


def ms_to_dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def post_row(value: dict) -> list:
    processed_at = datetime.fromisoformat(value["processed_at"])
    return [
        value["id"],
        value["source"],
        value["author"],
        value["text"],
        parse_ts(value["created_at"], processed_at),
        parse_ts(value["fetched_at"], processed_at),
        processed_at,
        value["url"],
        value["sentiment_label"],
        value["sentiment_score"],
        value["polarity"],
    ]


def window_row(key: str, value: dict, written_at: datetime) -> list:
    return [
        ms_to_dt(value["start"]),
        ms_to_dt(value["end"]),
        key,
        value["message_count"],
        value["avg_polarity"] or 0.0,
        value["positive_count"],
        value["negative_count"],
        value["neutral_count"],
        written_at,
    ]


def term_row(value: dict, written_at: datetime) -> list:
    return [
        ms_to_dt(value["start"]),
        ms_to_dt(value["end"]),
        value["term"],
        value["mention_count"],
        value["baseline_mean"],
        value["baseline_std"],
        value["baseline_windows"],
        value["zscore"],
        value["is_trending"],
        written_at,
    ]


class _ClickHouseSink(BatchingSink):
    table: str
    columns: list[str]

    def __init__(self, client: Client) -> None:
        super().__init__()
        self._client = client

    def to_row(self, key, value: dict, written_at: datetime) -> list:
        raise NotImplementedError

    def write(self, batch: SinkBatch) -> None:
        written_at = datetime.now(timezone.utc)
        rows = [self.to_row(item.key, item.value, written_at) for item in batch]
        if not rows:
            return
        self._client.insert(self.table, rows, column_names=self.columns)
        logger.info("wrote %d rows to %s partition=%s", len(rows), self.table, batch.partition)


class PostsSink(_ClickHouseSink):
    table = "scored_posts"
    columns = POST_COLUMNS

    def to_row(self, key, value, written_at):
        return post_row(value)


class SentimentWindowsSink(_ClickHouseSink):
    table = "sentiment_windows"
    columns = WINDOW_COLUMNS

    def to_row(self, key, value, written_at):
        return window_row(key, value, written_at)


class TermWindowsSink(_ClickHouseSink):
    table = "term_windows"
    columns = TERM_COLUMNS

    def to_row(self, key, value, written_at):
        return term_row(value, written_at)
