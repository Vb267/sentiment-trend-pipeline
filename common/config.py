"""Central place to read configuration from the environment / .env file.

Nothing in this file talks to Kafka, a model, or a database - it just
resolves settings, so every service (producer, processor, dashboard)
constructs the same Settings object from the same env vars.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True, slots=True)
class Settings:
    source: str
    sentiment_backend: str

    kafka_brokers: str
    kafka_topic_raw: str
    kafka_consumer_group: str

    sqlite_path: str

    clickhouse_host: str
    clickhouse_port: int
    clickhouse_user: str
    clickhouse_password: str
    clickhouse_database: str

    window_minutes: int
    trend_zscore_threshold: float
    trend_baseline_windows: int
    trend_min_baseline_windows: int
    trend_min_count: int

    bluesky_wanted_collections: str

    reddit_client_id: str | None
    reddit_client_secret: str | None
    reddit_user_agent: str
    reddit_subreddits: str

    log_level: str


def get_settings() -> Settings:
    return Settings(
        source=os.environ.get("SOURCE", "bluesky"),
        sentiment_backend=os.environ.get("SENTIMENT_BACKEND", "vader"),
        kafka_brokers=os.environ.get("KAFKA_BROKERS", "localhost:19092"),
        kafka_topic_raw=os.environ.get("KAFKA_TOPIC_RAW", "raw_posts"),
        kafka_consumer_group=os.environ.get("KAFKA_CONSUMER_GROUP", "sentiment-processor"),
        sqlite_path=os.environ.get("SQLITE_PATH", "data/phase2.db"),
        clickhouse_host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        clickhouse_port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        clickhouse_user=os.environ.get("CLICKHOUSE_USER", "pipeline"),
        clickhouse_password=os.environ.get("CLICKHOUSE_PASSWORD", "pipeline"),
        clickhouse_database=os.environ.get("CLICKHOUSE_DATABASE", "sentiment"),
        window_minutes=int(os.environ.get("WINDOW_MINUTES", "5")),
        trend_zscore_threshold=float(os.environ.get("TREND_ZSCORE_THRESHOLD", "2.5")),
        trend_baseline_windows=int(os.environ.get("TREND_BASELINE_WINDOWS", "12")),
        trend_min_baseline_windows=int(os.environ.get("TREND_MIN_BASELINE_WINDOWS", "3")),
        trend_min_count=int(os.environ.get("TREND_MIN_COUNT", "10")),
        bluesky_wanted_collections=os.environ.get("BLUESKY_WANTED_COLLECTIONS", "app.bsky.feed.post"),
        reddit_client_id=os.environ.get("REDDIT_CLIENT_ID") or None,
        reddit_client_secret=os.environ.get("REDDIT_CLIENT_SECRET") or None,
        reddit_user_agent=os.environ.get("REDDIT_USER_AGENT", "sentiment-pipeline/0.1"),
        reddit_subreddits=os.environ.get("REDDIT_SUBREDDITS", "news"),
        log_level=os.environ.get("LOG_LEVEL", "INFO"),
    )
