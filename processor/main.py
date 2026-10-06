"""Phase 2 processor: Redpanda topic `raw_posts` -> sentiment -> SQLite.

Built on Quix Streams' Application/StreamingDataFrame, which wraps a
Kafka consumer group and handles offset checkpointing and graceful
SIGTERM/SIGINT shutdown for us. We use it from Phase 2 onward (rather than
a plain confluent-kafka consumer loop) because Phase 3 needs its windowing
API anyway, and it's one less thing to swap out later.

Inference is one-message-at-a-time here; Phase 5 batches 32-64 messages
per `classify()` call to amortize model overhead.
"""

from __future__ import annotations

import logging

from quixstreams import Application

from common.config import get_settings
from common.logging_setup import configure_logging
from common.schema import RawPost, ScoredPost
from common.sentiment import get_sentiment_backend
from common.store import SqliteStore

logger = logging.getLogger(__name__)


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)

    backend = get_sentiment_backend(settings.sentiment_backend)
    store = SqliteStore(settings.sqlite_path)

    app = Application(
        broker_address=settings.kafka_brokers,
        consumer_group=settings.kafka_consumer_group,
        auto_offset_reset="earliest",
    )
    raw_topic = app.topic(settings.kafka_topic_raw, value_deserializer="json")

    def score_and_store(value: dict) -> dict:
        post = RawPost.from_dict(value)
        (label, score), = backend.classify([post.text])
        scored = ScoredPost.from_raw(post, sentiment_label=label, sentiment_score=score)
        store.upsert(scored)
        return {**value, "sentiment_label": label, "sentiment_score": score}

    sdf = app.dataframe(topic=raw_topic)
    sdf = sdf.apply(score_and_store)

    logger.info(
        "processor starting sentiment_backend=%s topic=%s consumer_group=%s brokers=%s",
        backend.name, settings.kafka_topic_raw, settings.kafka_consumer_group, settings.kafka_brokers,
    )

    try:
        app.run()
    finally:
        logger.info("processor shutting down")
        store.close()


if __name__ == "__main__":
    main()
