"""Phase 2 producer: source -> Redpanda topic `raw_posts`.

Deliberately dumb: no inference here. The producer's only job is getting
messages onto the topic reliably and keyed by id (for idempotency
downstream), so that it can be scaled or swapped independently of the
processor. Uses confluent-kafka directly (librdkafka under the hood) -
Quix Streams is used on the processor side where its windowing/consumer-
group ergonomics actually pay for themselves (from Phase 3 onward); for a
plain produce loop, confluent-kafka is the simpler, more standard choice.
"""

from __future__ import annotations

import logging
import signal
import types

from confluent_kafka import KafkaError, Message, Producer

from common.config import get_settings
from common.logging_setup import configure_logging
from common.sources import get_source

logger = logging.getLogger(__name__)

_shutdown = False


def _handle_signal(signum: int, frame: types.FrameType | None) -> None:
    global _shutdown
    logger.info("shutdown signal received signum=%s", signum)
    _shutdown = True


def _delivery_report(err: KafkaError | None, msg: Message) -> None:
    if err is not None:
        logger.error("delivery failed key=%s error=%s", msg.key(), err)


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    source = get_source(settings)
    producer = Producer({"bootstrap.servers": settings.kafka_brokers})

    logger.info(
        "producer starting source=%s topic=%s brokers=%s",
        source.name, settings.kafka_topic_raw, settings.kafka_brokers,
    )

    produced = 0
    try:
        for post in source.stream():
            if _shutdown:
                break
            producer.produce(
                topic=settings.kafka_topic_raw,
                key=post.id,
                value=post.to_json(),
                callback=_delivery_report,
            )
            producer.poll(0)  # trigger delivery-report callbacks without blocking
            produced += 1
            if produced % 50 == 0:
                logger.info("produced %d messages", produced)
    finally:
        logger.info("flushing producer, produced=%d messages total", produced)
        producer.flush(10)


if __name__ == "__main__":
    main()
