"""Phase 3 processor: Redpanda `raw_posts` -> sentiment -> windows + trends -> ClickHouse.

Built on Quix Streams' Application/StreamingDataFrame, which wraps a Kafka
consumer group and handles offset checkpointing, state, and graceful
SIGTERM/SIGINT shutdown for us.

Topology (one consumer, three branches off the scored stream):

    raw_posts -> score -+-> scored_posts                        (every message)
                        +-> group_by(source) -> 5-min tumbling
                        |     window -> sentiment_windows        (one row per source per window)
                        +-> explode into terms -> group_by(term)
                              -> 5-min tumbling count -> z-score vs
                              trailing baseline -> term_windows  (one row per busy term per window)

Every message on `raw_posts` is keyed by post id, so to aggregate per source
or per term the stream has to be re-keyed first. `group_by` does that by
writing to an internal `repartition__...` topic and reading it back, so all
messages for one key land on one partition (and one consumer). Quix creates
those topics, and the changelog topics backing window state, on its own.

Windows are **processing-time**: the topic's timestamp extractor stamps each
message with the wall clock when the processor reads it, ignoring the
post's `created_at`. With a live firehose the two are seconds apart. Where
event-time *would* differ is a backlog: if the processor is down for an
hour and then catches up, processing-time puts that hour's posts in the
catch-up windows, not the windows they were posted in. Event-time would
fix that, but needs a grace period for late data and trusting
client-supplied timestamps, which on Bluesky can be anything.

Windows close on the **partition** watermark rather than per key. With the
default per-key closing, a term's window would only be emitted the next time
that term shows up, which could be hours later, far too late to call it
trending. The flip side: a window only closes once a newer message arrives
on its partition, so if the stream stops entirely, the last window stays
open until traffic resumes.

Inference is still one message at a time; Phase 5 batches it.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone

from quixstreams import Application, State
from quixstreams.dataframe.windows import Count, Mean, Sum

from common.config import get_settings
from common.logging_setup import configure_logging
from common.schema import RawPost
from common.sentiment import get_sentiment_backend, signed_polarity
from common.terms import extract_terms
from common.trends import TrendParams, evaluate
from processor.clickhouse import PostsSink, SentimentWindowsSink, TermWindowsSink, connect

logger = logging.getLogger(__name__)


def _processing_time(value, headers, timestamp, timestamp_type) -> int:
    return int(time.time() * 1000)


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)

    backend = get_sentiment_backend(settings.sentiment_backend)
    clickhouse = connect(settings)

    window = timedelta(minutes=settings.window_minutes)
    window_ms = int(window.total_seconds() * 1000)
    trend_params = TrendParams(
        window_ms=window_ms,
        zscore_threshold=settings.trend_zscore_threshold,
        baseline_windows=settings.trend_baseline_windows,
        min_baseline_windows=settings.trend_min_baseline_windows,
        min_count=settings.trend_min_count,
    )

    app = Application(
        broker_address=settings.kafka_brokers,
        consumer_group=settings.kafka_consumer_group,
        auto_offset_reset="earliest",
    )
    raw_topic = app.topic(
        settings.kafka_topic_raw,
        value_deserializer="json",
        timestamp_extractor=_processing_time,
    )

    def score(value: dict) -> dict:
        post = RawPost.from_dict(value)
        (label, score), = backend.classify([post.text])
        return {
            **value,
            "sentiment_label": label,
            "sentiment_score": score,
            "polarity": signed_polarity(label, score),
            "processed_at": datetime.now(timezone.utc).isoformat(),
        }

    scored = app.dataframe(topic=raw_topic).apply(score)

    # Branch 1: every scored post.
    scored.sink(PostsSink(clickhouse))

    # Branch 2: per-source sentiment windows. Only the fields the window
    # needs are carried into the repartition topic, to keep it small.
    by_source = scored.apply(
        lambda v: {
            "source": v["source"],
            "polarity": v["polarity"],
            "is_positive": int(v["sentiment_label"] == "positive"),
            "is_negative": int(v["sentiment_label"] == "negative"),
            "is_neutral": int(v["sentiment_label"] == "neutral"),
        }
    ).group_by("source")
    sentiment_windows = (
        by_source.tumbling_window(window, name="sentiment_by_source")
        .agg(
            message_count=Count(),
            avg_polarity=Mean("polarity"),
            positive_count=Sum("is_positive"),
            negative_count=Sum("is_negative"),
            neutral_count=Sum("is_neutral"),
        )
        .final(closing_strategy="partition")
    )
    sentiment_windows.sink(SentimentWindowsSink(clickhouse))

    # Branch 3: per-term volume windows, then z-score against each term's
    # own trailing baseline (kept in Quix state, keyed by term).
    by_term = scored.apply(
        lambda v: [{"term": t} for t in extract_terms(v["text"])], expand=True
    ).group_by("term")
    term_windows = (
        by_term.tumbling_window(window, name="term_volume")
        .agg(mention_count=Count())
        .final(closing_strategy="partition")
    )

    # Earliest window this process has closed. A never-before-seen term is
    # treated as having had 0 mentions in every window since then (see
    # common/trends.py). Process-local on purpose: after a restart it resets,
    # which only shortens new terms' implied baseline, i.e. errs toward not
    # flagging.
    first_window_start: list[int] = []

    def score_trend(value: dict, key, timestamp, headers, state: State) -> dict:
        if not first_window_start:
            first_window_start.append(value["start"])
        implied = (value["start"] - first_window_start[0]) // window_ms
        result, new_history = evaluate(
            state.get("history"), value["start"], value["mention_count"], trend_params, implied
        )
        state.set("history", new_history)
        return {
            "start": value["start"],
            "end": value["end"],
            "term": key,
            "mention_count": result.count,
            "baseline_mean": result.baseline_mean,
            "baseline_std": result.baseline_std,
            "baseline_windows": result.baseline_windows,
            "zscore": result.zscore,
            "is_trending": result.is_trending,
        }

    # Every term window updates its history, but only terms with real volume
    # are written out; the long tail of 1-2 mention terms would otherwise be
    # most of the table and none of the signal.
    trends = term_windows.apply(score_trend, stateful=True, metadata=True).filter(
        lambda v: v["mention_count"] >= trend_params.min_count
    )
    def log_if_trending(value: dict) -> None:
        if value["is_trending"]:
            logger.info(
                "trending term=%s count=%d baseline_mean=%.1f zscore=%.1f",
                value["term"], value["mention_count"], value["baseline_mean"], value["zscore"],
            )

    trends.update(log_if_trending)
    trends.sink(TermWindowsSink(clickhouse))

    logger.info(
        "processor starting sentiment_backend=%s topic=%s consumer_group=%s brokers=%s "
        "clickhouse=%s:%s window_minutes=%d",
        backend.name, settings.kafka_topic_raw, settings.kafka_consumer_group,
        settings.kafka_brokers, settings.clickhouse_host, settings.clickhouse_port,
        settings.window_minutes,
    )

    try:
        app.run()
    finally:
        logger.info("processor shutting down")
        clickhouse.close()


if __name__ == "__main__":
    main()
