from datetime import datetime, timezone

from common.sentiment import signed_polarity
from processor.clickhouse import POST_COLUMNS, parse_ts, post_row

FALLBACK = datetime(2026, 1, 1, tzinfo=timezone.utc)


def test_parse_ts_handles_z_suffix_naive_and_junk() -> None:
    assert parse_ts("2026-03-04T05:06:07.123Z", FALLBACK) == datetime(
        2026, 3, 4, 5, 6, 7, 123000, tzinfo=timezone.utc
    )
    assert parse_ts("2026-03-04T05:06:07", FALLBACK).tzinfo == timezone.utc
    assert parse_ts("not a date", FALLBACK) == FALLBACK
    assert parse_ts("0001-01-01T00:00:00Z", FALLBACK) == FALLBACK


def test_post_row_matches_columns_and_falls_back_to_processed_at() -> None:
    value = {
        "id": "1", "source": "bluesky", "author": "did:plc:x", "text": "hi",
        "created_at": "garbage", "fetched_at": "2026-01-01T00:00:01+00:00",
        "processed_at": "2026-01-01T00:00:02+00:00", "url": "u",
        "sentiment_label": "negative", "sentiment_score": 0.5, "polarity": -0.5,
    }
    row = post_row(value)
    assert len(row) == len(POST_COLUMNS)
    assert row[POST_COLUMNS.index("created_at")] == datetime(2026, 1, 1, 0, 0, 2, tzinfo=timezone.utc)


def test_signed_polarity() -> None:
    assert signed_polarity("positive", 0.8) == 0.8
    assert signed_polarity("negative", 0.8) == -0.8
    assert signed_polarity("neutral", 0.8) == 0.0
