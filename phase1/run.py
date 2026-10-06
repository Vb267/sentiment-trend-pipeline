"""Phase 1: prove the inference loop. No broker, no queue.

One process: pull from one source -> score sentiment -> write to SQLite.
This exists to validate the two riskiest pieces (a live source adapter and
the sentiment interface) in isolation, before adding Redpanda in Phase 2
makes failures harder to tell apart (is it the source? the broker? the
consumer?).

Run with:  uv run python -m phase1.run
Then in another terminal:  uv run streamlit run phase1/dashboard.py
"""

from __future__ import annotations

import logging
import signal
import sys
import types

from common.config import get_settings
from common.logging_setup import configure_logging
from common.schema import ScoredPost
from common.sentiment import get_sentiment_backend
from common.sources import get_source
from common.store import SqliteStore

logger = logging.getLogger(__name__)

_shutdown = False


def _handle_signal(signum: int, frame: types.FrameType | None) -> None:
    global _shutdown
    logger.info("shutdown signal received signum=%s", signum)
    _shutdown = True


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    backend = get_sentiment_backend(settings.sentiment_backend)
    source = get_source(settings)
    store = SqliteStore(settings.sqlite_path.replace("phase2.db", "phase1.db"))

    logger.info(
        "phase1 starting source=%s sentiment_backend=%s db=%s",
        source.name, backend.name, store,
    )

    processed = 0
    try:
        for post in source.stream():
            if _shutdown:
                break
            (label, score), = backend.classify([post.text])
            scored = ScoredPost.from_raw(post, sentiment_label=label, sentiment_score=score)
            store.upsert(scored)
            processed += 1
            if processed % 10 == 0:
                logger.info("processed %d messages", processed)
    finally:
        logger.info("shutting down, processed=%d messages total", processed)
        store.close()


if __name__ == "__main__":
    sys.exit(main() or 0)
