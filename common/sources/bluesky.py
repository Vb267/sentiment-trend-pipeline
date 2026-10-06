"""Bluesky Jetstream firehose source.

Jetstream (wss://jetstream2.us-east.bsky.network/subscribe) is a free,
no-auth-required, filtered firehose of the AT Protocol network - it emits
one JSON event per commit (post create/update/delete, like, follow, ...).
We filter to `app.bsky.feed.post` creates server-side via
`wantedCollections` so we don't pay the bandwidth/parsing cost for events
we'd throw away anyway.

Known tradeoff: the event carries the author's DID (a stable but opaque
id, e.g. did:plc:abc123...), not their human-readable handle. Resolving a
handle requires a separate API call per author; doing that inline would
add latency and a rate-limit dependency to the hot path, so we keep the
DID as-is and treat handle resolution as a possible enrichment step later.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import Iterator

import websocket

from common.schema import RawPost

logger = logging.getLogger(__name__)

JETSTREAM_URL = "wss://jetstream2.us-east.bsky.network/subscribe"
RECONNECT_DELAY_SECONDS = 3


class BlueskySource:
    name = "bluesky"

    def __init__(self, wanted_collections: str = "app.bsky.feed.post") -> None:
        self._url = f"{JETSTREAM_URL}?wantedCollections={wanted_collections}"

    def stream(self) -> Iterator[RawPost]:
        while True:
            try:
                yield from self._stream_once()
            except Exception:
                logger.exception("jetstream connection dropped, reconnecting")
                time.sleep(RECONNECT_DELAY_SECONDS)

    def _stream_once(self) -> Iterator[RawPost]:
        ws = websocket.create_connection(self._url, timeout=30)
        logger.info("connected to bluesky jetstream url=%s", self._url)
        try:
            while True:
                raw = ws.recv()
                post = self._parse(raw)
                if post is not None:
                    yield post
        finally:
            ws.close()

    @staticmethod
    def _parse(raw: str) -> RawPost | None:
        try:
            event = json.loads(raw)
        except json.JSONDecodeError:
            return None

        if event.get("kind") != "commit":
            return None

        commit = event.get("commit") or {}
        if commit.get("operation") != "create" or commit.get("collection") != "app.bsky.feed.post":
            return None

        record = commit.get("record") or {}
        text = record.get("text")
        if not text:
            return None

        did = event.get("did", "unknown")
        rkey = commit.get("rkey", "")
        return RawPost(
            id=f"{did}/{rkey}",
            source="bluesky",
            author=did,
            text=text,
            created_at=record.get("createdAt") or datetime.now(timezone.utc).isoformat(),
            fetched_at=datetime.now(timezone.utc).isoformat(),
            url=f"https://bsky.app/profile/{did}/post/{rkey}",
        )
