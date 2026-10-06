from __future__ import annotations

from common.config import Settings
from common.sources.base import Source
from common.sources.bluesky import BlueskySource
from common.sources.reddit import RedditSource

__all__ = ["Source", "get_source"]


def get_source(settings: Settings) -> Source:
    if settings.source == "bluesky":
        return BlueskySource(wanted_collections=settings.bluesky_wanted_collections)
    if settings.source == "reddit":
        if not settings.reddit_client_id or not settings.reddit_client_secret:
            raise ValueError(
                "REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET are required for SOURCE=reddit "
                "(free to create at https://www.reddit.com/prefs/apps)"
            )
        return RedditSource(
            client_id=settings.reddit_client_id,
            client_secret=settings.reddit_client_secret,
            user_agent=settings.reddit_user_agent,
            subreddits=settings.reddit_subreddits,
        )
    raise ValueError(f"unknown source: {settings.source!r} (expected 'bluesky' or 'reddit')")
