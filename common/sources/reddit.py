"""Reddit source via PRAW.

PRAW's `subreddit.stream.submissions()` is already a blocking generator
that polls Reddit's listing endpoints internally (with backoff), which is
exactly the shape our `Source.stream()` interface wants - no extra
polling loop needed here. `skip_existing=True` avoids replaying a
subreddit's entire history the first time the process starts.

Requires a free Reddit "script" app (client id + secret) from
https://www.reddit.com/prefs/apps - no cost, but it is a manual signup
step, which is why Bluesky (zero setup) is the default source.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Iterator

import praw

from common.schema import RawPost

logger = logging.getLogger(__name__)


class RedditSource:
    name = "reddit"

    def __init__(self, client_id: str, client_secret: str, user_agent: str, subreddits: str) -> None:
        self._reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent,
        )
        self._subreddits = subreddits  # e.g. "news" or "news+worldnews+technology"

    def stream(self) -> Iterator[RawPost]:
        subreddit = self._reddit.subreddit(self._subreddits)
        logger.info("streaming reddit submissions subreddits=%s", self._subreddits)
        for submission in subreddit.stream.submissions(skip_existing=True):
            text = submission.title
            if submission.selftext:
                text = f"{text}\n\n{submission.selftext}"
            yield RawPost(
                id=f"t3_{submission.id}",
                source="reddit",
                author=str(submission.author) if submission.author else "[deleted]",
                text=text,
                created_at=datetime.fromtimestamp(submission.created_utc, tz=timezone.utc).isoformat(),
                fetched_at=datetime.now(timezone.utc).isoformat(),
                url=f"https://reddit.com{submission.permalink}",
            )
