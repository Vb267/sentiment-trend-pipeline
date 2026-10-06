"""The interface every source adapter implements.

Deliberately a plain sync generator rather than async: Bluesky's firehose
is naturally a blocking recv-loop and Reddit's PRAW stream is already a
blocking generator, so sync is the common denominator and keeps the
producer trivial (`for post in source.stream(): produce(post)`).
Adding a new source means adding a new class with this shape - nothing
else in the producer changes.
"""

from __future__ import annotations

from typing import Iterator, Protocol

from common.schema import RawPost


class Source(Protocol):
    name: str

    def stream(self) -> Iterator[RawPost]:
        """Yield RawPost items. May run forever; callers decide when to stop."""
        ...
