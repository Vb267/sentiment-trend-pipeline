"""Shared message schema for the pipeline.

RawPost is what every source produces and what lands on the `raw_posts`
topic. ScoredPost is a RawPost plus the fields the inference step adds.
Keeping this in one place means the producer, processor, and dashboard all
agree on field names without importing each other.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, fields


@dataclass(slots=True)
class RawPost:
    id: str
    source: str
    author: str
    text: str
    created_at: str  # ISO8601
    fetched_at: str  # ISO8601
    url: str

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_dict(cls, data: dict) -> "RawPost":
        known = {f.name for f in fields(cls)}
        return cls(**{k: data[k] for k in known})

    @classmethod
    def from_json(cls, raw: str) -> "RawPost":
        return cls.from_dict(json.loads(raw))


@dataclass(slots=True)
class ScoredPost(RawPost):
    sentiment_label: str
    sentiment_score: float

    @classmethod
    def from_raw(cls, post: RawPost, sentiment_label: str, sentiment_score: float) -> "ScoredPost":
        return cls(
            **{f.name: getattr(post, f.name) for f in fields(RawPost)},
            sentiment_label=sentiment_label,
            sentiment_score=sentiment_score,
        )
