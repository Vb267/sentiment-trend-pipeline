from common.schema import RawPost, ScoredPost


def test_raw_post_roundtrips_through_json() -> None:
    post = RawPost(
        id="1",
        source="bluesky",
        author="did:plc:abc",
        text="hello world",
        created_at="2026-01-01T00:00:00Z",
        fetched_at="2026-01-01T00:00:01Z",
        url="https://bsky.app/profile/did:plc:abc/post/1",
    )
    restored = RawPost.from_json(post.to_json())
    assert restored == post


def test_scored_post_from_raw_preserves_raw_fields() -> None:
    post = RawPost(
        id="1",
        source="reddit",
        author="someone",
        text="great news",
        created_at="2026-01-01T00:00:00Z",
        fetched_at="2026-01-01T00:00:01Z",
        url="https://reddit.com/r/news/comments/1",
    )
    scored = ScoredPost.from_raw(post, sentiment_label="positive", sentiment_score=0.9)

    assert scored.id == post.id
    assert scored.text == post.text
    assert scored.sentiment_label == "positive"
    assert scored.sentiment_score == 0.9


def test_raw_post_from_dict_ignores_extra_fields() -> None:
    data = {
        "id": "1",
        "source": "bluesky",
        "author": "did:plc:abc",
        "text": "hi",
        "created_at": "2026-01-01T00:00:00Z",
        "fetched_at": "2026-01-01T00:00:01Z",
        "url": "https://bsky.app/profile/did:plc:abc/post/1",
        "sentiment_label": "positive",  # already-scored message; RawPost should ignore this
        "sentiment_score": 0.9,
    }
    post = RawPost.from_dict(data)
    assert post.id == "1"
