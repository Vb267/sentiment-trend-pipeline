import json

from common.sources.bluesky import BlueskySource


def test_parse_ignores_non_commit_events() -> None:
    assert BlueskySource._parse(json.dumps({"kind": "identity"})) is None


def test_parse_ignores_non_post_collections() -> None:
    event = {
        "did": "did:plc:abc",
        "kind": "commit",
        "commit": {
            "operation": "create",
            "collection": "app.bsky.feed.like",
            "rkey": "xyz",
            "record": {"$type": "app.bsky.feed.like"},
        },
    }
    assert BlueskySource._parse(json.dumps(event)) is None


def test_parse_extracts_post_create_event() -> None:
    event = {
        "did": "did:plc:abc",
        "kind": "commit",
        "commit": {
            "operation": "create",
            "collection": "app.bsky.feed.post",
            "rkey": "xyz",
            "record": {
                "$type": "app.bsky.feed.post",
                "text": "hello world",
                "createdAt": "2026-01-01T00:00:00.000Z",
            },
        },
    }
    post = BlueskySource._parse(json.dumps(event))
    assert post is not None
    assert post.id == "did:plc:abc/xyz"
    assert post.text == "hello world"
    assert post.source == "bluesky"
    assert post.url == "https://bsky.app/profile/did:plc:abc/post/xyz"


def test_parse_ignores_malformed_json() -> None:
    assert BlueskySource._parse("not json") is None


def test_parse_ignores_post_without_text() -> None:
    event = {
        "did": "did:plc:abc",
        "kind": "commit",
        "commit": {
            "operation": "create",
            "collection": "app.bsky.feed.post",
            "rkey": "xyz",
            "record": {"$type": "app.bsky.feed.post", "createdAt": "2026-01-01T00:00:00.000Z"},
        },
    }
    assert BlueskySource._parse(json.dumps(event)) is None
