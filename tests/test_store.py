import tempfile
from pathlib import Path

from common.schema import RawPost, ScoredPost
from common.store import SqliteStore


def _make_post(post_id: str = "1") -> RawPost:
    return RawPost(
        id=post_id,
        source="bluesky",
        author="did:plc:abc",
        text="hello world",
        created_at="2026-01-01T00:00:00Z",
        fetched_at="2026-01-01T00:00:01Z",
        url="https://bsky.app/profile/did:plc:abc/post/1",
    )


def test_upsert_is_idempotent_on_replay() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        db_path = str(Path(tmp) / "test.db")
        store = SqliteStore(db_path)

        post = _make_post()
        scored = ScoredPost.from_raw(post, sentiment_label="positive", sentiment_score=0.5)
        store.upsert(scored)
        store.upsert(scored)  # simulate a replay after an uncommitted-offset restart

        count = store._conn.execute("SELECT COUNT(*) FROM scored_posts").fetchone()[0]
        assert count == 1

        store.close()


def test_upsert_updates_existing_row_in_place() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        db_path = str(Path(tmp) / "test.db")
        store = SqliteStore(db_path)

        post = _make_post()
        store.upsert(ScoredPost.from_raw(post, sentiment_label="neutral", sentiment_score=0.1))
        store.upsert(ScoredPost.from_raw(post, sentiment_label="positive", sentiment_score=0.9))

        row = store._conn.execute(
            "SELECT sentiment_label, sentiment_score FROM scored_posts WHERE id = ?", (post.id,)
        ).fetchone()
        assert row == ("positive", 0.9)

        store.close()
