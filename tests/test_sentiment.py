from common.sentiment import VaderSentimentBackend, get_sentiment_backend


def test_vader_classifies_obviously_positive_text() -> None:
    backend = VaderSentimentBackend()
    (label, score), = backend.classify(["I absolutely love this, it's wonderful!"])
    assert label == "positive"
    assert score > 0


def test_vader_classifies_obviously_negative_text() -> None:
    backend = VaderSentimentBackend()
    (label, score), = backend.classify(["This is terrible and awful, I hate it."])
    assert label == "negative"
    assert score > 0


def test_vader_handles_empty_batch() -> None:
    backend = VaderSentimentBackend()
    assert backend.classify([]) == []


def test_get_sentiment_backend_rejects_unknown_name() -> None:
    try:
        get_sentiment_backend("not-a-real-backend")
        assert False, "expected ValueError"
    except ValueError:
        pass
