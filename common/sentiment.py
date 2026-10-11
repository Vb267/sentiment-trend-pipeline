"""Sentiment backends, swappable behind one interface.

Two implementations on purpose:
  - VaderSentimentBackend: lexicon-based, instant, no download, no GPU/CPU
    cost worth mentioning. Weaker on sarcasm, slang, and short social-media
    text, but a fine default for "does the pipeline work end to end".
  - TransformerSentimentBackend: cardiffnlp/twitter-roberta-base-sentiment-latest,
    a RoBERTa model fine-tuned on tweets. Much better accuracy on real social
    text, but ~500MB download on first use and meaningfully slower per message
    (still fine on CPU in small batches).

Both take a *list* of texts and return a list of (label, score) tuples, so
callers can batch (required from Phase 5 onward) without changing the
interface.
"""

from __future__ import annotations

from typing import Protocol, Sequence


class SentimentBackend(Protocol):
    name: str

    def classify(self, texts: Sequence[str]) -> list[tuple[str, float]]: ...


class VaderSentimentBackend:
    name = "vader"

    def __init__(self) -> None:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

        self._analyzer = SentimentIntensityAnalyzer()

    def classify(self, texts: Sequence[str]) -> list[tuple[str, float]]:
        results: list[tuple[str, float]] = []
        for text in texts:
            compound = self._analyzer.polarity_scores(text)["compound"]
            if compound >= 0.05:
                label = "positive"
            elif compound <= -0.05:
                label = "negative"
            else:
                label = "neutral"
            results.append((label, abs(compound)))
        return results


class TransformerSentimentBackend:
    name = "transformer"
    MODEL_NAME = "cardiffnlp/twitter-roberta-base-sentiment-latest"

    def __init__(self) -> None:
        from transformers import pipeline

        self._pipe = pipeline(
            task="sentiment-analysis",
            model=self.MODEL_NAME,
            tokenizer=self.MODEL_NAME,
        )

    def classify(self, texts: Sequence[str]) -> list[tuple[str, float]]:
        if not texts:
            return []
        batch_size = min(32, len(texts))
        results = self._pipe(list(texts), truncation=True, batch_size=batch_size)
        return [(r["label"].lower(), float(r["score"])) for r in results]


def signed_polarity(label: str, score: float) -> float:
    """Collapse (label, score) into one number in [-1, 1] that can be averaged.

    `score` is a magnitude/confidence, not a direction, so averaging it
    directly would treat a confident negative the same as a confident
    positive. Neutral counts as 0 rather than being dropped, so a window full
    of neutral posts pulls the average toward 0, which is what it should do.
    """
    if label == "positive":
        return score
    if label == "negative":
        return -score
    return 0.0


def get_sentiment_backend(name: str) -> SentimentBackend:
    if name == "vader":
        return VaderSentimentBackend()
    if name == "transformer":
        return TransformerSentimentBackend()
    raise ValueError(f"unknown sentiment backend: {name!r} (expected 'vader' or 'transformer')")
