from common.terms import extract_terms


def test_drops_urls_mentions_stopwords_and_short_tokens() -> None:
    text = "The election results are in! @someone.bsky.social see https://example.com/x ok"
    assert extract_terms(text) == {"election", "results"}


def test_lowercases_and_dedupes_within_a_post() -> None:
    assert extract_terms("Eclipse eclipse ECLIPSE") == {"eclipse"}


def test_keeps_hashtags_distinct_from_bare_words() -> None:
    assert extract_terms("#eclipse eclipse") == {"#eclipse", "eclipse"}


def test_ignores_numbers_and_handles_empty_text() -> None:
    assert extract_terms("2026 123abc") == {"abc"}
    assert extract_terms("") == set()
