from common.trends import TrendParams, evaluate

WINDOW = 300_000
PARAMS = TrendParams(window_ms=WINDOW, zscore_threshold=2.5, baseline_windows=4,
                     min_baseline_windows=3, min_count=10)


def _feed(counts: list[int]) -> dict:
    state = None
    for i, c in enumerate(counts):
        _, state = evaluate(state, i * WINDOW, c, PARAMS)
    return state


def test_spike_over_steady_baseline_is_trending() -> None:
    state = _feed([10, 12, 11, 10])
    result, _ = evaluate(state, 4 * WINDOW, 40, PARAMS)
    assert result.baseline_windows == 4
    assert result.is_trending
    assert result.zscore > 2.5


def test_steady_volume_is_not_trending() -> None:
    state = _feed([10, 12, 11, 10])
    result, _ = evaluate(state, 4 * WINDOW, 12, PARAMS)
    assert not result.is_trending


def test_skipped_windows_are_filled_with_zeros() -> None:
    state = _feed([20])
    # term absent for windows 1-3, back at window 4
    result, new_state = evaluate(state, 4 * WINDOW, 20, PARAMS)
    assert result.baseline_mean == 5.0  # baseline [20, 0, 0, 0]
    assert new_state["counts"] == [0, 0, 0, 20]
    assert new_state["last_start"] == 4 * WINDOW


def test_needs_minimum_baseline_before_flagging() -> None:
    state = _feed([0, 0])
    result, _ = evaluate(state, 2 * WINDOW, 100, PARAMS)
    assert result.baseline_windows == 2
    assert not result.is_trending


def test_new_term_uses_implied_zero_history() -> None:
    result, state = evaluate(None, 10 * WINDOW, 25, PARAMS, implied_zero_windows=10)
    assert result.baseline_windows == 4  # capped at baseline_windows
    assert result.baseline_mean == 0.0
    assert result.zscore == 25.0  # std floor of 1.0 avoids divide-by-zero
    assert result.is_trending
    assert state["counts"] == [0, 0, 0, 25]


def test_brand_new_term_with_no_runtime_history_is_not_flagged() -> None:
    result, _ = evaluate(None, 0, 500, PARAMS, implied_zero_windows=0)
    assert not result.is_trending


def test_below_min_count_is_never_trending() -> None:
    state = _feed([0, 0, 0, 0])
    result, _ = evaluate(state, 4 * WINDOW, 9, PARAMS)
    assert result.zscore > 2.5
    assert not result.is_trending


def test_replayed_window_does_not_change_state() -> None:
    state = _feed([10, 12, 11])
    _, replayed = evaluate(state, 2 * WINDOW, 11, PARAMS)
    assert replayed == state
