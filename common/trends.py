"""Rolling z-score trend detection on per-term window volume.

Pure logic, no Kafka or state-store dependency, so it's unit-testable. The
processor keeps one `TermHistory` per term in Quix Streams state and calls
`evaluate()` each time a 5-minute window for that term closes.

The one non-obvious part is **gap filling**. A term's window only gets
emitted when the term was mentioned at least once in it, so "mentioned 0
times" never shows up as an event. Without correction the baseline would
only contain the windows where the term appeared, which inflates the mean
and hides spikes. So when a term's next window arrives, the windows it
skipped are filled in as zeros before scoring.

A term seen for the first time has no history in state, but it isn't truly
unknown: if the processor has been running for N windows, the term had 0
mentions in each of them. `implied_zero_windows` lets the caller pass that
in, which is what lets a brand-new term (the most interesting kind of trend)
get flagged at all.
"""

from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class TrendParams:
    window_ms: int
    zscore_threshold: float = 2.5
    baseline_windows: int = 12  # trailing windows kept as the baseline (12 x 5min = 1h)
    min_baseline_windows: int = 3  # don't score until the baseline is at least this long
    min_count: int = 10  # ignore terms below this volume; z-scores of tiny counts are noise
    std_floor: float = 1.0  # stops a flat (e.g. all-zero) baseline from producing infinite z


@dataclass(frozen=True, slots=True)
class TrendResult:
    count: int
    baseline_mean: float
    baseline_std: float
    baseline_windows: int
    zscore: float
    is_trending: bool


def evaluate(
    history: dict | None,
    window_start_ms: int,
    count: int,
    params: TrendParams,
    implied_zero_windows: int = 0,
) -> tuple[TrendResult, dict]:
    """Score `count` against the term's trailing baseline.

    `history` is the term's stored state (`{"last_start": ms, "counts": [...]}`)
    or None for a never-seen term. Returns the result plus the new state to
    store. State is a plain dict so Quix can JSON-serialize it.
    """
    if history is None:
        counts = [0] * min(implied_zero_windows, params.baseline_windows)
    else:
        counts = list(history["counts"])
        skipped = (window_start_ms - history["last_start"]) // params.window_ms - 1
        if skipped < 0:
            # Same or older window seen again (e.g. replay after a restart):
            # score it against the current baseline but don't record it twice.
            result = _score(count, counts, params)
            return result, history
        counts.extend([0] * min(skipped, params.baseline_windows))

    counts = counts[-params.baseline_windows :]
    result = _score(count, counts, params)
    counts.append(count)
    new_state = {"last_start": window_start_ms, "counts": counts[-params.baseline_windows :]}
    return result, new_state


def _score(count: int, baseline: list[int], params: TrendParams) -> TrendResult:
    n = len(baseline)
    mean = sum(baseline) / n if n else 0.0
    std = math.sqrt(sum((c - mean) ** 2 for c in baseline) / n) if n else 0.0
    z = (count - mean) / max(std, params.std_floor)
    is_trending = (
        n >= params.min_baseline_windows
        and count >= params.min_count
        and z >= params.zscore_threshold
    )
    return TrendResult(
        count=count,
        baseline_mean=mean,
        baseline_std=std,
        baseline_windows=n,
        zscore=z,
        is_trending=is_trending,
    )
