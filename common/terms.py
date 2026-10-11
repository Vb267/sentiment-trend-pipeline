"""Turn a post's text into the set of terms it mentions, for trend detection.

Deliberately simple: lowercase, drop URLs and @mentions, keep word-ish tokens
(letters only, 3-30 chars, optional leading `#` so hashtags count separately
from the bare word), drop English stopwords, dedupe within the post.

Dedupe matters: trend volume counts *posts mentioning a term*, not raw
occurrences, so one post repeating a word 50 times can't fake a spike.

Known limits, accepted for now: stopwords are English-only (Bluesky is
multilingual, but common words in any language have a stable baseline and
so don't produce z-score spikes anyway), and languages written without
spaces (Japanese, Chinese) come out as whole-phrase tokens that rarely
repeat, so they're effectively invisible to trend detection.
"""

from __future__ import annotations

import re

_URL_RE = re.compile(r"https?://\S+|www\.\S+")
_MENTION_RE = re.compile(r"@[\w.-]+")
_TOKEN_RE = re.compile(r"#?[^\W\d_]{3,30}")

STOPWORDS = frozenset(
    """
    about above after again against all also and any are aren't because been before
    being below between both but can cannot could did does doing don't down during
    each even ever every few for from further get gets getting going gonna got had
    has have having her here hers herself him himself his how into its itself just
    let like lol made make many more most much must myself never new not now off
    once one only other our ours ourselves out over own really same say said she
    should since some still such than that thats the their theirs them themselves
    then there these they thing things think this those though through too under
    until upon very want was way were what when where which while who whom why will
    with would yeah yes yet you your yours yourself yourselves know see time people
    good day today back well right need
    """.split()
)


def extract_terms(text: str) -> set[str]:
    text = _URL_RE.sub(" ", text.lower())
    text = _MENTION_RE.sub(" ", text)
    return {t for t in _TOKEN_RE.findall(text) if t.lstrip("#") not in STOPWORDS}
