"""Segment readability classification for EOT URL analysis.

The core methodological primitive for the "human-readable -> machine-readable"
research question. Every notebook imports `classify_segment` from here so the
definition is single-sourced and defensible.

A URL path segment is classified into exactly one of three classes:

  'human'   - contains at least one natural-language word (per `wordfreq`),
              e.g. "press-releases", "about", "national-parks"
  'acronym' - short all-alphabetic token that is not a dictionary word; likely
              an agency abbreviation or initialism, e.g. "oia", "blm", "foia"
  'machine' - numeric IDs, hashes, query-construction characters, or
              digit-dominated / gibberish tokens, e.g. "12345", "node"->"12345",
              "a3f9c1e0b2d4", "id=27", "viewpage%3f"

Design notes
------------
- Classification is purely *linguistic*: it judges whether a segment reads like
  language, NOT whether it belongs to a CMS. "node" is a real English word, so
  it is 'human' here; the Drupal interpretation of /node/<id> is handled
  separately in the rq2 (construction / CMS-tell) analysis, where the *pair*
  (node, 12345) reads as machine because the id segment is machine.
- Word membership uses `wordfreq.zipf_frequency`. Zipf is a log10 frequency on a
  1-8 scale; ~3.0 is a reasonably common word, ~2.0 is rare-but-real. We default
  to 2.5. Falls back to the system word list if wordfreq is unavailable.

Tunables are module-level constants so the methods notebook can display and
sensitivity-test them.
"""
from __future__ import annotations

import re
from functools import lru_cache

# --- tunables (shown/validated in 00_data_and_methods.ipynb) ------------------
WORD_ZIPF_MIN = 2.5        # min wordfreq zipf score to count a token as a word
MIN_WORD_LEN = 3           # tokens shorter than this are never "words"
ACRONYM_MAX_LEN = 6        # max length of an all-alpha token to be an acronym
DIGIT_RATIO_MACHINE = 0.4  # segment with >40% digits is machine
HEX_MIN_LEN = 12           # single all-hex token >= this length is a hash

CLASSES = ("human", "acronym", "machine")

# query/construction characters: their presence signals machine assembly
_CONSTRUCTION_RE = re.compile(r"[=;+&%$,@!*()\[\]{}|<>]")
_HEX_RE = re.compile(r"^[0-9a-f]+$")
_ALPHA_RUN_RE = re.compile(r"[^a-z]+")  # split on any non-letter run

# --- word lookup backend ------------------------------------------------------
try:
    from wordfreq import zipf_frequency as _zipf

    HAVE_WORDFREQ = True

    def _is_word(token: str) -> bool:
        if len(token) < MIN_WORD_LEN:
            return False
        return _zipf(token, "en") >= WORD_ZIPF_MIN

    def word_score(token: str) -> float:
        """Zipf frequency (1-8) of a token; 0.0 if unknown. For validation UIs."""
        return _zipf(token, "en")

except ImportError:  # pragma: no cover - fallback path for bare environments
    HAVE_WORDFREQ = False
    _FALLBACK_WORDS: set[str] = set()
    for _p in ("/usr/share/dict/words", "/usr/dict/words"):
        try:
            with open(_p) as _f:
                _FALLBACK_WORDS = {w.strip().lower() for w in _f if len(w.strip()) >= MIN_WORD_LEN}
            break
        except OSError:
            continue

    def _is_word(token: str) -> bool:
        return len(token) >= MIN_WORD_LEN and token in _FALLBACK_WORDS

    def word_score(token: str) -> float:
        return 1.0 if _is_word(token) else 0.0


@lru_cache(maxsize=500_000)
def classify_segment(seg: str | None) -> str | None:
    """Classify a single URL path segment as 'human', 'acronym', or 'machine'.

    Returns None for empty / null input (caller should drop these).
    """
    if seg is None:
        return None
    s = seg.strip().lower()
    if s == "":
        return None

    # 1. construction characters => machine-assembled
    if _CONSTRUCTION_RE.search(s):
        return "machine"

    # 2. digit dominance
    digits = sum(c.isdigit() for c in s)
    if digits / len(s) > DIGIT_RATIO_MACHINE:
        return "machine"

    # 3. alphabetic tokens (letters only; digits/punct act as separators)
    tokens = [t for t in _ALPHA_RUN_RE.split(s) if t]
    if not tokens:  # pure numeric / punctuation, e.g. "12345"
        return "machine"

    # 4. single long all-hex token => hash / uuid-ish
    if len(tokens) == 1 and len(s) >= HEX_MIN_LEN and _HEX_RE.match(s):
        return "machine"

    # 5. any real dictionary word => human
    if any(_is_word(t) for t in tokens):
        return "human"

    # 6. no real word, but short & alphabetic => acronym / abbreviation
    if all(len(t) <= ACRONYM_MAX_LEN for t in tokens) and len(s) <= ACRONYM_MAX_LEN + 2:
        return "acronym"

    # 7. otherwise: unpronounceable / long non-word => machine
    return "machine"


def classify_series(seg_iter):
    """Vectorized helper: map an iterable of segments to a list of classes."""
    return [classify_segment(s) for s in seg_iter]


def config() -> dict:
    """Return the active tunables (for provenance / methods reporting)."""
    return {
        "backend": "wordfreq" if HAVE_WORDFREQ else "system-dict-fallback",
        "WORD_ZIPF_MIN": WORD_ZIPF_MIN,
        "MIN_WORD_LEN": MIN_WORD_LEN,
        "ACRONYM_MAX_LEN": ACRONYM_MAX_LEN,
        "DIGIT_RATIO_MACHINE": DIGIT_RATIO_MACHINE,
        "HEX_MIN_LEN": HEX_MIN_LEN,
    }


if __name__ == "__main__":
    # quick self-check
    samples = [
        "press-releases", "about", "national-parks", "node", "dataset",
        "12345", "a3f9c1e0b2d4", "oia", "blm", "foia", "uhtbin",
        "index.cfm", "viewpage%3fid", "wp-content", "sites", "id=27",
    ]
    for s in samples:
        print(f"  {s:20s} -> {classify_segment(s)}")
    print(config())
