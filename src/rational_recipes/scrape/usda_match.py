"""Confidence-gated ingredient-name resolution against ingredients.db.

Used by the regex parser (vwt.17) to decide whether a candidate
ingredient name is trustworthy enough to skip the LLM. Two tiers:

1. Exact synonym hit via ``IngredientFactory.get_by_name`` — confidence 1.0.
2. Fuzzy ``difflib.SequenceMatcher`` match against synonym candidates
   from the same DB — accepted only if similarity ≥ ``threshold``.

The bias is conservative: a miss returns ``None`` so the caller falls
through to the LLM. A wrong name in a regex parse poisons variant
statistics; an LLM cost is cheap by comparison (vwt.17 acceptance note).

English-canonical guarantee (r6w): Some FDC/FAO rows store a Swedish
canonical_name (``valnötter``, ``tomat``, ``olja``, ``pekannötter``).
The PWA shows English unconditionally per project policy
(``project_english_display``), and the LLM hot path emits English
because of the e4s NEUTRAL_PROMPT update. The resolver matches that
contract by post-translating any DB result through
``scrape.canonical.SWEDISH_TO_ENGLISH`` — so regex hits and LLM hits on
the same line produce the same canonical and land in the same variant.
The same dict pre-translates the input, so a Swedish noun whose only
DB hit is a Swedish-canonical row resolves to English in one hop.
"""

from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path

from rational_recipes.ingredient import Factory as IngredientFactory
from rational_recipes.scrape.canonical import (
    SWEDISH_TO_ENGLISH,
    canonicalize_name,
)

DEFAULT_SIMILARITY_THRESHOLD = 0.85
"""Minimum SequenceMatcher.ratio() for fuzzy matches to be accepted.

0.85 catches obvious typos ("flouur" → "flour") and minor English
spelling variants without admitting Swedish/German look-alikes
("mjöl" vs "mil", "sucker" vs "sugar"). Tune via the threshold arg
on resolve_canonical_name once real corpus residue is measured.
"""


_DB_PATH = (
    Path(__file__).resolve().parent.parent / "data" / "ingredients.db"
)


@dataclass(frozen=True, slots=True)
class CanonicalMatch:
    """Resolution outcome for one raw ingredient name."""

    canonical: str
    similarity: float
    """1.0 when the synonym table had an exact hit; SequenceMatcher
    ratio for fuzzy hits; 0.0 only on synthetic test paths."""


def _to_english(name: str) -> str:
    """Map a Swedish canonical/synonym to English; passthrough on miss.

    Mirrors ``scrape.canonical._translate_swedish``. Imported via the
    shared dict so a single source of truth governs both the
    canonicalize_name path (LLM-fed) and the regex-resolve path (this
    module). Empty / non-string input returns the input unchanged.
    """
    if not name:
        return name
    return SWEDISH_TO_ENGLISH.get(name.lower().strip(), name)


def resolve_canonical_name(
    name: str,
    *,
    threshold: float = DEFAULT_SIMILARITY_THRESHOLD,
    candidate_limit: int = 8,
) -> CanonicalMatch | None:
    """Resolve ``name`` against ingredients.db with a confidence floor.

    Returns ``None`` when the best match is below ``threshold`` so
    callers can fall through to the LLM. ``candidate_limit`` caps the
    number of LIKE-matched synonyms scored for fuzzy similarity — kept
    small because each is a string-distance computation.

    Per-synonym canonical (dfm): exact hits return ``canonicalize_name``'s
    output (which preserves specificity — ``cheddar`` vs ``cheese``,
    ``red onion`` vs ``onion`` — while still collapsing plural/singular
    pairs and translating Swedish to English). Fuzzy hits route through
    the same function so all paths share one canonical vocabulary.
    """
    if not name:
        return None
    normalized = name.lower().strip()
    if not normalized:
        return None
    pre_translated = _to_english(normalized)

    # Tier 1: exact synonym hit (per-synonym canonical via canonicalize_name).
    for candidate in {pre_translated, normalized}:
        try:
            IngredientFactory.get_by_name(candidate)
        except KeyError:
            continue
        canonical = canonicalize_name(candidate)
        if canonical:
            return CanonicalMatch(canonical=canonical, similarity=1.0)

    # Tier 2: fuzzy. Pull a small candidate set whose synonym contains
    # at least one query word, then score by SequenceMatcher.
    candidates = _candidate_synonyms(pre_translated, limit=candidate_limit)
    if not candidates:
        return None

    best_synonym: str | None = None
    best_score = 0.0
    for synonym in candidates:
        score = SequenceMatcher(
            None, pre_translated.lower(), synonym.lower()
        ).ratio()
        if score > best_score:
            best_score = score
            best_synonym = synonym

    if best_synonym is None or best_score < threshold:
        return None

    # Route the fuzzy synonym through canonicalize_name so it lands on the
    # same per-synonym/plural-collapsed/post-translated form as exact hits.
    fuzzy_canonical = canonicalize_name(best_synonym.lower())
    if not fuzzy_canonical:
        return None
    return CanonicalMatch(canonical=fuzzy_canonical, similarity=best_score)


def _candidate_synonyms(name: str, *, limit: int) -> list[str]:
    """SQL-search synonyms whose name shares at least one word with ``name``.

    Returns a tight candidate set so SequenceMatcher only runs on
    plausible matches — full-table scoring would be too slow for the
    per-line hot path. Thread-safe via ``_DB_LOCK``.
    """
    words = [w for w in name.split() if w]
    if not words:
        return []

    # For multi-word queries, require all words to appear (AND); for
    # single-word queries we still match substring.
    conditions = " AND ".join("LOWER(name) LIKE ?" for _ in words)
    params: list[object] = [f"%{w}%" for w in words]
    params.append(limit)
    with _DB_LOCK:
        conn = _open_db()
        rows = conn.execute(
            f"SELECT name FROM synonym WHERE {conditions} "
            f"ORDER BY length(name) ASC LIMIT ?",
            params,
        ).fetchall()
    return [r[0] for r in rows]


_CACHED_CONN: sqlite3.Connection | None = None
_DB_LOCK = threading.Lock()


def _open_db() -> sqlite3.Connection:
    """One process-wide read-only handle on ingredients.db."""
    global _CACHED_CONN
    if _CACHED_CONN is None:
        _CACHED_CONN = sqlite3.connect(
            str(_DB_PATH),
            check_same_thread=False,
        )
    return _CACHED_CONN
