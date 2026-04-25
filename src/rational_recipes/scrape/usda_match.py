"""Confidence-gated ingredient-name resolution against ingredients.db.

Used by the regex parser (vwt.17) to decide whether a candidate
ingredient name is trustworthy enough to skip the LLM. Two tiers:

1. Exact synonym hit via ``IngredientFactory.get_by_name`` — confidence 1.0.
2. Fuzzy ``difflib.SequenceMatcher`` match against synonym candidates
   from the same DB — accepted only if similarity ≥ ``threshold``.

The bias is conservative: a miss returns ``None`` so the caller falls
through to the LLM. A wrong name in a regex parse poisons variant
statistics; an LLM cost is cheap by comparison (vwt.17 acceptance note).
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path

from rational_recipes.ingredient import Factory as IngredientFactory

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
    """
    if not name:
        return None
    normalized = name.lower().strip()
    if not normalized:
        return None

    # Tier 1: exact synonym hit (the canonical resolution path used by
    # canonicalize_name + the rest of the pipeline).
    try:
        ingredient = IngredientFactory.get_by_name(normalized)
    except KeyError:
        ingredient = None
    if ingredient is not None:
        canonical = ingredient.canonical_name()
        if canonical:
            return CanonicalMatch(canonical=canonical, similarity=1.0)

    # Tier 2: fuzzy. Pull a small candidate set whose synonym contains
    # at least one query word, then score by SequenceMatcher.
    candidates = _candidate_synonyms(normalized, limit=candidate_limit)
    if not candidates:
        return None

    best_synonym: str | None = None
    best_score = 0.0
    for synonym in candidates:
        score = SequenceMatcher(None, normalized, synonym.lower()).ratio()
        if score > best_score:
            best_score = score
            best_synonym = synonym

    if best_synonym is None or best_score < threshold:
        return None

    # Promote the fuzzy synonym to its canonical name so downstream
    # variant grouping sees the same shared vocabulary as exact matches.
    try:
        fuzzy_ingredient = IngredientFactory.get_by_name(best_synonym.lower())
    except KeyError:
        return None
    fuzzy_canonical = (
        fuzzy_ingredient.canonical_name() if fuzzy_ingredient else None
    )
    if not fuzzy_canonical:
        return None
    return CanonicalMatch(canonical=fuzzy_canonical, similarity=best_score)


def _candidate_synonyms(name: str, *, limit: int) -> list[str]:
    """SQL-search synonyms whose name shares at least one word with ``name``.

    Returns a tight candidate set so SequenceMatcher only runs on
    plausible matches — full-table scoring would be too slow for the
    per-line hot path.
    """
    conn = _open_db()
    words = [w for w in name.split() if w]
    if not words:
        return []

    # For multi-word queries, require all words to appear (AND); for
    # single-word queries we still match substring.
    conditions = " AND ".join("LOWER(name) LIKE ?" for _ in words)
    params: list[object] = [f"%{w}%" for w in words]
    params.append(limit)
    rows = conn.execute(
        f"SELECT name FROM synonym WHERE {conditions} "
        f"ORDER BY length(name) ASC LIMIT ?",
        params,
    ).fetchall()
    return [r[0] for r in rows]


_CACHED_CONN: sqlite3.Connection | None = None


def _open_db() -> sqlite3.Connection:
    """One process-wide read-only handle on ingredients.db."""
    global _CACHED_CONN
    if _CACHED_CONN is None:
        _CACHED_CONN = sqlite3.connect(
            str(_DB_PATH),
            check_same_thread=False,
        )
    return _CACHED_CONN
