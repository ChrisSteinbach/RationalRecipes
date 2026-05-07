"""Heuristic picker: most-complete instructions among the top-N central sources.

Per RationalRecipes-ie1a (F10 in the ehe7 friction journal): the
literal-median picker — ``active_sources[0]`` after ORDER BY
``outlier_score`` — sometimes returns a recipe with terse instructions
when a runner-up has substantially better text. For ehe7's CCC variant
``b34c2dce79e2`` (n=98) the literal median was cookbooks.com id=473872,
three sentences with no preheat, no shape guidance, and no cooling.
Top-5 runner-ups had real, useful instructions.

This module replaces the literal median with a top-N completeness
ranker that's purely heuristic, deterministic, and free of any LLM
dependency. Weights are documented in ``score_instructions`` and
defended in the bead's commit message.

Soft-coupled to F5 (RationalRecipes-15g4): F5 adds
``recipes.directions_text``. Until it lands, the SELECT raises
``OperationalError`` and the picker falls back to ``active_sources[0]``
— the existing behavior. Once F5 lands, the picker reads the column
directly with no further wiring required.
"""

from __future__ import annotations

import re
import sqlite3
from collections.abc import Sequence
from typing import Any

# A "step" is either a numbered list item ("1. ", "2) ", ...) or, when
# the text isn't numbered, a non-blank line. Numbered detection wins
# when present because it's the convention RecipeNLG and WDC follow.
_NUMBERED_STEP_PATTERN = re.compile(r"(?:^|\n)\s*\d+\s*[.)]\s+", re.MULTILINE)

# "preheat" is the most-discriminating signal for the F10 failure
# mode: terse sources omit it; complete sources rarely do.
_HAS_PREHEAT = re.compile(r"\bpreheat", re.IGNORECASE)

# A temperature mention ("350°", "175°C", "350 degrees") is weaker
# than "preheat" because the failure case ("Bake at 350°") still
# clears it — so a smaller weight.
_HAS_TEMPERATURE = re.compile(
    r"\b\d{2,3}\s*(?:°|degrees?|deg)\b",
    re.IGNORECASE,
)

# Timing keywords: the second-most-discriminating signal. "10–12
# minutes" / "for 1 hour" / "until golden" all count.
_HAS_TIMING = re.compile(
    r"\b(?:\d+(?:\s*[-–]\s*\d+)?\s*(?:minutes?|min|hours?|hrs?))\b"
    r"|until\s+(?:golden|brown|set|done|firm|melted|smooth|crisp)",
    re.IGNORECASE,
)

# Cooling / finishing — wire rack, transfer, cool — rounds out a
# fully written recipe. Smaller weight: many cluster-typical recipes
# legitimately skip this (no-bake doughs, doughs eaten warm).
_HAS_COOLING = re.compile(
    r"\b(?:cool|cooling|cooled|rack|transfer\s+to)\b",
    re.IGNORECASE,
)

# Weights — see module docstring. Tuned so the F10 failure case (terse
# 3-sentence median) loses to a cluster runner-up by ~20 points, a
# comfortable margin under noise.
_WEIGHT_LENGTH_PER_100_CHARS = 1.0
_CAP_LENGTH = 10.0
_WEIGHT_PER_STEP = 2.0
_CAP_STEP = 20.0
_WEIGHT_PREHEAT = 5.0
_WEIGHT_TEMPERATURE = 2.0
_WEIGHT_TIMING = 5.0
_WEIGHT_COOLING = 3.0

DEFAULT_TOP_N = 5


def score_instructions(text: str | None) -> float:
    """Return a completeness score for an instruction string.

    Higher = more complete. Floor 0.0 (empty / None). Ceiling ~45 in
    the heaviest case (long, multi-step, all keyword classes hit).

    Components, all additive:

    - Length: 1 point per 100 characters, capped at 10 (≈1000 chars).
      Depth proxy without over-rewarding novelistic prose.
    - Step count: 2 points per detected step, capped at 20 (10 steps).
      Prefers numbered lists; falls back to non-blank lines.
    - Preheat keyword: 5 points (binary). Most-discriminating signal
      for the F10 failure mode.
    - Temperature mention: 2 points (binary). Weaker than preheat —
      a "Bake at 350°" still clears it.
    - Timing: 5 points (binary). Second-most-discriminating signal.
    - Cooling / finishing: 3 points (binary). Smaller because some
      cluster-typical recipes legitimately skip it.
    """
    if not text:
        return 0.0
    length_score = min(
        len(text) / 100.0 * _WEIGHT_LENGTH_PER_100_CHARS, _CAP_LENGTH
    )
    numbered = len(_NUMBERED_STEP_PATTERN.findall(text))
    if numbered:
        step_count = numbered
    else:
        step_count = sum(1 for line in text.splitlines() if line.strip())
    step_score = min(step_count * _WEIGHT_PER_STEP, _CAP_STEP)
    preheat = _WEIGHT_PREHEAT if _HAS_PREHEAT.search(text) else 0.0
    temperature = _WEIGHT_TEMPERATURE if _HAS_TEMPERATURE.search(text) else 0.0
    timing = _WEIGHT_TIMING if _HAS_TIMING.search(text) else 0.0
    cooling = _WEIGHT_COOLING if _HAS_COOLING.search(text) else 0.0
    return length_score + step_score + preheat + temperature + timing + cooling


def fetch_directions_text(
    conn: sqlite3.Connection, recipe_id: str
) -> str | None:
    """Return ``recipes.directions_text`` for a recipe, or None.

    Defensive against the F5 column not existing yet: the query
    raises ``OperationalError`` until ``RationalRecipes-15g4`` lands,
    in which case we return ``None`` and let the caller fall back.
    """
    try:
        row = conn.execute(
            "SELECT directions_text FROM recipes WHERE recipe_id = ?",
            (recipe_id,),
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    if row is None:
        return None
    text = row[0]
    if text is None:
        return None
    return str(text)


def pick_median_source(
    active_sources: Sequence[Any],
    conn: sqlite3.Connection,
    *,
    top_n: int = DEFAULT_TOP_N,
) -> Any:
    """Pick the most-complete instructions from the top-N central sources.

    ``active_sources`` must be ordered by ``outlier_score`` ascending
    (lowest = most central). The caller is responsible for ordering
    plus excluding filter-override recipes.

    Returns the chosen source row. Falls back to ``active_sources[0]``
    (the literal-median pick, pre-ie1a behavior) when none of the
    top-N candidates have populated ``directions_text`` — either
    because F5 hasn't landed (``OperationalError`` on SELECT) or every
    candidate row is NULL.

    Ties on completeness score are broken by ``outlier_score``
    ascending, so the pick stays deterministic.
    """
    if not active_sources:
        raise ValueError("active_sources must be non-empty")
    candidates = list(active_sources[:top_n])
    scored: list[tuple[float, float, Any]] = []
    any_with_text = False
    for src in candidates:
        text = fetch_directions_text(conn, src["recipe_id"])
        if text:
            any_with_text = True
        score = score_instructions(text)
        scored.append((score, float(src["outlier_score"]), src))
    if not any_with_text:
        return candidates[0]
    scored.sort(key=lambda triple: (-triple[0], triple[1]))
    return scored[0][2]
