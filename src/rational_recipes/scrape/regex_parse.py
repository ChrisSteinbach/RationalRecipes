"""Regex-first ingredient line parser with confidence gating (vwt.17).

Hot path for the ~80% of corpus lines that fit a simple shape:

    QTY [UNIT] NAME [, PREP]

These get parsed locally in microseconds; lines that don't fit cleanly
fall through to ``parse.parse_ingredient_lines``' LLM path. The
confidence gate is intentionally conservative — when in doubt, return
``None`` so the caller pays the LLM cost rather than poisoning variant
statistics with a mis-extracted field (vwt.17 acceptance note).

A line is accepted iff:

* A QTY parses cleanly (integer, decimal, simple fraction, mixed
  number, unicode fraction, or numeric range).
* The token immediately after QTY is a registered unit OR there's no
  unit at all (bare-quantity items like "2 eggs").
* The remaining name resolves to a USDA match at or above
  ``DEFAULT_SIMILARITY_THRESHOLD`` (see ``usda_match``).

Range handling: midpoint. "2-3 eggs" → quantity=2.5. Matches the LLM
prompt's documented behavior so regex and LLM paths produce the same
quantity for the same line.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from rational_recipes.scrape.parse import ParsedIngredient
from rational_recipes.scrape.usda_match import (
    DEFAULT_SIMILARITY_THRESHOLD,
    resolve_canonical_name,
)
from rational_recipes.units import Factory as UnitFactory

# --- Quantity tokens ---

_UNICODE_FRACTIONS: dict[str, float] = {
    "½": 0.5,
    "¼": 0.25,
    "¾": 0.75,
    "⅓": 1.0 / 3.0,
    "⅔": 2.0 / 3.0,
    "⅛": 0.125,
    "⅜": 0.375,
    "⅝": 0.625,
    "⅞": 0.875,
    "⅕": 0.2,
    "⅖": 0.4,
    "⅗": 0.6,
    "⅘": 0.8,
    "⅙": 1.0 / 6.0,
    "⅚": 5.0 / 6.0,
}

# A standalone integer or decimal: 2, 1.5, 0.25.
_NUMBER_RE = re.compile(r"^\d+(?:\.\d+)?$")
# A simple fraction: 1/2, 3/4.
_FRACTION_RE = re.compile(r"^(\d+)/(\d+)$")
# A mixed number written with a space: "1 1/2".
_MIXED_RE = re.compile(r"^(\d+)\s+(\d+)/(\d+)$")
# A range using - or –, optionally surrounded by whitespace: "2-3", "1 - 2".
_RANGE_SEP = re.compile(r"\s*[-–]\s*")


@dataclass(frozen=True, slots=True)
class _QuantityParse:
    quantity: float
    consumed: int  # Number of leading characters consumed off the input.


def _parse_simple_token(tok: str) -> float | None:
    """Single quantity token: integer, decimal, fraction, unicode fraction."""
    if not tok:
        return None
    if tok in _UNICODE_FRACTIONS:
        return _UNICODE_FRACTIONS[tok]
    if _NUMBER_RE.match(tok):
        return float(tok)
    m = _FRACTION_RE.match(tok)
    if m:
        num, den = int(m.group(1)), int(m.group(2))
        if den == 0:
            return None
        return num / den
    return None


def parse_quantity(text: str) -> _QuantityParse | None:
    """Consume a leading quantity from ``text``.

    Tries, in order: range (X-Y), mixed number (1 1/2), unicode fraction,
    simple fraction, decimal, integer. Returns the value plus the number
    of input characters consumed so the caller knows where the rest of
    the line begins.
    """
    text = text.lstrip()
    if not text:
        return None

    # Range: split first to detect, then parse each side as a single
    # quantity (which may itself be a mixed number or fraction).
    range_match = _RANGE_SEP.search(text)
    if range_match:
        lhs = text[: range_match.start()]
        rhs_start = range_match.end()
        # Find the end of the rhs quantity by walking until a non-qty token.
        rhs_text = text[rhs_start:]
        rhs_qp = _parse_atomic_quantity(rhs_text)
        lhs_q = _parse_atomic_quantity(lhs)
        if rhs_qp is not None and lhs_q is not None:
            midpoint = (lhs_q.quantity + rhs_qp.quantity) / 2.0
            return _QuantityParse(
                quantity=midpoint,
                consumed=rhs_start + rhs_qp.consumed,
            )
        # Fall through to atomic parse if range structure didn't validate.

    return _parse_atomic_quantity(text)


def _parse_atomic_quantity(text: str) -> _QuantityParse | None:
    """Quantity without range syntax. Mixed-number aware."""
    text = text.lstrip()
    if not text:
        return None

    # Mixed-number form: "<int> <int>/<int>". Greedy two-token attempt.
    parts = text.split(maxsplit=2)
    if len(parts) >= 2:
        candidate = f"{parts[0]} {parts[1]}"
        m = _MIXED_RE.match(candidate)
        if m is not None:
            whole_int = int(m.group(1))
            num = int(m.group(2))
            den = int(m.group(3))
            if den != 0:
                qty_mixed = whole_int + num / den
                # Consumed = leading whitespace already trimmed + matched span.
                return _QuantityParse(
                    quantity=qty_mixed, consumed=len(candidate)
                )

    # Unicode fraction directly attached to integer? "1½" → 1.5.
    if len(text) >= 2 and text[0].isdigit():
        # Walk while digits/dot.
        i = 0
        while i < len(text) and (text[i].isdigit() or text[i] == "."):
            i += 1
        whole_str = text[:i]
        rest = text[i:]
        if rest and rest[0] in _UNICODE_FRACTIONS:
            whole_f = float(whole_str)
            qty_unicode = whole_f + _UNICODE_FRACTIONS[rest[0]]
            return _QuantityParse(quantity=qty_unicode, consumed=i + 1)

    # Single token: number, fraction, or unicode fraction.
    first = parts[0]
    qty_simple = _parse_simple_token(first)
    if qty_simple is not None:
        return _QuantityParse(quantity=qty_simple, consumed=len(first))
    # Pure unicode-fraction with no leading digit ("½ cup flour").
    if first and first[0] in _UNICODE_FRACTIONS:
        return _QuantityParse(
            quantity=_UNICODE_FRACTIONS[first[0]], consumed=1
        )
    return None


# --- Unit resolution ---


def _is_known_unit(token: str) -> str | None:
    """Return the registered unit name (canonical case) or None."""
    if not token:
        return None
    unit = UnitFactory.get_by_name(token.strip())
    if unit is None:
        return None
    return token.strip().lower()


def _consume_unit(text: str) -> tuple[str | None, str]:
    """Try to peel a leading unit token from ``text``.

    Tries 2-word units first ("fl oz", "metric tbsp") then single
    tokens. Returns ``(unit | None, remainder_text)``.
    """
    stripped = text.lstrip()
    if not stripped:
        return None, stripped
    parts = stripped.split(maxsplit=2)
    # 2-word unit attempt.
    if len(parts) >= 2:
        two = f"{parts[0]} {parts[1]}"
        if _is_known_unit(two):
            rest = parts[2] if len(parts) == 3 else ""
            return two.lower(), rest
    one = parts[0]
    if _is_known_unit(one):
        rest = stripped[len(one):].lstrip()
        return one.lower(), rest
    return None, stripped


# --- Top-level parse with confidence gate ---


@dataclass(frozen=True, slots=True)
class RegexParseResult:
    """A confident regex parse, ready to merge with the LLM batch."""

    parsed: ParsedIngredient
    similarity: float


def regex_parse_line(
    line: str,
    *,
    similarity_threshold: float = DEFAULT_SIMILARITY_THRESHOLD,
) -> RegexParseResult | None:
    """Parse one ingredient line; return None if not confidently parseable.

    See module docstring for the acceptance rules. Bias is conservative:
    when in doubt return ``None`` so the LLM gets a chance.
    """
    if not line:
        return None
    raw = line
    text = line.strip()

    qty_parse = parse_quantity(text)
    if qty_parse is None:
        return None
    after_qty = text[qty_parse.consumed:].lstrip()

    unit, after_unit = _consume_unit(after_qty)

    # Split off prep at the first comma.
    name_part, prep = _split_name_and_prep(after_unit)
    name_part = name_part.strip()
    if not name_part:
        return None

    # Sanity: refuse trailing free-form clutter that isn't a clean
    # "name [, prep]" — e.g. parenthetical "1 cup flour (about 4 oz)"
    # has after-parens content that defeats clean canonicalization. Let
    # the LLM handle anything with parentheses or slashes inside the name.
    if any(c in name_part for c in "()/+"):
        return None
    # Same for any "or"/"and" pivots that signal alternates.
    lower_name = f" {name_part.lower()} "
    if any(p in lower_name for p in (" or ", " and ", " plus ", " & ")):
        return None

    canonical = resolve_canonical_name(
        name_part, threshold=similarity_threshold
    )
    if canonical is None:
        return None

    # Default unit if none found: choose by registered ingredient hints.
    # We don't have shape metadata at this layer, so the conservative
    # choice mirrors the LLM prompt: bare-quantity countable items use
    # MEDIUM. This MAY misclassify a weight-default item, so as an
    # additional safety we only emit MEDIUM when the input had no unit
    # token at all (i.e. a bare "2 eggs" / "3 apples" shape).
    if unit is None:
        unit = "MEDIUM"

    parsed = ParsedIngredient(
        quantity=qty_parse.quantity,
        unit=unit,
        ingredient=canonical.canonical,
        preparation=prep.strip(),
        raw=raw,
    )
    return RegexParseResult(parsed=parsed, similarity=canonical.similarity)


def _split_name_and_prep(text: str) -> tuple[str, str]:
    """Split on the first comma — name before, preparation after."""
    if "," not in text:
        return text, ""
    name, _, prep = text.partition(",")
    return name, prep
