"""Regex-first ingredient line parser with confidence gating (vwt.17 + r6w).

Hot path for ingredient lines that fit a simple shape:

    [bullet] QTY [UNIT[.]] [LEADING_PREP...] NAME [, TRAILING_PREP] [(aside)]

Lines that fit get parsed locally in microseconds; lines that don't
fit fall through to ``parse.parse_ingredient_lines``' LLM path. The
confidence gate is intentionally conservative — when in doubt, return
``None`` so the caller pays the LLM cost rather than poisoning variant
statistics with a mis-extracted field (vwt.17 acceptance note).

Acceptance shape, in order:

* Strip a leading bullet token (``•``, ``·``, ``-``, ``*``).
* Strip parenthetical asides — they describe but don't override the
  leading quantity (an aside containing alternate units is informational
  and the line's leading qty is the authoritative one for downstream
  conversion).
* A QTY parses cleanly (integer, decimal, simple fraction, mixed
  number, unicode fraction, or numeric range).
* A UNIT token, optionally followed by a literal ``.`` (for the very
  common ``c.`` / ``tsp.`` / ``Tbsp.`` / ``lb.`` shape in RecipeNLG),
  is registered or absent.
* Leading preparation keywords on the name (``chopped``, ``sliced``,
  ``thinly sliced``, ``melted``, ``fresh``, …) are peeled off and added
  to the preparation field so the regex output mirrors the LLM's
  convention (which always strips these from the name).
* The remaining name resolves to a USDA match at or above
  ``DEFAULT_SIMILARITY_THRESHOLD`` (see ``usda_match``). The match is
  always English-canonical — ``valnötter`` / ``tomat`` / ``olja`` get
  post-translated before they leave this layer.

Range handling: midpoint. ``2-3 eggs`` → quantity=2.5. Matches the LLM
prompt's documented behavior so regex and LLM paths produce the same
quantity for the same line.

Whole-unit canonicalization: the LLM's prompt asks for ``MEDIUM`` /
``LARGE`` / ``SMALL`` (uppercase) for size-specifier units like
``2 large eggs``. The regex emits the same form so cache rows merge
on string equality, not just on the equivalence-group fallback used by
the shadow comparator.
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
from rational_recipes.units import VolumeUnit, WeightUnit, WholeUnit

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

# A standalone integer or decimal: 2, 1.5, 0.25, .25 (leading-decimal
# form, common in "(.25 oz.) pkg. yeast" packaging shorthand).
_NUMBER_RE = re.compile(r"^(?:\d+(?:\.\d+)?|\.\d+)$")
# A simple fraction: 1/2, 3/4.
_FRACTION_RE = re.compile(r"^(\d+)/(\d+)$")
# A mixed number written with a space: "1 1/2".
_MIXED_RE = re.compile(r"^(\d+)\s+(\d+)/(\d+)$")
# A range using - or – (or the natural-language "to"), optionally
# surrounded by whitespace: "2-3", "1 - 2", "8 to 10". RecipeNLG carries
# both shapes; both should resolve to the midpoint. The two alternatives
# are kept separate so the "to" form requires explicit whitespace on
# both sides — that prevents bare "to" inside a word ("tomato") from
# accidentally matching as a range separator (the lhs/rhs qty parses
# would still reject the false positive, but failing fast is cleaner).
_RANGE_SEP = re.compile(r"\s*[-–]\s*|\s+to\s+")

# --- Preprocessing helpers ---

# Bullet/list markers that some recipe sites (notably ica.se mirrors and
# blog scrapes) leave at the start of a line. Strip the leading run of
# these so the quantity parser sees a clean numeric prefix. Includes the
# unicode bullet (•), middle dot (·) and the ASCII dash/asterisk pair.
_LEADING_BULLET_RE = re.compile(r"^[\s•·\-*]+")

# Approximation words that recipe writers prepend to a quantity:
# "approximately 1 lb. pork chops", "about 2 cups flour", "roughly 1 tbsp".
# We strip these wholesale; the LLM does the same (its examples show
# qty=1 emitted for "approximately 1 lb"). Kept narrow — only words that
# are unambiguously quantity-prefix decoration, not adjectives that might
# be part of an ingredient name ("light", "heavy" can describe both qty
# and ingredient — leave to LLM).
_APPROX_PREFIX_RE = re.compile(
    r"^(?:approximately|approx\.?|about|roughly|around|nearly)\s+",
    re.IGNORECASE,
)

# Parenthetical asides like "(8 oz)" / "(optional)" / "(about 4 cups)".
# Stripped wholesale before the quantity parse — the leading qty is the
# authoritative one for conversion. Unbalanced parens (no closing) are
# left intact and rejected later as part of the "unsafe" name guard.
_PAREN_RE = re.compile(r"\([^()]*\)")

# Detects a paren that hugs the leading quantity AND carries a digit
# inside — the "N (Y unit) ingredient" shape. Examples:
#   "2 (8-oz.) cream cheese"       — match (qty hugs paren, paren has 8)
#   "1 (8 oz.) carton sour cream"  — match
#   "1 (16-oz) jar peanut butter"  — match
# Counter-examples (no match — paren is informational):
#   "1 cup flour (about 4 oz)"     — leading qty already has unit (cup)
#   "60 grams (2 ounces) walnuts"  — leading qty already has unit (grams)
#   "3 eggs (optional)"            — paren has no digit
#   "3 eggs (about 8 oz)"          — paren is AFTER ingredient, not hugging
#
# Used as a guard *before* paren stripping. When fired AND the parser
# fails to resolve a leading unit, we kick to the LLM rather than
# guessing MEDIUM — packaged-goods quantities are too easy to silently
# miscount.
_QTY_HUGGING_PAREN_RE = re.compile(
    r"^\s*\d+(?:[\s./-]+\d+)*\s+\(\s*[^)]*\d"
)


def _strip_bullet(text: str) -> str:
    """Strip leading bullet/list markers from ``text``."""
    return _LEADING_BULLET_RE.sub("", text, count=1)


def _strip_approx_prefix(text: str) -> str:
    """Strip an approximation adverb (``approximately``, ``about``, …) at line start."""
    return _APPROX_PREFIX_RE.sub("", text, count=1)


def _strip_parentheticals(text: str) -> str:
    """Strip balanced parenthetical asides; collapse the resulting whitespace.

    Iterates so nested patterns like ``a (b (c) d) e`` get peeled off
    from the inside out. Unbalanced parens are left in the output so
    the downstream rejection guard catches them.
    """
    prev = None
    while prev != text:
        prev = text
        text = _PAREN_RE.sub(" ", text)
    return re.sub(r"\s{2,}", " ", text).strip()


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


def _canonical_unit_name(token: str) -> str | None:
    """Return the canonical unit form for ``token`` or None.

    Handles the trailing-``.`` shape (``c.`` → ``c``, ``tsp.`` → ``tsp``,
    ``Tbsp.`` → ``tbsp``) that's near-ubiquitous in RecipeNLG. For
    WholeUnit (size-style) hits, returns the unit's first synonym so the
    output matches the LLM's uppercase ``MEDIUM`` / ``LARGE`` / ``SMALL``
    convention; other units lowercase the (period-stripped) input.
    """
    if not token:
        return None
    cleaned = token.strip().rstrip(".")
    if not cleaned:
        return None
    unit = UnitFactory.get_by_name(cleaned)
    if unit is None:
        return None
    if isinstance(unit, WholeUnit):
        # synonyms()[0] is the canonical: "MEDIUM"/"LARGE"/"SMALL"/"XL"
        # for size units (uppercase to match the LLM prompt), or
        # "stick"/"cube"/"knob" for the lowercase whole units.
        return unit.synonyms()[0]
    return cleaned.lower()


def _consume_unit(text: str) -> tuple[str | None, str]:
    """Try to peel a leading unit token from ``text``.

    Tries 2-word units first ("fl oz", "metric tbsp") then single
    tokens. Trailing periods on the unit token (``c.`` / ``tsp.``) are
    treated as belonging to the unit, not as a name separator. Returns
    ``(canonical_unit | None, remainder_text)``.
    """
    stripped = text.lstrip()
    if not stripped:
        return None, stripped
    parts = stripped.split(maxsplit=2)
    # 2-word unit attempt — e.g. "fl oz" / "metric tbsp".
    if len(parts) >= 2:
        two_raw = f"{parts[0]} {parts[1]}"
        two_canon = _canonical_unit_name(two_raw)
        if two_canon:
            rest = parts[2] if len(parts) == 3 else ""
            return two_canon, rest
    one_raw = parts[0]
    one_canon = _canonical_unit_name(one_raw)
    if one_canon:
        rest = stripped[len(one_raw):].lstrip()
        return one_canon, rest
    return None, stripped


def _is_weight_or_volume(unit_name: str | None) -> bool:
    """True when the registered unit is a weight or volume measure.

    Used by the unit-preference rule: weight > volume > container.
    A leading qty whose unit is already a weight or volume should not
    be overridden by a paren'd alternate; everything else (containers,
    sizes, bare counts) loses to a paren weight/volume.
    """
    if unit_name is None:
        return False
    unit = UnitFactory.get_by_name(unit_name)
    return unit is not None and isinstance(unit, (WeightUnit, VolumeUnit))


# Packaging WholeUnits whose gram conversion depends on the contents of
# the container, not the container shape — there's no ingredient-side
# portion table entry for "can" the way there is for "stick" (of
# butter) or "MEDIUM" (apple). When a line offers both a container
# count and a paren weight/volume, the paren is the only quantity that
# can be converted — see _extract_paren_weight_or_volume / r6w user
# preference note. Stick / cube / knob / size units (MEDIUM/LARGE/...)
# are intentionally excluded: they DO carry gram conversions per
# ingredient, so their leading qty stays authoritative.
_CONTAINER_UNIT_NAMES = frozenset({
    "can", "cans", "jar", "jars", "pkg", "package", "packages", "pkgs", "pkg.",
    "box", "boxes", "carton", "cartons", "bag", "bags", "bottle", "bottles",
    "head", "heads", "loaf", "loaves",
})


def _is_container_unit(unit_name: str | None) -> bool:
    if unit_name is None:
        return False
    return unit_name.lower() in _CONTAINER_UNIT_NAMES


def _extract_paren_weight_or_volume(text: str) -> tuple[float, str] | None:
    """Pull a (qty, weight/volume unit) tuple out of the first paren in
    ``text`` that contains both. Returns ``None`` when:

    * No paren exists, or the first paren doesn't have a quantity.
    * The first paren's unit is a container (can/jar/pkg…) or size
      (MEDIUM/LARGE/SMALL/XL) — those don't satisfy the preference rule
      and we let normal parsing handle them.

    Used to implement the unit-preference rule (weight > volume >
    container) when the leading qty is a bare count or a packaging
    unit. Examples:
      * "1 (10 oz.) pkg. spinach"      → (10.0, "oz")
      * "2 (20 oz. each) cans X"       → (20.0, "oz")
      * "1 (8 oz.) carton sour cream"  → (8.0, "oz")
      * "1 (8-oz.) cream cheese"       → (8.0, "oz")  (hyphenated form)
      * "3 eggs (optional)"            → None (no qty in paren)
    """
    m = re.search(r"\(([^()]*)\)", text)
    if m is None:
        return None
    inside = m.group(1).strip()
    # Drop a trailing "each" qualifier so "(20 oz. each)" parses
    # cleanly. The multiplication is applied by the caller using the
    # leading qty as multiplier.
    inside = re.sub(r"\s*\beach\b\s*", " ", inside, flags=re.IGNORECASE).strip()
    # Inside a paren, "8-oz" / "1/4-oz" is hyphenation between a qty
    # and its unit (compound-adjective form), not a range. Normalize to
    # space-separated so parse_quantity + _canonical_unit_name resolve
    # both halves cleanly.
    inside = re.sub(r"(\d)\s*-\s*([A-Za-z])", r"\1 \2", inside)
    if not inside:
        return None
    qty_parse = parse_quantity(inside)
    if qty_parse is None:
        return None
    after_qty = inside[qty_parse.consumed:].strip()
    unit_canon = _canonical_unit_name(after_qty)
    if unit_canon is None:
        return None
    if not _is_weight_or_volume(unit_canon):
        return None
    return (qty_parse.quantity, unit_canon)


# --- Pre-name preparation extraction ---

# Leading-prep keywords: words that tend to appear before the
# ingredient name and don't change the ingredient's identity. The LLM
# strips these consistently into the preparation field — the regex
# follows the same convention so cache rows from both paths merge.
#
# Carefully includes only:
#   * Past-participle prep verbs (chopped, sliced, melted, ...).
#   * State adjectives that preserve identity (fresh, frozen, dried, ...).
#   * Adverbs that modify those (thinly, finely, freshly, ...).
#
# Deliberately excludes type-modifier adjectives ("brown" → brown sugar,
# "white" → white wine, "rolled" → rolled oats) where the modifier IS
# part of the canonical identity. Those should fall through to the LLM,
# which has the world-knowledge to decide.
_LEADING_PREP_KEYWORDS = frozenset({
    # Past-participle prep verbs.
    "chopped", "sliced", "diced", "minced", "grated", "melted", "softened",
    "crushed", "ground", "mashed", "peeled", "pitted", "drained", "rinsed",
    "beaten", "sifted", "packed", "shredded", "halved", "quartered",
    "cubed", "julienned", "blanched", "boiled", "smoked", "roasted",
    "cooked", "baked", "toasted", "whipped", "creamed", "broken",
    # State adjectives.
    "fresh", "frozen", "dried", "canned", "raw",
    # Adverbs that modify the above. ("thinly sliced" → both go to prep.)
    "thinly", "finely", "coarsely", "roughly", "lightly", "freshly",
})


def _extract_leading_prep(name: str) -> tuple[str, str]:
    """Peel leading prep keywords off ``name``.

    Returns ``(remaining_name, prep_phrase)``. ``prep_phrase`` is the
    space-joined sequence of consumed keywords in original order, so
    "thinly sliced" stays "thinly sliced" (not "sliced thinly").
    """
    tokens = name.split()
    consumed: list[str] = []
    while tokens and tokens[0].lower() in _LEADING_PREP_KEYWORDS:
        consumed.append(tokens.pop(0))
    return " ".join(tokens), " ".join(consumed)


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
    if not text:
        return None

    # Preprocess: strip leading bullet, then approximation prefix.
    # Snapshot the line's qty-hugging paren — if present, its weight/
    # volume content may override the leading qty/unit per the
    # weight > volume > container preference rule (r6w).
    text = _strip_bullet(text).strip()
    text = _strip_approx_prefix(text).strip()
    paren_wt_vol = (
        _extract_paren_weight_or_volume(text)
        if _QTY_HUGGING_PAREN_RE.match(text)
        else None
    )
    text = _strip_parentheticals(text)
    if not text:
        return None

    qty_parse = parse_quantity(text)
    if qty_parse is None:
        return None
    after_qty = text[qty_parse.consumed:].lstrip()

    unit, after_unit = _consume_unit(after_qty)

    # Apply the unit-preference rule (weight > volume > container):
    # when the line has a paren with weight/volume AND the leading
    # qty's unit is bare or a container (no inherent gram conversion
    # — can, jar, pkg, …), promote the paren to authoritative.
    # Multiply the leading qty (count of containers) into the paren qty
    # so "2 (20 oz. each) cans" → 40 oz, "1 (10 oz.) pkg." → 10 oz.
    # Volume / size / specific-shape units stay authoritative — those
    # carry their own gram conversion path so swapping in a paren weight
    # would just lose the recipe writer's measurement intent.
    if paren_wt_vol is not None and (unit is None or _is_container_unit(unit)):
        paren_qty, paren_unit = paren_wt_vol
        qty_parse = _QuantityParse(
            quantity=qty_parse.quantity * paren_qty,
            consumed=qty_parse.consumed,
        )
        unit = paren_unit
    elif unit is None and _QTY_HUGGING_PAREN_RE.match(line.strip()):
        # Qty-hugging paren without a weight/volume inside means the
        # line is shaped like "2 (mix of stuff) cream cheese" — too
        # ambiguous to parse cleanly, let the LLM handle it.
        return None

    # Split off trailing prep at the first comma.
    name_part, trailing_prep = _split_name_and_prep(after_unit)
    name_part = name_part.strip()

    # Pull leading prep keywords off the name (chopped, thinly sliced, …).
    name_part, leading_prep = _extract_leading_prep(name_part)
    if not name_part:
        return None

    # Sanity: refuse residual unsafe punctuation in the name. After
    # paren-stripping the only way "(" or ")" survives is unbalanced
    # parens (intentional fall-through). "/" / "+" still signal alternate
    # ingredients that the LLM is better suited to disambiguate.
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

    # Compose the preparation field: leading prep + trailing prep,
    # comma-joined when both are present so the cache shape mirrors the
    # LLM's "chopped, sifted" output.
    prep_parts = [p for p in (leading_prep.strip(), trailing_prep.strip()) if p]
    preparation = ", ".join(prep_parts)

    parsed = ParsedIngredient(
        quantity=qty_parse.quantity,
        unit=unit,
        ingredient=canonical.canonical,
        preparation=preparation,
        raw=raw,
    )
    return RegexParseResult(parsed=parsed, similarity=canonical.similarity)


def _split_name_and_prep(text: str) -> tuple[str, str]:
    """Split on the first comma — name before, preparation after."""
    if "," not in text:
        return text, ""
    name, _, prep = text.partition(",")
    return name, prep
