#!/usr/bin/env python3
"""Generate proposed gold labels for Swedish ica.se candidates.

Reads ``swedish_ica_se_candidates.jsonl``, applies a rule-based labeler,
and writes ``swedish_ica_se_structured_gold.jsonl``.

Conventions (consistent with the NEUTRAL_PROMPT examples):
- Keep ingredient + prep in Swedish, lowercased.
- No quantity → 1.0, unit "".
- Countable with no explicit unit (``3 ägg``, ``1 gurka``) → unit="MEDIUM",
  mirroring the English ``_SYSTEM_PROMPT`` sentinel. Size adjectives as
  the only modifier (``1 stort ägg``, ``2 stora ägg``) → unit="LARGE";
  ``1 liten burk`` style (adj + known unit) still leaves ``burk`` as the
  unit and peels the adjective into preparation.
- Ranges "2 - 3" use the midpoint.
- "1 1/2" and "3/4" become 1.5 and 0.75.
- Parentheticals: ``(till X)`` / ``(à X)`` / ``(NN%)`` / ``(gärna X)`` /
  ``(eller X)`` are gloss. ``(till X)`` content is lifted into preparation;
  everything else is dropped.
- Multi-option lines ("smör eller margarin", "X, Y, Z eller W") keep the
  first head noun. "X eller Y" where both are adjectives collapses into
  a prep list (first only).
- "X och Y" between adjectives is treated like a comma.
- Multi-ingredient lines ("salt och peppar") take only the first head
  noun for per-line scoring. This is a known limitation of the per-line
  gold shape — a line can't encode two ingredients. Recipe-level
  name-set F1 is not harmed if the second ingredient appears elsewhere
  in the same recipe (which is the case for r111, the only current
  example). A future schema extension (``also_ingredients`` array)
  could lift this, tracked under RationalRecipes-zqo's scope.
- Leading size adjectives before a package unit ("1 liten burk") are
  peeled into preparation.
- Trailing qualifiers "till stekning", "i olja", "med skal" move to prep.

The labeler is deliberately imperfect: it gets most of the 200 Swedish
lines right, the tricky remainder is caught by eyeball in the owner
validation pass.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

CANDIDATES = Path("scripts/benchmark_data/swedish_ica_se_candidates.jsonl")
OUT = Path("scripts/benchmark_data/swedish_ica_se_structured_gold.jsonl")

# Swedish units. Multi-word variants first so regex doesn't match inside.
UNITS = [
    "msk",
    "tsk",
    "krm",
    "dl",
    "ml",
    "cl",
    "l",
    "liter",
    "g",
    "kg",
    "förpackning",
    "förp",
    "paket",
    "glasburk",
    "burk",
    "kruka",
    "knippe",
    "ask",
    "påse",
    "sats",
    "kvistar",
    "kvist",
]
UNIT_PATTERN = r"(?:" + "|".join(re.escape(u) for u in UNITS) + r")"

# Preceding-noun adjectives, each inflected form listed individually.
PREP_ADJECTIVES = {
    "hackad",
    "hackade",
    "finhackad",
    "finhackade",
    "strimlad",
    "strimlade",
    "rökt",
    "rökta",
    "krossad",
    "krossade",
    "fryst",
    "frysta",
    "halvtinad",
    "halvtinade",
    "färsk",
    "färska",
    "torkad",
    "torkade",
    "malen",
    "malet",
    "smält",
    "smälta",
    "kyld",
    "kylt",
    "kylda",
    "kallrökt",
    "rumsvarm",
    "rumsvarmt",
    "siktad",
    "siktat",
    "självjäsande",
    "finrivet",
    "finriven",
    "tvättad",
    "färdigskuren",
    "färdigskurna",
    "färdigkokt",
    "färdigkokta",
    "kokt",
    "kokta",
    "naturell",
    "mörk",
    "mörka",
    "röd",
    "röda",
    "gul",
    "gula",
    "stor",
    "stora",
    "stort",
    "liten",
    "små",
    "packad",
    "packade",
    "växtbaserad",
    "växtbaserade",
    "grekisk",
    "turkisk",
    "flytande",
    "salta",
    "saltad",  # NB: bare "salt" is the noun (salt), not an adjective
    "vit",
    "vita",
    "vintermix",
    "soltorkad",
    "soltorkade",
    "avrunna",
    "avrunnen",
    "hel",
    "hela",
    "skalad",
    "skalade",
    "rensad",
    "rensade",
    "extra",
    "fet",
    "feta",
    "fett",  # fatty (3% / 10% etc.)
    "havrebaserad",
    "havrebaserade",  # oat-based
    "kokosbaserad",
    "mandelbaserad",
    "lätt",
    "lätta",  # light / low-fat
    "naturlig",
    "naturligt",  # natural
}  # noqa: E501

# Size adjectives that map to LARGE/SMALL sentinels, mirroring the
# English _SYSTEM_PROMPT convention (and the aligned NEUTRAL_PROMPT).
SIZE_ADJ_LARGE = {"stor", "stort", "stora"}
SIZE_ADJ_SMALL = {"liten", "litet", "små"}
SIZE_ADJ_AS_UNIT = SIZE_ADJ_LARGE | SIZE_ADJ_SMALL


def _size_sentinel(adj: str) -> str:
    """Map a Swedish size adjective to its MEDIUM/LARGE/SMALL sentinel."""
    a = adj.lower()
    if a in SIZE_ADJ_LARGE:
        return "LARGE"
    if a in SIZE_ADJ_SMALL:
        return "SMALL"
    return "MEDIUM"


# Trailing qualifier phrases to lift into preparation
TRAILING_QUALIFIER_RE = re.compile(
    r"^(?P<head>.+?)\s+(?P<qual>"
    r"till\s+\w+(?:\s+\w+)?"
    r"|i\s+olja"
    r"|i\s+portvin"
    r"|i\s+vatten"
    r"|med\s+\w+(?:\s+\w+)?"
    r")\s*$",
    re.IGNORECASE,
)

# Parenthetical extractors
PAREN_TILL_RE = re.compile(r"\(\s*(till\s+[^)]+)\)", re.IGNORECASE)
PAREN_ADJ_RE = re.compile(r"\(\s*([a-zäåö]+)\s*\)", re.IGNORECASE)
PAREN_ANY_RE = re.compile(r"\s*\([^)]*\)\s*")
CA_RE = re.compile(r"^\s*ca\.?\s+", re.IGNORECASE)

# Leading-number patterns
FRAC_RE = re.compile(r"^(\d+)/(\d+)$")


def _parse_num_token(s: str) -> float | None:
    s = s.strip().replace(",", ".")
    m = FRAC_RE.match(s)
    if m:
        return float(m.group(1)) / float(m.group(2))
    try:
        return float(s)
    except ValueError:
        return None


def _extract_quantity(line: str) -> tuple[float | None, str]:
    """Peel leading quantity (with range/mixed/fraction support)."""
    line = CA_RE.sub("", line.strip())
    # Range: "2 - 3" or "1 - 1 1/2"
    m = re.match(
        r"^(\d+(?:\s+\d+/\d+)?|\d+/\d+|\d+[,.]\d+)"
        r"\s*-\s*"
        r"(\d+(?:\s+\d+/\d+)?|\d+/\d+|\d+[,.]\d+)"
        r"\s+(.*)$",
        line,
    )
    if m:
        lo = _parse_mixed_num(m.group(1))
        hi = _parse_mixed_num(m.group(2))
        if lo is not None and hi is not None:
            return (lo + hi) / 2, m.group(3)
    # Mixed fraction "1 1/2 unit ..."
    m = re.match(r"^(\d+)\s+(\d+)/(\d+)\s+(.*)$", line)
    if m:
        return (
            float(m.group(1)) + float(m.group(2)) / float(m.group(3)),
            m.group(4),
        )
    # Simple int/fraction/decimal "3 unit ..."
    m = re.match(r"^(\d+(?:[,.]\d+)?|\d+/\d+)\s+(.*)$", line)
    if m:
        val = _parse_num_token(m.group(1))
        if val is not None:
            return val, m.group(2)
    return None, line


def _parse_mixed_num(token: str) -> float | None:
    """Parse either a simple num, fraction, mixed fraction, or decimal."""
    m = re.match(r"^(\d+)\s+(\d+)/(\d+)$", token.strip())
    if m:
        return float(m.group(1)) + float(m.group(2)) / float(m.group(3))
    return _parse_num_token(token)


def _extract_unit_with_adj(rest: str) -> tuple[str, str, str]:
    """Peel leading unit, optionally preceded by a size adjective.

    Returns (unit, leading_prep, remainder). A bare size adjective
    (stor/stort/stora/liten/små) becomes a LARGE/SMALL sentinel when no
    known unit follows — mirrors the English _SYSTEM_PROMPT and aligned
    NEUTRAL_PROMPT convention.
    """
    stripped = rest.lstrip()
    # Pattern 1: adjective + known unit (e.g. "liten burk")
    m = re.match(
        rf"^([a-zäåö]+)\s+({UNIT_PATTERN})\b\s*(.*)$",
        stripped,
        re.IGNORECASE,
    )
    if m and m.group(1).lower() in PREP_ADJECTIVES:
        return m.group(2).lower(), m.group(1).lower(), m.group(3).strip()
    # Pattern 2: plain known unit
    m = re.match(rf"^({UNIT_PATTERN})\b\s*(.*)$", stripped, re.IGNORECASE)
    if m:
        return m.group(1).lower(), "", m.group(2).strip()
    # Pattern 3: lone size adjective acting as a size sentinel ("1 stort ägg")
    m = re.match(r"^([a-zäåö]+)\s+(.*)$", stripped, re.IGNORECASE)
    if m and m.group(1).lower() in SIZE_ADJ_AS_UNIT:
        return _size_sentinel(m.group(1)), "", m.group(2).strip()
    return "", "", stripped


def _split_prep_and_noun(rest: str, leading_prep: str) -> tuple[str, str]:
    """Split ingredient noun from preparation qualifiers.

    ``leading_prep`` is any adjective already peeled before the unit.

    Handles "eller" two ways depending on context:

    - ``ADJ eller ADJ NOUN`` (e.g. "grekisk eller turkisk yoghurt") →
      merge both adjectives into prep, keep the noun.
    - ``NOUN eller NOUN`` (e.g. "smör eller margarin") → keep the
      first head noun, drop the alternative.
    """
    rest = rest.strip().lower()
    leading_prep_parts: list[str] = [leading_prep] if leading_prep else []
    trailing_preps: list[str] = []

    # Peel trailing qualifier phrases iteratively
    changed = True
    while changed:
        changed = False
        m = TRAILING_QUALIFIER_RE.match(rest)
        if m:
            trailing_preps.insert(0, m.group("qual").lower())
            rest = m.group("head").strip()
            changed = True

    # Handle "eller" alternatives: if the left chunk is all-adjectives,
    # merge both sides; otherwise keep only the left head.
    if " eller " in rest:
        left, _, right = rest.partition(" eller ")
        left_tokens = [t for t in re.split(r"[,\s]+", left) if t]
        if left_tokens and all(t in PREP_ADJECTIVES for t in left_tokens):
            rest = f"{left} , {right}"  # collapse into combined sequence
        else:
            rest = left.strip()

    # Treat "och" between adjective tokens as a comma separator
    rest = re.sub(r"\boch\b", ",", rest)
    tokens = [t.strip() for t in re.split(r"[,\s]+", rest) if t.strip()]

    # Peel leading adjectives
    while tokens and tokens[0] in PREP_ADJECTIVES:
        leading_prep_parts.append(tokens.pop(0))

    # Peel trailing adjective-like tokens (e.g. "skinka rökt")
    trailing_adj: list[str] = []
    while tokens and tokens[-1] in PREP_ADJECTIVES:
        trailing_adj.insert(0, tokens.pop())

    # What's left is the noun phrase (usually 1-2 tokens, occasionally
    # a multi-option list like "blåbär jordgubbar hallon")
    if len(tokens) >= 3 and all(t.isalpha() and len(t) <= 12 for t in tokens):
        noun = tokens[0]
    else:
        noun = " ".join(tokens).strip()

    prep_parts = leading_prep_parts + trailing_adj + trailing_preps
    seen: set[str] = set()
    unique_preps: list[str] = []
    for p in prep_parts:
        if p and p not in seen:
            seen.add(p)
            unique_preps.append(p)
    prep = ", ".join(unique_preps)
    return noun, prep


def label_line(line: str) -> dict:
    # Paren pre-processing: pull out (till X) first, then drop everything else
    carry_preps: list[str] = []
    m = PAREN_TILL_RE.search(line)
    if m:
        carry_preps.append(m.group(1).strip().lower())
        line = PAREN_TILL_RE.sub(" ", line)
    # Single-word parens that are adjectives → keep as prep
    for adj in PAREN_ADJ_RE.findall(line):
        if adj.lower() in PREP_ADJECTIVES:
            carry_preps.append(adj.lower())
    line = PAREN_ANY_RE.sub(" ", line)
    line = re.sub(r"\s+", " ", line).strip()

    qty, rest = _extract_quantity(line)
    unit, leading_prep, rest = _extract_unit_with_adj(rest)
    ingredient, prep = _split_prep_and_noun(rest, leading_prep)
    # Bare countable with leading quantity (``3 ägg``, ``1 gurka``) → MEDIUM
    # sentinel. Lines with no leading number (``smör``, ``salt``) stay "".
    if qty is not None and unit == "" and ingredient:
        unit = "MEDIUM"
    if qty is None:
        qty = 1.0

    # Merge carry_preps (from parens) into final prep
    if carry_preps:
        extra = ", ".join(carry_preps)
        prep = f"{prep}, {extra}" if prep else extra

    return {
        "quantity": qty,
        "unit": unit,
        "ingredient": ingredient,
        "preparation": prep,
    }


def category_for(unit: str) -> str:
    volume = {"dl", "ml", "l", "cl", "liter"}
    weight = {"g", "kg"}
    spoon = {"msk", "tsk", "krm"}
    package = {
        "förpackning",
        "förp",
        "paket",
        "glasburk",
        "burk",
        "kruka",
        "knippe",
        "ask",
        "påse",
        "sats",
        "kvist",
        "kvistar",
    }
    if unit in volume:
        return "volume"
    if unit in weight:
        return "weight"
    if unit in spoon:
        return "spoon"
    if unit in package:
        return "package"
    # MEDIUM/LARGE/SMALL sentinels and bare "" both fall under count.
    return "count"


# Manual overrides for lines the rule labeler can't cleanly resolve.
# Keyed on the raw line (exact match, post-strip) → expected dict.
OVERRIDES: dict[str, dict[str, object]] = {
    # Two head nouns joined by "och" — label first, the data-extraction
    # layer naturally loses the second ingredient anyway.
    "salt och peppar": {
        "quantity": 1.0,
        "unit": "",
        "ingredient": "salt",
        "preparation": "",
    },
    # Zest + juice of half a washed lemon — the "1/2" belongs to the
    # head noun at the end, not a mid-phrase quantity. Citron is
    # countable, so unit is the MEDIUM sentinel.
    "finrivet skal och juice av 1/2 tvättad citron": {
        "quantity": 0.5,
        "unit": "MEDIUM",
        "ingredient": "citron",
        "preparation": "finrivet skal, juice, tvättad",
    },
    # Equipment, not an ingredient. Gold with empty ingredient means
    # "no extractable ingredient here" — an LLM that returns anything
    # will register as a false positive against this recipe's name set.
    "spritspåse": {
        "quantity": 1.0,
        "unit": "",
        "ingredient": "",
        "preparation": "",
    },
}


def main() -> None:
    records = []
    with CANDIDATES.open() as f:
        for raw in f:
            raw = raw.strip()
            if not raw:
                continue
            c = json.loads(raw)
            line = c["line"]
            if line.strip() in OVERRIDES:
                expected = OVERRIDES[line.strip()]
            else:
                expected = label_line(line)
            records.append(
                {
                    "row_id": c["row_id"],
                    "line": line,
                    "category": category_for(str(expected["unit"])),
                    "expected": expected,
                }
            )
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(f"wrote {len(records)} gold entries to {OUT}")


if __name__ == "__main__":
    main()
