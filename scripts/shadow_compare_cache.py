#!/usr/bin/env python3
"""Shadow-compare the regex parser against the cached LLM parses (r6w).

Reads N rows from ``parsed_ingredient_lines`` in ``recipes.db`` (the
1.4M-row gemma4:e2b cache), runs ``regex_parse_line`` on each
``raw_line``, and reports agreement against the cached LLM output.

The cache IS the source of truth here. ``temperature=0`` + ``seed=42``
means the cached parses are byte-stable; comparing the regex against
them is a direct measurement of "would the regex have produced an
equivalent answer if we'd used it for this line." No LLM is contacted.

Outputs three populations:

  * ``regex_hit``   — regex returned a confident parse. Agreement is
                      computed against the cached LLM parse using the
                      shadow_compare_parse.py equivalence rules
                      (qty ±5%, unit equiv classes, ingredient
                      substring overlap ≥3 chars).
  * ``regex_decline`` — regex returned None. These lines stay on the
                      LLM hot path. The hit-rate metric is
                      ``regex_hit / total``.
  * ``llm_failed``  — cached parsed_json is NULL (LLM gave up). Treated
                      as a regex-win if the regex did parse, else
                      noise.

The acceptance bar (per RationalRecipes-r6w):

  * Hit-rate ≥ ~50% so the regex actually reduces LLM calls.
  * Among hits, full-agreement ≥ 90%.

Usage:
    python3 scripts/shadow_compare_cache.py
    python3 scripts/shadow_compare_cache.py --sample 5000
    python3 scripts/shadow_compare_cache.py --db output/catalog/recipes.db
    python3 scripts/shadow_compare_cache.py --out scripts/benchmark_data/shadow_cache.json
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import time
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from rational_recipes.scrape.canonical import canonicalize_name
from rational_recipes.scrape.parse import ParsedIngredient
from rational_recipes.scrape.regex_parse import regex_parse_line

# Re-use the equivalence rules from the LLM-vs-LLM shadow harness so
# both comparisons score against the same yardstick.
_EQUIV_GROUPS = [
    {"tsk", "tsp", "teaspoon", "teaspoons"},
    {"msk", "tbsp", "tbs", "tablespoon", "tablespoons"},
    {"dl", "deciliter", "deciliters"},
    {"g", "gram", "grams", ""},
    {"kg", "kilogram", "kilograms"},
    {"ml", "milliliter", "milliliters"},
    {"l", "liter", "litre", "liters"},
    {"cup", "cups", "c"},
    {"oz", "ounce", "ounces"},
    {"lb", "lbs", "pound", "pounds"},
    {"krm", "pinch"},
    {"medium", "large", "small", "piece", "pieces", ""},
]


def _norm_unit(u: str) -> str:
    return u.strip().lower().rstrip(".")


def _unit_equiv(a: str, b: str) -> bool:
    a, b = _norm_unit(a), _norm_unit(b)
    if a == b:
        return True
    for group in _EQUIV_GROUPS:
        if a in group and b in group:
            return True
    return False


def _qty_close(a: float, b: float, tol: float = 0.05) -> bool:
    if a == b:
        return True
    if max(abs(a), abs(b)) == 0:
        return True
    return abs(a - b) / max(abs(a), abs(b)) <= tol


def _ingredient_close(a: str, b: str) -> bool:
    """Compare ingredient names through the same canonicalization path
    that downstream variant grouping uses.

    The cached LLM rows are inconsistent on Swedish→English (the e4s
    prompt update doesn't catch every line — gemma4:e2b still
    occasionally emits ``morot`` / ``apelsin`` / ``persilja``). The
    regex via the r6w SWEDISH_TO_ENGLISH post-translation produces
    English consistently. To score "would these end up in the same
    variant cluster", we apply ``canonicalize_name`` to both sides —
    the same call ``catalog_pipeline`` uses to build the variant
    ingredient set. Equal-after-canonicalize means equivalent for
    grouping purposes.
    """
    a_canon = canonicalize_name(a).lower().strip()
    b_canon = canonicalize_name(b).lower().strip()
    if a_canon == b_canon:
        return True
    short = min(a_canon, b_canon, key=len)
    long = max(a_canon, b_canon, key=len)
    if not short:
        return False
    # Substring match with a 3-char floor to keep "egg" vs "egg yolk"
    # mergeable but not "ic" vs "icing sugar".
    if short in long and len(short) >= 3:
        return True
    # Common English plural↔singular pairs ("blueberry" ↔ "blueberries",
    # "tomato" ↔ "tomatoes") that don't substring-match cleanly.
    for ending, replacement in [("ies", "y"), ("es", ""), ("s", "")]:
        if a_canon.endswith(ending) and a_canon[: -len(ending)] + replacement == b_canon:
            return True
        if b_canon.endswith(ending) and b_canon[: -len(ending)] + replacement == a_canon:
            return True
    return False


@dataclass(frozen=True, slots=True)
class _LLMParse:
    quantity: float
    unit: str
    ingredient: str
    preparation: str


def _llm_from_json(raw: str | None) -> _LLMParse | None:
    if raw is None:
        return None
    try:
        d = json.loads(raw)
    except json.JSONDecodeError:
        return None
    try:
        # The LLM occasionally misspells "ingredient" — the live parse
        # path tolerates any "ingr*" key, so do the same here.
        ing_key = next(
            (k for k in d if isinstance(k, str) and k.lower().startswith("ingr")),
            None,
        )
        if ing_key is None:
            return None
        return _LLMParse(
            quantity=float(d["quantity"]),
            unit=str(d["unit"]),
            ingredient=str(d[ing_key]).lower().strip(),
            preparation=str(d.get("preparation", "")),
        )
    except (KeyError, ValueError, TypeError):
        return None


@dataclass
class Buckets:
    total: int = 0
    regex_hit_llm_hit: int = 0
    regex_hit_llm_failed: int = 0
    regex_decline_llm_hit: int = 0
    regex_decline_llm_failed: int = 0

    # Agreement (within regex_hit_llm_hit only).
    ing_match: int = 0
    unit_match: int = 0
    qty_match: int = 0
    full_match: int = 0

    @property
    def hit_rate(self) -> float:
        if not self.total:
            return 0.0
        return (self.regex_hit_llm_hit + self.regex_hit_llm_failed) / self.total

    @property
    def llm_only_share(self) -> float:
        if not self.total:
            return 0.0
        return self.regex_decline_llm_hit / self.total

    @property
    def full_agreement(self) -> float:
        n = self.regex_hit_llm_hit
        return self.full_match / n if n else 0.0

    @property
    def ing_agreement(self) -> float:
        n = self.regex_hit_llm_hit
        return self.ing_match / n if n else 0.0

    @property
    def unit_agreement(self) -> float:
        n = self.regex_hit_llm_hit
        return self.unit_match / n if n else 0.0

    @property
    def qty_agreement(self) -> float:
        n = self.regex_hit_llm_hit
        return self.qty_match / n if n else 0.0


def _serialize_regex(p: ParsedIngredient | None) -> dict[str, Any] | None:
    if p is None:
        return None
    return {
        "quantity": p.quantity,
        "unit": p.unit,
        "ingredient": p.ingredient,
        "preparation": p.preparation,
    }


def _serialize_llm(p: _LLMParse | None) -> dict[str, Any] | None:
    if p is None:
        return None
    return asdict(p)


def _sample_rows(
    db_path: Path,
    sample: int,
    *,
    seed: int = 42,
    distinct_lines: bool = True,
) -> list[tuple[str, str | None]]:
    """Return ``sample`` rows of ``(raw_line, parsed_json)`` from cache.

    ``distinct_lines=True`` collapses duplicates on ``raw_line`` so the
    sample reflects the diversity of the cache rather than the natural
    skew toward "1 c. flour" / "1 tsp. salt". With raw counts this is
    closer to what the parser actually faces in production: the same
    "1 c. flour" hits the regex on every recipe.
    """
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        # SQLite RANDOM() is fine for this scale; the conn is read-only.
        if distinct_lines:
            sql = (
                "SELECT raw_line, MIN(parsed_json) FROM parsed_ingredient_lines "
                "GROUP BY raw_line ORDER BY RANDOM() LIMIT ?"
            )
        else:
            sql = (
                "SELECT raw_line, parsed_json FROM parsed_ingredient_lines "
                "ORDER BY RANDOM() LIMIT ?"
            )
        rows = conn.execute(sql, (sample,)).fetchall()
        return [(r[0], r[1]) for r in rows]
    finally:
        conn.close()


def _compare_one(
    raw: str, llm_json: str | None
) -> tuple[ParsedIngredient | None, _LLMParse | None, dict[str, bool]]:
    rx = regex_parse_line(raw)
    rx_parsed = rx.parsed if rx is not None else None
    llm = _llm_from_json(llm_json)
    if rx_parsed is None or llm is None:
        return rx_parsed, llm, {}
    matches = {
        "ing": _ingredient_close(rx_parsed.ingredient, llm.ingredient),
        "unit": _unit_equiv(rx_parsed.unit, llm.unit),
        "qty": _qty_close(rx_parsed.quantity, llm.quantity),
    }
    matches["all"] = all(matches.values())
    return rx_parsed, llm, matches


def run(
    db_path: Path,
    sample: int,
    *,
    distinct_lines: bool = True,
    max_disagreements: int = 60,
) -> tuple[Buckets, list[dict[str, Any]], list[dict[str, Any]]]:
    rows = _sample_rows(db_path, sample, distinct_lines=distinct_lines)
    buckets = Buckets()
    disagreements: list[dict[str, Any]] = []
    declines: list[dict[str, Any]] = []
    t0 = time.monotonic()
    for raw, llm_json in rows:
        buckets.total += 1
        rx_parsed, llm, matches = _compare_one(raw, llm_json)
        if rx_parsed is not None and llm is not None:
            buckets.regex_hit_llm_hit += 1
            if matches["ing"]:
                buckets.ing_match += 1
            if matches["unit"]:
                buckets.unit_match += 1
            if matches["qty"]:
                buckets.qty_match += 1
            if matches["all"]:
                buckets.full_match += 1
            elif len(disagreements) < max_disagreements:
                disagreements.append({
                    "line": raw,
                    "regex": _serialize_regex(rx_parsed),
                    "llm": _serialize_llm(llm),
                    "match": matches,
                })
        elif rx_parsed is not None and llm is None:
            buckets.regex_hit_llm_failed += 1
        elif rx_parsed is None and llm is not None:
            buckets.regex_decline_llm_hit += 1
            if len(declines) < max_disagreements:
                declines.append({
                    "line": raw,
                    "llm": _serialize_llm(llm),
                })
        else:
            buckets.regex_decline_llm_failed += 1
    elapsed = time.monotonic() - t0
    print(f"\nProcessed {buckets.total} lines in {elapsed:.2f}s "
          f"({buckets.total / elapsed:.0f} lines/s, regex-only)")
    return buckets, disagreements, declines


def _print_report(
    b: Buckets,
    disagreements: list[dict[str, Any]],
    declines: list[dict[str, Any]],
) -> None:
    print("\n" + "=" * 70)
    print("Population breakdown")
    print("-" * 70)
    print(f"  total                      : {b.total}")
    print(f"  regex hit + LLM hit        : {b.regex_hit_llm_hit}")
    print(f"  regex hit + LLM failed     : {b.regex_hit_llm_failed}")
    print(f"  regex decline + LLM hit    : {b.regex_decline_llm_hit}")
    print(f"  regex decline + LLM failed : {b.regex_decline_llm_failed}")

    print("\n" + "=" * 70)
    print("Headline metrics")
    print("-" * 70)
    print(f"  regex hit-rate            : {b.hit_rate * 100:6.2f}%")
    print(f"  share routed to LLM       : {b.llm_only_share * 100:6.2f}%")
    print(f"  full agreement (on hits)  : {b.full_agreement * 100:6.2f}%")
    print(f"    ingredient agreement    : {b.ing_agreement * 100:6.2f}%")
    print(f"    unit agreement          : {b.unit_agreement * 100:6.2f}%")
    print(f"    quantity agreement      : {b.qty_agreement * 100:6.2f}%")

    print("\n" + "=" * 70)
    print(f"Sample disagreements ({min(len(disagreements), 15)} of {len(disagreements)})")
    print("-" * 70)
    for d in disagreements[:15]:
        rx = d["regex"]
        llm = d["llm"]
        which = ", ".join(k for k, v in d["match"].items() if k != "all" and not v)
        print(
            f"  {d['line']!r:<55}  diff={which}\n"
            f"    regex: qty={rx['quantity']:>5} "
            f"unit={rx['unit']!r:<10} ing={rx['ingredient']!r}\n"
            f"    llm:   qty={llm['quantity']:>5} "
            f"unit={llm['unit']!r:<10} ing={llm['ingredient']!r}"
        )

    print("\n" + "=" * 70)
    print(
        f"Sample regex-declines (LLM handled {min(len(declines), 15)} "
        f"of {len(declines)} shown)"
    )
    print("-" * 70)
    for d in declines[:15]:
        llm = d["llm"]
        print(
            f"  {d['line']!r:<55}\n"
            f"    llm:   qty={llm['quantity']:>5} "
            f"unit={llm['unit']!r:<10} ing={llm['ingredient']!r}"
        )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        type=Path,
        default=Path("output/catalog/recipes.db"),
        help="Path to recipes.db (must contain parsed_ingredient_lines)",
    )
    parser.add_argument(
        "--sample",
        type=int,
        default=2000,
        help="Number of cache rows to compare (distinct lines by default)",
    )
    parser.add_argument(
        "--include-duplicates",
        action="store_true",
        help="Sample raw rows including duplicates (default: distinct raw_line)",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Write JSON results to this path (optional)",
    )
    parser.add_argument(
        "--max-disagreements",
        type=int,
        default=60,
        help="Cap on stored disagreement / decline samples for the JSON output",
    )
    args = parser.parse_args(argv)

    if not args.db.exists():
        print(f"DB not found: {args.db}")
        return 1

    print(f"Sampling {args.sample} rows from {args.db}"
          f"{' (with duplicates)' if args.include_duplicates else ' (distinct)'} …")
    buckets, disagreements, declines = run(
        args.db,
        args.sample,
        distinct_lines=not args.include_duplicates,
        max_disagreements=args.max_disagreements,
    )
    _print_report(buckets, disagreements, declines)

    if args.out is not None:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "schema_version": 1,
            "db": str(args.db),
            "sample": args.sample,
            "distinct_lines": not args.include_duplicates,
            "buckets": asdict(buckets),
            "metrics": {
                "hit_rate": buckets.hit_rate,
                "llm_only_share": buckets.llm_only_share,
                "ing_agreement": buckets.ing_agreement,
                "unit_agreement": buckets.unit_agreement,
                "qty_agreement": buckets.qty_agreement,
                "full_agreement": buckets.full_agreement,
            },
            "disagreements": disagreements,
            "declines": declines,
        }
        args.out.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        print(f"\nWrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
