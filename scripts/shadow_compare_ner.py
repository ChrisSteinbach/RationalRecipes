#!/usr/bin/env python3
"""Shadow-compare RecipeNLG NER ingredient names against the LLM cache (am5).

For each sampled (raw_line, ner_list) pair from the RecipeNLG corpus:

  1. Resolve a NER candidate for the line via ``resolve_ner_for_line``.
  2. Canonicalize the NER candidate via ``canonicalize_name`` (the same
     path Pass 2's variant grouping uses).
  3. Look up the cached LLM parse for the line in
     ``parsed_ingredient_lines`` and canonicalize its ``ingredient`` field.
  4. Score whether the two canonicalized names agree under the same
     equivalence rules as ``shadow_compare_cache.py``.

Reports, against the bead's ≥85% acceptance bar:

  * ``hit_rate``      — share of lines where NER resolved a candidate
                        AND the LLM cache had a parse to compare against.
  * ``ingredient_agreement`` — within hits, share where the two
                        canonical names match.

Lines where NER declines (no substring match) are the "LLM-only"
population — those still need the regex / LLM hot path.

The script reads RecipeNLG rows from ``dataset/full_dataset.csv`` and
the cache from ``recipes.db``; both are I/O-bound so the sample size
is the wallclock knob. Defaults to 5000 rows for a few-second run.

Usage:

    python3 scripts/shadow_compare_ner.py
    python3 scripts/shadow_compare_ner.py --sample 10000
    python3 scripts/shadow_compare_ner.py --out scripts/benchmark_data/shadow_ner.json
"""

from __future__ import annotations

import argparse
import ast
import csv
import json
import random
import sqlite3
import time
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from rational_recipes.scrape.canonical import canonicalize_name
from rational_recipes.scrape.ner_match import resolve_ner_for_line


def _ingredient_close(a: str, b: str) -> bool:
    """Equivalence rule shared with ``shadow_compare_cache.py`` (r6w).

    Both inputs are pre-canonicalized via ``canonicalize_name``. We treat
    them as equivalent when:
      * Equal after lowercasing.
      * One is a substring of the other (≥3 char floor — keeps "egg" vs
        "egg yolk" mergeable but excludes 1-2 char accidents).
      * One is a regular English plural of the other (``ies → y``,
        ``es → ``, ``s → ``) — covers ``tomatoes`` ↔ ``tomato`` etc.
    """
    a_norm = a.lower().strip()
    b_norm = b.lower().strip()
    if not a_norm or not b_norm:
        return False
    if a_norm == b_norm:
        return True
    short = min(a_norm, b_norm, key=len)
    long = max(a_norm, b_norm, key=len)
    if short in long and len(short) >= 3:
        return True
    for ending, replacement in [("ies", "y"), ("es", ""), ("s", "")]:
        if (
            a_norm.endswith(ending)
            and a_norm[: -len(ending)] + replacement == b_norm
        ):
            return True
        if (
            b_norm.endswith(ending)
            and b_norm[: -len(ending)] + replacement == a_norm
        ):
            return True
    return False


@dataclass(frozen=True, slots=True)
class _LLMParse:
    ingredient: str


def _llm_ingredient_from_json(raw: str | None) -> _LLMParse | None:
    """Decode the cached parsed_json into the LLM-extracted ingredient name.

    Mirrors the live parse path's tolerance for misspelled ``ingredient``
    keys (``ingruedient`` etc. — gemma3n:e2b oddity) so the LLM-side
    "got an answer" rate matches what production sees.
    """
    if raw is None:
        return None
    try:
        d = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(d, dict):
        return None
    ing_key = next(
        (k for k in d if isinstance(k, str) and k.lower().startswith("ingr")),
        None,
    )
    if ing_key is None:
        return None
    value = d.get(ing_key)
    if not isinstance(value, str):
        return None
    return _LLMParse(ingredient=value.lower().strip())


@dataclass
class Buckets:
    total_lines: int = 0
    ner_hit_llm_hit: int = 0
    ner_hit_llm_failed: int = 0
    ner_decline_llm_hit: int = 0
    ner_decline_llm_failed: int = 0
    ner_hit_no_cache: int = 0
    ner_decline_no_cache: int = 0

    # Within ner_hit_llm_hit, did the canonical names match?
    ingredient_match: int = 0

    @property
    def comparable(self) -> int:
        return self.ner_hit_llm_hit

    @property
    def hit_rate(self) -> float:
        """Lines where NER resolved a candidate, regardless of LLM state."""
        ner_hits = (
            self.ner_hit_llm_hit
            + self.ner_hit_llm_failed
            + self.ner_hit_no_cache
        )
        return ner_hits / self.total_lines if self.total_lines else 0.0

    @property
    def ingredient_agreement(self) -> float:
        return (
            self.ingredient_match / self.comparable if self.comparable else 0.0
        )

    @property
    def llm_only_share(self) -> float:
        """Lines NER declined but LLM solved — the irreducible LLM call rate."""
        if not self.total_lines:
            return 0.0
        return self.ner_decline_llm_hit / self.total_lines


def _sample_recipenlg(
    csv_path: Path,
    sample: int,
    *,
    seed: int = 42,
) -> list[tuple[tuple[str, ...], tuple[str, ...]]]:
    """Reservoir-sample ``sample`` (ingredients, ner) pairs from the CSV.

    Reservoir sampling keeps a single pass over the 2.2M-row CSV and
    avoids loading the whole file into memory. Determinism via ``seed``
    so reruns are stable for benchmark comparisons.
    """
    rng = random.Random(seed)
    reservoir: list[tuple[tuple[str, ...], tuple[str, ...]]] = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            try:
                ingredients = ast.literal_eval(row.get("ingredients", "[]"))
                ner = ast.literal_eval(row.get("NER", "[]"))
            except (ValueError, SyntaxError):
                continue
            if not isinstance(ingredients, list) or not isinstance(ner, list):
                continue
            pair = (
                tuple(str(x) for x in ingredients),
                tuple(str(x) for x in ner),
            )
            if len(reservoir) < sample:
                reservoir.append(pair)
            else:
                j = rng.randint(0, i)
                if j < sample:
                    reservoir[j] = pair
    return reservoir


def run(
    csv_path: Path,
    db_path: Path,
    sample_recipes: int,
    *,
    seed: int = 42,
    max_disagreements: int = 60,
) -> tuple[Buckets, list[dict[str, Any]], list[dict[str, Any]]]:
    """Stream RecipeNLG, score NER vs cached LLM, return summary + samples."""
    pairs = _sample_recipenlg(csv_path, sample_recipes, seed=seed)

    # Open the cache read-only; lookups are by raw line text + model + seed
    # mirroring the production lookup_cached_parse path.
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        buckets = Buckets()
        disagreements: list[dict[str, Any]] = []
        declines: list[dict[str, Any]] = []
        t0 = time.monotonic()

        for ingredients, ner_list in pairs:
            for line in ingredients:
                buckets.total_lines += 1

                ner_candidate = resolve_ner_for_line(line, ner_list)
                row = conn.execute(
                    "SELECT parsed_json FROM parsed_ingredient_lines "
                    "WHERE raw_line = ? AND corpus = 'recipenlg' LIMIT 1",
                    (line,),
                ).fetchone()
                cached_json: str | None = row[0] if row is not None else None
                cache_present = row is not None

                if ner_candidate is None:
                    if not cache_present:
                        buckets.ner_decline_no_cache += 1
                    elif cached_json is None:
                        buckets.ner_decline_llm_failed += 1
                    else:
                        buckets.ner_decline_llm_hit += 1
                        if len(declines) < max_disagreements:
                            llm = _llm_ingredient_from_json(cached_json)
                            declines.append({
                                "line": line,
                                "ner_list": list(ner_list),
                                "llm_ingredient": (
                                    llm.ingredient if llm else None
                                ),
                            })
                    continue

                ner_canonical = canonicalize_name(ner_candidate)

                if not cache_present:
                    buckets.ner_hit_no_cache += 1
                    continue
                if cached_json is None:
                    buckets.ner_hit_llm_failed += 1
                    continue
                llm = _llm_ingredient_from_json(cached_json)
                if llm is None:
                    buckets.ner_hit_llm_failed += 1
                    continue

                buckets.ner_hit_llm_hit += 1
                llm_canonical = canonicalize_name(llm.ingredient)
                if _ingredient_close(ner_canonical, llm_canonical):
                    buckets.ingredient_match += 1
                elif len(disagreements) < max_disagreements:
                    disagreements.append({
                        "line": line,
                        "ner_candidate": ner_candidate,
                        "ner_canonical": ner_canonical,
                        "llm_ingredient": llm.ingredient,
                        "llm_canonical": llm_canonical,
                    })

        elapsed = time.monotonic() - t0
        print(
            f"\nCompared {buckets.total_lines} lines "
            f"({len(pairs)} recipes) in {elapsed:.2f}s"
        )
        return buckets, disagreements, declines
    finally:
        conn.close()


def _print_report(
    b: Buckets,
    disagreements: list[dict[str, Any]],
    declines: list[dict[str, Any]],
) -> None:
    print("\n" + "=" * 70)
    print("Population breakdown")
    print("-" * 70)
    print(f"  total lines                : {b.total_lines}")
    print(f"  NER hit + LLM hit          : {b.ner_hit_llm_hit}")
    print(f"  NER hit + LLM failed       : {b.ner_hit_llm_failed}")
    print(f"  NER hit + line not cached  : {b.ner_hit_no_cache}")
    print(f"  NER decline + LLM hit      : {b.ner_decline_llm_hit}")
    print(f"  NER decline + LLM failed   : {b.ner_decline_llm_failed}")
    print(f"  NER decline + not cached   : {b.ner_decline_no_cache}")

    print("\n" + "=" * 70)
    print("Headline metrics (am5 acceptance: hit ≥85% of LLM agreement)")
    print("-" * 70)
    print(f"  NER hit-rate              : {b.hit_rate * 100:6.2f}%")
    print(f"  share routed to LLM       : {b.llm_only_share * 100:6.2f}%")
    print(f"  ingredient agreement      : {b.ingredient_agreement * 100:6.2f}%")
    print(f"    (within {b.comparable} comparable hits)")

    print("\n" + "=" * 70)
    print(
        f"Sample disagreements ({min(len(disagreements), 15)} "
        f"of {len(disagreements)})"
    )
    print("-" * 70)
    for d in disagreements[:15]:
        print(
            f"  {d['line']!r}\n"
            f"    NER: {d['ner_candidate']!r:<35} "
            f"→ canon={d['ner_canonical']!r}\n"
            f"    LLM: {d['llm_ingredient']!r:<35} "
            f"→ canon={d['llm_canonical']!r}"
        )

    print("\n" + "=" * 70)
    print(
        f"Sample NER-declines (LLM handled {min(len(declines), 15)} "
        f"of {len(declines)} shown)"
    )
    print("-" * 70)
    for d in declines[:15]:
        print(
            f"  {d['line']!r}\n"
            f"    ner_list: {d['ner_list']}\n"
            f"    LLM: {d['llm_ingredient']!r}"
        )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--csv",
        type=Path,
        default=Path("dataset/full_dataset.csv"),
        help="RecipeNLG CSV path",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=Path("output/catalog/recipes.db"),
        help="recipes.db (must contain parsed_ingredient_lines)",
    )
    parser.add_argument(
        "--sample",
        type=int,
        default=5000,
        help="Number of recipes to sample (each contributes multiple lines)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="RNG seed for reservoir sampling",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Optional JSON output path",
    )
    parser.add_argument(
        "--max-disagreements",
        type=int,
        default=60,
        help="Cap on stored disagreement / decline samples",
    )
    args = parser.parse_args(argv)

    if not args.csv.exists():
        print(f"CSV not found: {args.csv}")
        return 1
    if not args.db.exists():
        print(f"DB not found: {args.db}")
        return 1

    print(
        f"Sampling {args.sample} recipes from {args.csv} (seed={args.seed}) …"
    )
    buckets, disagreements, declines = run(
        args.csv,
        args.db,
        args.sample,
        seed=args.seed,
        max_disagreements=args.max_disagreements,
    )
    _print_report(buckets, disagreements, declines)

    if args.out is not None:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "schema_version": 1,
            "csv": str(args.csv),
            "db": str(args.db),
            "sample_recipes": args.sample,
            "seed": args.seed,
            "buckets": asdict(buckets),
            "metrics": {
                "hit_rate": buckets.hit_rate,
                "llm_only_share": buckets.llm_only_share,
                "ingredient_agreement": buckets.ingredient_agreement,
            },
            "disagreements": disagreements,
            "declines": declines,
        }
        args.out.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        print(f"\nWrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
