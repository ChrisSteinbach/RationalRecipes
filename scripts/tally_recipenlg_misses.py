#!/usr/bin/env python3
"""Tally ingredient-name misses on the RecipeNLG corpus (bead RationalRecipes-b7t.1).

RecipeNLG ships with a pre-extracted `NER` column — the same clean
ingredient-name strings that the scrape pipeline feeds into
`IngredientFactory` for RecipeNLG rows (see
`scrape/recipenlg.py::Recipe.ingredient_names`). That means a miss on
NER is a miss on the pipeline, so we can measure DB coverage without
running the LLM extractor against 2.2M recipes.

This script streams the corpus, tallies hits and misses against the
ingredient synonym table, and prints:

- Overall ingredient-mention hit rate
- Fraction of recipes whose every NER name resolves (pipeline-clean recipes)
- Top-N misses by frequency
- Top-N misses restricted to a title substring (useful for dish-family slices)

Usage:
    python3 scripts/tally_recipenlg_misses.py                 # whole corpus
    python3 scripts/tally_recipenlg_misses.py --limit 50000   # first N rows
    python3 scripts/tally_recipenlg_misses.py --title pancake # subset by title
    python3 scripts/tally_recipenlg_misses.py --top 50        # more top-misses
"""

from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
import time
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_RECIPENLG = REPO_ROOT / "dataset" / "full_dataset.csv"
DB_PATH = REPO_ROOT / "src" / "rational_recipes" / "data" / "ingredients.db"

# RecipeNLG's NER column is a stringified Python list. Use the existing
# loader's parser so behavior matches the scrape path.
sys.path.insert(0, str(REPO_ROOT / "src"))
from rational_recipes.scrape.recipenlg import _parse_string_list  # noqa: E402


def build_hit_checker(db_path: Path) -> tuple[callable, dict[str, bool]]:
    """Return (is_hit, cache) — cache lookups against the synonym table.

    Bypasses `IngredientFactory.get_by_name`'s KeyError + suggestion path,
    which runs an O(N) LIKE query per miss that would dominate a 22M-lookup
    scan. The synonym table is small (~8K rows) and lookups are O(log N)
    via the case-insensitive index on `synonym.name`.
    """
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    cursor = conn.cursor()
    cache: dict[str, bool] = {}

    def is_hit(raw_name: str) -> bool:
        key = raw_name.lower().strip()
        if not key:
            return False
        if key in cache:
            return cache[key]
        row = cursor.execute(
            "SELECT 1 FROM synonym WHERE name = ? COLLATE NOCASE LIMIT 1",
            (key,),
        ).fetchone()
        hit = row is not None
        cache[key] = hit
        return hit

    return is_hit, cache


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--path",
        type=Path,
        default=DEFAULT_RECIPENLG,
        help=(
            f"RecipeNLG CSV path (default: {DEFAULT_RECIPENLG.relative_to(REPO_ROOT)})"
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Only process the first N rows (0 = all).",
    )
    parser.add_argument(
        "--title",
        default="",
        help=(
            "Only process recipes whose title contains this substring"
            " (case-insensitive)."
        ),
    )
    parser.add_argument(
        "--top",
        type=int,
        default=30,
        help="Show the top-N missing ingredient names (default: 30).",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=DB_PATH,
        help=f"ingredients.db path (default: {DB_PATH.relative_to(REPO_ROOT)})",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=100_000,
        help="Print progress every N rows (0 = no progress).",
    )
    args = parser.parse_args()

    if not args.path.exists():
        print(f"RecipeNLG not found at {args.path}", file=sys.stderr)
        sys.exit(1)
    if not args.db.exists():
        print(f"ingredients.db not found at {args.db}", file=sys.stderr)
        sys.exit(1)

    is_hit, cache = build_hit_checker(args.db)
    title_filter = args.title.lower().strip()

    recipes_seen = 0
    recipes_matched = 0
    recipes_all_hit = 0
    mentions = 0
    hits = 0
    miss_counter: Counter[str] = Counter()
    start = time.monotonic()

    with open(args.path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            recipes_seen += 1
            if args.limit and recipes_seen > args.limit:
                recipes_seen -= 1
                break
            if title_filter and title_filter not in row.get("title", "").lower():
                continue
            recipes_matched += 1
            names = _parse_string_list(row.get("NER", "[]"))
            if not names:
                continue
            all_hit = True
            for name in names:
                mentions += 1
                if is_hit(name):
                    hits += 1
                else:
                    miss_counter[name.lower().strip()] += 1
                    all_hit = False
            if all_hit:
                recipes_all_hit += 1
            if args.progress_every and recipes_seen % args.progress_every == 0:
                elapsed = time.monotonic() - start
                rate = recipes_seen / elapsed if elapsed > 0 else 0
                print(
                    f"  ... {recipes_seen:,} rows ({rate:,.0f}/s, "
                    f"cache={len(cache):,})",
                    file=sys.stderr,
                )

    elapsed = time.monotonic() - start
    print()
    print(f"Scanned {recipes_seen:,} recipes in {elapsed:,.1f}s", end="")
    if title_filter:
        print(f" (matched {recipes_matched:,} with title~{title_filter!r})")
    else:
        print()
    if mentions == 0:
        print("No ingredient mentions found.")
        return
    miss_mentions = mentions - hits
    print(
        f"Ingredient mentions: {mentions:,} "
        f"(hit {hits:,} = {hits / mentions:.1%}, "
        f"miss {miss_mentions:,} = {miss_mentions / mentions:.1%})"
    )
    if recipes_matched:
        print(
            f"Recipes with every NER name resolving: "
            f"{recipes_all_hit:,} / {recipes_matched:,} "
            f"= {recipes_all_hit / recipes_matched:.1%}"
        )
    print(f"Distinct miss names: {len(miss_counter):,}")
    print()
    print(f"Top {args.top} missing ingredient names:")
    for name, count in miss_counter.most_common(args.top):
        share = count / miss_mentions if miss_mentions else 0
        print(f"  {count:>7,}  {share:>5.1%}  {name}")


if __name__ == "__main__":
    main()
