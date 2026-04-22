#!/usr/bin/env python3
"""One-shot sampler: pull messy English ingredient lines from RecipeNLG.

Filters each line into one of three categories defined by RationalRecipes-5i1:
- plurals (no unit, bare count like "3 eggs")
- comma-preps ("1 cup flour, sifted")
- packaging units ("1 can (14 oz) tomatoes", "1 (6 oz) pkg lemon gelatin")

Prints candidates to stdout. Not a production pipeline — use to curate
benchmark_data/english_messy_gold.jsonl by hand.
"""

from __future__ import annotations

import ast
import csv
import random
import re
import sys
from pathlib import Path

DATASET = Path("dataset/full_dataset.csv")
RANDOM_SEED = 42
ROWS_TO_SCAN = 50000  # first N rows is fine; RecipeNLG is well-mixed

PREP_WORDS = (
    "chopped",
    "sifted",
    "melted",
    "diced",
    "minced",
    "beaten",
    "softened",
    "peeled",
    "sliced",
    "crushed",
    "shredded",
    "grated",
    "drained",
    "rinsed",
    "cubed",
    "halved",
    "separated",
    "juiced",
)
PACKAGING = (
    "can",
    "cans",
    "pkg",
    "pkgs",
    "package",
    "packages",
    "jar",
    "jars",
    "box",
    "boxes",
    "bag",
    "bags",
    "bottle",
    "bottles",
)

BARE_UNIT_RE = re.compile(
    r"\b(cup|tsp|tbsp|oz|lb|g|kg|ml|pound|tablespoon|teaspoon)\b", re.I
)
PAREN_OZ_RE = re.compile(r"\(\s*\d[\d./]*\s*(oz|lb|ml|g|pound)", re.I)


def categorize(line: str) -> str | None:
    low = line.lower()
    # packaging units: an actual package word, with or without parenthetical size
    if any(re.search(rf"\b\d[\d./]*\s*{p}\b", low) for p in PACKAGING):
        return "packaging"
    if PAREN_OZ_RE.search(low) and any(p in low for p in PACKAGING):
        return "packaging"
    # comma-prep: contains comma followed by prep word within ~20 chars
    if "," in line and any(re.search(rf",\s*[^,]{{0,25}}{w}", low) for w in PREP_WORDS):
        return "comma_prep"
    # plural: bare count of items (no standard unit) like "3 eggs" or "2 onions"
    m = re.match(r"^\s*\d+\s+([a-z][a-z\s-]*s)\b", low)
    if m and not BARE_UNIT_RE.search(line):
        return "plural"
    return None


def main() -> None:
    if not DATASET.exists():
        sys.exit(f"dataset missing: {DATASET}")

    random.seed(RANDOM_SEED)
    buckets: dict[str, list[str]] = {"plural": [], "comma_prep": [], "packaging": []}

    with DATASET.open() as f:
        reader = csv.reader(f)
        next(reader, None)  # header
        for i, row in enumerate(reader):
            if i >= ROWS_TO_SCAN:
                break
            if len(row) < 3:
                continue
            try:
                ings = ast.literal_eval(row[2])
            except (ValueError, SyntaxError):
                continue
            for line in ings:
                cat = categorize(line)
                if cat is None:
                    continue
                buckets[cat].append(line.strip())

    for cat, lines in buckets.items():
        random.shuffle(lines)
        print(f"\n=== {cat} ({len(lines)} total, showing 25) ===")
        for line in lines[:25]:
            print(f"  {line!r}")


if __name__ == "__main__":
    main()
