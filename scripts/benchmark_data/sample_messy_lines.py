#!/usr/bin/env python3
"""Sample messy English ingredient lines from RecipeNLG for benchmark gold curation.

Produces candidate lines grouped into four categories:

- ``plural``: bare count of items (no standard unit), e.g. "3 eggs", "2 onions".
- ``comma_prep``: comma followed by a preparation word, e.g. "1 onion, chopped".
- ``packaging``: packaging-unit quantities, e.g. "1 can soup", "1 (8 oz.) box".
- ``mixed_adversarial``: lines hitting at least two of the above — the tricky
  edge cases that broke v1 gold judgment calls (hamburger buns, butternut
  squash).

The candidates are deduplicated against an existing gold file so the sampler
doesn't re-propose already-labeled lines.

Two modes:

- **Default**: print candidates to stdout, grouped by category. Useful for
  a quick eyeball pass.
- **``--out PATH``**: write candidates as JSONL with
  ``{"line": ..., "category": ...}`` records for an editor-based labeling
  pass. The labeler fills in an ``expected`` key and promotes validated
  entries into ``english_messy_gold.jsonl``.
"""

from __future__ import annotations

import argparse
import ast
import csv
import json
import random
import re
import sys
from pathlib import Path

DATASET = Path("dataset/full_dataset.csv")
GOLD = Path("scripts/benchmark_data/english_messy_gold.jsonl")

# Larger than v1's 50k so the new buckets get enough material to diversify
# without hunting through the entire 2.2 GB file.
ROWS_TO_SCAN = 200_000
RANDOM_SEED = 42
PER_CATEGORY = 60  # candidates per bucket, oversampled so the labeler can cull

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
    "divided",
    "packed",
    "cooled",
    "mashed",
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
    "envelope",
    "envelopes",
    "sleeve",
    "sleeves",
    "stick",
    "sticks",
)

BARE_UNIT_RE = re.compile(
    r"\b(cup|tsp|tbsp|oz|lb|g|kg|ml|pound|tablespoon|teaspoon)\b", re.I
)
PAREN_OZ_RE = re.compile(r"\(\s*\d[\d./]*\s*(oz|lb|ml|g|pound)", re.I)


def _packaging_hit(line: str) -> bool:
    low = line.lower()
    return any(re.search(rf"\b\d[\d./]*\s*{p}\b", low) for p in PACKAGING) or (
        PAREN_OZ_RE.search(low) is not None and any(p in low for p in PACKAGING)
    )


def _comma_prep_hit(line: str) -> bool:
    low = line.lower()
    return "," in line and any(
        re.search(rf",\s*[^,]{{0,25}}{w}", low) for w in PREP_WORDS
    )


def _plural_hit(line: str) -> bool:
    low = line.lower()
    m = re.match(r"^\s*\d+\s+([a-z][a-z\s-]*s)\b", low)
    return m is not None and not BARE_UNIT_RE.search(line)


def categorize(line: str) -> str | None:
    """Assign a line to exactly one bucket. Mixed wins over singletons."""
    hits = [
        _packaging_hit(line),
        _comma_prep_hit(line),
        _plural_hit(line),
    ]
    if sum(hits) >= 2:
        return "mixed_adversarial"
    if hits[0]:
        return "packaging"
    if hits[1]:
        return "comma_prep"
    if hits[2]:
        return "plural"
    return None


def _load_existing_gold_lines() -> set[str]:
    """Normalized set of already-labeled lines so samples don't overlap."""
    if not GOLD.exists():
        return set()
    lines: set[str] = set()
    with GOLD.open() as f:
        for raw in f:
            raw = raw.strip()
            if not raw:
                continue
            lines.add(json.loads(raw)["line"].strip())
    return lines


def sample() -> dict[str, list[str]]:
    """Return a dict of bucket -> list of candidate lines."""
    if not DATASET.exists():
        sys.exit(f"dataset missing: {DATASET}")

    random.seed(RANDOM_SEED)
    already_labeled = _load_existing_gold_lines()
    buckets: dict[str, list[str]] = {
        "plural": [],
        "comma_prep": [],
        "packaging": [],
        "mixed_adversarial": [],
    }

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
            for raw_line in ings:
                line = raw_line.strip()
                if not line or line in already_labeled:
                    continue
                cat = categorize(line)
                if cat is None:
                    continue
                buckets[cat].append(line)

    # Deduplicate within each bucket (RecipeNLG has many near-duplicates).
    for cat, lines in buckets.items():
        seen: set[str] = set()
        deduped: list[str] = []
        for line in lines:
            if line in seen:
                continue
            seen.add(line)
            deduped.append(line)
        random.shuffle(deduped)
        buckets[cat] = deduped
    return buckets


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help=(
            "Write JSONL candidates to this path (per_category entries per "
            "bucket). Default: print buckets to stdout."
        ),
    )
    parser.add_argument(
        "--per-category",
        type=int,
        default=PER_CATEGORY,
        help="Candidates per bucket (default %(default)d)",
    )
    args = parser.parse_args()

    buckets = sample()

    if args.out is None:
        for cat in ("plural", "comma_prep", "packaging", "mixed_adversarial"):
            lines = buckets[cat]
            print(f"\n=== {cat} ({len(lines)} total, showing 25) ===")
            for line in lines[:25]:
                print(f"  {line!r}")
        return

    # Write JSONL candidates — labeler fills `expected` in an editor.
    n_written = 0
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w") as out:
        for cat in ("plural", "comma_prep", "packaging", "mixed_adversarial"):
            for line in buckets[cat][: args.per_category]:
                out.write(
                    json.dumps(
                        {"line": line, "category": cat, "expected": None},
                        ensure_ascii=False,
                    )
                    + "\n"
                )
                n_written += 1
    print(f"wrote {n_written} candidates to {args.out}", file=sys.stderr)


if __name__ == "__main__":
    main()
