#!/usr/bin/env python3
"""Emit all ingredient lines from the 20 canonical ica.se recipes as candidates
for per-field Swedish gold labeling.

The row_id list is frozen to match the v1 name-set gold (b7t.25) so both
v1 and v2 evaluate the same recipes. Each line becomes one JSONL record with
``row_id``, ``line``, and an ``expected: null`` placeholder for the labeler to
fill in.

Usage::

    python3 scripts/benchmark_data/sample_swedish_structured.py \\
        --out scripts/benchmark_data/swedish_ica_se_candidates.jsonl
"""

from __future__ import annotations

import argparse
import gzip
import json
import sys
from pathlib import Path

ICA_GZ = Path("dataset/wdc/Recipe_ica.se_October2023.json.gz")

# Frozen to match the 20 rows chosen in the a1k spike and used by the v1
# Swedish gold (swedish_ica_se_names_gold.json).
CANONICAL_ROWS = [
    0,
    13,
    111,
    136,
    267,
    272,
    280,
    337,
    399,
    517,
    528,
    607,
    761,
    820,
    1157,
    1270,
    1489,
    1700,
    2019,
    2172,
]


def emit() -> list[dict]:
    """Yield one candidate per ingredient line across all canonical recipes."""
    if not ICA_GZ.exists():
        sys.exit(f"ica.se WDC file missing: {ICA_GZ}")
    wanted = set(CANONICAL_ROWS)
    candidates: list[dict] = []
    with gzip.open(ICA_GZ, "rt", encoding="utf-8") as f:
        for raw in f:
            rec = json.loads(raw)
            rid = rec.get("row_id")
            if rid not in wanted:
                continue
            name = rec.get("name", "")
            for line in rec.get("recipeingredient", []) or []:
                if not isinstance(line, str):
                    continue
                stripped = line.strip()
                if not stripped:
                    continue
                # category vocabulary the labeler may choose from:
                #   volume | weight | spoon | count | package
                candidates.append(
                    {
                        "row_id": rid,
                        "recipe_name": name,
                        "line": stripped,
                        "category": "",
                        "expected": None,
                    }
                )
    missing = wanted - {c["row_id"] for c in candidates}
    if missing:
        sys.exit(f"canonical rows missing from corpus: {sorted(missing)}")
    return candidates


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Write JSONL candidates here. Default: print a summary.",
    )
    args = parser.parse_args()

    candidates = emit()

    if args.out is None:
        by_row: dict[int, list[dict]] = {}
        for c in candidates:
            by_row.setdefault(c["row_id"], []).append(c)
        print(f"{len(candidates)} total lines across {len(by_row)} recipes\n")
        for rid in sorted(by_row):
            cs = by_row[rid]
            name = cs[0]["recipe_name"]
            print(f"=== r{rid}: {name} ({len(cs)} lines) ===")
            for c in cs:
                print(f"  {c['line']!r}")
            print()
        return

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w") as out:
        for c in candidates:
            out.write(json.dumps(c, ensure_ascii=False) + "\n")
    print(f"wrote {len(candidates)} candidates to {args.out}", file=sys.stderr)


if __name__ == "__main__":
    main()
