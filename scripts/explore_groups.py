#!/usr/bin/env python3
"""Explore Level 1/2 grouping on RecipeNLG — no LLM needed, fast.

Usage:
    python3 scripts/explore_groups.py pancake
    python3 scripts/explore_groups.py pannkak
    python3 scripts/explore_groups.py "chocolate cake"
    python3 scripts/explore_groups.py pancake --l1-min=20 --l2-min=5
"""

import argparse
import sys
from pathlib import Path

from rational_recipes.scrape.grouping import group_by_ingredients, group_by_title
from rational_recipes.scrape.recipenlg import RecipeNLGLoader

DATASET = Path("dataset/full_dataset.csv")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("query", help="Title substring to search for")
    parser.add_argument(
        "--dataset", type=Path, default=DATASET, help="Path to RecipeNLG CSV"
    )
    parser.add_argument("--l1-min", type=int, default=5, help="Level 1 min group size")
    parser.add_argument(
        "--l2-threshold", type=float, default=0.5, help="Level 2 Jaccard threshold"
    )
    parser.add_argument("--l2-min", type=int, default=3, help="Level 2 min group size")
    parser.add_argument(
        "--max-groups", type=int, default=20, help="Max L1 groups to show"
    )
    args = parser.parse_args()

    if not args.dataset.exists():
        print(f"Dataset not found: {args.dataset}", file=sys.stderr)
        sys.exit(1)

    loader = RecipeNLGLoader(path=args.dataset)
    print(f"Searching for {args.query!r}...")
    recipes = list(loader.search_title(args.query))
    print(f"Found {len(recipes)} recipes\n")

    if not recipes:
        return

    groups = group_by_title(recipes, min_group_size=args.l1_min)
    print(f"Level 1 groups (min size {args.l1_min}): {len(groups)}\n")

    shown = 0
    for key, grp in sorted(groups.items(), key=lambda x: -len(x[1])):
        if shown >= args.max_groups:
            remaining = len(groups) - shown
            print(f"  ... and {remaining} more groups")
            break

        print(f"  {key} ({len(grp)} recipes)")
        l2 = group_by_ingredients(
            grp,
            similarity_threshold=args.l2_threshold,
            min_group_size=args.l2_min,
        )
        for i, cluster in enumerate(l2):
            ingredients = ", ".join(sorted(cluster.canonical_ingredients))
            print(f"    L2[{i}]: {cluster.size:3d} recipes — {ingredients}")
        if not l2:
            print("    (no L2 clusters above min size)")
        print()
        shown += 1


if __name__ == "__main__":
    main()
