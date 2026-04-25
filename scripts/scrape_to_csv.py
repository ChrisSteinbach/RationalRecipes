#!/usr/bin/env python3
"""Run the full scrape pipeline: search → group → LLM parse → CSV.

Produces CSV files compatible with rr-stats. LLM parsing via Ollama
(~10s per ingredient line), so start small.

Usage:
    python3 scripts/scrape_to_csv.py pannkak
    python3 scripts/scrape_to_csv.py pannkak --l1-min=1 --l2-min=1
    python3 scripts/scrape_to_csv.py "swedish pancake" -o output/

    # Then run rr-stats on the output:
    rr-stats output/swedish_pancakes_l2_0.csv
"""

import argparse
import logging
import re
import sys
from pathlib import Path

from rational_recipes.scrape.pipeline import run_pipeline

DATASET = Path("dataset/full_dataset.csv")


def _slugify(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("query", help="Title substring to search for")
    parser.add_argument(
        "--dataset", type=Path, default=DATASET, help="Path to RecipeNLG CSV"
    )
    parser.add_argument("-o", "--output-dir", type=Path, default=Path("output"))
    parser.add_argument("--l1-min", type=int, default=3, help="Level 1 min group size")
    parser.add_argument(
        "--l2-threshold", type=float, default=0.6, help="Level 2 Jaccard threshold"
    )
    parser.add_argument("--l2-min", type=int, default=3, help="Level 2 min group size")
    parser.add_argument(
        "--model", default="gemma4:e2b", help="Ollama model for parsing"
    )
    parser.add_argument(
        "--ollama-url",
        default="http://localhost:11434",
        help="Ollama API base URL (default: http://localhost:11434)",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(message)s",
    )

    if not args.dataset.exists():
        print(f"Dataset not found: {args.dataset}", file=sys.stderr)
        sys.exit(1)

    args.output_dir.mkdir(parents=True, exist_ok=True)

    results = run_pipeline(
        args.dataset,
        args.query,
        l1_min_group_size=args.l1_min,
        l2_similarity_threshold=args.l2_threshold,
        l2_min_group_size=args.l2_min,
        llm_model=args.model,
        ollama_url=args.ollama_url,
    )

    if not results:
        print("No results found.")
        return

    for i, result in enumerate(results):
        slug = _slugify(result.group_title)
        filename = f"{slug}_l2_{i}.csv"
        path = args.output_dir / filename
        result.write_csv(path)

        print(f"\n--- {result.group_title} (L2 group {i}) ---")
        print(f"  Recipes: {result.total_recipes_in}")
        print(f"  Rows output: {len(result.normalized_rows)}")
        print(f"  Header: {', '.join(result.header_ingredients)}")
        print(f"  Parse success: {result.parse_success_rate:.0%}")
        if result.ingredient_db_misses:
            misses = ", ".join(
                f"{k} ({v})"
                for k, v in sorted(
                    result.ingredient_db_misses.items(), key=lambda x: -x[1]
                )[:5]
            )
            print(f"  DB misses: {misses}")
        print(f"  Written to: {path}")

    example = _slugify(results[0].group_title)
    print("\nDone. Run rr-stats on any output file, e.g.:")
    print(f"  rr-stats {args.output_dir}/{example}_l2_0.csv")


if __name__ == "__main__":
    main()
