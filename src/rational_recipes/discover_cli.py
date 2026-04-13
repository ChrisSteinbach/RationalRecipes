"""CLI entry point for dish discovery on RecipeNLG."""

from __future__ import annotations

import argparse
import csv
import io
import json
import sys
from pathlib import Path

from rational_recipes.discover import DiscoveryResult, discover
from rational_recipes.scrape.recipenlg import RecipeNLGLoader


def _format_text(results: list[DiscoveryResult]) -> str:
    if not results:
        return ""
    width = len(str(results[0].count))
    return "\n".join(f"  {r.count:>{width}}  {r.normalized_title}" for r in results)


def _format_csv(results: list[DiscoveryResult]) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["count", "normalized_title"])
    for r in results:
        writer.writerow([r.count, r.normalized_title])
    return buf.getvalue().rstrip("\n")


def _format_json(results: list[DiscoveryResult]) -> str:
    return json.dumps(
        [{"count": r.count, "normalized_title": r.normalized_title} for r in results],
        indent=2,
    )


FORMATTERS = {
    "text": _format_text,
    "csv": _format_csv,
    "json": _format_json,
}


def run() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Discover common dish names in a RecipeNLG-format corpus by "
            "streaming titles and counting normalized forms."
        )
    )
    parser.add_argument(
        "--corpus",
        type=Path,
        default=Path("dataset/full_dataset.csv"),
        help="Path to the RecipeNLG CSV (default: %(default)s)",
    )
    parser.add_argument(
        "--min",
        type=int,
        default=20,
        dest="min_count",
        help="Minimum count for a title to be included (default: %(default)s)",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=100,
        dest="top_k",
        help="Number of top results to show (default: %(default)s)",
    )
    parser.add_argument(
        "--format",
        choices=tuple(FORMATTERS),
        default="text",
        help="Output format (default: %(default)s)",
    )
    args = parser.parse_args()

    if not args.corpus.exists():
        print(f"Corpus not found: {args.corpus}", file=sys.stderr)
        sys.exit(1)

    loader = RecipeNLGLoader(path=args.corpus)
    results = discover(
        (r.title for r in loader.iter_recipes()),
        min_count=args.min_count,
        top_k=args.top_k,
    )

    print(FORMATTERS[args.format](results))


if __name__ == "__main__":
    run()
