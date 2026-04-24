#!/usr/bin/env python3
"""Run the full merged-corpus scrape pipeline: RecipeNLG + WDC → CSVs + manifest.

Thin wrapper around ``scrape.pipeline_merged.run_merged_pipeline``. Per variant,
writes one rr-stats-compatible CSV plus a shared ``manifest.json`` that the
review shell (bead ``eco``), SQLite writer (``5ub``), and L3 splitter (``7eo``)
consume.

Each surviving recipe is LLM-parsed once for ingredient-line structure and,
on the WDC side, once before merge for ingredient-name extraction — so this
is slow. Start with small title queries and tight min-group-sizes when
exercising.

Usage:
    python3 scripts/scrape_merged.py pannkak \\
        --ollama-url http://192.168.50.189:11434 \\
        --model qwen3.6:35b-a3b \\
        --l1-min=2 --l2-min=2 -v
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from rational_recipes.scrape.parse import OLLAMA_BASE_URL
from rational_recipes.scrape.pipeline_merged import run_merged_pipeline

DEFAULT_RECIPENLG = Path("dataset/full_dataset.csv")
DEFAULT_WDC_ZIP = Path("dataset/wdc/Recipe_top100.zip")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("query", help="Title substring to search for (both corpora)")
    parser.add_argument(
        "--recipenlg",
        type=Path,
        default=DEFAULT_RECIPENLG,
        help=f"Path to RecipeNLG CSV (default: {DEFAULT_RECIPENLG})",
    )
    parser.add_argument(
        "--wdc-zip",
        type=Path,
        default=DEFAULT_WDC_ZIP,
        help=f"Path to WDC top-100 zip (default: {DEFAULT_WDC_ZIP})",
    )
    parser.add_argument(
        "--wdc-host",
        action="append",
        help="Restrict WDC load to these hosts (repeatable). Default: all hosts.",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        type=Path,
        default=Path("output/merged"),
        help="Directory for per-variant CSVs + manifest.json",
    )
    parser.add_argument("--l1-min", type=int, default=3, help="L1 min group size")
    parser.add_argument(
        "--l2-threshold",
        type=float,
        default=0.6,
        help="L2 Jaccard similarity threshold",
    )
    parser.add_argument("--l2-min", type=int, default=3, help="L2 min group size")
    parser.add_argument(
        "--bucket-size",
        type=float,
        default=2.0,
        help="Within-variant proportion-bucket dedup width (g/100g)",
    )
    parser.add_argument(
        "--model",
        default="qwen3.6:35b-a3b",
        help="Ollama model for both name extraction and line parsing",
    )
    parser.add_argument(
        "--ollama-url",
        default=OLLAMA_BASE_URL,
        help=f"Ollama API base URL (default: {OLLAMA_BASE_URL})",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(message)s",
    )

    if not args.recipenlg.exists():
        print(f"RecipeNLG dataset not found: {args.recipenlg}", file=sys.stderr)
        return 1
    if not args.wdc_zip.exists():
        print(f"WDC zip not found: {args.wdc_zip}", file=sys.stderr)
        return 1

    args.output_dir.mkdir(parents=True, exist_ok=True)

    manifest, stats = run_merged_pipeline(
        recipenlg_path=args.recipenlg,
        wdc_zip_path=args.wdc_zip,
        title_query=args.query,
        output_dir=args.output_dir,
        wdc_hosts=args.wdc_host,
        l1_min_group_size=args.l1_min,
        l2_similarity_threshold=args.l2_threshold,
        l2_min_group_size=args.l2_min,
        bucket_size=args.bucket_size,
        llm_model=args.model,
        ollama_url=args.ollama_url,
    )

    print(f"Loaded rnlg={stats.recipenlg_in} wdc={stats.wdc_in}")
    print(
        f"Merged: {stats.merge_stats.merged_out} rows "
        f"(url_dups={stats.merge_stats.url_duplicates}, "
        f"near_dups={stats.merge_stats.near_dup_duplicates})"
    )
    print(
        f"Grouped: {stats.l1_groups_kept} L1 groups → "
        f"{stats.l2_variants_kept} L2 variants"
    )
    print(
        f"Parsed: {stats.rows_parsed} rows → {stats.rows_normalized} normalized "
        f"(dedup dropped {stats.rows_dedup_dropped})"
    )
    print(f"Emitted {len(manifest.variants)} variant(s) to {args.output_dir}/")
    if stats.db_misses:
        top = sorted(stats.db_misses.items(), key=lambda kv: -kv[1])[:10]
        print("Top DB misses: " + ", ".join(f"{k} ({v})" for k, v in top))
    return 0


if __name__ == "__main__":
    sys.exit(main())
