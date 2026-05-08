#!/usr/bin/env python3
"""Run the full merged-corpus scrape pipeline: RecipeNLG + WDC → recipes.db.

Thin wrapper around ``scrape.pipeline_merged.run_merged_pipeline``. Writes
each surviving variant directly into ``recipes.db`` (variants +
variant_members + variant_ingredient_stats) so ``review_variants.py`` and
``render_drop.py`` work on the output without a separate import step
(RationalRecipes-v61w). The legacy CSV+manifest emission is preserved as
a debugging affordance — pass ``--no-csv`` to skip it.

Each surviving recipe is LLM-parsed once for ingredient-line structure and,
on the WDC side, once before merge for ingredient-name extraction — so this
is slow. Start with small title queries and tight min-group-sizes when
exercising.

Usage:
    python3 scripts/scrape_merged.py pannkak \\
        --ollama-url http://192.168.50.189:11444 \\
        --model gemma4:e2b \\
        --l1-min=2 --l2-min=2 -v
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from rational_recipes.scrape.parse import DEFAULT_NUM_CTX, OLLAMA_BASE_URL
from rational_recipes.scrape.pipeline_merged import (
    DEFAULT_PARSE_CONCURRENCY,
    ProgressEvent,
    run_merged_pipeline,
)

DEFAULT_RECIPENLG = Path("dataset/full_dataset.csv")
DEFAULT_WDC_ZIP = Path("dataset/wdc/Recipe_top100.zip")

# Progress reporting cadence (1g5h / F8). Print a line after at least
# this many recipes parsed OR after this many seconds, whichever comes
# first. 50 / 10 was picked to be useful in the ehe7 timescale (a few
# minutes per drop): roughly every 10 s during the LLM phase, never
# more than 50 recipes apart even on a fast cache-warm rerun.
_PROGRESS_EVERY_N = 50
_PROGRESS_EVERY_SECONDS = 10.0


class _ProgressPrinter:
    """Throttle ``ProgressEvent`` emissions to one line per K rows / T seconds.

    The pipeline emits an event after every recipe — the printer
    decides which ones reach stdout. ``final=True`` events always
    print, in summary form, regardless of throttle state.
    """

    def __init__(
        self,
        *,
        every_n: int = _PROGRESS_EVERY_N,
        every_seconds: float = _PROGRESS_EVERY_SECONDS,
    ) -> None:
        self.every_n = every_n
        self.every_seconds = every_seconds
        self._last_count = 0
        self._last_seconds = -every_seconds

    def __call__(self, ev: ProgressEvent) -> None:
        if ev.final:
            recipes_per_hour = (
                ev.parsed_count / ev.elapsed_seconds * 3600.0
                if ev.elapsed_seconds > 0
                else 0.0
            )
            print(
                f"Final: parsed {ev.parsed_count} recipes in "
                f"{ev.elapsed_seconds:.1f}s "
                f"(cache_hits={ev.cache_hits}, ollama_lines={ev.ollama_lines}, "
                f"throughput={recipes_per_hour:.0f} recipes/hour)"
            )
            return
        n_delta = ev.parsed_count - self._last_count
        t_delta = ev.elapsed_seconds - self._last_seconds
        if n_delta >= self.every_n or t_delta >= self.every_seconds:
            print(
                f"Progress: parsed {ev.parsed_count}/{ev.total} "
                f"(cache_hits={ev.cache_hits}, ollama_lines={ev.ollama_lines}, "
                f"elapsed={ev.elapsed_seconds:.1f}s)"
            )
            self._last_count = ev.parsed_count
            self._last_seconds = ev.elapsed_seconds


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
        "--min-variant-size",
        type=int,
        default=5,
        help="Minimum recipes per variant — drop L2 clusters below this",
    )
    parser.add_argument(
        "--max-variants-per-l1",
        type=int,
        default=5,
        help="Cap variants per L1 (top-N by n_recipes); 0 disables",
    )
    parser.add_argument(
        "--bucket-size",
        type=float,
        default=2.0,
        help="Within-variant proportion-bucket dedup width (g/100g)",
    )
    parser.add_argument(
        "--model",
        default="gemma4:e2b",
        help="Ollama model for both name extraction and line parsing",
    )
    parser.add_argument(
        "--ollama-url",
        default=OLLAMA_BASE_URL,
        help=f"Ollama API base URL (default: {OLLAMA_BASE_URL})",
    )
    parser.add_argument(
        "--parse-concurrency",
        type=int,
        default=DEFAULT_PARSE_CONCURRENCY,
        help=(
            "Concurrent ingredient-line parser calls (RationalRecipes-e6rl). "
            f"Default {DEFAULT_PARSE_CONCURRENCY} matches parse-fast NUM_PARALLEL. "
            "Set to 1 for fully sequential dispatch (debugging or non-parse-fast "
            "endpoints)."
        ),
    )
    parser.add_argument(
        "--num-ctx",
        type=int,
        default=DEFAULT_NUM_CTX,
        help=(
            "Per-call Ollama num_ctx (RationalRecipes-rjqg). "
            f"Default {DEFAULT_NUM_CTX} matches the parse-fast tuning report. "
            "Without an explicit value Ollama allocates each model's NATIVE "
            "context window per slot — for a 128k-ctx model on parse-fast "
            "(NP=4) that demands ~150 GiB and OOMs the GPU."
        ),
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=Path("output/catalog/recipes.db"),
        help=(
            "SQLite catalog DB to write variants to. "
            "Created if missing. Default: output/catalog/recipes.db"
        ),
    )
    parser.add_argument(
        "--no-db",
        action="store_true",
        help="Skip writing to the catalog DB (CSV+manifest only).",
    )
    parser.add_argument(
        "--no-csv",
        action="store_true",
        help="Skip CSV+manifest output (DB only).",
    )
    parser.add_argument(
        "--clean-l1",
        action="store_true",
        help=(
            "Delete variants under each L1 key touched by this run that "
            "this run did not produce. Useful for re-running the same "
            "title query and converging on the new variant set."
        ),
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
    if args.no_db and args.no_csv:
        print(
            "--no-db and --no-csv together produce no output. Pick one.",
            file=sys.stderr,
        )
        return 2

    args.output_dir.mkdir(parents=True, exist_ok=True)

    db_path = None if args.no_db else args.db
    emit_csv = not args.no_csv

    # Verbose runs register a throttled progress printer (1g5h / F8) so
    # long Ollama phases give incremental signs of life. Default off so
    # non-verbose runs keep their tidy single-line summary.
    progress_callback = _ProgressPrinter() if args.verbose else None

    manifest, stats = run_merged_pipeline(
        recipenlg_path=args.recipenlg,
        wdc_zip_path=args.wdc_zip,
        title_query=args.query,
        output_dir=args.output_dir,
        wdc_hosts=args.wdc_host,
        l1_min_group_size=args.l1_min,
        l2_similarity_threshold=args.l2_threshold,
        l2_min_group_size=args.l2_min,
        min_variant_size=args.min_variant_size,
        max_variants_per_l1=args.max_variants_per_l1,
        bucket_size=args.bucket_size,
        llm_model=args.model,
        ollama_url=args.ollama_url,
        db_path=db_path,
        delete_stale_l1=args.clean_l1,
        emit_csv=emit_csv,
        progress_callback=progress_callback,
        parse_concurrency=args.parse_concurrency,
        num_ctx=args.num_ctx,
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
    if emit_csv:
        print(f"Emitted {len(manifest.variants)} variant(s) to {args.output_dir}/")
    if db_path is not None:
        print(f"Wrote {len(manifest.variants)} variant(s) to {db_path}")
    if stats.db_misses:
        top = sorted(stats.db_misses.items(), key=lambda kv: -kv[1])[:10]
        print("Top DB misses: " + ", ".join(f"{k} ({v})" for k, v in top))
    return 0


if __name__ == "__main__":
    sys.exit(main())
