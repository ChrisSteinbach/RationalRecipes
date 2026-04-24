#!/usr/bin/env python3
"""Whole-corpus extraction pipeline → recipes.db (bead vwt.2).

Streams both corpora, groups by normalized title, thresholds each L1
group, LLM-parses survivors, and writes variants directly into the
SQLite catalog DB. Resumable at the L1-group boundary via the
``query_runs`` table — kill mid-run, re-invoke, and completed groups are
skipped.

Usage:
    python3 scripts/scrape_catalog.py --ollama-url http://host:11434
    python3 scripts/scrape_catalog.py --title-filter pannkak  # dev slice
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import urllib.error
import urllib.request
from collections.abc import Sequence
from pathlib import Path

from rational_recipes.catalog_db import CatalogDB
from rational_recipes.corpus_title_survey import (
    LANGUAGE_FILTER_EN_SV,
    LANGUAGE_FILTERS,
)
from rational_recipes.scrape.catalog_pipeline import (
    ExtractFn,
    ParseFn,
    compute_corpus_revisions,
    run_catalog_pipeline,
)
from rational_recipes.scrape.merge import DEFAULT_BUCKET_SIZE
from rational_recipes.scrape.parse import OLLAMA_BASE_URL, parse_ingredient_lines
from rational_recipes.scrape.recipenlg import RecipeNLGLoader
from rational_recipes.scrape.wdc import WDCLoader, WDCRecipe, extract_batch

DEFAULT_RECIPENLG = Path("dataset/full_dataset.csv")
DEFAULT_WDC_ZIP = Path("dataset/wdc/Recipe_top100.zip")
DEFAULT_OUTPUT_DB = Path("output/catalog/recipes.db")
DEFAULT_CACHE = Path("output/catalog/extraction_cache/wdc_names.json")


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--recipenlg", type=Path, default=DEFAULT_RECIPENLG)
    parser.add_argument("--wdc-zip", type=Path, default=DEFAULT_WDC_ZIP)
    parser.add_argument(
        "--wdc-host",
        action="append",
        help="Restrict WDC load to these hosts (repeatable). Default: all hosts.",
    )
    parser.add_argument("--output-db", type=Path, default=DEFAULT_OUTPUT_DB)
    parser.add_argument(
        "--cache-path",
        type=Path,
        default=DEFAULT_CACHE,
        help="WDC ingredient-name extraction cache (JSON, page_url → names)",
    )
    parser.add_argument("--l1-min", type=int, default=5)
    parser.add_argument("--l2-threshold", type=float, default=0.6)
    parser.add_argument("--l2-min", type=int, default=3)
    parser.add_argument("--l3-min", type=int, default=3)
    parser.add_argument("--bucket-size", type=float, default=DEFAULT_BUCKET_SIZE)
    parser.add_argument(
        "--title-filter",
        default=None,
        help="Restrict processed L1 keys to those containing this substring.",
    )
    parser.add_argument(
        "--language-filter",
        choices=LANGUAGE_FILTERS,
        default=LANGUAGE_FILTER_EN_SV,
    )
    parser.add_argument("--model", default="qwen3.6:35b-a3b")
    parser.add_argument("--ollama-url", default=OLLAMA_BASE_URL)
    parser.add_argument(
        "--skip-preflight",
        action="store_true",
        help="Skip the Ollama reachability check (test-only)",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    return parser.parse_args(argv)


def _preflight_ollama(base_url: str, timeout: float = 5.0) -> bool:
    """Probe ``/api/tags`` — if Ollama is up, the endpoint responds with JSON."""
    url = base_url.rstrip("/") + "/api/tags"
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return bool(resp.status == 200)
    except (urllib.error.URLError, TimeoutError, OSError):
        return False


def _load_cache(path: Path) -> dict[str, frozenset[str]]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return {str(k): frozenset(v) for k, v in data.items() if isinstance(v, list)}


def _save_cache(path: Path, cache: dict[str, frozenset[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {k: sorted(v) for k, v in sorted(cache.items())}
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def run(
    argv: Sequence[str] | None = None,
    *,
    parse_fn: ParseFn | None = None,
    extract_fn: ExtractFn | None = None,
) -> int:
    """CLI entrypoint. ``parse_fn``/``extract_fn`` let tests bypass Ollama."""
    args = _parse_args(argv)
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(message)s",
    )

    if not args.recipenlg.exists():
        print(f"RecipeNLG corpus not found: {args.recipenlg}", file=sys.stderr)
        return 1
    if not args.wdc_zip.exists():
        print(f"WDC zip not found: {args.wdc_zip}", file=sys.stderr)
        return 1

    if parse_fn is None and extract_fn is None and not args.skip_preflight:
        if not _preflight_ollama(args.ollama_url):
            print(
                f"Ollama unreachable at {args.ollama_url}; "
                "start it or pass --ollama-url to a live host.",
                file=sys.stderr,
            )
            return 1

    cache: dict[str, frozenset[str]] = _load_cache(args.cache_path)

    if extract_fn is None:

        def default_extract(recipes: Sequence[WDCRecipe]) -> list[WDCRecipe]:
            return extract_batch(
                recipes,
                model=args.model,
                base_url=args.ollama_url,
                cache=cache,
            )

        extract_fn = default_extract

    if parse_fn is None:

        def default_parse(lines: list[str]):  # type: ignore[no-untyped-def]
            return parse_ingredient_lines(
                lines, model=args.model, base_url=args.ollama_url
            )

        parse_fn = default_parse

    args.output_db.parent.mkdir(parents=True, exist_ok=True)
    db = CatalogDB.open(args.output_db)
    try:
        revisions = compute_corpus_revisions(args.recipenlg, args.wdc_zip)
        stats = run_catalog_pipeline(
            db=db,
            rnlg_loader=RecipeNLGLoader(path=args.recipenlg),
            wdc_loader=WDCLoader(zip_path=args.wdc_zip),
            parse_fn=parse_fn,
            extract_fn=extract_fn,
            corpus_revisions=revisions,
            wdc_hosts=args.wdc_host,
            language_filter=args.language_filter,
            l1_min=args.l1_min,
            l2_threshold=args.l2_threshold,
            l2_min=args.l2_min,
            l3_min=args.l3_min,
            bucket_size=args.bucket_size,
            title_filter=args.title_filter,
            # Persist the cache between groups so a killed run doesn't
            # lose already-extracted names.
            on_group_done=lambda _k, _v: _save_cache(args.cache_path, cache),
        )
    finally:
        _save_cache(args.cache_path, cache)
        db.close()

    print(
        f"L1 groups: total={stats.l1_groups_total} "
        f"processed={stats.l1_groups_processed} "
        f"skipped={stats.l1_groups_skipped} "
        f"dry={stats.l1_groups_dry}"
    )
    print(
        f"variants produced: {stats.variants_produced}  "
        f"LLM calls: parse={stats.llm_parse_calls} extract={stats.llm_extract_calls}"
    )
    print(f"wallclock: {stats.wallclock_seconds:.1f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
