"""Pass 3 profiler — drives ``run_pass3`` against an existing recipes.db
and dumps per-call timing data for offline analysis (vwt.29).

Why a separate CLI? ``scripts/scrape_catalog.py --pass3-only`` works,
but it streams both corpora into L1 groups before any pass runs, which
adds minutes to startup. Pass 3 reads only the DB; this CLI skips the
corpus stream.

Usage:
    python3 scripts/profile_pass3.py --pass3-workers 1 \\
        --output output/catalog/pass3_profile_serial.jsonl
    python3 scripts/profile_pass3.py --pass3-workers 4 \\
        --output output/catalog/pass3_profile_4workers.jsonl

Each output row is one ``Pass3CallTiming`` dict (see
``pass3_titles.py``). The summary block is also printed to stdout for
quick inspection.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Sequence
from pathlib import Path

from rational_recipes.catalog_db import CatalogDB
from rational_recipes.scrape.parse import OLLAMA_BASE_URL
from rational_recipes.scrape.pass3_titles import (
    Pass3CallTiming,
    Pass3Stats,
    build_default_title_fn,
    format_pass3_summary,
    run_pass3,
    summarize_pass3_timings,
)

DEFAULT_DB = Path("output/catalog/recipes.db")
DEFAULT_OUTPUT = Path("output/catalog/pass3_profile.jsonl")


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB,
        help="Path to recipes.db (default %(default)s).",
    )
    parser.add_argument(
        "--model",
        default="gemma4:e2b",
        help="Ollama model (default %(default)s).",
    )
    parser.add_argument(
        "--ollama-url",
        default=OLLAMA_BASE_URL,
        help="Ollama base URL (default %(default)s).",
    )
    parser.add_argument(
        "--pass3-workers",
        type=int,
        default=1,
        help="ThreadPool size for the run (default %(default)s).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        default=True,
        help="Pass force=True so every multi-variant row triggers an LLM "
        "call (default; otherwise already-titled rows are skipped and "
        "you measure nothing).",
    )
    parser.add_argument(
        "--no-force",
        dest="force",
        action="store_false",
        help="Don't force; skip already-titled rows (rarely useful — "
        "you'll only measure the first run).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="JSONL dump path for per-call timing records (default %(default)s).",
    )
    parser.add_argument(
        "--skip-preflight",
        action="store_true",
        help="Skip the Ollama reachability probe (test-only).",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    return parser.parse_args(argv)


def _preflight_ollama(base_url: str, timeout: float = 5.0) -> bool:
    """Probe ``/api/tags`` — same shape as scrape_catalog's check."""
    url = base_url.rstrip("/") + "/api/tags"
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return bool(resp.status == 200)
    except (urllib.error.URLError, TimeoutError, OSError):
        return False


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(message)s",
    )

    if not args.db.exists():
        print(f"recipes.db not found: {args.db}", file=sys.stderr)
        return 1

    if not args.skip_preflight and not _preflight_ollama(args.ollama_url):
        print(
            f"Ollama unreachable at {args.ollama_url}; "
            "start it or pass --ollama-url to a live host.",
            file=sys.stderr,
        )
        return 1

    args.output.parent.mkdir(parents=True, exist_ok=True)

    # Thread-safe collector. LLM calls run unlocked under the
    # ThreadPool, so the append must be guarded.
    collected: list[Pass3CallTiming] = []
    lock = threading.Lock()

    def collect(rec: Pass3CallTiming) -> None:
        with lock:
            collected.append(rec)

    title_fn = build_default_title_fn(
        args.model,
        base_url=args.ollama_url,
        timing_collector=collect,
    )

    def _progress(position: int, total: int) -> None:
        pct = position / total * 100 if total else 0
        print(
            f"  pass3: {position}/{total} groups ({pct:.1f}%) "
            f"titled={stats.variants_titled} llm_calls={stats.llm_calls}",
            flush=True,
        )

    db = CatalogDB.open(args.db)
    stats = Pass3Stats()
    try:
        wall_start = time.monotonic()
        run_pass3(
            db=db,
            title_fn=title_fn,
            max_workers=args.pass3_workers,
            force=args.force,
            stats=stats,
            on_group_done=_progress,
        )
        wall_seconds = time.monotonic() - wall_start
    finally:
        db.close()

    # Stash the collected timings on the stats so format_pass3_summary
    # can reuse the same code path the main pipeline uses.
    stats.timings = collected

    print(
        f"pass 3 wallclock: {wall_seconds:.1f}s "
        f"workers={args.pass3_workers} "
        f"variants_total={stats.variants_total} "
        f"singletons={stats.variants_singleton} "
        f"titled={stats.variants_titled} "
        f"skipped={stats.variants_skipped} "
        f"llm_calls={stats.llm_calls} "
        f"llm_failures={stats.llm_failures}"
    )
    for line in format_pass3_summary(stats):
        print(line)

    # Write JSONL dump.
    with args.output.open("w", encoding="utf-8") as f:
        for rec in collected:
            f.write(json.dumps(rec.to_dict(), ensure_ascii=False) + "\n")
    print(f"wrote {len(collected)} timing records to {args.output}")

    # Also dump the aggregate summary as a sibling JSON for tooling.
    summary_path = args.output.with_suffix(args.output.suffix + ".summary.json")
    summary = summarize_pass3_timings(collected)
    summary["wallclock_seconds"] = wall_seconds
    summary["workers"] = args.pass3_workers
    summary["variants_total"] = stats.variants_total
    summary["variants_titled"] = stats.variants_titled
    summary["variants_singleton"] = stats.variants_singleton
    summary["variants_skipped"] = stats.variants_skipped
    summary["llm_failures"] = stats.llm_failures
    summary_path.write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    print(f"wrote summary to {summary_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
