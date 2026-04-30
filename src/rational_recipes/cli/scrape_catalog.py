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
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import IO

from rational_recipes.catalog_db import CatalogDB
from rational_recipes.corpus_title_survey import (
    LANGUAGE_FILTER_EN_SV,
    LANGUAGE_FILTERS,
)
from rational_recipes.scrape.catalog_pipeline import (
    DEFAULT_PARSE_SEED,
    ExtractFn,
    HeartbeatFn,
    HeartbeatSnapshot,
    ParseFn,
    compute_corpus_revisions,
    run_catalog_pipeline,
)
from rational_recipes.scrape.merge import (
    DEFAULT_BUCKET_SIZE,
    DEFAULT_NEAR_DUP_THRESHOLD,
)
from rational_recipes.scrape.parse import OLLAMA_BASE_URL, parse_ingredient_lines
from rational_recipes.scrape.pass3_titles import (
    Pass3CallTiming,
    TitleFn,
    build_default_title_fn,
    format_pass3_summary,
)
from rational_recipes.scrape.recipenlg import RecipeNLGLoader
from rational_recipes.scrape.wdc import WDCLoader, WDCRecipe, extract_batch

DEFAULT_RECIPENLG = Path("dataset/full_dataset.csv")
DEFAULT_WDC_ZIP = Path("dataset/wdc/Recipe_top100.zip")
DEFAULT_OUTPUT_DB = Path("output/catalog/recipes.db")
DEFAULT_CACHE = Path("output/catalog/extraction_cache/wdc_names.json")
DEFAULT_HEARTBEAT_SECONDS = 30.0


def _format_duration(seconds: float) -> str:
    if seconds < 0 or seconds != seconds:  # NaN-safe
        return "-"
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h{m:02d}m{s:02d}s"
    if m:
        return f"{m}m{s:02d}s"
    return f"{s}s"


class _HeartbeatPrinter:
    """Throttled printer of ``HeartbeatSnapshot`` to a stream.

    Emits one snapshot, then drops further snapshots until ``interval``
    seconds have passed. Thread-safe — Pass 1's parallel mode emits from
    the main thread anyway, but lock cheaply guards the timestamp so a
    future change can't accidentally double-emit.
    """

    def __init__(
        self,
        *,
        interval_seconds: float = DEFAULT_HEARTBEAT_SECONDS,
        stream: IO[str] | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._interval = interval_seconds
        self._stream = stream if stream is not None else sys.stdout
        self._clock = clock
        self._last = -float("inf")
        self._lock = threading.Lock()

    def __call__(self, snap: HeartbeatSnapshot) -> None:
        now = self._clock()
        with self._lock:
            if (now - self._last) < self._interval:
                return
            self._last = now
        self._emit(snap)

    def _emit(self, snap: HeartbeatSnapshot) -> None:
        elapsed = _format_duration(snap.elapsed_seconds)
        if snap.position > 0 and snap.total > 0 and snap.elapsed_seconds > 0:
            rate = snap.position / snap.elapsed_seconds
            remaining = max(snap.total - snap.position, 0)
            eta = _format_duration(remaining / rate) if rate > 0 else "-"
            pct = f"{snap.position / snap.total * 100:5.1f}%"
        else:
            eta = "-"
            pct = "  -.-%"
        counters = " ".join(f"{k}={v}" for k, v in snap.counters.items())
        line = (
            f"[hb {snap.pass_name} {snap.position}/{snap.total} {pct}] "
            f"elapsed={elapsed} eta={eta} {counters}"
        )
        print(line, file=self._stream, flush=True)


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
        "--title-exact",
        default=None,
        help="Restrict to exactly one L1 key (takes precedence over --title-filter).",
    )
    parser.add_argument(
        "--near-dup-threshold",
        type=float,
        default=DEFAULT_NEAR_DUP_THRESHOLD,
        help=(
            "Jaccard threshold for cross-corpus near-dup collapse "
            "(default %(default)s; raise for WDC-rich groups where basic "
            "ingredient overlap falsely flags unique recipes)."
        ),
    )
    parser.add_argument(
        "--language-filter",
        choices=LANGUAGE_FILTERS,
        default=LANGUAGE_FILTER_EN_SV,
    )
    parser.add_argument("--model", default="gemma4:e2b")
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_PARSE_SEED,
        help=(
            "LLM seed recorded in parsed_ingredient_lines.seed. Mismatch "
            "with prior runs invalidates the cache for that recipe."
        ),
    )
    parser.add_argument("--ollama-url", default=OLLAMA_BASE_URL)
    parser.add_argument(
        "--skip-preflight",
        action="store_true",
        help="Skip the Ollama reachability check (test-only)",
    )
    pass_group = parser.add_mutually_exclusive_group()
    pass_group.add_argument(
        "--pass1-only",
        action="store_true",
        help=(
            "vwt.16: parse + persist ingredient lines into the cache "
            "table; skip clustering + variant write. Use to warm the "
            "cache before a series of threshold-sweep Pass 2 runs."
        ),
    )
    pass_group.add_argument(
        "--pass2-only",
        action="store_true",
        help=(
            "vwt.16: cluster + write variants from the existing cache "
            "table; skip parsing. Re-runnable in seconds for "
            "threshold sweeps. Will produce empty variants for any "
            "L1 group whose recipes weren't covered by an earlier Pass 1."
        ),
    )
    pass_group.add_argument(
        "--pass3-only",
        action="store_true",
        help=(
            "vwt.24: regenerate distinctive display_titles for every "
            "variant in the existing DB; skip Pass 1 + Pass 2."
        ),
    )
    parser.add_argument(
        "--skip-pass3",
        action="store_true",
        help=(
            "vwt.24: skip the Pass 3 title-generation step (default: "
            "run after Pass 2)."
        ),
    )
    parser.add_argument(
        "--pass3-force",
        action="store_true",
        help=(
            "vwt.24: re-title every variant even if display_title is "
            "already distinct from normalized_title (default: skip "
            "already-titled variants)."
        ),
    )
    parser.add_argument(
        "--pass3-workers",
        type=int,
        default=4,
        metavar="N",
        help=(
            "Thread-pool size for Pass 3 title generation. Default: 4."
        ),
    )
    parser.add_argument(
        "--pass1-workers",
        type=int,
        default=4,
        metavar="N",
        help=(
            "Thread-pool size for Pass 1 recipe parsing. Each worker "
            "sends one Ollama request at a time; tune alongside "
            "OLLAMA_NUM_PARALLEL on the server. Default: 4."
        ),
    )
    parser.add_argument(
        "--heartbeat-seconds",
        type=float,
        default=DEFAULT_HEARTBEAT_SECONDS,
        metavar="SECS",
        help=(
            "Throttle interval for the always-on progress heartbeat printed "
            "to stdout (default %(default)ss). Set to 0 to print every "
            "snapshot; set negative to silence."
        ),
    )
    parser.add_argument(
        "--num-ctx",
        type=int,
        default=16384,
        metavar="N",
        help=(
            "Ollama num_ctx for LLM calls. Set to 0 to omit (use model "
            "default). Default: %(default)s."
        ),
    )
    parser.add_argument(
        "--max-siblings",
        type=int,
        default=20,
        metavar="N",
        help=(
            "Max sibling ingredient sets included in each Pass 3 LLM "
            "prompt. Larger values give the model more context for "
            "choosing distinctive titles but increase prompt size. "
            "Post-LLM dedup handles collisions regardless. "
            "Default: %(default)s."
        ),
    )
    parser.add_argument(
        "--pass3-profile",
        type=Path,
        default=None,
        metavar="PATH",
        help=(
            "vwt.29: dump per-call Pass 3 timing records to this JSONL "
            "path. Always-on summary lines are printed regardless; this "
            "flag is for offline analysis (sibling-count buckets, "
            "per-call histograms)."
        ),
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
    title_fn: TitleFn | None = None,
) -> int:
    """CLI entrypoint. ``parse_fn``/``extract_fn``/``title_fn`` let tests
    bypass Ollama."""
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

    if args.pass3_only:
        do_pass1 = False
        do_pass2 = False
        do_pass3 = True
    else:
        do_pass1 = not args.pass2_only
        do_pass2 = not args.pass1_only
        do_pass3 = not (args.pass1_only or args.pass2_only or args.skip_pass3)

    # Ollama is only needed when Pass 1 or Pass 3 will run with the live LLM.
    needs_live_llm = (do_pass1 and parse_fn is None and extract_fn is None) or (
        do_pass3 and title_fn is None
    )
    if needs_live_llm and not args.skip_preflight:
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
            return list(
                extract_batch(
                    recipes,
                    model=args.model,
                    base_url=args.ollama_url,
                    cache=cache,
                )
            )

        extract_fn = default_extract

    if parse_fn is None:

        def default_parse(lines: list[str]):  # type: ignore[no-untyped-def]
            return parse_ingredient_lines(
                lines, model=args.model, base_url=args.ollama_url
            )

        parse_fn = default_parse

    # vwt.29: collect Pass 3 timings whenever the live LLM path is in use
    # (only the production batch_title_fn captures them — stub fns from
    # tests don't). Lock-protected because run_pass3 dispatches LLM calls
    # on a ThreadPoolExecutor.
    pass3_timings: list[Pass3CallTiming] = []
    pass3_lock = threading.Lock()

    def collect_pass3_timing(rec: Pass3CallTiming) -> None:
        with pass3_lock:
            pass3_timings.append(rec)

    num_ctx: int | None = args.num_ctx if args.num_ctx > 0 else None

    if title_fn is None:
        title_fn = build_default_title_fn(
            args.model,
            base_url=args.ollama_url,
            num_ctx=num_ctx,
            timing_collector=collect_pass3_timing,
        )

    heartbeat: HeartbeatFn
    if args.heartbeat_seconds < 0:
        heartbeat = lambda _: None  # noqa: E731 - inline noop is clearer here
    else:
        heartbeat = _HeartbeatPrinter(interval_seconds=args.heartbeat_seconds)

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
            near_dup_threshold=args.near_dup_threshold,
            title_filter=args.title_filter,
            title_exact=args.title_exact,
            model=args.model,
            seed=args.seed,
            do_pass1=do_pass1,
            do_pass2=do_pass2,
            do_pass3=do_pass3,
            pass1_workers=args.pass1_workers,
            pass3_workers=args.pass3_workers,
            pass3_force=args.pass3_force,
            title_fn=title_fn,
            max_siblings=args.max_siblings,
            heartbeat=heartbeat,
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
    print(
        f"pass 1: recipes_seen={stats.pass1_recipes_seen} "
        f"recipes_skipped={stats.pass1_recipes_skipped} "
        f"lines_parsed={stats.pass1_lines_parsed} "
        f"line_cache_hits={stats.pass1_lines_cache_hits} "
        f"llm_batches={stats.pass1_llm_batches}"
    )
    print(
        f"pass 3: variants_total={stats.pass3.variants_total} "
        f"singletons={stats.pass3.variants_singleton} "
        f"titled={stats.pass3.variants_titled} "
        f"skipped={stats.pass3.variants_skipped} "
        f"deduped={stats.pass3.variants_deduped} "
        f"llm_calls={stats.pass3.llm_calls} "
        f"llm_failures={stats.pass3.llm_failures}"
    )
    # vwt.29: summary lines from Pass 3 instrumentation, plus optional
    # JSONL dump for offline histogram / scatter-plot analysis.
    stats.pass3.timings = pass3_timings
    for line in format_pass3_summary(stats.pass3):
        print(line)
    if args.pass3_profile is not None and pass3_timings:
        args.pass3_profile.parent.mkdir(parents=True, exist_ok=True)
        with args.pass3_profile.open("w", encoding="utf-8") as f:
            for rec in pass3_timings:
                f.write(json.dumps(rec.to_dict(), ensure_ascii=False) + "\n")
        print(
            f"wrote {len(pass3_timings)} Pass 3 timing records to "
            f"{args.pass3_profile}"
        )
    print(f"wallclock: {stats.wallclock_seconds:.1f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
