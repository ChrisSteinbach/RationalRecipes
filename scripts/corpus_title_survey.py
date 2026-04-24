#!/usr/bin/env python3
"""Cross-corpus normalized-title frequency survey (vwt.1 diagnostic).

Streams RecipeNLG + WDC top-100, counts ``normalize_title(title)`` per
corpus, merges, and writes a ranked JSON to ``artifacts/corpus_title_survey.json``
(or a maintainer-chosen path). Used to inform ``--l1-min`` for the
whole-corpus extraction pipeline (vwt.2).

Usage:
    python3 scripts/corpus_title_survey.py
    python3 scripts/corpus_title_survey.py --min-combined 10
    python3 scripts/corpus_title_survey.py --language-filter en+sv
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from rational_recipes.corpus_title_survey import (
    LANGUAGE_FILTER_ALL,
    LANGUAGE_FILTERS,
    build_survey,
)

DEFAULT_RECIPENLG = Path("dataset/full_dataset.csv")
DEFAULT_WDC_ZIP = Path("dataset/wdc/Recipe_top100.zip")
DEFAULT_OUTPUT = Path("artifacts/corpus_title_survey.json")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--recipenlg",
        type=Path,
        default=DEFAULT_RECIPENLG,
        help="Path to the RecipeNLG CSV (default: %(default)s)",
    )
    parser.add_argument(
        "--wdc-zip",
        type=Path,
        default=DEFAULT_WDC_ZIP,
        help="Path to the WDC top-100 zip (default: %(default)s)",
    )
    parser.add_argument(
        "--hosts",
        nargs="+",
        default=None,
        help="Restrict the WDC scan to these hostnames (default: all hosts)",
    )
    parser.add_argument(
        "--min-combined",
        type=int,
        default=None,
        help="Drop titles whose combined count is below this (default: no filter)",
    )
    parser.add_argument(
        "--language-filter",
        choices=LANGUAGE_FILTERS,
        default=LANGUAGE_FILTER_ALL,
        help="Title acceptance heuristic (default: %(default)s)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Where to write the survey JSON (default: %(default)s)",
    )
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    if not args.recipenlg.exists():
        print(f"RecipeNLG corpus not found: {args.recipenlg}", file=sys.stderr)
        return 1
    if not args.wdc_zip.exists():
        print(f"WDC zip not found: {args.wdc_zip}", file=sys.stderr)
        return 1

    survey = build_survey(
        recipenlg_path=args.recipenlg,
        wdc_zip_path=args.wdc_zip,
        language_filter=args.language_filter,
        min_combined=args.min_combined,
        hosts=args.hosts,
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    # ensure_ascii=False keeps Swedish/UTF-8 titles readable; sort_keys + the
    # deterministic in-memory ordering give byte-identical reruns.
    payload = json.dumps(survey, ensure_ascii=False, indent=2, sort_keys=True)
    args.output.write_text(payload + "\n", encoding="utf-8")

    print(
        f"Wrote {args.output} — {len(survey['titles'])} titles, "
        f"recipenlg_rows={survey['corpus_revisions']['recipenlg_rows']}, "
        f"wdc_rows={survey['corpus_revisions']['wdc_rows']}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
