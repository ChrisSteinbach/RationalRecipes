#!/usr/bin/env python3
"""Sample multilingual ingredient lines from WDC hosts for benchmark gold curation.

Targets the three hosts validated in RationalRecipes-a1k: chefkoch.de (German),
the-challenger.ru (Russian), delishkitchen.tv (Japanese).

Produces deduplicated JSONL candidates (one record per line) with host and
language tags. The labeler picks ~15 per language that exercise distinct
patterns (unit diversity, quantity-absent lines, parenthetical specs).

Usage::

    python3 scripts/benchmark_data/sample_multilingual_lines.py \\
        --out scripts/benchmark_data/multilingual_candidates.jsonl
"""

from __future__ import annotations

import argparse
import gzip
import json
import random
import sys
import zipfile
from pathlib import Path

WDC_ZIP = Path("dataset/wdc/Recipe_top100.zip")
PER_LANGUAGE = 50
RANDOM_SEED = 42
MIN_LINE_LEN = 3
MAX_LINE_LEN = 200
MAX_ROWS_PER_HOST = 500  # enough for diverse candidates without streaming the world

# (host, language) — the a1k-validated surface
HOSTS = [
    ("chefkoch.de", "de"),
    ("the-challenger.ru", "ru"),
    ("delishkitchen.tv", "ja"),
]


def _iter_host(host: str) -> list[dict]:
    """Return up to MAX_ROWS_PER_HOST raw recipe dicts from a WDC host file."""
    entry_name = f"Recipe_{host}_October2023.json.gz"
    rows: list[dict] = []
    with zipfile.ZipFile(WDC_ZIP) as zf:
        with zf.open(entry_name) as raw:
            with gzip.open(raw, "rt", encoding="utf-8") as gz:
                for line in gz:
                    rows.append(json.loads(line))
                    if len(rows) >= MAX_ROWS_PER_HOST:
                        break
    return rows


def _clean_line(raw: object) -> str | None:
    """Normalize, filter obviously broken lines. Returns None to skip."""
    if not isinstance(raw, str):
        return None
    line = raw.strip()
    if not (MIN_LINE_LEN <= len(line) <= MAX_LINE_LEN):
        return None
    # Skip lines that are pure whitespace/control or look like boilerplate
    if line.startswith(("http://", "https://")):
        return None
    return line


def sample_host(host: str, language: str, per_language: int) -> list[dict]:
    """Pick deduped candidate lines for one host/language pair."""
    rows = _iter_host(host)
    seen: set[str] = set()
    candidates: list[dict] = []
    for row in rows:
        row_id = row.get("row_id")
        ings = row.get("recipeingredient") or []
        if not isinstance(ings, list):
            continue
        for raw in ings:
            cleaned = _clean_line(raw)
            if cleaned is None or cleaned in seen:
                continue
            seen.add(cleaned)
            candidates.append(
                {
                    "language": language,
                    "host": host,
                    "row_id": row_id,
                    "line": cleaned,
                    "expected": None,
                }
            )
    random.seed(RANDOM_SEED)
    random.shuffle(candidates)
    return candidates[:per_language]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Write JSONL candidates here. Default: print a preview.",
    )
    parser.add_argument(
        "--per-language",
        type=int,
        default=PER_LANGUAGE,
        help="Candidates per language (default %(default)d)",
    )
    args = parser.parse_args()

    if not WDC_ZIP.exists():
        sys.exit(f"WDC zip missing: {WDC_ZIP}")

    all_candidates: list[dict] = []
    for host, language in HOSTS:
        picked = sample_host(host, language, args.per_language)
        all_candidates.extend(picked)
        print(
            f"{host} ({language}): {len(picked)} candidates",
            file=sys.stderr,
        )

    if args.out is None:
        for host, language in HOSTS:
            host_candidates = [c for c in all_candidates if c["host"] == host]
            print(f"\n=== {host} ({language}, {len(host_candidates)} shown: 15) ===")
            for c in host_candidates[:15]:
                print(f"  r{c['row_id']}: {c['line']!r}")
        return

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w") as out:
        for c in all_candidates:
            out.write(json.dumps(c, ensure_ascii=False) + "\n")
    print(f"wrote {len(all_candidates)} candidates to {args.out}", file=sys.stderr)


if __name__ == "__main__":
    main()
