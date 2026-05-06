#!/usr/bin/env python3
"""One-shot importer: reconstruct merged-pipeline variants into recipes.db.

Built for ``RationalRecipes-ehe7``: the chocolate-chip-cookies hand-cycle
ran ``scrape_merged.py`` before ``scrape_merged.py`` wrote to recipes.db
(per ``RationalRecipes-v61w``), so its 111-min extraction sits in
``output/merged/ehe7-ccc/`` as CSVs+manifest. This script reverses that
without re-running the LLM: it parses each CSV's display-string cells
back to (quantity, unit), normalizes via the same Factory machinery
``pipeline_merged.normalize_merged_row`` uses, and writes
variants + variant_members + variant_ingredient_stats to recipes.db
through the same ``upsert_variant`` path the live pipeline uses.

Usage:
    python3 scripts/import_merged_artifacts.py output/merged/ehe7-ccc/

By default writes to ``output/catalog/recipes.db``; pass ``--db PATH``
to override.

Limitations: see ``rational_recipes.scrape.reconstruct``.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from rational_recipes.catalog_db import CatalogDB, emit_variants_to_db
from rational_recipes.scrape.reconstruct import reconstruct_variants


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "directory",
        type=Path,
        help="Directory containing manifest.json + per-variant CSVs",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=Path("output/catalog/recipes.db"),
        help="SQLite catalog DB to write into (default: %(default)s)",
    )
    parser.add_argument(
        "--corpus",
        default="recipenlg",
        choices=("recipenlg", "wdc", "curated"),
        help=(
            "Default corpus tag for imported recipes. The schema enforces "
            "this enum. RecipeNLG-dominated extractions (the typical case) "
            "default to 'recipenlg'; pass --corpus=wdc for WDC-only runs."
        ),
    )
    parser.add_argument(
        "--clean-l1",
        action="store_true",
        help=(
            "Delete variants under each L1 key touched by this import "
            "that the import did not produce."
        ),
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(message)s",
    )

    directory: Path = args.directory
    if not (directory / "manifest.json").exists():
        print(f"manifest.json not found in {directory}", file=sys.stderr)
        return 1

    args.db.parent.mkdir(parents=True, exist_ok=True)
    db = CatalogDB.open(args.db)
    try:
        variants = list(
            reconstruct_variants(directory, corpus=args.corpus)
        )
        written = emit_variants_to_db(
            variants, db, delete_stale_for_l1=args.clean_l1
        )
    finally:
        db.close()

    skipped = len(variants) - written
    print(f"Imported {written} variant(s) from {directory} into {args.db}")
    if skipped:
        print(f"  ({skipped} variant(s) skipped — empty after reconstruction)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
