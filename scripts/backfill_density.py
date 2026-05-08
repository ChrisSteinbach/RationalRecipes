#!/usr/bin/env python3
"""Backfill density + whole-unit metadata in an existing recipes.db (4ba4 / F4).

Walks every row in ``variant_ingredient_stats`` and populates
``density_g_per_ml`` / ``whole_unit_name`` / ``whole_unit_grams`` from
``ingredients.db`` (USDA/FAO) when the canonical ingredient resolves.

Idempotent: running the script twice leaves the same values in place.
Rows whose canonical doesn't resolve in ``ingredients.db`` are left as
NULL — that's the pre-F4 baseline behavior and matches what
``render_drop.py`` already falls back to.

Usage:
    python3 scripts/backfill_density.py --db output/catalog/recipes.db
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

from rational_recipes.catalog_db import CatalogDB, lookup_ingredient_metadata


def backfill(db_path: Path) -> tuple[int, int]:
    """Update density / whole-unit columns; return (updated, skipped)."""
    # Open via CatalogDB once so any pending schema migrations run before
    # the backfill SELECTs touch the table; close and reopen via plain
    # sqlite3 for the row iteration.
    CatalogDB.open(db_path).close()
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT DISTINCT canonical_name FROM variant_ingredient_stats"
        ).fetchall()
        updated = 0
        skipped = 0
        for (canonical,) in rows:
            density, whole_unit_name, whole_unit_grams = (
                lookup_ingredient_metadata(canonical)
            )
            if density is None and whole_unit_grams is None:
                skipped += 1
                continue
            cursor = conn.execute(
                """
                UPDATE variant_ingredient_stats
                   SET density_g_per_ml = ?,
                       whole_unit_name = ?,
                       whole_unit_grams = ?
                 WHERE canonical_name = ?
                """,
                (density, whole_unit_name, whole_unit_grams, canonical),
            )
            updated += cursor.rowcount
        conn.commit()
    finally:
        conn.close()
    return updated, skipped


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        type=Path,
        default=Path("output/catalog/recipes.db"),
        help="Path to recipes.db (default: output/catalog/recipes.db)",
    )
    args = parser.parse_args(argv)
    if not args.db.exists():
        print(f"recipes.db not found: {args.db}", file=sys.stderr)
        return 1
    updated, skipped = backfill(args.db)
    print(
        f"Updated {updated} ingredient stat row(s); "
        f"{skipped} canonical(s) unresolved"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
