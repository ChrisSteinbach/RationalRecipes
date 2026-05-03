"""Backfill the ``variants.category`` column over an existing recipes.db.

The pipeline writes ``category`` for every new variant via
``categorize(l1_key)`` (vwt.33). This CLI applies the same mapping
retroactively to a DB produced before that change so the PWA dropdown
populates without a full re-scrape.

Usage::

    python3 -m rational_recipes.cli.backfill_categories \\
        --db output/catalog/recipes.db

By default, only rows where ``category`` is NULL are touched.
``--force`` re-categorizes every row, overwriting existing values.
``--dry-run`` reports the would-be label distribution without writing.
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter
from collections.abc import Sequence
from pathlib import Path

from rational_recipes.catalog_db import CatalogDB
from rational_recipes.categories import categorize


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        type=Path,
        default=Path("output/catalog/recipes.db"),
        help="Path to recipes.db (default: %(default)s).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-categorize every variant, overwriting existing values.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the target distribution without writing.",
    )
    return parser.parse_args(argv)


def run(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    if not args.db.exists():
        print(f"recipes.db not found: {args.db}", file=sys.stderr)
        return 1

    db = CatalogDB.open(args.db)
    try:
        rows = list(db.iter_variant_ids_titles())
        if not args.force:
            existing = {
                vid
                for (vid,) in db.connection.execute(
                    "SELECT variant_id FROM variants WHERE category IS NOT NULL"
                )
            }
            rows = [(vid, t) for vid, t in rows if vid not in existing]

        counts: Counter[str] = Counter()
        updates: list[tuple[str, str | None]] = []
        for variant_id, title in rows:
            category = categorize(title)
            counts[category or "(none)"] += 1
            updates.append((variant_id, category))

        total_targeted = len(updates)
        print(
            f"would update {total_targeted} variants "
            f"(force={args.force}, dry_run={args.dry_run})"
        )
        for label, n in sorted(counts.items(), key=lambda x: (-x[1], x[0])):
            print(f"  {label:12} {n}")

        if args.dry_run:
            return 0

        # Single transaction for the whole backfill — ~38k UPDATEs is
        # cheap and atomicity matters more than per-row durability.
        with db.connection:
            db.connection.executemany(
                "UPDATE variants SET category = ? WHERE variant_id = ?",
                [(cat, vid) for vid, cat in updates],
            )
        print(f"updated {total_targeted} rows")
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
