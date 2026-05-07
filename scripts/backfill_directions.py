#!/usr/bin/env python3
"""Backfill RNLG source directions in recipes.db (F5 / 15g4).

For every RNLG-corpus row in ``recipes.recipe_id`` whose
``directions_text`` is NULL, look up the matching row in
``dataset/full_dataset.csv`` (RecipeNLG corpus) by ``link`` and
populate ``directions_text``. Skips WDC rows (out of scope per the
bead — file a follow-up if needed) and leaves rows that can't be
matched alone.

Idempotent: pre-populated rows are skipped, so re-running is cheap.

Usage:
    python3 scripts/backfill_directions.py \\
        --db output/catalog/recipes.db \\
        --recipenlg dataset/full_dataset.csv
"""

from __future__ import annotations

import argparse
import ast
import csv
import sqlite3
import sys
from pathlib import Path

from rational_recipes.catalog_db import CatalogDB

DEFAULT_DB = Path("output/catalog/recipes.db")
DEFAULT_RECIPENLG = Path("dataset/full_dataset.csv")


def _parse_string_list(raw: str) -> tuple[str, ...]:
    """Decode a stringified Python list (matches recipenlg loader)."""
    try:
        parsed = ast.literal_eval(raw)
        if isinstance(parsed, list):
            return tuple(str(item) for item in parsed)
    except (ValueError, SyntaxError):
        pass
    return ()


def _build_link_index(csv_path: Path, wanted: set[str]) -> dict[str, str]:
    """Return ``{link: directions_text}`` for every wanted link.

    ``wanted`` is the set of RNLG ``recipes.url`` values still missing
    directions. The CSV is streamed once; rows whose ``link`` isn't in
    ``wanted`` are skipped without parsing. Joining ``directions`` with
    newlines mirrors the in-pipeline ``MergedRecipe.directions_text``
    construction so backfilled rows are byte-identical to fresh ones.
    """
    index: dict[str, str] = {}
    if not wanted:
        return index
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            link = row.get("link", "")
            if link not in wanted:
                continue
            directions = _parse_string_list(row.get("directions", "[]"))
            if directions:
                index[link] = "\n".join(directions)
            if len(index) == len(wanted):
                break
    return index


def backfill(db_path: Path, csv_path: Path) -> tuple[int, int]:
    """Populate directions_text for RNLG rows; return (updated, skipped)."""
    CatalogDB.open(db_path).close()
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT recipe_id, url FROM recipes "
            "WHERE corpus = 'recipenlg' AND directions_text IS NULL "
            "AND url IS NOT NULL AND url != ''"
        ).fetchall()
        if not rows:
            return 0, 0
        url_to_recipe_id = {url: recipe_id for recipe_id, url in rows}
        wanted = set(url_to_recipe_id)
        index = _build_link_index(csv_path, wanted)

        updated = 0
        for link, directions_text in index.items():
            recipe_id = url_to_recipe_id.get(link)
            if recipe_id is None:
                continue
            conn.execute(
                "UPDATE recipes SET directions_text = ? WHERE recipe_id = ?",
                (directions_text, recipe_id),
            )
            updated += 1
        conn.commit()
        skipped = len(wanted) - updated
    finally:
        conn.close()
    return updated, skipped


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB,
        help=f"Path to recipes.db (default: {DEFAULT_DB})",
    )
    parser.add_argument(
        "--recipenlg",
        type=Path,
        default=DEFAULT_RECIPENLG,
        help=f"Path to RecipeNLG CSV (default: {DEFAULT_RECIPENLG})",
    )
    args = parser.parse_args(argv)
    if not args.db.exists():
        print(f"recipes.db not found: {args.db}", file=sys.stderr)
        return 1
    if not args.recipenlg.exists():
        print(
            f"RecipeNLG CSV not found: {args.recipenlg}", file=sys.stderr
        )
        return 1
    updated, skipped = backfill(args.db, args.recipenlg)
    print(
        f"Updated directions_text on {updated} recipe(s); "
        f"{skipped} unmatched (RNLG row not in corpus or directions empty)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
