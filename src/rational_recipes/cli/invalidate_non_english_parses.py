"""Invalidate parsed_ingredient_lines rows whose parsed_json is non-ASCII.

One-shot migration for bead e4s. After Pass 1 was switched to translate
ingredient names to English (``scrape/wdc.py::NEUTRAL_PROMPT``), any
cached parses still carrying source-language strings would otherwise
short-circuit the new prompt via ``lookup_cached_parse``. This script
deletes those stale rows so they get re-parsed under the English-output
prompt. ASCII-only parses (already English) are left untouched.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from rational_recipes.catalog_db import CatalogDB

_DEFAULT_DB = Path("output/catalog/recipes.db")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        type=Path,
        default=_DEFAULT_DB,
        help=f"Path to recipes.db (default: {_DEFAULT_DB})",
    )
    args = parser.parse_args(argv)

    db = CatalogDB.open(args.db)
    try:
        deleted = db.invalidate_non_english_parses()
    finally:
        db.close()
    print(
        f"Deleted {deleted} non-English parsed_ingredient_lines row(s) "
        f"from {args.db}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
