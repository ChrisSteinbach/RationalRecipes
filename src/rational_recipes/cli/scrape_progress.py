"""Read recipes.db and print where the scrape_catalog pipeline stands.

Independent of any in-process state — works mid-run (the WAL DB stays
readable while a writer holds the file), after a crash, or hours after
the run is done. Reads three tables:

  * ``parsed_ingredient_lines``  → Pass 1 throughput
  * ``query_runs``               → Pass 2 commit boundary
  * ``variants``                 → final output

Usage::

    python3 scripts/scrape_progress.py
    python3 scripts/scrape_progress.py --db /path/to/recipes.db
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

DEFAULT_DB = Path("output/catalog/recipes.db")


def _human(n: int) -> str:
    return f"{n:,}"


def _query_one(
    conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()
) -> Any:
    row = conn.execute(sql, params).fetchone()
    return row[0] if row else None


def _format_pass1(conn: sqlite3.Connection) -> list[str]:
    total_lines = _query_one(conn, "SELECT COUNT(*) FROM parsed_ingredient_lines") or 0
    failures = (
        _query_one(
            conn,
            "SELECT COUNT(*) FROM parsed_ingredient_lines WHERE parsed_json IS NULL",
        )
        or 0
    )
    distinct_recipes = (
        _query_one(
            conn,
            "SELECT COUNT(*) FROM ("
            "SELECT 1 FROM parsed_ingredient_lines GROUP BY corpus, recipe_id"
            ")",
        )
        or 0
    )

    lines = ["Pass 1 — parsed_ingredient_lines:"]
    lines.append(
        f"  rows: {_human(total_lines)}  (parse failures: {_human(failures)})"
    )
    lines.append(f"  recipes covered: {_human(distinct_recipes)}")

    by_corpus = conn.execute(
        "SELECT corpus, COUNT(*) FROM parsed_ingredient_lines GROUP BY corpus"
    ).fetchall()
    if by_corpus:
        breakdown = "  ".join(f"{c}={_human(n)}" for c, n in by_corpus)
        lines.append(f"  by corpus: {breakdown}")

    by_model = conn.execute(
        "SELECT model, COUNT(*) FROM parsed_ingredient_lines GROUP BY model"
    ).fetchall()
    if by_model:
        breakdown = "  ".join(f"{m}={_human(n)}" for m, n in by_model)
        lines.append(f"  by model: {breakdown}")
    return lines


def _format_pass2(conn: sqlite3.Connection) -> list[str]:
    total = _query_one(conn, "SELECT COUNT(*) FROM query_runs") or 0
    dry = _query_one(conn, "SELECT COUNT(*) FROM query_runs WHERE dry = 1") or 0
    last_run = _query_one(conn, "SELECT MAX(run_at) FROM query_runs")

    lines = ["Pass 2 — query_runs (committed L1 groups):"]
    lines.append(
        f"  total: {_human(total)}  "
        f"(dry: {_human(dry)}, with-variants: {_human(total - dry)})"
    )
    if last_run:
        lines.append(f"  most recent run_at: {last_run}")
    return lines


def _format_variants(conn: sqlite3.Connection) -> list[str]:
    total = _query_one(conn, "SELECT COUNT(*) FROM variants") or 0
    by_status = conn.execute(
        "SELECT COALESCE(review_status, 'pending'), COUNT(*) "
        "FROM variants GROUP BY review_status"
    ).fetchall()

    lines = ["Variants table:"]
    lines.append(f"  total: {_human(total)}")
    if by_status:
        breakdown = "  ".join(f"{s}={_human(n)}" for s, n in by_status)
        lines.append(f"  by review_status: {breakdown}")
    return lines


def render_report(conn: sqlite3.Connection, db_label: str) -> str:
    sections: list[list[str]] = [
        [f"=== scrape_catalog progress for {db_label} ==="],
        _format_pass1(conn),
        _format_pass2(conn),
        _format_variants(conn),
    ]
    return "\n".join("\n".join(section) for section in sections)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB,
        help="Path to the recipes.db file (default: %(default)s)",
    )
    args = parser.parse_args(argv)
    if not args.db.exists():
        print(f"DB not found: {args.db}", file=sys.stderr)
        return 1
    # Read-only URI — a concurrent writer can hold the WAL without
    # blocking us; we just observe the most recently committed state.
    conn = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    try:
        print(render_report(conn, str(args.db)))
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
