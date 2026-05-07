"""Tests for ``scripts/backfill_directions.py`` (RationalRecipes-15g4 / F5)."""

from __future__ import annotations

import csv
import sqlite3
import sys
from pathlib import Path

from rational_recipes.catalog_db import CatalogDB
from rational_recipes.scrape.pipeline_merged import (
    MergedNormalizedRow,
    MergedVariantResult,
)


def _ensure_scripts_on_path() -> None:
    scripts = Path(__file__).resolve().parent.parent / "scripts"
    if str(scripts) not in sys.path:
        sys.path.insert(0, str(scripts))


_ensure_scripts_on_path()

import backfill_directions  # noqa: E402


def _seed_variant_without_directions(
    db: CatalogDB, *, urls: list[str]
) -> str:
    rows = [
        MergedNormalizedRow(
            url=urls[i],
            title="pannkakor",
            corpus="recipenlg",
            cells={"flour": "100 g", "milk": "250 ml"},
            proportions={"flour": 28.5 + i * 0.01, "milk": 71.5 - i * 0.01},
            directions_text=None,
        )
        for i in range(len(urls))
    ]
    variant = MergedVariantResult(
        variant_title="pannkakor",
        canonical_ingredients=frozenset({"flour", "milk"}),
        cooking_methods=frozenset(),
        normalized_rows=rows,
        header_ingredients=["flour", "milk"],
    )
    db.upsert_variant(variant, l1_key="pannkakor", base_ingredient="flour")
    return variant.variant_id


def _write_recipenlg_csv(
    csv_path: Path, rows: list[dict[str, str]]
) -> None:
    columns = ["", "title", "ingredients", "directions", "link", "source", "NER"]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


class TestBackfillDirections:
    def test_populates_from_csv(self, tmp_path: Path) -> None:
        db_path = tmp_path / "recipes.db"
        urls = ["https://x.com/r/0", "https://x.com/r/1"]
        db = CatalogDB.open(db_path)
        try:
            _seed_variant_without_directions(db, urls=urls)
        finally:
            db.close()

        csv_path = tmp_path / "rnlg.csv"
        _write_recipenlg_csv(
            csv_path,
            [
                {
                    "": "0",
                    "title": "pannkakor",
                    "ingredients": "['100 g flour', '250 ml milk']",
                    "directions": "['Mix.', 'Cook.']",
                    "link": urls[0],
                    "source": "test",
                    "NER": "['flour', 'milk']",
                },
                {
                    "": "1",
                    "title": "pannkakor",
                    "ingredients": "['100 g flour']",
                    "directions": "['Whisk.']",
                    "link": urls[1],
                    "source": "test",
                    "NER": "['flour']",
                },
            ],
        )

        updated, _skipped = backfill_directions.backfill(db_path, csv_path)
        assert updated == 2

        conn = sqlite3.connect(db_path)
        try:
            results = dict(
                conn.execute(
                    "SELECT url, directions_text FROM recipes ORDER BY url"
                ).fetchall()
            )
        finally:
            conn.close()
        assert "Mix." in results[urls[0]]
        assert "Cook." in results[urls[0]]
        assert results[urls[1]] == "Whisk."

    def test_idempotent(self, tmp_path: Path) -> None:
        db_path = tmp_path / "recipes.db"
        urls = ["https://x.com/r/0"]
        db = CatalogDB.open(db_path)
        try:
            _seed_variant_without_directions(db, urls=urls)
        finally:
            db.close()

        csv_path = tmp_path / "rnlg.csv"
        _write_recipenlg_csv(
            csv_path,
            [
                {
                    "": "0",
                    "title": "pannkakor",
                    "ingredients": "['100 g flour']",
                    "directions": "['Mix.']",
                    "link": urls[0],
                    "source": "test",
                    "NER": "['flour']",
                }
            ],
        )

        first_updated, _ = backfill_directions.backfill(db_path, csv_path)
        second_updated, _ = backfill_directions.backfill(db_path, csv_path)
        assert first_updated == 1
        # Second run finds no NULL rows, so it has nothing to do.
        assert second_updated == 0

    def test_unmatched_link_left_null(self, tmp_path: Path) -> None:
        db_path = tmp_path / "recipes.db"
        urls = ["https://x.com/r/0"]
        db = CatalogDB.open(db_path)
        try:
            _seed_variant_without_directions(db, urls=urls)
        finally:
            db.close()

        csv_path = tmp_path / "rnlg.csv"
        # CSV doesn't contain the url that's in the DB.
        _write_recipenlg_csv(
            csv_path,
            [
                {
                    "": "9",
                    "title": "ostkaka",
                    "ingredients": "['']",
                    "directions": "['Bake.']",
                    "link": "https://other.example/u/9",
                    "source": "test",
                    "NER": "[]",
                }
            ],
        )

        updated, skipped = backfill_directions.backfill(db_path, csv_path)
        assert updated == 0
        assert skipped == 1

        conn = sqlite3.connect(db_path)
        try:
            (val,) = conn.execute(
                "SELECT directions_text FROM recipes WHERE url = ?", (urls[0],)
            ).fetchone()
        finally:
            conn.close()
        assert val is None
