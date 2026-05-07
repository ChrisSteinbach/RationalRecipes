"""Tests for ``scripts/backfill_density.py`` (RationalRecipes-4ba4 / F4)."""

from __future__ import annotations

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

import backfill_density  # noqa: E402


def _seed_variant_with_null_metadata(db: CatalogDB) -> str:
    rows = [
        MergedNormalizedRow(
            url=f"https://example.com/r/{i}",
            title="pannkakor",
            corpus="recipenlg",
            cells={"flour": "100 g", "milk": "250 ml"},
            proportions={"flour": 28.5 + i * 0.01, "milk": 71.5 - i * 0.01},
        )
        for i in range(3)
    ]
    variant = MergedVariantResult(
        variant_title="pannkakor",
        canonical_ingredients=frozenset({"flour", "milk"}),
        cooking_methods=frozenset(),
        normalized_rows=rows,
        header_ingredients=["flour", "milk"],
    )
    db.upsert_variant(variant, l1_key="pannkakor", base_ingredient="flour")
    # Wipe metadata so the backfill has work to do.
    db.connection.execute(
        "UPDATE variant_ingredient_stats SET density_g_per_ml = NULL, "
        "whole_unit_name = NULL, whole_unit_grams = NULL"
    )
    db.connection.commit()
    return variant.variant_id


class TestBackfillDensity:
    def test_populates_density_for_known_canonicals(self, tmp_path: Path) -> None:
        db_path = tmp_path / "recipes.db"
        db = CatalogDB.open(db_path)
        try:
            vid = _seed_variant_with_null_metadata(db)
        finally:
            db.close()

        updated, _skipped = backfill_density.backfill(db_path)
        assert updated > 0

        db = CatalogDB.open(db_path)
        try:
            stats = {
                s.canonical_name: s
                for s in db.get_ingredient_stats(vid)
            }
        finally:
            db.close()
        # Flour has a USDA-derived density.
        assert stats["flour"].density_g_per_ml is not None

    def test_idempotent(self, tmp_path: Path) -> None:
        """Running the backfill twice produces the same final values."""
        db_path = tmp_path / "recipes.db"
        db = CatalogDB.open(db_path)
        try:
            vid = _seed_variant_with_null_metadata(db)
        finally:
            db.close()

        backfill_density.backfill(db_path)
        db = CatalogDB.open(db_path)
        try:
            after_first = db.get_ingredient_stats(vid)
        finally:
            db.close()

        backfill_density.backfill(db_path)
        db = CatalogDB.open(db_path)
        try:
            after_second = db.get_ingredient_stats(vid)
        finally:
            db.close()

        assert after_first == after_second
