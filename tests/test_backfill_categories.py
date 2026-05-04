"""Tests for the variants.category backfill CLI (vwt.33)."""

from __future__ import annotations

from pathlib import Path

from rational_recipes.catalog_db import CatalogDB
from rational_recipes.cli.backfill_categories import run as backfill_run
from rational_recipes.scrape.pipeline_merged import (
    MergedNormalizedRow,
    MergedVariantResult,
)


def _seed_variant(
    db: CatalogDB,
    *,
    title: str,
    category: str | None = None,
) -> str:
    """Insert one minimal variant via upsert_variant; return its id."""
    row = MergedNormalizedRow(
        url="https://example.com/r/1",
        title=title,
        corpus="recipenlg",
        cells={"flour": "100 g"},
        proportions={"flour": 100.0},
    )
    variant = MergedVariantResult(
        variant_title=title,
        canonical_ingredients=frozenset({"flour"}),
        cooking_methods=frozenset(),
        normalized_rows=[row],
        header_ingredients=["flour"],
    )
    db.upsert_variant(variant, l1_key=title, category=category)
    return variant.variant_id


def _category_of(db: CatalogDB, variant_id: str) -> str | None:
    row = db.connection.execute(
        "SELECT category FROM variants WHERE variant_id = ?", (variant_id,)
    ).fetchone()
    return row[0]


class TestBackfillCli:
    def test_populates_null_categories(self, tmp_path: Path) -> None:
        db_path = tmp_path / "recipes.db"
        db = CatalogDB.open(db_path)
        bread_id = _seed_variant(db, title="banana bread")
        cake_id = _seed_variant(db, title="carrot cake")
        unknown_id = _seed_variant(db, title="xyzzy plover snickersnack")
        db.close()

        rc = backfill_run(["--db", str(db_path)])
        assert rc == 0

        db = CatalogDB.open(db_path)
        try:
            assert _category_of(db, bread_id) == "bread"
            assert _category_of(db, cake_id) == "dessert"
            assert _category_of(db, unknown_id) is None
        finally:
            db.close()

    def test_skips_existing_without_force(self, tmp_path: Path) -> None:
        db_path = tmp_path / "recipes.db"
        db = CatalogDB.open(db_path)
        # Seed with a hand-set category that disagrees with the rule.
        cake_id = _seed_variant(db, title="carrot cake", category="manual-label")
        db.close()

        rc = backfill_run(["--db", str(db_path)])
        assert rc == 0

        db = CatalogDB.open(db_path)
        try:
            # Category was preserved because it was already set.
            assert _category_of(db, cake_id) == "manual-label"
        finally:
            db.close()

    def test_force_overwrites_existing(self, tmp_path: Path) -> None:
        db_path = tmp_path / "recipes.db"
        db = CatalogDB.open(db_path)
        cake_id = _seed_variant(db, title="carrot cake", category="manual-label")
        db.close()

        rc = backfill_run(["--db", str(db_path), "--force"])
        assert rc == 0

        db = CatalogDB.open(db_path)
        try:
            assert _category_of(db, cake_id) == "dessert"
        finally:
            db.close()

    def test_dry_run_writes_nothing(self, tmp_path: Path) -> None:
        db_path = tmp_path / "recipes.db"
        db = CatalogDB.open(db_path)
        bread_id = _seed_variant(db, title="banana bread")
        db.close()

        rc = backfill_run(["--db", str(db_path), "--dry-run"])
        assert rc == 0

        db = CatalogDB.open(db_path)
        try:
            # Still NULL — dry run prints distribution but doesn't write.
            assert _category_of(db, bread_id) is None
        finally:
            db.close()

    def test_missing_db_returns_nonzero(self, tmp_path: Path) -> None:
        rc = backfill_run(["--db", str(tmp_path / "missing.db")])
        assert rc == 1


class TestUpdateCategoryHelper:
    def test_update_category_round_trip(self, tmp_path: Path) -> None:
        db_path = tmp_path / "recipes.db"
        db = CatalogDB.open(db_path)
        try:
            vid = _seed_variant(db, title="banana bread")
            assert _category_of(db, vid) is None
            db.update_category(vid, "bread")
            assert _category_of(db, vid) == "bread"
            db.update_category(vid, None)
            assert _category_of(db, vid) is None
        finally:
            db.close()
