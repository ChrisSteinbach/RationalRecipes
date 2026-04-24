"""Round-trip tests for the SQLite catalog backing store (bead vwt.6)."""

from __future__ import annotations

from pathlib import Path

import pytest

from rational_recipes.catalog_db import (
    CatalogDB,
    ListFilters,
    ParsedIngredientRow,
)
from rational_recipes.scrape.pipeline_merged import (
    MergedNormalizedRow,
    MergedVariantResult,
)


def _row(
    url: str,
    cells: dict[str, str],
    proportions: dict[str, float],
    *,
    corpus: str = "recipenlg",
    title: str = "pannkakor",
) -> MergedNormalizedRow:
    return MergedNormalizedRow(
        url=url,
        title=title,
        corpus=corpus,
        cells=cells,
        proportions=proportions,
    )


def _variant(
    *,
    n_rows: int = 3,
    title: str = "pannkakor",
    cooking_methods: frozenset[str] = frozenset(),
) -> MergedVariantResult:
    rows = [
        _row(
            f"https://example.com/r/{i}",
            {"flour": "100 g", "milk": "250 ml"},
            {"flour": 28.5 + i * 0.01, "milk": 71.5 - i * 0.01},
            corpus="recipenlg" if i % 2 == 0 else "wdc",
            title=title,
        )
        for i in range(n_rows)
    ]
    return MergedVariantResult(
        variant_title=title,
        canonical_ingredients=frozenset({"flour", "milk"}),
        cooking_methods=cooking_methods,
        normalized_rows=rows,
        header_ingredients=["flour", "milk"],
    )


class TestSchema:
    def test_open_creates_expected_tables(self, tmp_path: Path) -> None:
        db = CatalogDB.open(tmp_path / "recipes.db")
        names = {
            r[0]
            for r in db.connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        assert {
            "recipes",
            "raw_ingredients",
            "parsed_ingredients",
            "variants",
            "variant_members",
            "variant_ingredient_stats",
            "variant_sources",
            "query_runs",
        }.issubset(names)

    def test_open_is_idempotent(self, tmp_path: Path) -> None:
        path = tmp_path / "recipes.db"
        CatalogDB.open(path).close()
        # Re-opening must succeed without a schema conflict.
        db = CatalogDB.open(path)
        db.close()

    def test_indexes_on_variants(self, tmp_path: Path) -> None:
        db = CatalogDB.open(tmp_path / "recipes.db")
        names = {
            r[0]
            for r in db.connection.execute(
                "SELECT name FROM sqlite_master WHERE type='index'"
            )
        }
        assert "idx_variants_nrecipes" in names
        assert "idx_variants_category" in names
        assert "idx_variants_title" in names


class TestUpsertVariant:
    def test_round_trip_preserves_variant_core(self) -> None:
        db = CatalogDB.in_memory()
        variant = _variant(n_rows=4, cooking_methods=frozenset({"stekt"}))

        db.upsert_variant(
            variant,
            l1_key="pannkakor",
            category="crepes",
            description="Thin pancakes.",
            base_ingredient="flour",
            confidence_level=0.95,
        )

        listed = db.list_variants()
        assert len(listed) == 1
        v = listed[0]
        assert v.variant_id == variant.variant_id
        assert v.normalized_title == "pannkakor"
        assert v.display_title == "pannkakor"
        assert v.category == "crepes"
        assert v.description == "Thin pancakes."
        assert v.base_ingredient == "flour"
        assert v.n_recipes == 4
        assert v.cooking_methods == ("stekt",)
        assert v.canonical_ingredient_set == ("flour", "milk")
        assert v.confidence_level == 0.95

    def test_round_trip_writes_member_rows(self) -> None:
        db = CatalogDB.in_memory()
        variant = _variant(n_rows=3)
        db.upsert_variant(variant, l1_key="pannkakor", base_ingredient="flour")

        members = db.get_variant_members(variant.variant_id)
        assert len(members) == 3
        urls = {m.url for m in members}
        assert urls == {
            "https://example.com/r/0",
            "https://example.com/r/1",
            "https://example.com/r/2",
        }

    def test_round_trip_computes_stats_in_fraction_form(self) -> None:
        db = CatalogDB.in_memory()
        variant = _variant(n_rows=3)
        db.upsert_variant(variant, l1_key="pannkakor", base_ingredient="flour")

        stats = db.get_ingredient_stats(variant.variant_id)
        assert [s.canonical_name for s in stats] == ["flour", "milk"]
        # Proportions are fractions (0..1) not percents.
        assert 0.2 < stats[0].mean_proportion < 0.4
        assert 0.6 < stats[1].mean_proportion < 0.8
        assert stats[0].ratio == pytest.approx(1.0)
        assert stats[1].ratio == pytest.approx(
            stats[1].mean_proportion / stats[0].mean_proportion
        )

    def test_upsert_is_idempotent(self) -> None:
        db = CatalogDB.in_memory()
        variant = _variant(n_rows=3)
        db.upsert_variant(variant, l1_key="pannkakor", base_ingredient="flour")
        db.upsert_variant(variant, l1_key="pannkakor", base_ingredient="flour")

        assert len(db.list_variants()) == 1
        assert len(db.get_variant_members(variant.variant_id)) == 3
        assert len(db.get_ingredient_stats(variant.variant_id)) == 2

    def test_upsert_replaces_old_members(self) -> None:
        db = CatalogDB.in_memory()
        first = _variant(n_rows=4)
        db.upsert_variant(first, l1_key="pannkakor", base_ingredient="flour")

        smaller = MergedVariantResult(
            variant_title=first.variant_title,
            canonical_ingredients=first.canonical_ingredients,
            cooking_methods=first.cooking_methods,
            normalized_rows=first.normalized_rows[:2],
            header_ingredients=first.header_ingredients,
        )
        db.upsert_variant(smaller, l1_key="pannkakor", base_ingredient="flour")

        members = db.get_variant_members(first.variant_id)
        assert len(members) == 2

    def test_filter_by_min_sample_size(self) -> None:
        db = CatalogDB.in_memory()
        big = _variant(n_rows=5, title="pannkakor")
        small = _variant(n_rows=2, title="crepes")
        db.upsert_variant(big, l1_key="pannkakor", base_ingredient="flour")
        db.upsert_variant(small, l1_key="crepes", base_ingredient="flour")

        listed = db.list_variants(ListFilters(min_sample_size=3))
        assert {v.normalized_title for v in listed} == {"pannkakor"}

    def test_filter_by_category(self) -> None:
        db = CatalogDB.in_memory()
        a = _variant(title="pannkakor")
        b = _variant(title="bread")
        db.upsert_variant(a, l1_key="pannkakor", category="crepes")
        db.upsert_variant(b, l1_key="bread", category="bread")

        listed = db.list_variants(ListFilters(category="crepes"))
        assert {v.normalized_title for v in listed} == {"pannkakor"}

    def test_filter_by_title_substring_case_insensitive(self) -> None:
        db = CatalogDB.in_memory()
        db.upsert_variant(
            _variant(title="Pannkakor"), l1_key="pannkakor", category="crepes"
        )
        db.upsert_variant(
            _variant(title="sourdough"), l1_key="sourdough", category="bread"
        )

        listed = db.list_variants(ListFilters(title_search="PANN"))
        assert len(listed) == 1
        assert listed[0].normalized_title == "pannkakor"

    def test_drop_variant_hidden_by_default(self) -> None:
        db = CatalogDB.in_memory()
        v = _variant(n_rows=3)
        db.upsert_variant(v, l1_key="pannkakor", base_ingredient="flour")
        with db.connection:
            db.connection.execute(
                "UPDATE variants SET review_status='drop' WHERE variant_id = ?",
                (v.variant_id,),
            )
        assert db.list_variants() == []
        assert len(db.list_variants(ListFilters(include_dropped=True))) == 1


class TestL1RunTracking:
    def test_is_l1_fresh_false_before_run(self) -> None:
        db = CatalogDB.in_memory()
        assert db.is_l1_fresh("pannkakor", corpus_revisions="abc") is False

    def test_record_l1_run_then_fresh(self) -> None:
        db = CatalogDB.in_memory()
        db.record_l1_run(
            "pannkakor",
            corpus_revisions="rev-abc",
            variants_produced=2,
            dry=False,
            run_at="2026-04-24T10:00:00Z",
        )
        assert db.is_l1_fresh("pannkakor", corpus_revisions="rev-abc") is True

    def test_fingerprint_mismatch_reports_stale(self) -> None:
        db = CatalogDB.in_memory()
        db.record_l1_run(
            "pannkakor",
            corpus_revisions="rev-abc",
            variants_produced=2,
            dry=False,
            run_at="2026-04-24T10:00:00Z",
        )
        assert db.is_l1_fresh("pannkakor", corpus_revisions="rev-xyz") is False

    def test_record_is_idempotent(self) -> None:
        db = CatalogDB.in_memory()
        db.record_l1_run(
            "pannkakor",
            corpus_revisions="rev",
            variants_produced=1,
            dry=False,
            run_at="2026-04-24T10:00:00Z",
        )
        db.record_l1_run(
            "pannkakor",
            corpus_revisions="rev",
            variants_produced=3,
            dry=True,
            run_at="2026-04-24T11:00:00Z",
        )
        row = db.connection.execute(
            "SELECT variants_produced, dry FROM query_runs WHERE l1_group_key = ?",
            ("pannkakor",),
        ).fetchone()
        assert row == (3, 1)


class TestUpsertRecipe:
    def test_recipe_with_raw_and_parsed_rows(self) -> None:
        db = CatalogDB.in_memory()
        db.upsert_recipe(
            recipe_id="abc123",
            url="https://example.com/r",
            title="Pannkakor",
            corpus="wdc",
            language="sv",
            source_type="url",
            raw_lines=("2 dl vetemjöl", "5 dl mjölk", "3 ägg"),
            parsed=(
                ParsedIngredientRow(
                    canonical_name="flour",
                    quantity=200.0,
                    unit="ml",
                    grams=105.66,
                ),
                ParsedIngredientRow(
                    canonical_name="milk",
                    quantity=500.0,
                    unit="ml",
                    grams=515.6,
                ),
            ),
        )
        raw = db.connection.execute(
            "SELECT raw_line FROM raw_ingredients WHERE recipe_id = ?"
            " ORDER BY line_index",
            ("abc123",),
        ).fetchall()
        assert [r[0] for r in raw] == ["2 dl vetemjöl", "5 dl mjölk", "3 ägg"]

        parsed = db.connection.execute(
            "SELECT canonical_name, quantity, unit, grams"
            " FROM parsed_ingredients WHERE recipe_id = ?"
            " ORDER BY canonical_name",
            ("abc123",),
        ).fetchall()
        assert parsed == [
            ("flour", 200.0, "ml", 105.66),
            ("milk", 500.0, "ml", 515.6),
        ]

    def test_upsert_recipe_replaces_children(self) -> None:
        db = CatalogDB.in_memory()
        db.upsert_recipe(
            recipe_id="r",
            url=None,
            title="x",
            corpus="curated",
            raw_lines=("a", "b"),
            parsed=(ParsedIngredientRow(canonical_name="flour", quantity=1.0),),
        )
        db.upsert_recipe(
            recipe_id="r",
            url=None,
            title="x",
            corpus="curated",
            raw_lines=("c",),
            parsed=(),
        )
        raw = db.connection.execute(
            "SELECT raw_line FROM raw_ingredients WHERE recipe_id = ?",
            ("r",),
        ).fetchall()
        assert [r[0] for r in raw] == ["c"]
        parsed = db.connection.execute(
            "SELECT count(*) FROM parsed_ingredients WHERE recipe_id = ?",
            ("r",),
        ).fetchone()
        assert parsed[0] == 0


class TestVariantSources:
    def test_add_and_retrieve(self) -> None:
        db = CatalogDB.in_memory()
        v = _variant(n_rows=2)
        db.upsert_variant(v, l1_key="pannkakor", base_ingredient="flour")
        db.add_variant_source(
            v.variant_id,
            ordinal=0,
            source_type="text",
            ref="Swedish pannkakor recipes.",
            title="Aggregated Swedish recipes",
        )
        sources = db.get_variant_sources(v.variant_id)
        assert len(sources) == 1
        assert sources[0].source_type == "text"
        assert sources[0].title == "Aggregated Swedish recipes"
