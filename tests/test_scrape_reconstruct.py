"""Tests for the CSV+manifest → MergedVariantResult reconstruction path.

Covers RationalRecipes-v61w's importer: existing scrape_merged.py output
must round-trip into recipes.db without re-running the LLM, so the
hand-cycle drop (RationalRecipes-ehe7) can complete.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from rational_recipes.catalog_db import CatalogDB, emit_variants_to_db
from rational_recipes.scrape.manifest import Manifest, VariantManifestEntry
from rational_recipes.scrape.pipeline_merged import (
    MergedNormalizedRow,
    MergedVariantResult,
    emit_variants,
)
from rational_recipes.scrape.reconstruct import (
    normalize_row_from_cells,
    parse_cell,
    reconstruct_variant,
    reconstruct_variants,
)


class TestParseCell:
    def test_blank_returns_none(self) -> None:
        assert parse_cell("") is None
        assert parse_cell("   ") is None

    def test_zero_returns_zero_grams(self) -> None:
        assert parse_cell("0") == (0.0, "g")

    def test_quantity_with_unit(self) -> None:
        assert parse_cell("12 tbsp") == (12.0, "tbsp")
        assert parse_cell("0.75 c") == (0.75, "c")
        assert parse_cell("2 MEDIUM") == (2.0, "MEDIUM")

    def test_decimal_quantity(self) -> None:
        assert parse_cell("1.5 cup") == (1.5, "cup")

    def test_bare_number_falls_back_to_grams(self) -> None:
        assert parse_cell("42") == (42.0, "g")

    def test_unparseable_returns_none(self) -> None:
        assert parse_cell("a lot") is None


class TestNormalizeRowFromCells:
    def test_resolved_cells_produce_proportions_summing_to_100(self) -> None:
        # 200 g flour + 200 g milk → 50/50 proportions.
        row = normalize_row_from_cells(
            url="https://x/r/1",
            title="pancakes",
            header=["flour", "milk"],
            cell_values=["200 g", "200 g"],
        )
        assert row.cells == {"flour": "200 g", "milk": "200 g"}
        assert sum(row.proportions.values()) == pytest.approx(100.0)
        assert row.proportions["flour"] == pytest.approx(50.0)
        assert row.proportions["milk"] == pytest.approx(50.0)

    def test_zero_cell_kept_as_zero_proportion(self) -> None:
        row = normalize_row_from_cells(
            url="u",
            title="t",
            header=["flour", "salt"],
            cell_values=["100 g", "0"],
        )
        assert row.cells["salt"] == "0"
        # Zero-quantity ingredient contributes nothing to the total.
        assert row.proportions["flour"] == pytest.approx(100.0)
        assert row.proportions.get("salt", 0.0) == pytest.approx(0.0)

    def test_volume_unit_with_density_resolves(self) -> None:
        # 1 cup of flour ≈ 125 g (USDA), 1 cup of milk ≈ 244 g —
        # exact numbers depend on the ingredients DB, just sanity-check
        # that volume cells resolve to nonzero proportions.
        row = normalize_row_from_cells(
            url="u",
            title="t",
            header=["flour", "milk"],
            cell_values=["1 cup", "1 cup"],
        )
        assert row.proportions["flour"] > 0
        assert row.proportions["milk"] > 0
        assert sum(row.proportions.values()) == pytest.approx(100.0)

    def test_unknown_unit_skipped_silently(self) -> None:
        row = normalize_row_from_cells(
            url="u",
            title="t",
            header=["flour", "salt"],
            cell_values=["100 g", "1 handful"],
        )
        # Unknown unit — salt drops out of grams totals but cell is preserved.
        assert row.cells["salt"] == "1 handful"
        assert row.proportions["flour"] == pytest.approx(100.0)
        assert "salt" not in row.proportions

    def test_unknown_ingredient_skipped_silently(self) -> None:
        row = normalize_row_from_cells(
            url="u",
            title="t",
            header=["flour", "zzzunknownfood"],
            cell_values=["100 g", "10 g"],
        )
        assert row.cells["zzzunknownfood"] == "10 g"
        assert "zzzunknownfood" not in row.proportions
        assert row.proportions["flour"] == pytest.approx(100.0)

    def test_corpus_tag_passed_through(self) -> None:
        row = normalize_row_from_cells(
            url="u",
            title="t",
            header=["flour"],
            cell_values=["100 g"],
            corpus="wdc",
        )
        assert row.corpus == "wdc"


class TestReconstructVariant:
    def _entry_for(
        self,
        variant: MergedVariantResult,
        csv_path: Path,
    ) -> VariantManifestEntry:
        return variant.to_manifest_entry(csv_path.name)

    def test_round_trip_via_csv_preserves_proportions(
        self, tmp_path: Path
    ) -> None:
        # Build a variant in memory, emit to CSV+manifest, read it back.
        # Proportions reconstructed from cells should match the original
        # (up to float rounding) because we use the same Factory + units.
        original = MergedVariantResult(
            variant_title="pannkakor",
            canonical_ingredients=frozenset({"flour", "milk"}),
            cooking_methods=frozenset(),
            normalized_rows=[
                MergedNormalizedRow(
                    url=f"https://x/{i}",
                    title="pannkakor",
                    corpus="recipenlg",
                    cells={"flour": f"{200 + i * 10} g", "milk": "200 g"},
                    proportions={},  # not used in to_csv; will be re-derived
                )
                for i in range(3)
            ],
            header_ingredients=["flour", "milk"],
        )
        # Recompute proportions on the in-memory variant so we have a
        # baseline to compare against.
        for row, qty in zip(
            original.normalized_rows, [200, 210, 220], strict=True
        ):
            total = float(qty) + 200.0
            row.proportions.update(
                flour=qty / total * 100,
                milk=200.0 / total * 100,
            )

        manifest = emit_variants([original], tmp_path)
        entry = manifest.variants[0]
        csv_path = tmp_path / entry.csv_path

        rebuilt = reconstruct_variant(entry, csv_path)

        assert rebuilt.variant_id == original.variant_id
        assert rebuilt.canonical_ingredients == original.canonical_ingredients
        assert rebuilt.cooking_methods == original.cooking_methods
        assert len(rebuilt.normalized_rows) == len(original.normalized_rows)
        for orig_row, rebuilt_row in zip(
            original.normalized_rows, rebuilt.normalized_rows, strict=True
        ):
            for ing in original.header_ingredients:
                assert orig_row.proportions[ing] == pytest.approx(
                    rebuilt_row.proportions[ing], rel=1e-3
                )

    def test_canonical_ingredients_preserved_from_manifest(
        self, tmp_path: Path
    ) -> None:
        """Non-header canonical ingredients are preserved on the rebuilt
        variant even though no CSV row carries values for them. Their
        stats will be zero (limitation documented in the module
        docstring), but the canonical_ingredient_set is intact, so the
        variant_id stays stable across the import."""
        original = MergedVariantResult(
            variant_title="pancakes",
            canonical_ingredients=frozenset(
                {"flour", "milk", "shortening"}  # shortening not in header
            ),
            cooking_methods=frozenset(),
            normalized_rows=[
                MergedNormalizedRow(
                    url=f"https://x/{i}",
                    title="pancakes",
                    corpus="recipenlg",
                    cells={"flour": "200 g", "milk": "200 g"},
                    proportions={"flour": 50.0, "milk": 50.0},
                )
                for i in range(3)
            ],
            header_ingredients=["flour", "milk"],
        )
        manifest = emit_variants([original], tmp_path)
        entry = manifest.variants[0]
        csv_path = tmp_path / entry.csv_path

        rebuilt = reconstruct_variant(entry, csv_path)
        assert rebuilt.canonical_ingredients == original.canonical_ingredients
        assert rebuilt.variant_id == original.variant_id


class TestReconstructAndEmitToDb:
    """The shipped use case: reconstruct + emit_variants_to_db.

    This is what scripts/import_merged_artifacts.py does end-to-end.
    Verifies the full import path produces queryable variants — without
    this, render_drop.py can't render the imported variants.
    """

    def test_end_to_end_into_recipes_db(self, tmp_path: Path) -> None:
        original = MergedVariantResult(
            variant_title="pannkakor",
            canonical_ingredients=frozenset({"flour", "milk"}),
            cooking_methods=frozenset(),
            normalized_rows=[
                MergedNormalizedRow(
                    url=f"https://x/{i}",
                    title="pannkakor",
                    corpus="recipenlg",
                    cells={"flour": f"{200 + i * 10} g", "milk": "200 g"},
                    proportions={
                        "flour": (200.0 + i * 10) / (400.0 + i * 10) * 100,
                        "milk": 200.0 / (400.0 + i * 10) * 100,
                    },
                )
                for i in range(4)
            ],
            header_ingredients=["flour", "milk"],
        )
        emit_variants([original], tmp_path)

        rebuilt = list(reconstruct_variants(tmp_path))
        assert len(rebuilt) == 1

        db = CatalogDB.in_memory()
        written = emit_variants_to_db(rebuilt, db)
        assert written == 1

        # Variant + members + stats are all queryable — render_drop.py's
        # three reads succeed.
        variants = db.list_variants()
        assert len(variants) == 1
        assert db.get_variant_members(variants[0].variant_id)
        stats = db.get_ingredient_stats(variants[0].variant_id)
        assert {s.canonical_name for s in stats} == {"flour", "milk"}
        # Stats are in fraction form (matches the schema invariant).
        for s in stats:
            assert 0.0 < s.mean_proportion < 1.0

    def test_skips_missing_csv_with_warning(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        # Manifest references a CSV that doesn't exist — should log and
        # skip rather than crash.
        manifest = Manifest(
            variants=[
                VariantManifestEntry(
                    variant_id="abc123",
                    title="ghost",
                    canonical_ingredients=("flour",),
                    cooking_methods=(),
                    n_recipes=0,
                    csv_path="ghost_abc123.csv",
                    source_urls=(),
                )
            ]
        )
        manifest.write(tmp_path / "manifest.json")

        with caplog.at_level("WARNING"):
            rebuilt = list(reconstruct_variants(tmp_path))
        assert rebuilt == []
        assert any("not found" in r.message for r in caplog.records)
