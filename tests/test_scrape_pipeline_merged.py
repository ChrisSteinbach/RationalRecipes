"""Tests for the merged-pipeline emission layer."""

from __future__ import annotations

from pathlib import Path

from rational_recipes.scrape.manifest import Manifest, compute_variant_id
from rational_recipes.scrape.pipeline_merged import (
    MergedNormalizedRow,
    MergedVariantResult,
    emit_variants,
)


def _row(
    url: str,
    cells: dict[str, str],
    proportions: dict[str, float],
    corpus: str = "wdc",
    title: str = "pannkakor",
) -> MergedNormalizedRow:
    return MergedNormalizedRow(
        url=url,
        title=title,
        corpus=corpus,
        cells=cells,
        proportions=proportions,
    )


class TestMergedVariantResult:
    def test_variant_id_is_stable(self) -> None:
        variant = MergedVariantResult(
            variant_title="Pannkakor",
            canonical_ingredients=frozenset({"flour", "milk", "egg"}),
            cooking_methods=frozenset({"stekt"}),
            normalized_rows=[],
            header_ingredients=[],
        )
        expected = compute_variant_id("pannkakor", {"flour", "milk", "egg"}, {"stekt"})
        assert variant.variant_id == expected

    def test_variant_id_unaffected_by_row_order(self) -> None:
        rows = [
            _row("https://a/1", {"flour": "100 g"}, {"flour": 100.0}),
            _row("https://b/2", {"flour": "200 g"}, {"flour": 200.0}),
        ]
        v1 = MergedVariantResult(
            variant_title="pannkakor",
            canonical_ingredients=frozenset({"flour"}),
            cooking_methods=frozenset(),
            normalized_rows=rows,
            header_ingredients=["flour"],
        )
        v2 = MergedVariantResult(
            variant_title="pannkakor",
            canonical_ingredients=frozenset({"flour"}),
            cooking_methods=frozenset(),
            normalized_rows=list(reversed(rows)),
            header_ingredients=["flour"],
        )
        assert v1.variant_id == v2.variant_id

    def test_to_csv_writes_header_and_rows(self) -> None:
        variant = MergedVariantResult(
            variant_title="pannkakor",
            canonical_ingredients=frozenset({"flour", "milk"}),
            cooking_methods=frozenset(),
            normalized_rows=[
                _row(
                    "u1",
                    {"flour": "100 g", "milk": "250 ml"},
                    {"flour": 28.5, "milk": 71.5},
                ),
                _row(
                    "u2",
                    {"flour": "200 g", "milk": "500 ml"},
                    {"flour": 28.5, "milk": 71.5},
                ),
            ],
            header_ingredients=["flour", "milk"],
        )
        csv_text = variant.to_csv()
        lines = csv_text.strip().splitlines()
        assert lines[0] == "flour,milk"
        assert lines[1] == "100 g,250 ml"
        assert lines[2] == "200 g,500 ml"

    def test_to_csv_fills_missing_cells_with_zero(self) -> None:
        variant = MergedVariantResult(
            variant_title="pannkakor",
            canonical_ingredients=frozenset({"flour", "milk", "egg"}),
            cooking_methods=frozenset(),
            normalized_rows=[
                _row(
                    "u1",
                    {"flour": "100 g", "milk": "250 ml"},
                    {"flour": 28.5, "milk": 71.5},
                ),
            ],
            header_ingredients=["flour", "milk", "egg"],
        )
        csv_text = variant.to_csv()
        lines = csv_text.strip().splitlines()
        assert lines[1] == "100 g,250 ml,0"

    def test_csv_filename_includes_variant_id(self) -> None:
        variant = MergedVariantResult(
            variant_title="Äppelpannkaka med vaniljyoghurt",
            canonical_ingredients=frozenset({"flour"}),
            cooking_methods=frozenset(),
            normalized_rows=[],
            header_ingredients=[],
        )
        name = variant.csv_filename()
        assert name.endswith(f"_{variant.variant_id}.csv")
        # Filesystem-safe: only \w and -
        assert all(c.isalnum() or c in "_-." for c in name)

    def test_dedup_in_place_collapses_identical_proportions(self) -> None:
        variant = MergedVariantResult(
            variant_title="pannkakor",
            canonical_ingredients=frozenset({"flour", "milk"}),
            cooking_methods=frozenset(),
            normalized_rows=[
                _row("u1", {"flour": "100 g"}, {"flour": 40.0, "milk": 50.0}),
                _row("u2", {"flour": "200 g"}, {"flour": 41.0, "milk": 51.0}),
                _row("u3", {"flour": "500 g"}, {"flour": 30.0, "milk": 60.0}),
            ],
            header_ingredients=["flour", "milk"],
        )
        dropped = variant.dedup_in_place()
        assert dropped == 1
        assert len(variant.normalized_rows) == 2

    def test_outlier_scores_aligned_with_rows(self) -> None:
        """Bead 0g3: each row gets a scalar distance-from-median score."""
        variant = MergedVariantResult(
            variant_title="pannkakor",
            canonical_ingredients=frozenset({"flour", "milk"}),
            cooking_methods=frozenset(),
            normalized_rows=[
                _row("u1", {"flour": "100 g"}, {"flour": 50.0, "milk": 50.0}),
                _row("u2", {"flour": "100 g"}, {"flour": 50.0, "milk": 50.0}),
                _row("u3", {"flour": "100 g"}, {"flour": 50.0, "milk": 50.0}),
                _row("u4", {"flour": "500 g"}, {"flour": 80.0, "milk": 20.0}),
            ],
            header_ingredients=["flour", "milk"],
        )
        scores = variant.outlier_scores()
        assert len(scores) == 4
        assert scores[0] == scores[1] == scores[2] == 0.0
        assert scores[3] > 0.0

    def test_manifest_entry_carries_outlier_scores(self) -> None:
        variant = MergedVariantResult(
            variant_title="pannkakor",
            canonical_ingredients=frozenset({"flour"}),
            cooking_methods=frozenset(),
            normalized_rows=[
                _row("u1", {"flour": "100 g"}, {"flour": 100.0}),
                _row("u2", {"flour": "100 g"}, {"flour": 100.0}),
            ],
            header_ingredients=["flour"],
        )
        entry = variant.to_manifest_entry("x.csv")
        assert entry.row_outlier_scores == (0.0, 0.0)

    def test_manifest_entry_single_row_has_empty_scores(self) -> None:
        """Single-row variants produce a (0.0,) score, but to_json_dict
        omits the field only when strictly empty. Single-row stays in."""
        variant = MergedVariantResult(
            variant_title="pannkakor",
            canonical_ingredients=frozenset({"flour"}),
            cooking_methods=frozenset(),
            normalized_rows=[
                _row("u1", {"flour": "100 g"}, {"flour": 100.0}),
            ],
            header_ingredients=["flour"],
        )
        entry = variant.to_manifest_entry("x.csv")
        assert entry.row_outlier_scores == (0.0,)


class TestEmitVariants:
    def test_round_trip_via_manifest(self, tmp_path: Path) -> None:
        variant = MergedVariantResult(
            variant_title="pannkakor",
            canonical_ingredients=frozenset({"flour", "milk"}),
            cooking_methods=frozenset(),
            normalized_rows=[
                _row(
                    "https://ica.se/r/1",
                    {"flour": "100 g", "milk": "250 ml"},
                    {"flour": 28.5, "milk": 71.5},
                ),
            ],
            header_ingredients=["flour", "milk"],
        )

        manifest = emit_variants([variant], tmp_path)

        # Manifest has the entry
        assert len(manifest.variants) == 1
        entry = manifest.variants[0]
        assert entry.variant_id == variant.variant_id
        assert entry.n_recipes == 1
        assert entry.source_urls == ("https://ica.se/r/1",)

        # CSV exists on disk at the advertised path
        csv_path = tmp_path / entry.csv_path
        assert csv_path.exists()
        assert csv_path.read_text(encoding="utf-8").startswith("flour,milk\n")

        # Manifest on disk matches what was returned
        reloaded = Manifest.read(tmp_path / "manifest.json")
        assert reloaded.variants == manifest.variants

    def test_empty_variants_are_skipped(self, tmp_path: Path) -> None:
        empty = MergedVariantResult(
            variant_title="ghost",
            canonical_ingredients=frozenset({"flour"}),
            cooking_methods=frozenset(),
            normalized_rows=[],
            header_ingredients=["flour"],
        )
        real = MergedVariantResult(
            variant_title="pannkakor",
            canonical_ingredients=frozenset({"flour"}),
            cooking_methods=frozenset(),
            normalized_rows=[_row("u1", {"flour": "100 g"}, {"flour": 100.0})],
            header_ingredients=["flour"],
        )

        manifest = emit_variants([empty, real], tmp_path)

        assert len(manifest.variants) == 1
        assert manifest.variants[0].variant_id == real.variant_id
        assert not (tmp_path / empty.csv_filename()).exists()

    def test_creates_output_directory(self, tmp_path: Path) -> None:
        out = tmp_path / "nested" / "output"
        variant = MergedVariantResult(
            variant_title="pannkakor",
            canonical_ingredients=frozenset({"flour"}),
            cooking_methods=frozenset(),
            normalized_rows=[_row("u1", {"flour": "100 g"}, {"flour": 100.0})],
            header_ingredients=["flour"],
        )
        emit_variants([variant], out)
        assert (out / "manifest.json").exists()

    def test_multiple_variants_get_unique_csvs(self, tmp_path: Path) -> None:
        """Same normalized title, different ingredient sets — CSV names
        must not collide because the variant_id suffix disambiguates."""
        a = MergedVariantResult(
            variant_title="pancakes",
            canonical_ingredients=frozenset({"flour", "milk", "egg"}),
            cooking_methods=frozenset(),
            normalized_rows=[_row("u1", {"flour": "1"}, {"flour": 100.0})],
            header_ingredients=["flour"],
        )
        b = MergedVariantResult(
            variant_title="pancakes",
            canonical_ingredients=frozenset({"flour", "buttermilk"}),
            cooking_methods=frozenset(),
            normalized_rows=[_row("u2", {"flour": "1"}, {"flour": 100.0})],
            header_ingredients=["flour"],
        )
        manifest = emit_variants([a, b], tmp_path)
        assert len({v.csv_path for v in manifest.variants}) == 2
        assert a.variant_id != b.variant_id
