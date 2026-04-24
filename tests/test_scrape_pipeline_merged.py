"""Tests for the merged-pipeline emission layer."""

from __future__ import annotations

from pathlib import Path

from rational_recipes.scrape.manifest import Manifest, compute_variant_id
from rational_recipes.scrape.merge import MergedRecipe
from rational_recipes.scrape.parse import ParsedIngredient
from rational_recipes.scrape.pipeline_merged import (
    MergedNormalizedRow,
    MergedVariantResult,
    build_variants,
    emit_variants,
    normalize_merged_row,
)
from rational_recipes.scrape.recipenlg import Recipe


def _parsed(ing: str, qty: float, unit: str, prep: str = "") -> ParsedIngredient:
    return ParsedIngredient(
        ingredient=ing,
        quantity=qty,
        unit=unit,
        preparation=prep,
        raw=f"{qty} {unit} {ing}".strip(),
    )


def _make_merged(
    title: str,
    ingredient_lines: tuple[str, ...],
    ingredient_names: frozenset[str],
    url: str = "https://example.com/r",
    corpus: str = "recipenlg",
) -> MergedRecipe:
    src = Recipe(
        row_index=0,
        title=title,
        ingredients=ingredient_lines,
        ner=tuple(ingredient_names),
        source="test",
        link=url,
    )
    return MergedRecipe(
        title=title,
        ingredients=ingredient_lines,
        ingredient_names=ingredient_names,
        url=url,
        cooking_methods=frozenset(),
        corpus=corpus,
        source=src,
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


class TestNormalizeMergedRow:
    def test_empty_parsed_returns_none(self) -> None:
        row, skipped = normalize_merged_row("u", "t", "wdc", [])
        assert row is None
        assert skipped == []

    def test_fully_resolved_row_proportions_sum_to_100(self) -> None:
        # flour 200g + milk 200g + egg (MEDIUM ≈ 50g) → proportions sum to 100.
        row, skipped = normalize_merged_row(
            "https://x/r/1",
            "Pancakes",
            "recipenlg",
            [
                _parsed("flour", 200, "g"),
                _parsed("milk", 200, "g"),
            ],
        )
        assert row is not None
        assert skipped == []
        assert "flour" in row.cells
        assert "milk" in row.cells
        total_prop = sum(row.proportions.values())
        assert abs(total_prop - 100.0) < 1e-6

    def test_db_miss_recorded_and_skipped(self) -> None:
        row, skipped = normalize_merged_row(
            "u",
            "t",
            "wdc",
            [
                _parsed("flour", 100, "g"),
                _parsed("zzzunknownfood", 10, "g"),
            ],
        )
        assert row is not None
        assert "flour" in row.cells
        assert "zzzunknownfood" not in row.cells
        assert any("zzzunknownfood" in s for s in skipped)

    def test_unit_synonym_registered_in_factory(self) -> None:
        # "cups" is a UnitFactory synonym for CUP, so it resolves directly
        # without needing the alias fallback — cell keeps the original word.
        row, _skipped = normalize_merged_row(
            "u",
            "t",
            "wdc",
            [_parsed("flour", 1, "cups")],
        )
        assert row is not None
        assert row.cells["flour"] == "1 cups"

    def test_alias_fallback_when_not_a_factory_synonym(self) -> None:
        # "ounce" (singular) is not a direct UnitFactory synonym — alias
        # maps it to "oz", which is. Cell shows the resolved canonical form.
        row, _skipped = normalize_merged_row(
            "u",
            "t",
            "wdc",
            [_parsed("flour", 2, "ounce")],
        )
        assert row is not None
        assert row.cells["flour"] == "2 oz"

    def test_unknown_unit_skipped_with_note(self) -> None:
        row, skipped = normalize_merged_row(
            "u",
            "t",
            "wdc",
            [
                _parsed("flour", 200, "g"),
                _parsed("salt", 1, "handful"),
            ],
        )
        assert row is not None
        assert "salt" not in row.cells
        assert any("unknown unit" in s and "handful" in s for s in skipped)

    def test_zero_quantity_kept_as_zero(self) -> None:
        row, _skipped = normalize_merged_row(
            "u",
            "t",
            "wdc",
            [
                _parsed("flour", 100, "g"),
                _parsed("salt", 0, "g"),
            ],
        )
        assert row is not None
        assert row.cells["salt"] == "0"
        # salt contributes 0 to proportion numerator; flour is 100%.
        assert abs(row.proportions["flour"] - 100.0) < 1e-6
        assert row.proportions["salt"] == 0.0


class TestBuildVariants:
    def test_empty_input_no_variants(self) -> None:
        variants, stats = build_variants(
            [],
            parse_fn=lambda lines: [],
            l1_min_group_size=2,
            l2_similarity_threshold=0.6,
            l2_min_group_size=2,
        )
        assert variants == []
        assert stats.l1_groups_kept == 0
        assert stats.l2_variants_kept == 0

    def test_below_l1_min_drops_group(self) -> None:
        merged = [
            _make_merged(
                "pannkakor",
                ("1 dl flour",),
                frozenset({"flour"}),
                url="https://x/1",
            ),
        ]
        variants, _stats = build_variants(
            merged,
            parse_fn=lambda lines: [_parsed("flour", 100, "g")],
            l1_min_group_size=3,  # only 1 recipe in group; below min
            l2_similarity_threshold=0.6,
            l2_min_group_size=1,
        )
        assert variants == []

    def test_builds_variant_from_homogeneous_group(self) -> None:
        lines = ("200 g flour", "200 ml milk")
        merged = [
            _make_merged(
                "pannkakor", lines, frozenset({"flour", "milk"}), url=f"https://x/{i}"
            )
            for i in range(3)
        ]

        def fake_parse(lines: list[str]) -> list[ParsedIngredient | None]:
            return [
                _parsed("flour", 200, "g"),
                _parsed("milk", 200, "g"),
            ]

        variants, stats = build_variants(
            merged,
            parse_fn=fake_parse,
            l1_min_group_size=2,
            l2_similarity_threshold=0.6,
            l2_min_group_size=2,
        )
        assert len(variants) == 1
        v = variants[0]
        # 3 identical rows → 2 should be dedup'd.
        assert stats.rows_parsed == 3
        assert stats.rows_dedup_dropped == 2
        # After dedup, one survivor.
        assert len(v.normalized_rows) == 1
        assert set(v.header_ingredients) == {"flour", "milk"}

    def test_parse_failure_skipped(self) -> None:
        merged = [
            _make_merged(
                "pannkakor",
                ("1 dl flour",),
                frozenset({"flour"}),
                url=f"https://x/{i}",
            )
            for i in range(3)
        ]

        def always_fail(lines: list[str]) -> list[ParsedIngredient | None]:
            return [None]

        variants, stats = build_variants(
            merged,
            parse_fn=always_fail,
            l1_min_group_size=2,
            l2_similarity_threshold=0.6,
            l2_min_group_size=2,
        )
        assert variants == []
        assert stats.rows_parsed == 3
        assert stats.rows_normalized == 0

    def test_db_misses_aggregated(self) -> None:
        merged = [
            _make_merged(
                "pannkakor",
                ("1",),
                frozenset({"flour"}),
                url=f"https://x/{i}",
            )
            for i in range(3)
        ]

        def fake_parse(lines: list[str]) -> list[ParsedIngredient | None]:
            return [
                _parsed("flour", 100, "g"),
                _parsed("zzzunknownfood", 5, "g"),
            ]

        _variants, stats = build_variants(
            merged,
            parse_fn=fake_parse,
            l1_min_group_size=2,
            l2_similarity_threshold=0.6,
            l2_min_group_size=2,
        )
        # Each of 3 recipes produced one unresolved "zzzunknownfood".
        assert stats.db_misses.get("zzzunknownfood") == 3
