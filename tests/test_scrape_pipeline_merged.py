"""Tests for the merged-pipeline emission layer."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from rational_recipes.catalog_db import (
    CatalogDB,
    ParsedLineRow,
    parsed_to_json,
)
from rational_recipes.scrape.manifest import Manifest, compute_variant_id
from rational_recipes.scrape.merge import MergedRecipe
from rational_recipes.scrape.parse import ParsedIngredient
from rational_recipes.scrape.pipeline_merged import (
    MergedNormalizedRow,
    MergedVariantResult,
    ParseCache,
    ProgressEvent,
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
    cooking_methods: frozenset[str] = frozenset(),
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
        cooking_methods=cooking_methods,
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


class TestNormalizeMergedRowDirections:
    """RationalRecipes-15g4 / F5: directions_text propagates through
    normalize_merged_row to the resulting MergedNormalizedRow."""

    def test_directions_text_default_is_none(self) -> None:
        row, _skipped = normalize_merged_row(
            "u",
            "t",
            "recipenlg",
            [_parsed("flour", 100, "g")],
        )
        assert row is not None
        assert row.directions_text is None

    def test_directions_text_passed_through(self) -> None:
        row, _skipped = normalize_merged_row(
            "u",
            "t",
            "recipenlg",
            [_parsed("flour", 100, "g")],
            directions_text="1. mix\n2. bake",
        )
        assert row is not None
        assert row.directions_text == "1. mix\n2. bake"


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
        # The alias map (``_UNIT_ALIASES``) is a safety net for unit names
        # the LLM might emit that aren't direct UnitFactory synonyms.
        # After r6w added "ounce"/"pound"/"kilogram" singulars to the
        # factory directly, every entry in the alias table now resolves
        # via the direct path, and the cell preserves the input wording
        # (matches ``test_unit_synonym_registered_in_factory``). The
        # alias map is kept as defensive code in case a future model
        # emits something the factory hasn't seen yet.
        row, _skipped = normalize_merged_row(
            "u",
            "t",
            "wdc",
            [_parsed("flour", 2, "ounce")],
        )
        assert row is not None
        # "ounce" is now a direct UnitFactory synonym → cell keeps the
        # input wording.
        assert row.cells["flour"] == "2 ounce"

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

    def test_identical_rows_deduped_below_min_are_dropped(self) -> None:
        """3 identical recipes dedup to 1 unique row — below min_variant_size."""
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
            min_variant_size=2,
        )
        # 3 identical rows dedup to 1 — below the explicit min_variant_size of 2.
        assert stats.rows_parsed == 3
        assert stats.rows_dedup_dropped == 2
        assert len(variants) == 0

    def test_builds_variant_from_distinct_rows(self) -> None:
        """3 recipes with varied proportions survive dedup and produce a variant."""
        merged = [
            _make_merged(
                "pannkakor",
                (f"{200 + i * 10} g flour", f"{200 - i * 10} ml milk"),
                frozenset({"flour", "milk"}),
                url=f"https://x/{i}",
            )
            for i in range(3)
        ]

        def fake_parse(lines: list[str]) -> list[ParsedIngredient | None]:
            parts = lines[0].split()
            qty = float(parts[0])
            return [
                _parsed("flour", qty, "g"),
                _parsed("milk", 400 - qty, "g"),
            ]

        variants, stats = build_variants(
            merged,
            parse_fn=fake_parse,
            l1_min_group_size=2,
            l2_similarity_threshold=0.6,
            l2_min_group_size=2,
            min_variant_size=2,
        )
        assert len(variants) == 1
        assert stats.rows_parsed == 3
        assert set(variants[0].header_ingredients) == {"flour", "milk"}

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

    def test_cooking_methods_never_split_variants(self) -> None:
        """RationalRecipes-gc7: L3 cookingMethod partition removed.

        Recipes with different cooking_methods sharing one L1+L2 cluster
        now collapse into a single variant whose ``cooking_methods`` is
        the empty frozenset. Per-recipe method data is still available
        through the underlying MergedRecipe.
        """
        # Quantities vary by 50g per recipe so dedup buckets don't collide
        # (bucket width is 2 g-per-100g; 50g shift in a ~400g recipe ≈ 12%).
        fried = [
            _make_merged(
                "pannkakor",
                (f"{200 + i * 50} g flour", f"{200 - i * 50} ml milk"),
                frozenset({"flour", "milk"}),
                url=f"https://x/fried/{i}",
                corpus="wdc",
                cooking_methods=frozenset({"stekt"}),
            )
            for i in range(3)
        ]
        baked = [
            _make_merged(
                "pannkakor",
                (f"{150 + i * 50} g flour", f"{300 - i * 50} ml milk"),
                frozenset({"flour", "milk"}),
                url=f"https://x/baked/{i}",
                corpus="wdc",
                cooking_methods=frozenset({"i ugn"}),
            )
            for i in range(3)
        ]

        def fake_parse(lines: list[str]) -> list[ParsedIngredient | None]:
            out: list[ParsedIngredient | None] = []
            for line in lines:
                parts = line.split()
                qty = float(parts[0])
                unit = parts[1]
                ing = parts[-1]
                out.append(_parsed(ing, qty, unit))
            return out

        variants, _stats = build_variants(
            fried + baked,
            parse_fn=fake_parse,
            l1_min_group_size=2,
            l2_similarity_threshold=0.6,
            l2_min_group_size=2,
            min_variant_size=3,
        )

        assert len(variants) == 1
        assert variants[0].cooking_methods == frozenset()

    def test_cooking_methods_empty_on_pure_recipenlg_stream(self) -> None:
        """RecipeNLG rows carry no cooking_methods — variant gets empty set."""
        # Two ingredients with varying proportions so dedup doesn't collapse.
        rows = [
            _make_merged(
                "pannkakor",
                (f"{200 + i * 50} g flour", f"{300 - i * 50} g milk"),
                frozenset({"flour", "milk"}),
                url=f"https://x/{i}",
                corpus="recipenlg",
                cooking_methods=frozenset(),
            )
            for i in range(4)
        ]

        def fake_parse(lines: list[str]) -> list[ParsedIngredient | None]:
            out: list[ParsedIngredient | None] = []
            for line in lines:
                parts = line.split()
                out.append(_parsed(parts[-1], float(parts[0]), "g"))
            return out

        variants, _stats = build_variants(
            rows,
            parse_fn=fake_parse,
            l1_min_group_size=2,
            l2_similarity_threshold=0.6,
            l2_min_group_size=2,
            min_variant_size=3,
        )
        assert len(variants) == 1
        assert variants[0].cooking_methods == frozenset()


class TestIngredientFrequencyFilter:
    """RationalRecipes-70o: filter low-frequency noise at variant formation."""

    def test_low_freq_ingredient_dropped_from_canonical(self) -> None:
        """ketchup appears in 1/10 = 10% — at threshold, kept; raise to
        1/15 = ~6.7% and it must drop."""
        merged = []
        for i in range(15):
            ingredients: tuple[str, ...] = (f"{200 + i * 5} g flour",
                                            f"{300 - i * 5} g milk")
            names = {"flour", "milk"}
            if i == 0:
                ingredients = ingredients + ("1 g ketchup",)
                names = names | {"ketchup"}
            merged.append(
                _make_merged(
                    "pancakes",
                    ingredients,
                    frozenset(names),
                    url=f"https://x/{i}",
                )
            )

        def fake_parse(lines: list[str]) -> list[ParsedIngredient | None]:
            out: list[ParsedIngredient | None] = []
            for line in lines:
                parts = line.split()
                out.append(_parsed(parts[-1], float(parts[0]), "g"))
            return out

        variants, _stats = build_variants(
            merged,
            parse_fn=fake_parse,
            l1_min_group_size=2,
            l2_similarity_threshold=0.6,
            l2_min_group_size=2,
            min_variant_size=3,
        )
        assert len(variants) == 1
        assert "ketchup" not in variants[0].canonical_ingredients
        assert {"flour", "milk"} <= variants[0].canonical_ingredients

    def test_filter_does_not_fire_below_min_n(self) -> None:
        """With < 5 recipes the filter is skipped — small variants
        keep all their ingredients regardless of how rare any one is."""
        merged = []
        for i in range(4):
            ingredients = (f"{200 + i * 10} g flour", f"{300 - i * 10} g milk")
            names = {"flour", "milk"}
            if i == 0:
                ingredients = ingredients + ("1 g ketchup",)
                names = names | {"ketchup"}
            merged.append(
                _make_merged(
                    "pancakes",
                    ingredients,
                    frozenset(names),
                    url=f"https://x/{i}",
                )
            )

        def fake_parse(lines: list[str]) -> list[ParsedIngredient | None]:
            out: list[ParsedIngredient | None] = []
            for line in lines:
                parts = line.split()
                out.append(_parsed(parts[-1], float(parts[0]), "g"))
            return out

        variants, _stats = build_variants(
            merged,
            parse_fn=fake_parse,
            l1_min_group_size=2,
            l2_similarity_threshold=0.6,
            l2_min_group_size=2,
            min_variant_size=3,
        )
        assert len(variants) == 1
        assert "ketchup" in variants[0].canonical_ingredients

    def test_variant_id_reflects_post_filter_set(self) -> None:
        """The variant_id is computed from the filtered canonical set —
        so two clusters whose pre-filter sets differ only in noise
        collapse to the same id."""
        # Cluster A: 10 recipes, ketchup in first row only (10%, kept at
        # exactly threshold). Cluster B: 10 recipes, mustard in first
        # row only. Different pre-filter sets, but if we made each noise
        # ingredient sub-threshold (1/15 each) the ids would collapse.
        # Easier proof: compute the expected id from {flour, milk} and
        # compare against a 15-row variant where only flour+milk survive.
        from rational_recipes.scrape.grouping import normalize_title
        from rational_recipes.scrape.manifest import compute_variant_id

        merged = []
        for i in range(15):
            ingredients = (f"{200 + i * 5} g flour", f"{300 - i * 5} g milk")
            names = {"flour", "milk"}
            if i == 0:
                ingredients = ingredients + ("1 g ketchup",)
                names = names | {"ketchup"}
            merged.append(
                _make_merged(
                    "pancakes",
                    ingredients,
                    frozenset(names),
                    url=f"https://x/{i}",
                )
            )

        def fake_parse(lines: list[str]) -> list[ParsedIngredient | None]:
            out: list[ParsedIngredient | None] = []
            for line in lines:
                parts = line.split()
                out.append(_parsed(parts[-1], float(parts[0]), "g"))
            return out

        variants, _ = build_variants(
            merged,
            parse_fn=fake_parse,
            l1_min_group_size=2,
            l2_similarity_threshold=0.6,
            l2_min_group_size=2,
            min_variant_size=3,
        )
        assert len(variants) == 1
        expected = compute_variant_id(
            normalize_title("pancakes"), {"flour", "milk"}, set()
        )
        assert variants[0].variant_id == expected


class TestMergeDuplicateVariants:
    """RationalRecipes-70o side effect: variants sharing a variant_id merge."""

    def test_two_clusters_collapse_when_noise_filtered(self) -> None:
        """Two L2 clusters distinguished only by sub-threshold noise
        share a variant_id post-filter and merge into one variant whose
        normalized_rows is the union of both clusters' rows."""
        # Cluster A: 8 recipes with ketchup as the only "extra"; ketchup
        # appears in 1/8 = 12.5% (above threshold within the cluster).
        # Cluster B: 8 recipes with mustard in 1/8 (above threshold
        # within the cluster). With l2_similarity_threshold low enough,
        # these are merged at L2 already; force them apart by making
        # ketchup/mustard distinct enough that L2 separates them. After
        # the freq filter, both end up with {flour, milk} and merge.
        a_rows = []
        for i in range(8):
            ingredients: tuple[str, ...] = (f"{200 + i * 5} g flour",
                                            f"{300 - i * 5} g milk")
            names = {"flour", "milk"}
            # ketchup in just the first row of cluster A
            if i == 0:
                ingredients = ingredients + ("1 g ketchup",)
                names = names | {"ketchup"}
            a_rows.append(
                _make_merged(
                    "pancakes",
                    ingredients,
                    frozenset(names),
                    url=f"https://a/{i}",
                )
            )
        # All A rows are flour+milk, so pre-filter they already share an
        # ingredient set with B's rows. Easiest direct test: build two
        # variants with the SAME variant_id by hand and feed them to
        # _merge_duplicate_variants.
        from rational_recipes.scrape.pipeline_merged import (
            _merge_duplicate_variants,
        )

        v1 = MergedVariantResult(
            variant_title="pancakes",
            canonical_ingredients=frozenset({"flour", "milk"}),
            cooking_methods=frozenset(),
            normalized_rows=[
                _row("https://a/1", {"flour": "100 g"}, {"flour": 30.0, "milk": 70.0}),
                _row("https://a/2", {"flour": "200 g"}, {"flour": 32.0, "milk": 68.0}),
            ],
            header_ingredients=["flour", "milk"],
        )
        v2 = MergedVariantResult(
            variant_title="pancakes",
            canonical_ingredients=frozenset({"flour", "milk"}),
            cooking_methods=frozenset(),
            normalized_rows=[
                _row("https://b/1", {"flour": "300 g"}, {"flour": 40.0, "milk": 60.0}),
                _row("https://b/2", {"flour": "400 g"}, {"flour": 42.0, "milk": 58.0}),
            ],
            header_ingredients=["flour", "milk"],
        )
        merged_list, _dropped = _merge_duplicate_variants(
            [v1, v2], bucket_size=2.0
        )
        assert len(merged_list) == 1
        urls = {r.url for r in merged_list[0].normalized_rows}
        assert urls == {"https://a/1", "https://a/2", "https://b/1", "https://b/2"}

    def test_noop_when_variant_ids_distinct(self) -> None:
        from rational_recipes.scrape.pipeline_merged import (
            _merge_duplicate_variants,
        )

        v1 = MergedVariantResult(
            variant_title="pancakes",
            canonical_ingredients=frozenset({"flour", "milk"}),
            cooking_methods=frozenset(),
            normalized_rows=[
                _row("https://a/1", {"flour": "100 g"}, {"flour": 30.0, "milk": 70.0}),
            ],
            header_ingredients=["flour", "milk"],
        )
        v2 = MergedVariantResult(
            variant_title="pancakes",
            canonical_ingredients=frozenset({"flour", "buttermilk"}),
            cooking_methods=frozenset(),
            normalized_rows=[
                _row(
                    "https://b/1",
                    {"flour": "100 g"},
                    {"flour": 30.0, "buttermilk": 70.0},
                ),
            ],
            header_ingredients=["flour", "buttermilk"],
        )
        merged_list, dropped = _merge_duplicate_variants([v1, v2], bucket_size=2.0)
        assert len(merged_list) == 2
        assert dropped == 0


class TestCapPerL1:
    """RationalRecipes-dos: cap variants per L1 to top-N by n_recipes."""

    def _variant(
        self, title: str, ingredients: tuple[str, ...], n_rows: int
    ) -> MergedVariantResult:
        rows = [
            _row(f"https://x/{title}/{i}", {"flour": "100 g"}, {"flour": 100.0})
            for i in range(n_rows)
        ]
        return MergedVariantResult(
            variant_title=title,
            canonical_ingredients=frozenset(ingredients),
            cooking_methods=frozenset(),
            normalized_rows=rows,
            header_ingredients=["flour"],
        )

    def test_keeps_top_n_per_l1(self) -> None:
        from rational_recipes.scrape.pipeline_merged import _cap_per_l1

        l1_a = [
            self._variant("pecan pie", ("flour", f"x{i}"), n_rows=10 * (10 - i))
            for i in range(8)
        ]  # 8 variants, sizes 100, 90, 80, 70, 60, 50, 40, 30
        l1_b = [
            self._variant("brownie", ("flour", f"y{i}"), n_rows=20 - i * 2)
            for i in range(3)
        ]  # 3 variants, sizes 20, 18, 16

        capped = _cap_per_l1(l1_a + l1_b, max_per_l1=5)
        sizes_by_title: dict[str, list[int]] = {}
        for v in capped:
            sizes_by_title.setdefault(v.variant_title, []).append(
                len(v.normalized_rows)
            )
        # pecan pie capped at top-5 (100, 90, 80, 70, 60); brownie unchanged.
        assert sorted(sizes_by_title["pecan pie"], reverse=True) == [
            100, 90, 80, 70, 60,
        ]
        assert sorted(sizes_by_title["brownie"], reverse=True) == [20, 18, 16]

    def test_zero_disables_cap(self) -> None:
        from rational_recipes.scrape.pipeline_merged import _cap_per_l1

        variants = [
            self._variant("pecan pie", ("flour", f"x{i}"), n_rows=10)
            for i in range(8)
        ]
        capped = _cap_per_l1(variants, max_per_l1=0)
        assert len(capped) == 8

    def test_deterministic_tiebreak_by_variant_id(self) -> None:
        from rational_recipes.scrape.pipeline_merged import _cap_per_l1

        # Three variants of identical size — cap to 2 should pick a stable
        # subset across runs (sorted by variant_id ascending after the
        # size-descending sort).
        a = self._variant("dish", ("flour", "a"), n_rows=10)
        b = self._variant("dish", ("flour", "b"), n_rows=10)
        c = self._variant("dish", ("flour", "c"), n_rows=10)
        first = _cap_per_l1([a, b, c], max_per_l1=2)
        second = _cap_per_l1([c, b, a], max_per_l1=2)
        assert [v.variant_id for v in first] == [v.variant_id for v in second]
        assert len(first) == 2

    def test_build_variants_caps_proliferation(self) -> None:
        """End-to-end: build_variants should cap top-N per L1.

        Cluster N L2 groups with distinct ingredient sets but the same
        L1 title. With max_variants_per_l1=2 only the two largest
        survive.
        """
        # Real ingredients (in the IngredientFactory DB) so they survive
        # normalize_merged_row's KeyError handling. Each cluster gets a
        # distinct "extra" plus shared flour/milk so L2 separates them
        # (Jaccard < 0.6).
        extras = ["sugar", "salt", "butter", "egg"]
        merged: list[MergedRecipe] = []
        for cluster_idx, extra in enumerate(extras):
            n = 7 - cluster_idx
            for i in range(n):
                merged.append(
                    _make_merged(
                        "pancakes",
                        (
                            f"{200 + i * 10} g flour",
                            f"{200 - i * 10} g milk",
                            f"100 g {extra}",
                        ),
                        frozenset({"flour", "milk", extra}),
                        url=f"https://x/{cluster_idx}/{i}",
                    )
                )

        def fake_parse(lines: list[str]) -> list[ParsedIngredient | None]:
            out: list[ParsedIngredient | None] = []
            for line in lines:
                parts = line.split()
                qty = float(parts[0])
                ing = parts[-1]
                out.append(_parsed(ing, qty, "g"))
            return out

        variants, _ = build_variants(
            merged,
            parse_fn=fake_parse,
            l1_min_group_size=2,
            l2_similarity_threshold=0.6,
            l2_min_group_size=2,
            min_variant_size=3,
            max_variants_per_l1=2,
        )
        # 4 clusters → 4 candidate variants → top-2 survive.
        assert len(variants) == 2
        sizes = sorted((len(v.normalized_rows) for v in variants), reverse=True)
        # Largest two clusters (size 7 and 6) survive.
        assert sizes == [7, 6]


class TestParseCache:
    """RationalRecipes-vj4b: parsed_ingredient_lines reuse."""

    def _seed_cache_row(
        self,
        db: CatalogDB,
        *,
        raw: str,
        parsed: ParsedIngredient,
        model: str,
        corpus: str = "wdc",
        recipe_id: str = "https://prior/r/0",
        line_index: int = 0,
        seed: int = 42,
    ) -> None:
        db.upsert_parsed_lines(
            [
                ParsedLineRow(
                    corpus=corpus,
                    recipe_id=recipe_id,
                    line_index=line_index,
                    raw_line=raw,
                    parsed_json=parsed_to_json(parsed),
                    model=model,
                    seed=seed,
                )
            ]
        )

    def test_cache_hit_skips_llm(self) -> None:
        """Lines whose parses are already cached short-circuit parse_fn."""
        db = CatalogDB.in_memory()
        try:
            cached = _parsed("flour", 1.0, "cup")
            self._seed_cache_row(db, raw="1 cup flour", parsed=cached, model="m")

            cache = ParseCache(db=db, model="m")
            llm = MagicMock(name="llm_parse_fn")

            result = cache.parse_with_cache(
                corpus="wdc",
                recipe_id="https://x/r/1",
                lines=["1 cup flour"],
                parse_fn=llm,
            )

            assert llm.call_count == 0
            assert len(result) == 1
            assert result[0] is not None
            assert result[0].ingredient == "flour"
            assert result[0].quantity == 1.0
            assert result[0].unit == "cup"
            # ``raw`` is reattached from the input line, not the cached row.
            assert result[0].raw == "1 cup flour"
        finally:
            db.close()

    def test_cache_miss_calls_llm_and_writes_back(self) -> None:
        """A line with no cached parse falls through to parse_fn and is
        persisted to ``parsed_ingredient_lines`` for future runs."""
        db = CatalogDB.in_memory()
        try:
            cache = ParseCache(db=db, model="m")
            new_parse = _parsed("egg", 2.0, "MEDIUM")
            llm = MagicMock(return_value=[new_parse])

            result = cache.parse_with_cache(
                corpus="wdc",
                recipe_id="https://x/r/1",
                lines=["2 eggs"],
                parse_fn=llm,
            )

            assert llm.call_count == 1
            assert llm.call_args.args[0] == ["2 eggs"]
            assert result == [new_parse]

            # The LLM result was written back to the cache.
            found, payload = db.lookup_cached_parse("2 eggs", model="m", seed=42)
            assert found is True
            assert payload is not None
            assert "egg" in payload
        finally:
            db.close()

    def test_mixed_hits_and_misses(self) -> None:
        """Only the missing lines reach parse_fn; results align with input order."""
        db = CatalogDB.in_memory()
        try:
            cached = _parsed("flour", 1.0, "cup")
            self._seed_cache_row(db, raw="1 cup flour", parsed=cached, model="m")

            cache = ParseCache(db=db, model="m")
            new_parse = _parsed("egg", 2.0, "MEDIUM")
            llm = MagicMock(return_value=[new_parse])

            result = cache.parse_with_cache(
                corpus="wdc",
                recipe_id="https://x/r/1",
                lines=["1 cup flour", "2 eggs"],
                parse_fn=llm,
            )

            # Only the cache miss reached the LLM.
            assert llm.call_count == 1
            assert llm.call_args.args[0] == ["2 eggs"]

            assert len(result) == 2
            assert result[0] is not None and result[0].ingredient == "flour"
            assert result[1] is not None and result[1].ingredient == "egg"

            # The previously-uncached line is now cached too.
            found, payload = db.lookup_cached_parse("2 eggs", model="m", seed=42)
            assert found is True
            assert payload is not None
        finally:
            db.close()

    def test_cached_failure_returns_none_without_calling_llm(self) -> None:
        """A cached NULL parsed_json signals a prior failure — no retry."""
        db = CatalogDB.in_memory()
        try:
            db.upsert_parsed_lines(
                [
                    ParsedLineRow(
                        corpus="wdc",
                        recipe_id="https://prior/r/0",
                        line_index=0,
                        raw_line="some gibberish line",
                        parsed_json=None,
                        model="m",
                        seed=42,
                    )
                ]
            )
            cache = ParseCache(db=db, model="m")
            llm = MagicMock()

            result = cache.parse_with_cache(
                corpus="wdc",
                recipe_id="https://x/r/1",
                lines=["some gibberish line"],
                parse_fn=llm,
            )
            assert llm.call_count == 0
            assert result == [None]
        finally:
            db.close()

    def test_empty_lines_returns_empty_no_llm(self) -> None:
        db = CatalogDB.in_memory()
        try:
            cache = ParseCache(db=db, model="m")
            llm = MagicMock()
            assert cache.parse_with_cache(
                corpus="wdc",
                recipe_id="https://x/r/1",
                lines=[],
                parse_fn=llm,
            ) == []
            assert llm.call_count == 0
        finally:
            db.close()

    def test_model_mismatch_is_a_miss(self) -> None:
        """Cache key includes ``model`` — a different model must miss."""
        db = CatalogDB.in_memory()
        try:
            cached = _parsed("flour", 1.0, "cup")
            self._seed_cache_row(db, raw="1 cup flour", parsed=cached, model="old")

            cache = ParseCache(db=db, model="new")
            new_parse = _parsed("flour", 1.0, "cup")
            llm = MagicMock(return_value=[new_parse])

            cache.parse_with_cache(
                corpus="wdc",
                recipe_id="https://x/r/1",
                lines=["1 cup flour"],
                parse_fn=llm,
            )
            assert llm.call_count == 1
        finally:
            db.close()


class TestBuildVariantsCacheReuse:
    """End-to-end: build_variants honors a ParseCache when supplied."""

    def _make_recipes(self, n: int) -> list[MergedRecipe]:
        return [
            _make_merged(
                "pancakes",
                (f"{200 + i * 10} g flour", f"{200 - i * 10} g milk"),
                frozenset({"flour", "milk"}),
                url=f"https://x/{i}",
            )
            for i in range(n)
        ]

    def _quantity_aware_parse(
        self, lines: list[str]
    ) -> list[ParsedIngredient | None]:
        out: list[ParsedIngredient | None] = []
        for line in lines:
            parts = line.split()
            qty = float(parts[0])
            ing = parts[-1]
            out.append(_parsed(ing, qty, "g"))
        return out

    def test_warm_cache_avoids_all_ollama_calls(self) -> None:
        """Pre-populating the cache for every raw line means parse_fn
        is never invoked — the F2 acceptance criterion."""
        db = CatalogDB.in_memory()
        try:
            recipes = self._make_recipes(3)
            # Seed cache for every raw line in every recipe.
            for r in recipes:
                for idx, line in enumerate(r.ingredients):
                    parsed_iter = self._quantity_aware_parse([line])
                    parsed = parsed_iter[0]
                    assert parsed is not None
                    db.upsert_parsed_lines(
                        [
                            ParsedLineRow(
                                corpus=r.corpus,
                                recipe_id=r.url,
                                line_index=idx,
                                raw_line=line,
                                parsed_json=parsed_to_json(parsed),
                                model="m",
                                seed=42,
                            )
                        ]
                    )

            llm = MagicMock(side_effect=AssertionError("LLM must not be called"))
            cache = ParseCache(db=db, model="m")

            variants, _stats = build_variants(
                recipes,
                parse_fn=llm,
                parse_cache=cache,
                l1_min_group_size=2,
                l2_similarity_threshold=0.6,
                l2_min_group_size=2,
                min_variant_size=2,
            )
            assert llm.call_count == 0
            assert len(variants) == 1
            assert {"flour", "milk"} <= variants[0].canonical_ingredients
        finally:
            db.close()

    def test_cold_cache_calls_llm_and_persists_results(self) -> None:
        """Empty cache → parse_fn invoked → results land in the cache."""
        db = CatalogDB.in_memory()
        try:
            recipes = self._make_recipes(3)
            llm = MagicMock(side_effect=self._quantity_aware_parse)
            cache = ParseCache(db=db, model="m")

            build_variants(
                recipes,
                parse_fn=llm,
                parse_cache=cache,
                l1_min_group_size=2,
                l2_similarity_threshold=0.6,
                l2_min_group_size=2,
                min_variant_size=2,
            )

            assert llm.call_count >= 1

            # Every raw line that the LLM saw is now in the cache.
            for r in recipes:
                for line in r.ingredients:
                    found, payload = db.lookup_cached_parse(
                        line, model="m", seed=42,
                    )
                    assert found is True, f"{line!r} not cached"
                    assert payload is not None
        finally:
            db.close()

    def test_rerun_after_cold_uses_cache_only(self) -> None:
        """Determinism on re-run: a second build_variants call with the
        warmed cache must not invoke the LLM, and must produce the same
        ``variant_id`` / ingredient set as the first run."""
        db = CatalogDB.in_memory()
        try:
            recipes = self._make_recipes(3)
            cache = ParseCache(db=db, model="m")

            llm_first = MagicMock(side_effect=self._quantity_aware_parse)
            variants_first, _ = build_variants(
                recipes,
                parse_fn=llm_first,
                parse_cache=cache,
                l1_min_group_size=2,
                l2_similarity_threshold=0.6,
                l2_min_group_size=2,
                min_variant_size=2,
            )
            assert llm_first.call_count >= 1

            llm_second = MagicMock(
                side_effect=AssertionError("re-run must hit cache only")
            )
            variants_second, _ = build_variants(
                recipes,
                parse_fn=llm_second,
                parse_cache=cache,
                l1_min_group_size=2,
                l2_similarity_threshold=0.6,
                l2_min_group_size=2,
                min_variant_size=2,
            )
            assert llm_second.call_count == 0

            assert len(variants_first) == len(variants_second) == 1
            assert variants_first[0].variant_id == variants_second[0].variant_id
            assert (
                variants_first[0].canonical_ingredients
                == variants_second[0].canonical_ingredients
            )
        finally:
            db.close()

    def test_no_cache_preserves_existing_behavior(self) -> None:
        """Backward compat: build_variants without parse_cache calls
        parse_fn directly, exactly as before vj4b."""
        recipes = self._make_recipes(3)
        llm = MagicMock(side_effect=self._quantity_aware_parse)
        variants, _stats = build_variants(
            recipes,
            parse_fn=llm,
            l1_min_group_size=2,
            l2_similarity_threshold=0.6,
            l2_min_group_size=2,
            min_variant_size=2,
        )
        # One parse_fn call per recipe.
        assert llm.call_count == 3
        assert len(variants) == 1


class TestProgressCallback:
    """RationalRecipes-1g5h / F8: build_variants emits ProgressEvents
    when a callback is registered."""

    def _quantity_aware_parse(
        self, lines: list[str]
    ) -> list[ParsedIngredient | None]:
        out: list[ParsedIngredient | None] = []
        for line in lines:
            parts = line.split()
            qty = float(parts[0])
            ing = parts[-1]
            out.append(_parsed(ing, qty, "g"))
        return out

    def _make_recipes(self, n: int) -> list:
        return [
            _make_merged(
                "pancakes",
                (f"{200 + i * 10} g flour", f"{200 - i * 10} g milk"),
                frozenset({"flour", "milk"}),
                url=f"https://x/{i}",
            )
            for i in range(n)
        ]

    def test_callback_receives_per_recipe_events(self) -> None:
        events: list[ProgressEvent] = []
        recipes = self._make_recipes(3)
        build_variants(
            recipes,
            parse_fn=self._quantity_aware_parse,
            l1_min_group_size=2,
            l2_similarity_threshold=0.6,
            l2_min_group_size=2,
            min_variant_size=2,
            progress_callback=events.append,
        )
        # One per-recipe event during the loop, plus one final summary.
        non_final = [e for e in events if not e.final]
        assert len(non_final) == 3
        # Final event marks the end of the run.
        finals = [e for e in events if e.final]
        assert len(finals) == 1
        assert finals[0].parsed_count == 3
        # total upper bound matches the cluster-passing recipes.
        assert finals[0].total >= 3

    def test_default_callback_is_none(self) -> None:
        """Backward compat: omitting progress_callback preserves the
        previous behavior (no-op for the new path)."""
        recipes = self._make_recipes(3)
        variants, _stats = build_variants(
            recipes,
            parse_fn=self._quantity_aware_parse,
            l1_min_group_size=2,
            l2_similarity_threshold=0.6,
            l2_min_group_size=2,
            min_variant_size=2,
        )
        assert len(variants) == 1

    def test_cache_hits_and_ollama_counters_track_lines(self) -> None:
        """ParseCache exposes per-line cache_hits / ollama_lines so the
        progress event can report them. F2-aware visibility."""
        from rational_recipes.catalog_db import (
            CatalogDB,
            ParsedLineRow,
            parsed_to_json,
        )
        recipes = self._make_recipes(3)
        db = CatalogDB.in_memory()
        try:
            cache = ParseCache(db=db, model="m")
            # Cold cache: every line is a miss → ollama_lines grows.
            events: list[ProgressEvent] = []
            build_variants(
                recipes,
                parse_fn=self._quantity_aware_parse,
                parse_cache=cache,
                l1_min_group_size=2,
                l2_similarity_threshold=0.6,
                l2_min_group_size=2,
                min_variant_size=2,
                progress_callback=events.append,
            )
            final = [e for e in events if e.final][0]
            assert final.cache_hits == 0
            assert final.ollama_lines > 0

            # Warm rerun: every line is a hit.
            events_warm: list[ProgressEvent] = []
            warm_cache = ParseCache(db=db, model="m")
            build_variants(
                recipes,
                parse_fn=self._quantity_aware_parse,
                parse_cache=warm_cache,
                l1_min_group_size=2,
                l2_similarity_threshold=0.6,
                l2_min_group_size=2,
                min_variant_size=2,
                progress_callback=events_warm.append,
            )
            final_warm = [e for e in events_warm if e.final][0]
            assert final_warm.cache_hits > 0
            assert final_warm.ollama_lines == 0

            # Suppress unused warning on imported types.
            _ = (ParsedLineRow, parsed_to_json)
        finally:
            db.close()


class TestParseConcurrency:
    """RationalRecipes-e6rl: parallel ingredient-line parsing dispatch.

    The byte-identical guarantee: ``build_variants(parse_concurrency=1)``
    and ``build_variants(parse_concurrency=N>1)`` MUST produce the same
    variant set on the same input — same ``variant_id``, same row order,
    same canonical ingredient set, same dedup outcome. The parallel
    dispatch only changes wall-clock; all derived state is deterministic.
    """

    def _quantity_aware_parse(
        self, lines: list[str]
    ) -> list[ParsedIngredient | None]:
        out: list[ParsedIngredient | None] = []
        for line in lines:
            parts = line.split()
            qty = float(parts[0])
            ing = parts[-1]
            out.append(_parsed(ing, qty, "g"))
        return out

    def _make_recipes(self, n: int) -> list[MergedRecipe]:
        return [
            _make_merged(
                "pancakes",
                (f"{200 + i * 10} g flour", f"{200 - i * 10} g milk"),
                frozenset({"flour", "milk"}),
                url=f"https://x/{i}",
            )
            for i in range(n)
        ]

    def _diverse_recipes(self) -> list[MergedRecipe]:
        """Several clusters across one L1 group so the parallel path
        actually exercises multiple cluster iterations + variants.
        """
        extras = ["sugar", "salt", "butter", "egg"]
        recipes: list[MergedRecipe] = []
        for cluster_idx, extra in enumerate(extras):
            for i in range(6 - cluster_idx):
                recipes.append(
                    _make_merged(
                        "pancakes",
                        (
                            f"{200 + i * 10} g flour",
                            f"{200 - i * 10} g milk",
                            f"100 g {extra}",
                        ),
                        frozenset({"flour", "milk", extra}),
                        url=f"https://x/{cluster_idx}/{i}",
                    )
                )
        return recipes

    def _summarize(
        self, variants: list[MergedVariantResult]
    ) -> list[tuple[str, tuple[str, ...], tuple[str, ...]]]:
        """Variant-set fingerprint robust to set ordering: sorted by id,
        with the row URLs in the order ``build_variants`` produced them.
        Captures variant_id (the merge-key) + canonical ingredients +
        the per-row URL sequence — those three pin down dedup + filter
        + freq-trim + ordering decisions byte-for-byte."""
        out = []
        for v in sorted(variants, key=lambda x: x.variant_id):
            urls = tuple(r.url for r in v.normalized_rows)
            ingredients = tuple(sorted(v.canonical_ingredients))
            out.append((v.variant_id, ingredients, urls))
        return out

    def test_concurrency_1_vs_4_byte_identical(self) -> None:
        """Regression assertion (RationalRecipes-e6rl acceptance):
        sequential and concurrent runs produce the same variant set."""
        recipes = self._diverse_recipes()

        seq_variants, seq_stats = build_variants(
            recipes,
            parse_fn=self._quantity_aware_parse,
            l1_min_group_size=2,
            l2_similarity_threshold=0.6,
            l2_min_group_size=2,
            min_variant_size=3,
            parse_concurrency=1,
        )
        par_variants, par_stats = build_variants(
            recipes,
            parse_fn=self._quantity_aware_parse,
            l1_min_group_size=2,
            l2_similarity_threshold=0.6,
            l2_min_group_size=2,
            min_variant_size=3,
            parse_concurrency=4,
        )

        assert self._summarize(seq_variants) == self._summarize(par_variants)
        # Stats counters that don't depend on dict iteration order:
        assert seq_stats.rows_parsed == par_stats.rows_parsed
        assert seq_stats.rows_normalized == par_stats.rows_normalized
        assert seq_stats.rows_dedup_dropped == par_stats.rows_dedup_dropped
        assert seq_stats.l1_groups_kept == par_stats.l1_groups_kept
        assert seq_stats.l2_variants_kept == par_stats.l2_variants_kept

    def test_concurrent_with_cache_byte_identical(self) -> None:
        """ParseCache (vj4b) under concurrent dispatch — F2-on-e6rl.

        Same seeded cache content, two runs (concurrency=1 and =4),
        identical variant set. Catches the failure mode where the
        cache lock leaks state across threads (e.g. counters out of
        order, or rows_to_write tied to the wrong recipe_id).
        """
        recipes = self._diverse_recipes()

        db_seq = CatalogDB.in_memory()
        db_par = CatalogDB.in_memory()
        try:
            cache_seq = ParseCache(db=db_seq, model="m")
            cache_par = ParseCache(db=db_par, model="m")

            seq_variants, _ = build_variants(
                recipes,
                parse_fn=self._quantity_aware_parse,
                parse_cache=cache_seq,
                l1_min_group_size=2,
                l2_similarity_threshold=0.6,
                l2_min_group_size=2,
                min_variant_size=3,
                parse_concurrency=1,
            )
            par_variants, _ = build_variants(
                recipes,
                parse_fn=self._quantity_aware_parse,
                parse_cache=cache_par,
                l1_min_group_size=2,
                l2_similarity_threshold=0.6,
                l2_min_group_size=2,
                min_variant_size=3,
                parse_concurrency=4,
            )

            assert self._summarize(seq_variants) == self._summarize(par_variants)
            # Cache_hits/ollama_lines differ legitimately across runs:
            # concurrent threads racing on a cold cache all see the
            # same novel line as a miss before any thread writes back,
            # so parallel runs make more redundant LLM calls than
            # sequential. The byte-identical guarantee is on the
            # *output* (variant_ingredient_stats), not on these
            # observational counters. Determinism still holds at the
            # per-line level because parse.py pins temperature=0 +
            # seed=42 — redundant calls return the same parse.
        finally:
            db_seq.close()
            db_par.close()

    def test_progress_events_in_input_order_under_concurrency(self) -> None:
        """F8 compatibility: events arrive in deterministic input order.

        Even under concurrency=4 the per-recipe events fire in the
        same input sequence as concurrency=1 — guaranteed by the
        ``executor.map`` iteration order in build_variants.
        """
        recipes = self._make_recipes(6)

        seq_events: list[ProgressEvent] = []
        build_variants(
            recipes,
            parse_fn=self._quantity_aware_parse,
            l1_min_group_size=2,
            l2_similarity_threshold=0.6,
            l2_min_group_size=2,
            min_variant_size=2,
            progress_callback=seq_events.append,
            parse_concurrency=1,
        )
        par_events: list[ProgressEvent] = []
        build_variants(
            recipes,
            parse_fn=self._quantity_aware_parse,
            l1_min_group_size=2,
            l2_similarity_threshold=0.6,
            l2_min_group_size=2,
            min_variant_size=2,
            progress_callback=par_events.append,
            parse_concurrency=4,
        )

        seq_counts = [e.parsed_count for e in seq_events if not e.final]
        par_counts = [e.parsed_count for e in par_events if not e.final]
        assert seq_counts == par_counts == [1, 2, 3, 4, 5, 6]

    def test_parallel_overlaps_blocking_parse_calls(self) -> None:
        """Smoke test: with a parse_fn that sleeps, concurrency=4 is
        meaningfully faster than concurrency=1 — confirms the executor
        actually overlaps the work (and exits the GIL during the
        ``time.sleep`` proxy for ``requests.post``).
        """
        import time as _time

        recipes = self._make_recipes(8)

        def slow_parse(lines: list[str]) -> list[ParsedIngredient | None]:
            _time.sleep(0.05)  # 50 ms ≈ a fast Ollama call
            return self._quantity_aware_parse(lines)

        t0 = _time.monotonic()
        build_variants(
            recipes,
            parse_fn=slow_parse,
            l1_min_group_size=2,
            l2_similarity_threshold=0.6,
            l2_min_group_size=2,
            min_variant_size=2,
            parse_concurrency=1,
        )
        seq_elapsed = _time.monotonic() - t0

        t0 = _time.monotonic()
        build_variants(
            recipes,
            parse_fn=slow_parse,
            l1_min_group_size=2,
            l2_similarity_threshold=0.6,
            l2_min_group_size=2,
            min_variant_size=2,
            parse_concurrency=4,
        )
        par_elapsed = _time.monotonic() - t0

        # 8 sleeps of 50 ms: sequential ≈ 400 ms, concurrency=4 ≈ 100 ms.
        # Generous bound to keep the test stable on loaded CI: parallel
        # is at least 1.5× faster than sequential.
        assert par_elapsed * 1.5 < seq_elapsed, (
            f"expected speedup; seq={seq_elapsed:.3f}s par={par_elapsed:.3f}s"
        )


class TestCliProgressPrinter:
    """Smoke test for ``scripts/scrape_merged.py``\047s _ProgressPrinter."""

    def test_throttle_prints_first_then_skips(self, capsys) -> None:
        import sys as _sys
        from pathlib import Path as _Path
        scripts_dir = _Path(__file__).resolve().parent.parent / "scripts"
        if str(scripts_dir) not in _sys.path:
            _sys.path.insert(0, str(scripts_dir))
        from scrape_merged import _ProgressPrinter

        printer = _ProgressPrinter(every_n=10, every_seconds=10.0)
        # First event clears the throttle (last_seconds is -every_seconds).
        printer(ProgressEvent(
            parsed_count=1, total=100, cache_hits=0, ollama_lines=1,
            elapsed_seconds=0.1, final=False,
        ))
        # Within the throttle window: must NOT print.
        printer(ProgressEvent(
            parsed_count=2, total=100, cache_hits=0, ollama_lines=2,
            elapsed_seconds=0.2, final=False,
        ))
        # Final event always prints.
        printer(ProgressEvent(
            parsed_count=3, total=100, cache_hits=1, ollama_lines=2,
            elapsed_seconds=0.3, final=True,
        ))
        captured = capsys.readouterr()
        # Two lines: first per-recipe + final summary. Substantive
        # fields appear in the output.
        assert "Progress: parsed 1/100" in captured.out
        assert "Final: parsed 3 recipes" in captured.out
        assert "cache_hits=1" in captured.out
        assert "ollama_lines=2" in captured.out
        assert "throughput=" in captured.out
        # Middle event (parsed_count=2) was throttled out.
        assert "parsed 2/" not in captured.out
