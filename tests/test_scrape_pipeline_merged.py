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
        """3 identical recipes dedup to 1 unique row — below l3_min."""
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
        # 3 identical rows dedup to 1 — below the default l3_min of 3.
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

    def test_l3_splits_l2_group_by_cooking_method(self) -> None:
        """Two cooking_methods at sufficient size → two separate variants."""
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
            l3_min_variant_size=3,
        )

        assert len(variants) == 2
        method_sets = {v.cooking_methods for v in variants}
        assert method_sets == {frozenset({"stekt"}), frozenset({"i ugn"})}
        assert variants[0].variant_id != variants[1].variant_id

    def test_l3_noop_on_pure_recipenlg_stream(self) -> None:
        """RecipeNLG rows carry no cooking_methods — all in unknown bucket."""
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
            l3_min_variant_size=3,
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
            l3_min_variant_size=3,
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
            l3_min_variant_size=3,
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
            l3_min_variant_size=3,
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
