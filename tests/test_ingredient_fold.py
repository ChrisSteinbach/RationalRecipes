"""Tests for the Pass 2 generic/specific ingredient fold (RationalRecipes-2p6)."""

from __future__ import annotations

from pathlib import Path

import pytest

from rational_recipes.catalog_db import CatalogDB
from rational_recipes.cli.backfill_ingredient_fold import _fold_one_variant
from rational_recipes.scrape.ingredient_fold import (
    FOLD_MAP,
    apply_fold_to_variant,
    families_present,
    pick_keeper,
)
from rational_recipes.scrape.pipeline_merged import (
    MergedNormalizedRow,
    MergedVariantResult,
)


def _row(
    cells: dict[str, str],
    proportions: dict[str, float],
    *,
    url: str = "https://example/r",
    title: str = "Pancakes",
) -> MergedNormalizedRow:
    return MergedNormalizedRow(
        url=url,
        title=title,
        corpus="recipenlg",
        cells=cells,
        proportions=proportions,
    )


def _variant(
    canonical: set[str],
    rows: list[MergedNormalizedRow],
    header: list[str] | None = None,
) -> MergedVariantResult:
    return MergedVariantResult(
        variant_title="Pancakes",
        canonical_ingredients=frozenset(canonical),
        cooking_methods=frozenset(),
        normalized_rows=rows,
        header_ingredients=header
        if header is not None
        else sorted(canonical),
    )


class TestFoldMapShape:
    def test_oil_family_present(self) -> None:
        assert "oil" in FOLD_MAP
        assert {"oil", "vegetable oil"} <= FOLD_MAP["oil"]

    def test_salt_family_present(self) -> None:
        assert "salt" in FOLD_MAP
        assert {"salt", "kosher salt"} <= FOLD_MAP["salt"]

    def test_butter_family_present(self) -> None:
        # Required by the bead's acceptance criterion (Basic Buttermilk
        # Pancakes folds butter + unsalted butter).
        assert "butter" in FOLD_MAP
        assert {"butter", "unsalted butter"} <= FOLD_MAP["butter"]

    def test_no_form_appears_in_two_families(self) -> None:
        seen: dict[str, str] = {}
        for family, forms in FOLD_MAP.items():
            for form in forms:
                assert (
                    form not in seen
                ), f"{form!r} in both {seen.get(form)} and {family}"
                seen[form] = family


class TestFamiliesPresent:
    def test_returns_only_families_with_two_or_more_forms(self) -> None:
        assert families_present(["salt", "kosher salt", "flour"]) == {
            "salt": ["kosher salt", "salt"]
        }

    def test_single_form_does_not_qualify(self) -> None:
        assert families_present(["salt", "flour"]) == {}

    def test_unrelated_canonicals_ignored(self) -> None:
        assert families_present(["cheddar", "cheese"]) == {}


class TestPickKeeper:
    def test_largest_total_wins(self) -> None:
        keeper = pick_keeper(
            ["salt", "kosher salt"],
            {"salt": 10.0, "kosher salt": 2.0},
        )
        assert keeper == "salt"

    def test_alphabetical_tiebreak_for_determinism(self) -> None:
        keeper = pick_keeper(
            ["salt", "kosher salt"],
            {"salt": 5.0, "kosher salt": 5.0},
        )
        # Sorted forms are ['kosher salt', 'salt'] — the first one with
        # max value wins under Python's max() iteration order.
        assert keeper == "kosher salt"

    def test_zero_totals_returns_alphabetical_first(self) -> None:
        keeper = pick_keeper(
            ["vegetable oil", "oil"],
            {"vegetable oil": 0.0, "oil": 0.0},
        )
        assert keeper == "oil"


class TestApplyFoldToVariant:
    def test_no_fold_when_unaffected(self) -> None:
        v = _variant(
            {"flour", "milk"},
            [_row({"flour": "100 g"}, {"flour": 30.0, "milk": 70.0})],
        )
        assert apply_fold_to_variant(v) is False
        assert v.canonical_ingredients == frozenset({"flour", "milk"})

    def test_folds_kosher_salt_into_salt_when_salt_dominates(self) -> None:
        v = _variant(
            {"flour", "salt", "kosher salt"},
            [
                _row(
                    {"flour": "100 g", "salt": "5 g", "kosher salt": "1 g"},
                    {"flour": 90.0, "salt": 5.0, "kosher salt": 1.0},
                ),
                _row(
                    {"flour": "100 g", "salt": "6 g"},
                    {"flour": 94.0, "salt": 6.0},
                ),
            ],
        )
        assert apply_fold_to_variant(v) is True
        assert v.canonical_ingredients == frozenset({"flour", "salt"})
        # Row 1: salt absorbs kosher salt — sum 5+1 = 6.
        assert v.normalized_rows[0].proportions == {"flour": 90.0, "salt": 6.0}
        assert "kosher salt" not in v.normalized_rows[0].proportions
        assert "kosher salt" not in v.normalized_rows[0].cells
        # Row 2: salt unchanged.
        assert v.normalized_rows[1].proportions == {"flour": 94.0, "salt": 6.0}

    def test_folds_butter_into_unsalted_butter_when_specific_dominates(
        self,
    ) -> None:
        # Mirrors the Basic Buttermilk Pancakes case where unsalted
        # butter (8.5 g) outweighs butter (0.2 g) — the keeper should be
        # ``unsalted butter`` so the merged display reads as the
        # higher-information form.
        v = _variant(
            {"flour", "butter", "unsalted butter"},
            [
                _row(
                    {"unsalted butter": "8.5 g", "butter": "0.2 g", "flour": "100 g"},
                    {"unsalted butter": 8.5, "butter": 0.2, "flour": 91.3},
                ),
                _row(
                    {"unsalted butter": "10 g", "flour": "100 g"},
                    {"unsalted butter": 10.0, "flour": 90.0},
                ),
            ],
        )
        assert apply_fold_to_variant(v) is True
        assert v.canonical_ingredients == frozenset({"flour", "unsalted butter"})
        # Row 1: unsalted butter absorbs butter (8.5 + 0.2 = 8.7).
        assert v.normalized_rows[0].proportions["unsalted butter"] == pytest.approx(
            8.7
        )

    def test_summed_mass_propagates_across_all_rows(self) -> None:
        v = _variant(
            {"oil", "vegetable oil"},
            [
                _row(
                    {"oil": "5 g", "vegetable oil": "10 g"},
                    {"oil": 5.0, "vegetable oil": 10.0},
                ),
                _row(
                    {"oil": "8 g", "vegetable oil": "12 g"},
                    {"oil": 8.0, "vegetable oil": 12.0},
                ),
            ],
        )
        apply_fold_to_variant(v)
        # Vegetable oil dominates (sum 22) → keeper = "vegetable oil".
        keeper_totals = [
            r.proportions["vegetable oil"] for r in v.normalized_rows
        ]
        assert keeper_totals == [15.0, 20.0]

    def test_header_drops_folded_form(self) -> None:
        v = _variant(
            {"flour", "salt", "kosher salt"},
            [
                _row(
                    {"flour": "100 g", "salt": "5 g", "kosher salt": "1 g"},
                    {"flour": 90.0, "salt": 5.0, "kosher salt": 1.0},
                ),
            ],
            header=["flour", "kosher salt", "salt"],
        )
        apply_fold_to_variant(v)
        assert "kosher salt" not in v.header_ingredients
        assert "salt" in v.header_ingredients

    def test_does_not_overfold_unrelated_substring(self) -> None:
        # ``red onion`` is a strict substring of ``onion`` but is NOT in
        # the fold map — the dfm commit's intent must hold.
        v = _variant(
            {"red onion", "onion", "flour"},
            [
                _row(
                    {"red onion": "30 g", "onion": "20 g", "flour": "100 g"},
                    {"red onion": 20.0, "onion": 13.0, "flour": 67.0},
                )
            ],
        )
        assert apply_fold_to_variant(v) is False
        assert v.canonical_ingredients == frozenset(
            {"red onion", "onion", "flour"}
        )

    def test_does_not_overfold_cheddar_into_cheese(self) -> None:
        v = _variant(
            {"cheddar", "cheese", "flour"},
            [
                _row(
                    {"cheddar": "30 g", "cheese": "20 g", "flour": "100 g"},
                    {"cheddar": 20.0, "cheese": 13.0, "flour": 67.0},
                )
            ],
        )
        assert apply_fold_to_variant(v) is False


class TestBuildVariantsIntegration:
    def test_pipeline_applies_fold_at_variant_build(self) -> None:
        """build_variants() should invoke the fold so the resulting
        variant has no generic/specific siblings."""
        from rational_recipes.scrape.merge import MergedRecipe
        from rational_recipes.scrape.parse import ParsedIngredient
        from rational_recipes.scrape.pipeline_merged import build_variants

        # Five recipes with the same title + ingredient set so they
        # form one L1 + one L2 cluster.
        recipes: list[MergedRecipe] = []
        for i in range(5):
            recipes.append(
                MergedRecipe(
                    title="Pancakes",
                    ingredients=(
                        "100 g flour",
                        "5 g salt",
                        "1 g kosher salt",
                    ),
                    ingredient_names=frozenset({"flour", "salt", "kosher salt"}),
                    url=f"https://example.com/r/{i}",
                    cooking_methods=frozenset(),
                    corpus="recipenlg",
                    source=None,
                )
            )

        canned = [
            ParsedIngredient(
                ingredient="flour", quantity=100, unit="g",
                preparation="", raw="100 g flour",
            ),
            ParsedIngredient(
                ingredient="salt", quantity=5, unit="g",
                preparation="", raw="5 g salt",
            ),
            ParsedIngredient(
                ingredient="kosher salt", quantity=1, unit="g",
                preparation="", raw="1 g kosher salt",
            ),
        ]

        variants, _ = build_variants(
            recipes,
            parse_fn=lambda lines: list(canned),
            l1_min_group_size=1,
            l2_similarity_threshold=0.5,
            l2_min_group_size=1,
            min_variant_size=1,
            max_variants_per_l1=10,
        )

        assert len(variants) == 1
        v = variants[0]
        # Folded: only one of {salt, kosher salt} survives.
        salt_forms = {"salt", "kosher salt"} & v.canonical_ingredients
        assert len(salt_forms) == 1


class TestBackfillCli:
    def _seed_basic_buttermilk_variant(self, db: CatalogDB) -> str:
        """Mirrors the bead's example: 14 ingredients including three
        generic/specific pairs. Uses _compute_ingredient_stats indirectly
        by going through upsert_variant."""
        rows: list[MergedNormalizedRow] = []
        for i in range(5):
            rows.append(
                _row(
                    {
                        "flour": "100 g",
                        "salt": "5 g",
                        "kosher salt": "1 g",
                        "butter": "1 g",
                        "unsalted butter": "10 g",
                        "oil": "8 g",
                        "vegetable oil": "5 g",
                        "egg": "50 g",
                        "milk": "100 g",
                        "sugar": "5 g",
                        "buttermilk": "200 g",
                        "baking powder": "2 g",
                        "baking soda": "1 g",
                        "vanilla": "1 g",
                    },
                    {
                        "flour": 20.0,
                        "salt": 1.0,
                        "kosher salt": 0.2,
                        "butter": 0.2,
                        "unsalted butter": 2.0,
                        "oil": 1.6,
                        "vegetable oil": 1.0,
                        "egg": 10.0,
                        "milk": 20.0,
                        "sugar": 1.0,
                        "buttermilk": 40.0,
                        "baking powder": 0.4,
                        "baking soda": 0.2,
                        "vanilla": 0.2,
                    },
                    url=f"https://example.com/r/{i}",
                    title="Basic Buttermilk Pancakes",
                )
            )
        v = MergedVariantResult(
            variant_title="basic buttermilk pancakes",
            canonical_ingredients=frozenset(rows[0].cells.keys()),
            cooking_methods=frozenset(),
            normalized_rows=rows,
            header_ingredients=sorted(rows[0].cells.keys()),
        )
        db.upsert_variant(v, l1_key="basic buttermilk pancakes")
        return v.variant_id

    def test_dry_run_does_not_mutate(self, tmp_path: Path) -> None:
        db_path = tmp_path / "recipes.db"
        db = CatalogDB.open(db_path)
        try:
            vid = self._seed_basic_buttermilk_variant(db)
            before = {
                s.canonical_name for s in db.get_ingredient_stats(vid)
            }
            assert "kosher salt" in before
            changed, _ = _fold_one_variant(
                db,
                vid,
                tuple(sorted(before)),
                apply=False,
            )
            assert changed is True
            after_dry = {
                s.canonical_name for s in db.get_ingredient_stats(vid)
            }
            assert before == after_dry
        finally:
            db.close()

    def test_apply_collapses_three_pairs(self, tmp_path: Path) -> None:
        db_path = tmp_path / "recipes.db"
        db = CatalogDB.open(db_path)
        try:
            vid = self._seed_basic_buttermilk_variant(db)
            canonical_csv = db.connection.execute(
                "SELECT canonical_ingredient_set FROM variants "
                "WHERE variant_id = ?",
                (vid,),
            ).fetchone()[0]
            canonical_set = tuple(s for s in canonical_csv.split(",") if s)
            changed, dropped = _fold_one_variant(
                db,
                vid,
                canonical_set,
                apply=True,
            )
            assert changed is True
            after = {
                s.canonical_name for s in db.get_ingredient_stats(vid)
            }
            # Pre-fold count was 14 ingredients; post-fold drops 3.
            assert len(after) == 11
            # Each family kept exactly one form.
            assert len({"salt", "kosher salt"} & after) == 1
            assert len({"butter", "unsalted butter"} & after) == 1
            assert len({"oil", "vegetable oil"} & after) == 1
            # canonical_ingredient_set on variants table stays in sync.
            new_csv = db.connection.execute(
                "SELECT canonical_ingredient_set FROM variants "
                "WHERE variant_id = ?",
                (vid,),
            ).fetchone()[0]
            assert set(new_csv.split(",")) == after
        finally:
            db.close()

    def test_summed_mean_proportion(self, tmp_path: Path) -> None:
        db_path = tmp_path / "recipes.db"
        db = CatalogDB.open(db_path)
        try:
            vid = self._seed_basic_buttermilk_variant(db)
            canonical_csv = db.connection.execute(
                "SELECT canonical_ingredient_set FROM variants "
                "WHERE variant_id = ?",
                (vid,),
            ).fetchone()[0]
            canonical_set = tuple(s for s in canonical_csv.split(",") if s)
            _fold_one_variant(db, vid, canonical_set, apply=True)
            stats = {s.canonical_name: s for s in db.get_ingredient_stats(vid)}
            # unsalted butter (mean 0.02) absorbs butter (mean 0.002).
            ub = stats["unsalted butter"]
            assert ub.mean_proportion == pytest.approx(0.022)
        finally:
            db.close()
