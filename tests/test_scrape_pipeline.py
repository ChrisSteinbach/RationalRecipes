"""Tests for the end-to-end scrape pipeline."""

from __future__ import annotations

from rational_recipes.scrape.parse import ParsedIngredient
from rational_recipes.scrape.pipeline import normalize_recipe
from rational_recipes.scrape.recipenlg import Recipe


def _recipe() -> Recipe:
    return Recipe(
        row_index=0,
        title="Fluffiga pannkakor",
        ingredients=("3 dl vetemjöl", "6 dl mjölk", "2 ägg", "1 krm salt"),
        ner=("vetemjöl", "mjölk", "ägg", "salt"),
        source="ica.se",
        link="http://example.com/pannkakor",
    )


class TestNormalizeRecipe:
    def test_cells_keyed_on_english_canonical(self) -> None:
        """Swedish ingredient names become English canonicals in CSV keys.

        Regression for RationalRecipes-rxd: without canonicalization the
        cells dict would be keyed on the raw LLM output ('vetemjöl',
        'mjölk', ...), breaking cross-corpus merge with English runs.
        """
        parsed = [
            ParsedIngredient(
                quantity=3,
                unit="dl",
                ingredient="vetemjöl",
                preparation="",
                raw="3 dl vetemjöl",
            ),
            ParsedIngredient(
                quantity=6,
                unit="dl",
                ingredient="mjölk",
                preparation="",
                raw="6 dl mjölk",
            ),
            ParsedIngredient(
                quantity=2,
                unit="MEDIUM",
                ingredient="ägg",
                preparation="",
                raw="2 ägg",
            ),
        ]
        row = normalize_recipe(_recipe(), parsed)
        assert set(row.cells) == {"flour", "milk", "egg"}
        assert row.skipped_ingredients == []

    def test_english_input_unchanged(self) -> None:
        """English canonicals pass through unchanged."""
        parsed = [
            ParsedIngredient(
                quantity=1,
                unit="cup",
                ingredient="flour",
                preparation="",
                raw="1 cup flour",
            ),
            ParsedIngredient(
                quantity=2,
                unit="MEDIUM",
                ingredient="eggs",
                preparation="",
                raw="2 eggs",
            ),
        ]
        row = normalize_recipe(_recipe(), parsed)
        assert set(row.cells) == {"flour", "egg"}

    def test_unknown_ingredient_skipped_with_canonical_form(self) -> None:
        """DB misses are recorded in lowercased-stripped (canonical-miss) form."""
        parsed = [
            ParsedIngredient(
                quantity=1,
                unit="tsp",
                ingredient="  UNOBTAINIUM  ",
                preparation="",
                raw="1 tsp UNOBTAINIUM",
            ),
        ]
        row = normalize_recipe(_recipe(), parsed)
        assert row.cells == {}
        assert row.skipped_ingredients == ["unobtainium"]
