"""Tests for RecipeNLG CSV loader."""

import textwrap
from pathlib import Path

from rational_recipes.scrape.recipenlg import (
    Recipe,
    RecipeNLGLoader,
    _parse_string_list,
)


class TestParseStringList:
    def test_normal_list(self) -> None:
        assert _parse_string_list('["a", "b", "c"]') == ("a", "b", "c")

    def test_empty_list(self) -> None:
        assert _parse_string_list("[]") == ()

    def test_malformed(self) -> None:
        assert _parse_string_list("not a list") == ()

    def test_single_item(self) -> None:
        assert _parse_string_list('["flour"]') == ("flour",)


class TestRecipeNLGLoader:
    def test_loads_recipes(self, tmp_path: Path) -> None:
        lines = [
            ",title,ingredients,directions,link,source,NER",
            '0,Test Pancakes,"[""1 cup flour"", ""2 eggs""]",'
            '"[""Mix.""]",http://example.com,Test,'
            '"[""flour"", ""eggs""]"',
            '1,Test Crepes,"[""1 cup flour""]",'
            '"[""Mix.""]",http://example.com,Test,'
            '"[""flour""]"',
        ]
        csv_content = "\n".join(lines) + "\n"
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content, encoding="utf-8")

        loader = RecipeNLGLoader(path=csv_file)
        recipes = list(loader.iter_recipes())

        assert len(recipes) == 2
        assert recipes[0].title == "Test Pancakes"
        assert recipes[0].ingredients == ("1 cup flour", "2 eggs")
        assert recipes[0].ner == ("flour", "eggs")
        assert recipes[0].row_index == 0

    def test_search_title(self, tmp_path: Path) -> None:
        csv_content = textwrap.dedent("""\
            ,title,ingredients,directions,link,source,NER
            0,Swedish Pancakes,"[]","[]",,Test,"[]"
            1,Chocolate Cake,"[]","[]",,Test,"[]"
            2,Pannkakor,"[]","[]",,Test,"[]"
        """)
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content, encoding="utf-8")

        loader = RecipeNLGLoader(path=csv_file)
        results = list(loader.search_title("pancake"))

        assert len(results) == 1
        assert results[0].title == "Swedish Pancakes"


class TestRecipeIngredientNames:
    def test_ingredient_names_from_ner(self) -> None:
        recipe = Recipe(
            row_index=0,
            title="Test",
            ingredients=(),
            ner=("Flour", " Eggs ", "BUTTER"),
            source="test",
            link="",
        )
        # Names are routed through IngredientFactory for cross-language
        # canonicalization; "Eggs" (plural) resolves to the canonical 'egg'.
        assert recipe.ingredient_names == frozenset({"flour", "egg", "butter"})

    def test_empty_ner_entries_filtered(self) -> None:
        recipe = Recipe(
            row_index=0,
            title="Test",
            ingredients=(),
            ner=("flour", "", " "),
            source="test",
            link="",
        )
        assert recipe.ingredient_names == frozenset({"flour"})

    def test_swedish_ner_canonicalized(self) -> None:
        """Swedish NER names resolve to English canonicals across the corpus."""
        recipe = Recipe(
            row_index=0,
            title="Pannkakor",
            ingredients=(),
            ner=("vetemjöl", "mjölk", "ägg", "smör"),
            source="test",
            link="",
        )
        assert recipe.ingredient_names == frozenset({"flour", "milk", "egg", "butter"})

    def test_unknown_ner_preserved(self) -> None:
        """Names absent from the DB survive as lowercased-stripped originals."""
        recipe = Recipe(
            row_index=0,
            title="Test",
            ingredients=(),
            ner=("flour", "  UNKNOWN_INGREDIENT  "),
            source="test",
            link="",
        )
        assert recipe.ingredient_names == frozenset({"flour", "unknown_ingredient"})
