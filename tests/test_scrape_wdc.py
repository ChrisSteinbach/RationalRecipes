"""Tests for WDC loader: ISO 8601 parser, WDCRecipe, WDCLoader."""

from __future__ import annotations

import gzip
import json
import zipfile
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from rational_recipes.scrape.grouping import (
    GroupableRecipe,
    group_by_ingredients,
    group_by_title,
)
from rational_recipes.scrape.parse import ParsedIngredient
from rational_recipes.scrape.wdc import (
    WDCLoader,
    WDCRecipe,
    extract_batch,
    extract_ingredient_names,
    parse_iso8601_duration,
)

# --- helpers ---


def _wdc_recipe(
    title: str = "",
    ingredient_names: frozenset[str] = frozenset(),
    row_id: int = 0,
) -> WDCRecipe:
    return WDCRecipe(
        row_id=row_id,
        host="test.com",
        title=title,
        ingredients=(),
        page_url="",
        cooking_methods=frozenset(),
        durations=(),
        recipe_category="",
        keywords=(),
        recipe_yield="",
        ingredient_names=ingredient_names,
    )


def _build_wdc_zip(path: Path, host_data: dict[str, list[dict[str, Any]]]) -> None:
    """Create a synthetic WDC-format zip at *path*."""
    with zipfile.ZipFile(path, "w") as zf:
        for host, rows in host_data.items():
            entry = f"Recipe_{host}_October2023.json.gz"
            compressed = gzip.compress("\n".join(json.dumps(r) for r in rows).encode())
            zf.writestr(entry, compressed)


# --- ISO 8601 duration parser ---


class TestParseISO8601Duration:
    def test_short_minutes(self) -> None:
        assert parse_iso8601_duration("PT20M") == 20.0

    def test_hours_and_minutes(self) -> None:
        assert parse_iso8601_duration("PT1H30M") == 90.0

    def test_verbose_minutes(self) -> None:
        assert parse_iso8601_duration("P0Y0M0DT0H35M0.000S") == 35.0

    def test_verbose_hours_and_seconds(self) -> None:
        result = parse_iso8601_duration("P0Y0M0DT1H0M30.000S")
        assert result == pytest.approx(60.5)

    def test_invalid_returns_none(self) -> None:
        assert parse_iso8601_duration("invalid") is None

    def test_empty_returns_none(self) -> None:
        assert parse_iso8601_duration("") is None


# --- WDCRecipe ---


class TestWDCRecipe:
    def test_construction(self) -> None:
        recipe = WDCRecipe(
            row_id=42,
            host="example.com",
            title="Pancakes",
            ingredients=("1 cup flour", "2 eggs"),
            page_url="https://example.com/pancakes",
            cooking_methods=frozenset({"baking"}),
            durations=(("totaltime", 30.0),),
            recipe_category="breakfast",
            keywords=("easy", "quick"),
            recipe_yield="4 servings",
            ingredient_names=frozenset({"flour", "eggs"}),
        )
        assert recipe.title == "Pancakes"
        assert recipe.row_id == 42
        assert recipe.ingredients == ("1 cup flour", "2 eggs")
        assert recipe.cooking_methods == frozenset({"baking"})
        assert recipe.durations == (("totaltime", 30.0),)
        assert recipe.ingredient_names == frozenset({"flour", "eggs"})

    def test_default_ingredient_names(self) -> None:
        recipe = _wdc_recipe(title="Test")
        assert recipe.ingredient_names == frozenset()

    def test_satisfies_groupable_protocol(self) -> None:
        recipe = _wdc_recipe(title="Test")
        assert isinstance(recipe, GroupableRecipe)


# --- WDCLoader ---

_SAMPLE_ROWS = [
    {
        "row_id": 1,
        "name": "Swedish Pancakes",
        "recipeingredient": ["1 cup flour", "2 eggs", "1 cup milk"],
        "page_url": "https://food.com/swedish-pancakes",
        "cookingmethod": "frying, baking",
        "totaltime": "PT30M",
        "recipecategory": "breakfast",
        "keywords": "easy, swedish, breakfast",
        "recipeyield": "4 servings",
    },
    {
        "row_id": 2,
        "name": "French Crepes",
        "recipeingredient": ["200g flour", "3 eggs"],
        "page_url": "https://food.com/crepes",
        "totaltime": "PT1H",
        "recipecategory": "dessert",
        "keywords": "french",
        "recipeyield": "6",
    },
    {
        "row_id": 3,
        "name": "American Pancakes",
        "recipeingredient": ["2 cups flour", "1 egg"],
        "page_url": "https://food.com/pancakes",
        "cookingmethod": "",
        "keywords": "",
        "recipecategory": "",
        "recipeyield": "",
    },
]


@pytest.fixture()
def wdc_zip(tmp_path: Path) -> Path:
    zip_path = tmp_path / "dataset.zip"
    _build_wdc_zip(
        zip_path,
        {
            "food.com": _SAMPLE_ROWS,
            "allrecipes.com": [
                {
                    "row_id": 10,
                    "name": "Swedish Pancakes",
                    "recipeingredient": ["flour", "milk"],
                    "page_url": "https://allrecipes.com/p",
                }
            ],
        },
    )
    return zip_path


class TestWDCLoader:
    def test_list_hosts(self, wdc_zip: Path) -> None:
        loader = WDCLoader(wdc_zip)
        hosts = loader.list_hosts()
        assert sorted(hosts) == ["allrecipes.com", "food.com"]

    def test_iter_host(self, wdc_zip: Path) -> None:
        loader = WDCLoader(wdc_zip)
        recipes = list(loader.iter_host("food.com"))
        assert len(recipes) == 3
        assert recipes[0].title == "Swedish Pancakes"
        assert recipes[0].row_id == 1
        assert recipes[0].host == "food.com"
        assert recipes[0].ingredients == ("1 cup flour", "2 eggs", "1 cup milk")
        assert recipes[0].cooking_methods == frozenset({"frying", "baking"})
        assert recipes[0].durations == (("totaltime", 30.0),)
        assert recipes[0].recipe_category == "breakfast"
        assert recipes[0].keywords == ("easy", "swedish", "breakfast")
        assert recipes[0].recipe_yield == "4 servings"

    def test_iter_all(self, wdc_zip: Path) -> None:
        loader = WDCLoader(wdc_zip)
        recipes = list(loader.iter_all())
        assert len(recipes) == 4

    def test_iter_all_filtered_hosts(self, wdc_zip: Path) -> None:
        loader = WDCLoader(wdc_zip)
        recipes = list(loader.iter_all(hosts=["allrecipes.com"]))
        assert len(recipes) == 1
        assert recipes[0].host == "allrecipes.com"

    def test_search_title(self, wdc_zip: Path) -> None:
        loader = WDCLoader(wdc_zip)
        results = list(loader.search_title("swedish"))
        assert len(results) == 2
        assert all("swedish" in r.title.lower() for r in results)

    def test_search_title_filtered_hosts(self, wdc_zip: Path) -> None:
        loader = WDCLoader(wdc_zip)
        results = list(loader.search_title("swedish", hosts=["food.com"]))
        assert len(results) == 1
        assert results[0].host == "food.com"

    def test_missing_fields_handled(self, tmp_path: Path) -> None:
        zip_path = tmp_path / "sparse.zip"
        _build_wdc_zip(
            zip_path,
            {
                "sparse.com": [
                    {"row_id": 99, "name": "Bare Recipe"},
                    {},
                ],
            },
        )
        loader = WDCLoader(zip_path)
        recipes = list(loader.iter_host("sparse.com"))
        assert len(recipes) == 2

        bare = recipes[0]
        assert bare.title == "Bare Recipe"
        assert bare.ingredients == ()
        assert bare.cooking_methods == frozenset()
        assert bare.durations == ()
        assert bare.recipe_category == ""
        assert bare.keywords == ()
        assert bare.recipe_yield == ""
        assert bare.ingredient_names == frozenset()

        empty = recipes[1]
        assert empty.title == ""
        assert empty.row_id == 0

    def test_explicit_null_title_coerced_to_empty(self, tmp_path: Path) -> None:
        """JSON null in the ``name`` field becomes an empty title.

        Without this guard, ``search_title`` crashes with
        ``AttributeError: 'NoneType' object has no attribute 'lower'``
        on real WDC rows that emit ``"name": null`` (encountered when
        scanning the full pannkak slice across the corpus).
        """
        zip_path = tmp_path / "nulls.zip"
        _build_wdc_zip(
            zip_path,
            {
                "nulls.com": [
                    {"row_id": 1, "name": None, "page_url": None},
                ],
            },
        )
        loader = WDCLoader(zip_path)
        recipes = list(loader.iter_host("nulls.com"))
        assert recipes[0].title == ""
        assert recipes[0].page_url == ""
        # search_title must not crash on the null-title row.
        assert list(loader.search_title("anything")) == []


# --- Protocol integration ---


class TestProtocolIntegration:
    def test_group_by_title_with_wdc(self) -> None:
        recipes = [_wdc_recipe(title="Pannkakor", row_id=i) for i in range(5)]
        groups = group_by_title(recipes, min_group_size=3)
        assert "pannkakor" in groups
        assert len(groups["pannkakor"]) == 5

    def test_group_by_ingredients_with_wdc(self) -> None:
        ings = frozenset({"flour", "milk", "egg"})
        recipes = [
            _wdc_recipe(title="P", ingredient_names=ings, row_id=i) for i in range(5)
        ]
        groups = group_by_ingredients(recipes, min_group_size=3)
        assert len(groups) == 1
        assert groups[0].size == 5


# --- Extraction ---


class TestExtractIngredientNames:
    def test_populates_names(self) -> None:
        recipe = WDCRecipe(
            row_id=0,
            host="test.com",
            title="Test",
            ingredients=("3 dl vetemjöl", "2 ägg", "5 dl mjölk"),
            page_url="http://test.com/1",
            cooking_methods=frozenset(),
            durations=(),
            recipe_category="",
            keywords=(),
            recipe_yield="",
        )
        mock_results = [
            ParsedIngredient(3.0, "dl", "vetemjöl", "", "3 dl vetemjöl"),
            ParsedIngredient(2.0, "", "ägg", "", "2 ägg"),
            ParsedIngredient(5.0, "dl", "mjölk", "", "5 dl mjölk"),
        ]
        with patch("rational_recipes.scrape.wdc.parse_ingredient_line") as mock_parse:
            mock_parse.side_effect = mock_results
            result = extract_ingredient_names(recipe)

        # Swedish names are canonicalized to English via IngredientFactory
        # so cross-corpus Jaccard compares apples-to-apples with RecipeNLG NER.
        assert result.ingredient_names == frozenset({"flour", "egg", "milk"})
        assert result.title == "Test"
        assert mock_parse.call_count == 3
        for call in mock_parse.call_args_list:
            assert call.kwargs["system_prompt"] is not None

    def test_handles_parse_failure(self) -> None:
        recipe = WDCRecipe(
            row_id=0,
            host="test.com",
            title="Test",
            ingredients=("3 dl vetemjöl", "bad line", "5 dl mjölk"),
            page_url="http://test.com/2",
            cooking_methods=frozenset(),
            durations=(),
            recipe_category="",
            keywords=(),
            recipe_yield="",
        )
        mock_results = [
            ParsedIngredient(3.0, "dl", "vetemjöl", "", "3 dl vetemjöl"),
            None,
            ParsedIngredient(5.0, "dl", "mjölk", "", "5 dl mjölk"),
        ]
        with patch("rational_recipes.scrape.wdc.parse_ingredient_line") as mock_parse:
            mock_parse.side_effect = mock_results
            result = extract_ingredient_names(recipe)

        assert result.ingredient_names == frozenset({"flour", "milk"})

    def test_unknown_names_preserved(self) -> None:
        """LLM outputs with no DB match survive as lowercased-stripped originals."""
        recipe = WDCRecipe(
            row_id=0,
            host="test.com",
            title="Test",
            ingredients=("1 okänd ingrediens",),
            page_url="http://test.com/3",
            cooking_methods=frozenset(),
            durations=(),
            recipe_category="",
            keywords=(),
            recipe_yield="",
        )
        with patch("rational_recipes.scrape.wdc.parse_ingredient_line") as mock_parse:
            mock_parse.return_value = ParsedIngredient(
                1.0, "", "UNKNOWN_X", "", "1 okänd ingrediens"
            )
            result = extract_ingredient_names(recipe)
        assert result.ingredient_names == frozenset({"unknown_x"})


class TestExtractBatch:
    def test_uses_cache(self) -> None:
        recipes = [
            WDCRecipe(
                row_id=0,
                host="test.com",
                title="Cached",
                ingredients=("flour",),
                page_url="http://test.com/cached",
                cooking_methods=frozenset(),
                durations=(),
                recipe_category="",
                keywords=(),
                recipe_yield="",
            ),
            WDCRecipe(
                row_id=1,
                host="test.com",
                title="Fresh",
                ingredients=("mjölk",),
                page_url="http://test.com/fresh",
                cooking_methods=frozenset(),
                durations=(),
                recipe_category="",
                keywords=(),
                recipe_yield="",
            ),
        ]
        cache: dict[str, frozenset[str]] = {
            "http://test.com/cached": frozenset({"flour"}),
        }
        with patch("rational_recipes.scrape.wdc.parse_ingredient_line") as mock_parse:
            mock_parse.return_value = ParsedIngredient(
                1.0,
                "dl",
                "mjölk",
                "",
                "mjölk",
            )
            result = extract_batch(recipes, cache=cache)

        assert result[0].ingredient_names == frozenset({"flour"})
        # 'mjölk' canonicalizes to 'milk' through IngredientFactory.
        assert result[1].ingredient_names == frozenset({"milk"})
        # Only the non-cached recipe should trigger a parse call
        assert mock_parse.call_count == 1
        # Cache should now contain the fresh entry (storing the canonicalized form).
        assert cache["http://test.com/fresh"] == frozenset({"milk"})


class TestSystemPromptForwarding:
    def test_parse_ingredient_line_forwards_system_prompt(self) -> None:
        with patch("rational_recipes.scrape.parse._ollama_generate") as mock_gen:
            mock_gen.return_value = (
                '{"quantity": 1.0, "unit": "dl",'
                ' "ingredient": "mjöl", "preparation": ""}'
            )
            from rational_recipes.scrape.parse import parse_ingredient_line

            parse_ingredient_line("1 dl mjöl", system_prompt="custom prompt")
            mock_gen.assert_called_once()
            assert mock_gen.call_args.kwargs["system"] == "custom prompt"

    def test_parse_ingredient_line_uses_default_without_system_prompt(self) -> None:
        with patch("rational_recipes.scrape.parse._ollama_generate") as mock_gen:
            mock_gen.return_value = (
                '{"quantity": 1.0, "unit": "cup",'
                ' "ingredient": "flour", "preparation": ""}'
            )
            from rational_recipes.scrape.parse import (
                _SYSTEM_PROMPT,
                parse_ingredient_line,
            )

            parse_ingredient_line("1 cup flour")
            mock_gen.assert_called_once()
            assert mock_gen.call_args.kwargs["system"] == _SYSTEM_PROMPT


class TestParseKeyTolerance:
    """Small Ollama models sometimes misspell the "ingredient" JSON key.

    We accept any ``ingr*`` key so occasional typos don't discard a parse.
    """

    def test_misspelled_ingruedient_accepted(self) -> None:
        with patch("rational_recipes.scrape.parse._ollama_generate") as mock_gen:
            mock_gen.return_value = (
                '{"quantity": 3.0, "unit": "dl",'
                ' "ingruedient": "vetemjöl", "preparation": ""}'
            )
            from rational_recipes.scrape.parse import parse_ingredient_line

            parsed = parse_ingredient_line("3 dl vetemjöl")
            assert parsed is not None
            assert parsed.ingredient == "vetemjöl"
            assert parsed.quantity == 3.0
            assert parsed.unit == "dl"

    def test_misspelled_ingrredient_accepted(self) -> None:
        with patch("rational_recipes.scrape.parse._ollama_generate") as mock_gen:
            mock_gen.return_value = (
                '{"quantity": 2.0, "unit": "",'
                ' "ingrredient": "citronskal", "preparation": "fintrivet"}'
            )
            from rational_recipes.scrape.parse import parse_ingredient_line

            parsed = parse_ingredient_line("2 citronskal")
            assert parsed is not None
            assert parsed.ingredient == "citronskal"

    def test_no_ingredient_key_returns_none(self) -> None:
        with patch("rational_recipes.scrape.parse._ollama_generate") as mock_gen:
            mock_gen.return_value = '{"quantity": 1.0, "unit": "g"}'
            from rational_recipes.scrape.parse import parse_ingredient_line

            parsed = parse_ingredient_line("1 g something")
            assert parsed is None


class TestNumPredictCap:
    """The ``num_predict`` cap bounds degenerate token-loop responses so
    they fail fast instead of waiting out the HTTP timeout.
    """

    def test_default_num_predict_sent_in_payload(self) -> None:
        from rational_recipes.scrape.parse import parse_ingredient_line

        with patch("rational_recipes.scrape.parse.urllib.request.urlopen") as mock_open:
            resp = mock_open.return_value.__enter__.return_value
            resp.read.return_value = json.dumps(
                {
                    "response": '{"quantity": 1.0, "unit": "cup",'
                    ' "ingredient": "flour", "preparation": ""}'
                }
            ).encode()

            parse_ingredient_line("1 cup flour")

            req = mock_open.call_args.args[0]
            body = json.loads(req.data.decode())
            assert body["options"] == {
                "num_predict": 256,
                "temperature": 0.0,
                "seed": 42,
            }

    def test_num_predict_override_forwarded(self) -> None:
        from rational_recipes.scrape.parse import parse_ingredient_line

        with patch("rational_recipes.scrape.parse.urllib.request.urlopen") as mock_open:
            resp = mock_open.return_value.__enter__.return_value
            resp.read.return_value = json.dumps(
                {
                    "response": '{"quantity": 1.0, "unit": "cup",'
                    ' "ingredient": "flour", "preparation": ""}'
                }
            ).encode()

            parse_ingredient_line("1 cup flour", num_predict=64)

            req = mock_open.call_args.args[0]
            body = json.loads(req.data.decode())
            assert body["options"] == {
                "num_predict": 64,
                "temperature": 0.0,
                "seed": 42,
            }

    def test_parse_ingredient_lines_forwards_num_predict(self) -> None:
        """Batched parse honors num_predict as a floor.

        With batching, parse_ingredient_lines makes ONE call per batch (not
        one per line), and scales num_predict up so the model can fit the
        whole array in its budget. The user's value is the floor.
        """
        from rational_recipes.scrape.parse import parse_ingredient_lines

        with patch("rational_recipes.scrape.parse._ollama_generate") as mock_gen:
            mock_gen.return_value = (
                '{"results": ['
                '{"quantity": 1.0, "unit": "cup",'
                ' "ingredient": "flour", "preparation": ""},'
                '{"quantity": 2.0, "unit": "MEDIUM",'
                ' "ingredient": "egg", "preparation": ""}'
                ']}'
            )
            # Pin use_regex_prefilter=False so the vwt.17 regex shortcut
            # doesn't intercept these easy lines before they reach the LLM.
            parse_ingredient_lines(
                ["1 cup flour", "2 eggs"],
                num_predict=128,
                use_regex_prefilter=False,
            )
            assert mock_gen.call_count == 1
            assert mock_gen.call_args.kwargs["num_predict"] >= 128
