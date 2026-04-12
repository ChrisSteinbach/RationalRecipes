"""Tests for WDC loader: ISO 8601 parser, WDCRecipe, WDCLoader."""

from __future__ import annotations

import gzip
import json
import zipfile
from pathlib import Path
from typing import Any

import pytest

from rational_recipes.scrape.grouping import (
    GroupableRecipe,
    group_by_ingredients,
    group_by_title,
)
from rational_recipes.scrape.wdc import (
    WDCLoader,
    WDCRecipe,
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
