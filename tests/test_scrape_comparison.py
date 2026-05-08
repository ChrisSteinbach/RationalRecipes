"""Tests for the cross-corpus comparison harness."""

from __future__ import annotations

import pytest

from rational_recipes.scrape.comparison import (
    _normalize_url,
    field_complementarity,
    url_overlap,
    within_variant_comparison,
)
from rational_recipes.scrape.grouping import IngredientGroup
from rational_recipes.scrape.recipenlg import Recipe
from rational_recipes.scrape.wdc import WDCRecipe

# --- Helpers ---


def _recipe(
    title: str = "Test",
    ner: tuple[str, ...] = (),
    link: str = "",
    row_index: int = 0,
) -> Recipe:
    return Recipe(
        row_index=row_index,
        title=title,
        ingredients=(),
        ner=ner,
        source="test",
        link=link,
    )


def _wdc_recipe(
    title: str = "Test",
    ingredient_names: frozenset[str] = frozenset(),
    page_url: str = "",
    row_id: int = 0,
    cooking_methods: frozenset[str] = frozenset(),
    durations: tuple[tuple[str, float], ...] = (),
    recipe_category: str = "",
    keywords: tuple[str, ...] = (),
    recipe_yield: str = "",
) -> WDCRecipe:
    return WDCRecipe(
        row_id=row_id,
        host="test.com",
        title=title,
        ingredients=(),
        page_url=page_url,
        cooking_methods=cooking_methods,
        durations=durations,
        recipe_category=recipe_category,
        keywords=keywords,
        recipe_yield=recipe_yield,
        ingredient_names=ingredient_names,
    )


# --- field_complementarity ---


class TestFieldComplementarity:
    def test_basic_fractions(self) -> None:
        rnlg = [
            _recipe(ner=("flour", "sugar"), link="http://example.com/a"),
            _recipe(ner=("flour",)),
            _recipe(ner=()),
        ]
        wdc = [
            _wdc_recipe(
                ingredient_names=frozenset({"flour"}),
                page_url="http://example.com/x",
                cooking_methods=frozenset({"baking"}),
                durations=(("totaltime", 30.0),),
                recipe_category="dessert",
                keywords=("sweet",),
                recipe_yield="4 servings",
            ),
            _wdc_recipe(ingredient_names=frozenset()),
        ]

        result = field_complementarity(rnlg, wdc)

        assert result["ingredient_names"]["recipenlg"] == pytest.approx(2 / 3)
        assert result["ingredient_names"]["wdc"] == pytest.approx(1 / 2)

        assert result["cooking_method"]["recipenlg"] == 0.0
        assert result["cooking_method"]["wdc"] == pytest.approx(1 / 2)

        assert result["total_time"]["recipenlg"] == 0.0
        assert result["total_time"]["wdc"] == pytest.approx(1 / 2)

        assert result["recipe_yield"]["recipenlg"] == 0.0
        assert result["recipe_yield"]["wdc"] == pytest.approx(1 / 2)

        assert result["recipe_category"]["recipenlg"] == 0.0
        assert result["recipe_category"]["wdc"] == pytest.approx(1 / 2)

        assert result["keywords"]["recipenlg"] == 0.0
        assert result["keywords"]["wdc"] == pytest.approx(1 / 2)

        assert result["ner_names"]["recipenlg"] == pytest.approx(2 / 3)
        assert result["ner_names"]["wdc"] == 0.0

        assert result["source_url"]["recipenlg"] == pytest.approx(1 / 3)
        assert result["source_url"]["wdc"] == pytest.approx(1 / 2)

    def test_empty_lists(self) -> None:
        result = field_complementarity([], [])

        for field_data in result.values():
            assert field_data["recipenlg"] == 0.0
            assert field_data["wdc"] == 0.0

    def test_duration_fields(self) -> None:
        wdc = [
            _wdc_recipe(durations=(("totaltime", 60.0), ("cooktime", 45.0))),
            _wdc_recipe(durations=(("preptime", 15.0),)),
            _wdc_recipe(durations=()),
        ]

        result = field_complementarity([], wdc)

        assert result["total_time"]["wdc"] == pytest.approx(1 / 3)
        assert result["cook_time"]["wdc"] == pytest.approx(1 / 3)
        assert result["prep_time"]["wdc"] == pytest.approx(1 / 3)


# --- _normalize_url ---


class TestNormalizeUrl:
    def test_empty(self) -> None:
        assert _normalize_url("") == ""

    def test_strips_trailing_slash(self) -> None:
        assert (
            _normalize_url("http://example.com/recipe/") == "http://example.com/recipe"
        )

    def test_strips_query_and_fragment(self) -> None:
        assert (
            _normalize_url("http://example.com/recipe?id=1#top")
            == "http://example.com/recipe"
        )

    def test_lowercases_host(self) -> None:
        assert (
            _normalize_url("HTTP://EXAMPLE.COM/Recipe") == "http://example.com/recipe"
        )


# --- url_overlap ---


class TestUrlOverlap:
    def test_exact_url_match(self) -> None:
        rnlg = [_recipe(link="http://example.com/recipe/1")]
        wdc = [_wdc_recipe(page_url="http://example.com/recipe/1")]

        result = url_overlap(rnlg, wdc)

        assert len(result.url_matches) == 1
        assert result.url_matches[0] == (rnlg[0], wdc[0])
        assert result.recipenlg_total == 1
        assert result.wdc_total == 1

    def test_url_match_with_normalization(self) -> None:
        rnlg = [_recipe(link="HTTP://Example.COM/recipe/1/")]
        wdc = [_wdc_recipe(page_url="http://example.com/recipe/1?ref=search")]

        result = url_overlap(rnlg, wdc)

        assert len(result.url_matches) == 1

    def test_near_dup_detection(self) -> None:
        # Same title, high ingredient overlap, different URLs.
        # RecipeNLG NER goes through IngredientFactory canonicalization:
        # 'eggs' → 'egg', 'cocoa' stays 'cocoa'. WDC.ingredient_names is set
        # directly here (simulating already-extracted canonicalized output).
        rnlg = [
            _recipe(
                title="Chocolate Cake",
                ner=("flour", "sugar", "cocoa", "eggs", "butter"),
                link="http://a.com/1",
            ),
        ]
        wdc = [
            _wdc_recipe(
                title="Chocolate Cake",
                ingredient_names=frozenset({"flour", "sugar", "cocoa", "egg", "milk"}),
                page_url="http://b.com/2",
            ),
        ]

        result = url_overlap(rnlg, wdc, similarity_threshold=0.5)

        assert len(result.url_matches) == 0
        assert len(result.near_dup_matches) == 1
        _, _, sim = result.near_dup_matches[0]
        # Jaccard: 4 shared ({flour, sugar, cocoa, egg}) / 6 union
        # ({flour, sugar, cocoa, egg, butter, milk})
        assert sim == pytest.approx(4 / 6)

    def test_no_overlap(self) -> None:
        rnlg = [_recipe(title="A", link="http://a.com/1")]
        wdc = [_wdc_recipe(title="B", page_url="http://b.com/2")]

        result = url_overlap(rnlg, wdc)

        assert len(result.url_matches) == 0
        assert len(result.near_dup_matches) == 0

    def test_empty_inputs(self) -> None:
        result = url_overlap([], [])

        assert result.url_matches == []
        assert result.near_dup_matches == []
        assert result.recipenlg_total == 0
        assert result.wdc_total == 0

    def test_url_matched_excluded_from_near_dup(self) -> None:
        """Recipes already matched by URL should not appear in near-dup results."""
        url = "http://example.com/recipe/1"
        rnlg = [
            _recipe(
                title="Pancakes",
                ner=("flour", "milk", "eggs"),
                link=url,
            ),
        ]
        wdc = [
            _wdc_recipe(
                title="Pancakes",
                ingredient_names=frozenset({"flour", "milk", "eggs"}),
                page_url=url,
            ),
        ]

        result = url_overlap(rnlg, wdc)

        assert len(result.url_matches) == 1
        assert len(result.near_dup_matches) == 0


# --- within_variant_comparison ---


class TestWithinVariantComparison:
    def test_overlapping_ingredients(self) -> None:
        rnlg_group = IngredientGroup(
            canonical_ingredients=frozenset({"flour", "sugar", "butter"}),
            recipes=[
                _recipe(ner=("flour", "sugar", "butter")),
                _recipe(ner=("flour", "sugar", "milk")),
            ],
        )
        wdc_group = IngredientGroup(
            canonical_ingredients=frozenset({"flour", "sugar", "eggs"}),
            recipes=[
                _wdc_recipe(ingredient_names=frozenset({"flour", "sugar", "eggs"})),
                _wdc_recipe(ingredient_names=frozenset({"flour", "eggs"})),
            ],
        )

        result = within_variant_comparison(rnlg_group, wdc_group)

        assert result.shared_ingredients == frozenset({"flour", "sugar"})
        assert result.recipenlg_only == frozenset({"butter", "milk"})
        assert result.wdc_only == frozenset({"eggs"})
        assert result.recipenlg_count == 2
        assert result.wdc_count == 2

    def test_per_ingredient_coverage(self) -> None:
        rnlg_group = IngredientGroup(
            canonical_ingredients=frozenset({"flour", "sugar"}),
            recipes=[
                _recipe(ner=("flour", "sugar")),
                _recipe(ner=("flour",)),
            ],
        )
        wdc_group = IngredientGroup(
            canonical_ingredients=frozenset({"flour", "eggs"}),
            recipes=[
                _wdc_recipe(ingredient_names=frozenset({"flour", "eggs"})),
                _wdc_recipe(ingredient_names=frozenset({"flour", "eggs"})),
                _wdc_recipe(ingredient_names=frozenset({"flour"})),
            ],
        )

        result = within_variant_comparison(rnlg_group, wdc_group)

        cov = result.per_ingredient_coverage
        assert cov["flour"]["recipenlg"] == pytest.approx(1.0)
        assert cov["flour"]["wdc"] == pytest.approx(1.0)
        assert cov["sugar"]["recipenlg"] == pytest.approx(1 / 2)
        assert cov["sugar"]["wdc"] == pytest.approx(0.0)
        assert cov["eggs"]["recipenlg"] == pytest.approx(0.0)
        assert cov["eggs"]["wdc"] == pytest.approx(2 / 3)

    def test_corpus_specific_ingredients(self) -> None:
        rnlg_group = IngredientGroup(
            canonical_ingredients=frozenset({"a", "b"}),
            recipes=[_recipe(ner=("a", "b"))],
        )
        wdc_group = IngredientGroup(
            canonical_ingredients=frozenset({"c", "d"}),
            recipes=[_wdc_recipe(ingredient_names=frozenset({"c", "d"}))],
        )

        result = within_variant_comparison(rnlg_group, wdc_group)

        assert result.shared_ingredients == frozenset()
        assert result.recipenlg_only == frozenset({"a", "b"})
        assert result.wdc_only == frozenset({"c", "d"})
