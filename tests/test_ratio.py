"""Unit tests for ratio data model and formatter"""

from typing import Any

from numpy import array

from rational_recipes.ingredient import Factory, Ingredient
from rational_recipes.ratio import Ratio
from rational_recipes.ratio_format import RatioFormatter
from rational_recipes.statistics import calculate_statistics

BUTTER = Factory.get_by_name("butter")
EGG = Factory.get_by_name("egg")
FLOUR = Factory.get_by_name("flour")


def calculate_ratio(
    ingredients: tuple[Ingredient, ...],
    proportions: Any,
    filter_zeros: list[str] | None = None,
) -> Ratio:
    """Calculate ratio proportions from input data."""
    statistics = calculate_statistics(proportions, ingredients, filter_zeros)
    return Ratio(ingredients, statistics.bakers_percentage())


def make_test_data() -> tuple[tuple[Ingredient, ...], Any]:
    """Shared test data"""
    ingredients = (FLOUR, EGG, BUTTER)
    flour = array([1, 1, 1])
    egg = array([2, 2, 2])
    butter = array([3, 3, 3])
    return ingredients, zip(flour, egg, butter, strict=False)


def make_test_data_with_zeros() -> tuple[tuple[Ingredient, ...], Any]:
    """Shared test data"""
    ingredients = (FLOUR, EGG, BUTTER)
    flour = array([1, 1, 1])
    egg = array([2, 2, 2])
    butter = array([6, 0, 0])
    return ingredients, zip(flour, egg, butter, strict=False)


def create_ratio(
    ingredients: tuple[Ingredient, ...],
    proportions: list[float],
    restrictions: list[tuple[str | int, float]] | None = None,
) -> Ratio:
    """Wrapper for Ratio class creation"""
    ratio = Ratio(ingredients, proportions)
    if restrictions is not None:
        ratio.set_restrictions(restrictions)
    return ratio


class TestRatioDataModel:
    """Tests for the Ratio data model (numeric values)"""

    def test_values(self) -> None:
        """Raw ratio values are returned correctly"""
        ingredients, _ = make_test_data()
        ratio = create_ratio(ingredients, [1, 2, 3])
        assert ratio.values() == [1, 2, 3]

    def test_values_scaled(self) -> None:
        """Scaled values multiply by scale factor"""
        ingredients, _ = make_test_data()
        ratio = create_ratio(ingredients, [1, 2, 3])
        assert ratio.values(2) == [2, 4, 6]

    def test_as_percentages(self) -> None:
        """Percentages sum to 100"""
        ingredients, _ = make_test_data()
        ratio = create_ratio(ingredients, [1, 2, 3])
        percentages = ratio.as_percentages()
        assert abs(sum(percentages) - 100) < 0.001
        assert abs(percentages[0] - 100 / 6) < 0.001

    def test_recipe_total_weight(self) -> None:
        """Recipe returns correct total weight"""
        ingredients, _ = make_test_data()
        ratio = create_ratio(ingredients, [1, 2, 3])
        total_weight, values = ratio.recipe(200)
        assert abs(total_weight - 200) < 0.001
        assert abs(sum(values) - 200) < 0.001

    def test_recipe_proportions(self) -> None:
        """Recipe preserves proportions"""
        ingredients, _ = make_test_data()
        ratio = create_ratio(ingredients, [1, 2, 3])
        _, values = ratio.recipe(600)
        assert abs(values[0] - 100) < 0.001
        assert abs(values[1] - 200) < 0.001
        assert abs(values[2] - 300) < 0.001

    def test_recipe_restricted_weight(self) -> None:
        """Recipe with ingredient restriction reduces total weight"""
        ingredients, _ = make_test_data()
        ratio = create_ratio(ingredients, [1, 2, 3], [(1, 31.5)])
        total_weight, values = ratio.recipe(200)
        assert total_weight < 200
        assert values[1] <= 31.5 + 0.001

    def test_ingredient_values(self) -> None:
        """ingredient_values returns correct (grams, ingredient) pairs"""
        ingredients, _ = make_test_data()
        ratio = create_ratio(ingredients, [1, 2, 3])
        pairs = ratio.ingredient_values("flour")
        assert len(pairs) == 1
        assert abs(pairs[0][0] - 1) < 0.001
        assert pairs[0][1].name() == "flour"

    def test_len(self) -> None:
        """len returns number of elements"""
        ingredients, _ = make_test_data()
        ratio = create_ratio(ingredients, [1, 2, 3])
        assert ratio.len() == 3


class TestRatioFormatter:
    """Tests for formatted ratio output"""

    def test_calculate_ratio(self) -> None:
        """Calculate simple ratio from three ingredients and three identical
        recipes"""
        ingredients, proportions = make_test_data()
        ratio = calculate_ratio(ingredients, proportions)
        formatter = RatioFormatter(precision=0)
        assert formatter.format_ratio(ratio).split()[0] == "1:2:3"

    def test_filter_zeros(self) -> None:
        """Calculate simple ratio from three ingredients and three identical
        recipes"""
        ingredients, proportions = make_test_data_with_zeros()
        ratio = calculate_ratio(ingredients, proportions, filter_zeros=["butter"])
        formatter = RatioFormatter(precision=0)
        assert formatter.format_ratio(ratio).split()[0] == "1:2:3"

    def test_precision(self) -> None:
        """Test output precision to two decimal places"""
        ingredients, _ = make_test_data()
        ratio = create_ratio(ingredients, [1, 2, 3])
        formatter = RatioFormatter(precision=2)
        assert formatter.format_ratio(ratio).split()[0] == "1.00:2.00:3.00"

    def test_describe_value(self) -> None:
        """Test ratio output as if in a recipe using weight and volume measures"""
        ingredients, _ = make_test_data()
        ratio = create_ratio(ingredients, [1, 2, 3])
        formatter = RatioFormatter()
        assert formatter.format_ingredient(ratio, "flour") == "1.00g or 1.89ml flour"

    def test_describe_wholeunit_ualue(self) -> None:
        """Test descriptive output of whole-unit values (eggs in  this case)"""
        ingredients, _ = make_test_data()
        ratio = create_ratio(ingredients, [1, 2, 3])
        formatter = RatioFormatter()
        assert formatter.format_ingredient(ratio, "egg") == (
            "2.00g, 1.95ml or 0.05 egg(s) where each egg is 44.00g"
        )

    def test_recipe_by_total_weight(self) -> None:
        """Test output of a recipe ratio scaled to a total weight"""
        ingredients, _ = make_test_data()
        ratio = create_ratio(ingredients, [1, 2, 3])
        formatter = RatioFormatter(precision=0)
        expected_recipe = """33g or 63ml flour
67g, 65ml or 2 egg(s) where each egg is 44g
100g or 104ml butter"""
        assert formatter.format_recipe(ratio, weight=200)[1] == expected_recipe

    def test_recipe_retricted_weight(self) -> None:
        """Test output of a recipe ratio scaled to a total weight and then
        restricted on one ingredient"""
        ingredients, _ = make_test_data()
        ratio = create_ratio(ingredients, [1, 2, 3], [(1, 31.5)])
        formatter = RatioFormatter(precision=0)
        expected_recipe = """16g or 30ml flour
32g, 31ml or 1 egg(s) where each egg is 44g
47g or 49ml butter"""
        assert formatter.format_recipe(ratio, weight=200)[1] == expected_recipe

    def test_retrict_weight_by_name(self) -> None:
        """Test output of a recipe ratio scaled to a total weight and then
        restricted on one ingredient"""
        ingredients, _ = make_test_data()
        ratio = create_ratio(ingredients, [1, 2, 3], [("egg", 31.5)])
        formatter = RatioFormatter(precision=0)
        expected_recipe = """16g or 30ml flour
32g, 31ml or 1 egg(s) where each egg is 44g
47g or 49ml butter"""
        assert formatter.format_recipe(ratio, weight=200)[1] == expected_recipe

    def test_retrict_repeat_ingredient(self) -> None:
        """Test output of a recipe ratio scaled to a total weight and then
        restricted on one ingredient"""
        ingredients = (FLOUR, EGG, BUTTER, BUTTER)
        ratio = create_ratio(ingredients, [1, 2, 3, 3], [("butter", 94)])
        formatter = RatioFormatter(precision=0)
        expected_recipe = """16g or 30ml flour
31g, 31ml or 1 egg(s) where each egg is 44g
47g or 49ml butter
47g or 49ml butter"""
        assert formatter.format_recipe(ratio, weight=200)[1] == expected_recipe

    def test_retricted_weight_multiple(self) -> None:
        """Test output of a recipe ratio scaled to a total weight and then
        restricted on one ingredient"""
        ingredients, _ = make_test_data()
        ratio = create_ratio(ingredients, [1, 2, 3], [(0, 17), (1, 31.5), (2, 48)])
        formatter = RatioFormatter(precision=0)
        expected_recipe = """16g or 30ml flour
32g, 31ml or 1 egg(s) where each egg is 44g
47g or 49ml butter"""
        assert formatter.format_recipe(ratio, weight=200)[1] == expected_recipe
