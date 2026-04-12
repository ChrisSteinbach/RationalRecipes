"""Tests for Level 1 (title) and Level 2 (ingredient-set) grouping."""

from rational_recipes.scrape.grouping import (
    group_by_ingredients,
    group_by_title,
    jaccard_similarity,
    normalize_title,
)
from rational_recipes.scrape.recipenlg import Recipe
from rational_recipes.scrape.wdc import WDCRecipe


def _recipe(
    title: str,
    ner: tuple[str, ...] = (),
    row_index: int = 0,
) -> Recipe:
    return Recipe(
        row_index=row_index,
        title=title,
        ingredients=(),
        ner=ner,
        source="test",
        link="",
    )


class TestNormalizeTitle:
    def test_lowercase(self) -> None:
        assert normalize_title("Swedish Pancakes") == "swedish pancakes"

    def test_strip_recipe_suffix(self) -> None:
        assert normalize_title("Pannkakor Recipe") == "pannkakor"
        assert normalize_title("Crepes Recipes") == "crepes"

    def test_strip_possessives(self) -> None:
        assert normalize_title("Grandma's Pancakes") == "grandma pancakes"

    def test_strip_parenthesized_text(self) -> None:
        title = "Fraspannkakor(Swedish Crisp Pancakes)"
        assert normalize_title(title) == "fraspannkakor"

    def test_collapse_whitespace(self) -> None:
        assert normalize_title("  Swedish   Pancakes  ") == "swedish pancakes"

    def test_combined(self) -> None:
        assert normalize_title("Mom's Best Pancake Recipe  ") == "mom best pancake"


class TestGroupByTitle:
    def test_groups_by_normalized_title(self) -> None:
        recipes = [
            _recipe("Pannkakor"),
            _recipe("pannkakor"),
            _recipe("Pannkakor Recipe"),
            _recipe("Unique Dish"),
        ]
        groups = group_by_title(recipes, min_group_size=2)
        assert "pannkakor" in groups
        assert len(groups["pannkakor"]) == 3
        assert "unique dish" not in groups

    def test_min_group_size_filter(self) -> None:
        recipes = [_recipe("Pancakes") for _ in range(4)]
        assert group_by_title(recipes, min_group_size=5) == {}
        assert "pancakes" in group_by_title(recipes, min_group_size=4)


class TestJaccardSimilarity:
    def test_identical_sets(self) -> None:
        s = frozenset(["flour", "milk", "egg"])
        assert jaccard_similarity(s, s) == 1.0

    def test_disjoint_sets(self) -> None:
        a = frozenset(["flour", "milk"])
        b = frozenset(["chicken", "rice"])
        assert jaccard_similarity(a, b) == 0.0

    def test_partial_overlap(self) -> None:
        a = frozenset(["flour", "milk", "egg"])
        b = frozenset(["flour", "milk", "sugar"])
        # intersection=2, union=4
        assert jaccard_similarity(a, b) == 0.5

    def test_empty_sets(self) -> None:
        assert jaccard_similarity(frozenset(), frozenset()) == 1.0


class TestGroupByIngredients:
    def test_clusters_similar_recipes(self) -> None:
        """Recipes with similar ingredient sets should cluster together."""
        pannkakor_ner = ("flour", "milk", "egg", "butter", "salt")
        american_ner = ("flour", "buttermilk", "baking powder", "egg", "sugar")

        recipes = [
            _recipe("Pancakes", ner=pannkakor_ner, row_index=i) for i in range(5)
        ] + [_recipe("Pancakes", ner=american_ner, row_index=i) for i in range(5, 10)]

        groups = group_by_ingredients(
            recipes, similarity_threshold=0.6, min_group_size=3
        )
        assert len(groups) == 2
        # Each cluster should have 5 recipes
        sizes = sorted(g.size for g in groups)
        assert sizes == [5, 5]

    def test_min_group_size_filter(self) -> None:
        recipes = [
            _recipe("P", ner=("flour", "milk", "egg"), row_index=i) for i in range(2)
        ]
        groups = group_by_ingredients(recipes, min_group_size=3)
        assert groups == []

    def test_empty_ner_skipped(self) -> None:
        recipes = [
            _recipe("P", ner=(), row_index=0),
            _recipe("P", ner=("flour", "milk"), row_index=1),
        ]
        groups = group_by_ingredients(recipes, min_group_size=1)
        assert len(groups) == 1
        assert groups[0].size == 1


# --- WDCRecipe integration ---


def _wdc_recipe(
    title: str,
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


class TestGroupByTitleWDC:
    def test_groups_wdc_recipes(self) -> None:
        recipes = [
            _wdc_recipe("Pannkakor", row_id=0),
            _wdc_recipe("pannkakor", row_id=1),
            _wdc_recipe("Pannkakor Recipe", row_id=2),
        ]
        groups = group_by_title(recipes, min_group_size=2)
        assert "pannkakor" in groups
        assert len(groups["pannkakor"]) == 3


class TestGroupByIngredientsWDC:
    def test_clusters_wdc_recipes(self) -> None:
        ings = frozenset({"flour", "milk", "egg"})
        recipes = [_wdc_recipe("P", ingredient_names=ings, row_id=i) for i in range(4)]
        groups = group_by_ingredients(recipes, min_group_size=3)
        assert len(groups) == 1
        assert groups[0].size == 4
