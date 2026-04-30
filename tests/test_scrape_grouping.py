"""Tests for Level 1 (title), Level 2 (ingredient-set),
and Level 3 (cookingMethod) grouping."""

from rational_recipes.scrape.grouping import (
    group_by_cooking_method,
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
    cooking_methods: frozenset[str] = frozenset(),
) -> WDCRecipe:
    return WDCRecipe(
        row_id=row_id,
        host="test.com",
        title=title,
        ingredients=(),
        page_url="",
        cooking_methods=cooking_methods,
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


class TestGroupByCookingMethod:
    """Level 3: partition within an L2 group by cookingMethod tag set."""

    def _r(self, cooking_methods: frozenset[str], row_id: int = 0) -> WDCRecipe:
        return _wdc_recipe(
            "pannkakor",
            ingredient_names=frozenset({"flour"}),
            row_id=row_id,
            cooking_methods=cooking_methods,
        )

    def test_splits_by_method_tag_set(self) -> None:
        stekt = [self._r(frozenset({"stekt"}), i) for i in range(3)]
        oven = [self._r(frozenset({"i ugn"}), i + 10) for i in range(4)]
        result = group_by_cooking_method(stekt + oven, min_variant_size=2)
        assert len(result) == 2
        # Largest first.
        assert result[0].cooking_methods == frozenset({"i ugn"})
        assert result[0].size == 4
        assert result[1].cooking_methods == frozenset({"stekt"})
        assert result[1].size == 3

    def test_drops_subgroup_below_min_variant_size(self) -> None:
        big = [self._r(frozenset({"stekt"}), i) for i in range(3)]
        small = [self._r(frozenset({"grillad"}), 100)]
        result = group_by_cooking_method(big + small, min_variant_size=2)
        assert len(result) == 1
        assert result[0].cooking_methods == frozenset({"stekt"})

    def test_singleton_unknown_bucket_merges_into_largest(self) -> None:
        """One empty-method row should not splinter off on its own."""
        named = [self._r(frozenset({"stekt"}), i) for i in range(3)]
        stray = [self._r(frozenset(), 100)]  # single empty-method row
        result = group_by_cooking_method(named + stray, min_variant_size=2)
        assert len(result) == 1
        merged = result[0]
        assert merged.cooking_methods == frozenset({"stekt"})
        assert merged.size == 4  # 3 named + 1 stray

    def test_multi_unknown_stays_as_its_own_bucket(self) -> None:
        """An unknown bucket with 2+ rows is not a singleton — no merge."""
        named = [self._r(frozenset({"stekt"}), i) for i in range(3)]
        unknown = [self._r(frozenset(), i + 10) for i in range(3)]
        result = group_by_cooking_method(named + unknown, min_variant_size=2)
        assert len(result) == 2
        method_sets = {v.cooking_methods for v in result}
        assert method_sets == {frozenset({"stekt"}), frozenset()}

    def test_all_empty_method_stays_as_unknown_bucket(self) -> None:
        """RecipeNLG-only input: no cookingMethod anywhere → single
        unknown bucket, which passes the min-size filter and survives."""
        rows = [self._r(frozenset(), i) for i in range(5)]
        result = group_by_cooking_method(rows, min_variant_size=3)
        assert len(result) == 1
        assert result[0].cooking_methods == frozenset()
        assert result[0].size == 5

    def test_distinct_multi_tag_sets_separate(self) -> None:
        """{stekt} and {stekt, i_ugn} are distinct tag sets."""
        a = [self._r(frozenset({"stekt"}), i) for i in range(3)]
        b = [self._r(frozenset({"stekt", "i ugn"}), i + 10) for i in range(3)]
        result = group_by_cooking_method(a + b, min_variant_size=2)
        assert len(result) == 2

    def test_empty_input_returns_empty_list(self) -> None:
        assert group_by_cooking_method([], min_variant_size=2) == []

    def test_singleton_unknown_merged_when_only_one_named_bucket_too_small(
        self,
    ) -> None:
        """Singleton unknown merges into largest even if that largest
        would itself fall below min after merge — merge happens first,
        filter applies to the merged result."""
        named = [self._r(frozenset({"stekt"}), 0)]  # 1 row
        stray = [self._r(frozenset(), 100)]
        # After merge: {stekt} has 2 rows. min_variant_size=2 → survives.
        result = group_by_cooking_method(named + stray, min_variant_size=2)
        assert len(result) == 1
        assert result[0].size == 2
