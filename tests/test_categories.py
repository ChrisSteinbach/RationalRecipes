"""Tests for the static title → category mapping (vwt.33)."""

from __future__ import annotations

import pytest

from rational_recipes.categories import CATEGORIES, categorize


class TestCategorize:
    def test_empty_or_none_returns_none(self) -> None:
        assert categorize("") is None
        assert categorize("   ") is None  # whitespace-only — no rule matches

    def test_returns_one_of_known_categories_or_none(self) -> None:
        # The rule walker either produces a known label or None.
        for title in ("banana bread", "tomato soup", "garbledy gook xyz"):
            cat = categorize(title)
            assert cat is None or cat in CATEGORIES

    @pytest.mark.parametrize(
        ("title", "expected"),
        [
            # Bread family: yeast/quick breads, biscuits, rolls, muffins
            ("banana bread", "bread"),
            ("pumpkin bread", "bread"),
            ("zucchini bread", "bread"),
            ("sourdough bread", "bread"),
            ("monkey bread", "bread"),
            ("dinner rolls", "bread"),
            ("yeast rolls", "bread"),
            ("cinnamon rolls", "bread"),
            ("blueberry muffins", "bread"),
            ("buttermilk biscuits", "bread"),
            ("angel biscuits", "bread"),
            ("corn bread", "bread"),
            ("cornbread", "bread"),
            ("hush puppies", "bread"),
            ("pizza dough", "bread"),
            ("pie crust", "bread"),
            # Desserts
            ("chocolate chip cookies", "dessert"),
            ("oatmeal cookies", "dessert"),
            ("snickerdoodles", "dessert"),
            ("carrot cake", "dessert"),
            ("pumpkin pie", "dessert"),
            ("apple pie", "dessert"),
            ("pecan pie", "dessert"),
            ("brownies", "dessert"),
            ("fudge", "dessert"),
            ("rice pudding", "dessert"),
            ("banana pudding", "dessert"),
            ("bread pudding", "dessert"),
            ("cheesecake", "dessert"),
            ("gingerbread", "dessert"),
            ("shortbread", "dessert"),
            ("pumpkin roll", "dessert"),
            ("jelly roll", "dessert"),
            ("fruit pizza", "dessert"),
            ("banana split", "dessert"),
            ("applesauce cookies", "dessert"),
            ("applesauce cake", "dessert"),
            ("lemon squares", "dessert"),
            # Mains
            ("chicken pot pie", "main"),
            ("shepherds pie", "main"),
            ("tamale pie", "main"),
            ("lasagna", "main"),
            ("manicotti", "main"),
            ("spaghetti", "main"),
            ("baked spaghetti", "main"),
            ("macaroni and cheese", "main"),
            ("chicken parmesan", "main"),
            ("eggplant parmigiana", "main"),
            ("chicken enchiladas", "main"),
            ("beef stroganoff", "main"),
            ("meat loaf", "main"),
            ("meatloaf", "main"),
            ("salmon patties", "main"),
            ("swedish meatballs", "main"),
            ("cabbage rolls", "main"),
            ("stuffed peppers", "main"),
            ("pizza", "main"),
            ("cheese pizza", "main"),
            ("broccoli casserole", "main"),
            ("hash brown casserole", "main"),
            ("pot roast", "main"),
            ("roast beef", "main"),
            # Soups
            ("potato soup", "soup"),
            ("vegetable soup", "soup"),
            ("chicken noodle soup", "soup"),
            ("clam chowder", "soup"),
            ("chili", "soup"),
            # "stew" wins via the soup family rule, ahead of main:
            ("beef stew", "soup"),
            ("gumbo", "soup"),
            ("gazpacho", "soup"),
            # Salads
            ("potato salad", "salad"),
            ("chicken salad", "salad"),
            ("broccoli salad", "salad"),
            ("cole slaw", "salad"),
            ("coleslaw", "salad"),
            ("taco salad", "salad"),
            # Sauces
            ("barbecue sauce", "sauce"),
            ("spaghetti sauce", "sauce"),
            ("pizza sauce", "sauce"),
            ("tomato sauce", "sauce"),
            ("hollandaise sauce", "sauce"),
            ("salsa", "sauce"),
            ("french dressing", "sauce"),
            ("chocolate gravy", "sauce"),
            # Condiments
            ("apple butter", "condiment"),
            ("dill pickles", "condiment"),
            ("freezer pickles", "condiment"),
            ("pepper relish", "condiment"),
            ("zucchini relish", "condiment"),
            ("strawberry jam", "condiment"),
            # Appetizers / dips / party food
            ("cheese ball", "appetizer"),
            ("spinach dip", "appetizer"),
            ("shrimp dip", "appetizer"),
            ("vegetable dip", "appetizer"),
            ("party mix", "appetizer"),
            ("deviled eggs", "appetizer"),
            ("guacamole", "appetizer"),
            ("hummus", "appetizer"),
            # Breakfast
            ("pancakes", "breakfast"),
            ("waffles", "breakfast"),
            ("french toast", "breakfast"),
            ("crepes", "breakfast"),
            ("french crepes", "breakfast"),
            ("swedish pancakes", "breakfast"),
            ("pannkakor", "breakfast"),
            ("quiche", "breakfast"),
            ("hash browns", "breakfast"),
            # Beverages
            ("party punch", "beverage"),
            ("russian tea", "beverage"),
            ("hot chocolate", "beverage"),
            ("eggnog", "beverage"),
            ("kahlua", "beverage"),
            # Sides
            ("scalloped potatoes", "side"),
            ("mashed potatoes", "side"),
            ("twice baked potatoes", "side"),
            ("candied yams", "side"),
            ("spanish rice", "side"),
            ("creamed spinach", "side"),
            ("harvard beets", "side"),
            ("copper pennies", "side"),
            ("baked apples", "side"),
            ("fried green tomatoes", "side"),
            ("yorkshire pudding", "side"),
            ("corn pudding", "side"),
            ("applesauce", "side"),
        ],
    )
    def test_known_titles(self, title: str, expected: str) -> None:
        assert categorize(title) == expected, f"{title!r} → {categorize(title)!r}"

    def test_unknown_title_returns_none(self) -> None:
        assert categorize("xyzzy plover snickersnack") is None

    def test_word_boundary_prevents_substring_false_positive(self) -> None:
        # "bread" is in the bread keyword list. A title like "spread"
        # should NOT match because of \b word boundaries — otherwise
        # appetizer "X spread" entries would be miscategorized as bread.
        # The word "spread" is in appetizer; verify it actually goes
        # there rather than slipping into bread via substring match.
        assert categorize("salmon spread") == "appetizer"
        # "shortbread" must NOT be matched by "bread" (separate word
        # boundary). It's a dessert by its own keyword.
        assert categorize("shortbread") == "dessert"

    def test_case_insensitive(self) -> None:
        assert categorize("Banana Bread") == "bread"
        assert categorize("PUMPKIN PIE") == "dessert"
        assert categorize("ChIcKeN PoT PiE") == "main"

    @pytest.mark.parametrize(
        ("title", "expected"),
        [
            # "Punch bowl cake" is a layered no-bake dessert assembled in
            # a punch bowl — must beat beverage's "punch" substring rule.
            # Real variant titles from the corpus.
            ("punch bowl cake", "dessert"),
            ("pineapple pecan punch bowl cake", "dessert"),
            ("pineapple nut punch bowl cake", "dessert"),
            ("sour cream punch bowl cake", "dessert"),
            ("coconut punch bowl cake", "dessert"),
            # Plain "punch" still routes to beverage.
            ("rum punch", "beverage"),
            ("hawaiian punch", "beverage"),
        ],
    )
    def test_punch_bowl_cake_overrides_beverage(
        self, title: str, expected: str
    ) -> None:
        assert categorize(title) == expected, f"{title!r} → {categorize(title)!r}"

    @pytest.mark.parametrize(
        ("title", "expected"),
        [
            # Peanut butter desserts must beat condiment's "peanut butter"
            # (which targets the spread itself). Real variant titles.
            ("peanut butter pie", "dessert"),
            ("milk peanut butter pie", "dessert"),
            ("cool whip confectioners sugar peanut butter pie", "dessert"),
            ("peanut butter fudge", "dessert"),
            ("cocoa peanut butter fudge", "dessert"),
            ("margarine peanut butter fudge", "dessert"),
            ("peanut butter cookies", "dessert"),
            ("light brown sugar peanut butter cookies", "dessert"),
            ("crisco peanut butter cookies", "dessert"),
            ("soda peanut butter cookies", "dessert"),
            ("white sugar butter peanut butter cookies", "dessert"),
            # Bare "peanut butter" (the spread) still routes to condiment.
            ("peanut butter", "condiment"),
            ("homemade peanut butter", "condiment"),
        ],
    )
    def test_peanut_butter_desserts_override_condiment(
        self, title: str, expected: str
    ) -> None:
        assert categorize(title) == expected, f"{title!r} → {categorize(title)!r}"

    @pytest.mark.parametrize(
        ("title", "expected"),
        [
            # Taco dip is a layered party appetizer — must beat main's
            # "taco" substring rule. Real variant title.
            ("taco dip", "appetizer"),
            ("black olives taco dip", "appetizer"),
            # Plain tacos still route to main.
            ("beef tacos", "main"),
            ("chicken tacos", "main"),
        ],
    )
    def test_taco_dip_overrides_main(self, title: str, expected: str) -> None:
        assert categorize(title) == expected, f"{title!r} → {categorize(title)!r}"
