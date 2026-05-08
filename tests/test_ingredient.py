"""Unit tests for ingredient classes"""

import pytest

from rational_recipes.ingredient import Factory, Ingredient


@pytest.fixture
def egg() -> Ingredient:
    """DB-loaded egg ingredient.

    Explicit at the callsite that this test depends on ingredients.db
    being present and on Factory's sqlite-backed lookup path.
    """
    return Factory.get_by_name("egg")


@pytest.fixture
def flour() -> Ingredient:
    """DB-loaded flour ingredient.

    Explicit at the callsite that this test depends on ingredients.db
    being present and on Factory's sqlite-backed lookup path.
    """
    return Factory.get_by_name("flour")


class TestIngredient:
    """Unit tests for ingredient classes"""

    def test_volume2weight(self, flour: Ingredient) -> None:
        """Test conversion from milliliters to grams"""
        assert flour.milliliters2grams(1) == pytest.approx(0.528345)

    def test_weight2volume(self, flour: Ingredient) -> None:
        """Test conversion from grams to milliliters"""
        assert flour.grams2milliliters(0.528345) == pytest.approx(1)

    def test_weight2whole_units_1egg(self, egg: Ingredient) -> None:
        """Test conversion of grams to whole units (1 egg)"""
        assert egg.grams2wholeunits(44) == pytest.approx(1, abs=1e-2)

    def test_weight2whole_units_2eggs(self, egg: Ingredient) -> None:
        """Test conversion of grams to whole units (2 eggs)"""
        assert egg.grams2wholeunits(44 * 2.0) == pytest.approx(2, abs=1e-2)

    def test_weight2whole_units_halfegg(self, egg: Ingredient) -> None:
        """Test conversion of grams to whole units (half an egg)"""
        assert egg.grams2wholeunits(44 / 2.0) == pytest.approx(0.5, abs=1e-2)


class TestDensityTransparency:
    """Tests for density source tracking and alternatives"""

    def test_density_property(self, flour: Ingredient) -> None:
        """density property returns the same value used for conversions"""
        assert flour.density == pytest.approx(0.528345)
        assert flour.density == flour.milliliters2grams(1)

    def test_db_loaded_flour_has_known_density_source(self, flour: Ingredient) -> None:
        """DB-loaded flour reports a real source, not 'default'"""
        assert flour.density_source in ("fdc_derived", "supplementary", "fao")

    def test_density_alternatives_non_empty(self, flour: Ingredient) -> None:
        """DB-loaded ingredients expose at least one alternative"""
        alts = flour.density_alternatives()
        assert len(alts) >= 1
        # First alternative matches the primary density
        assert alts[0][0] == pytest.approx(flour.density)
        assert alts[0][1] == flour.density_source

    def test_density_fallback_defaults(self) -> None:
        """Ingredient with no DB density gets source='default' and value=1.0"""
        ing = Ingredient(names=["unknown_test"], conversion=1.0)
        assert ing.density == 1.0
        assert ing.density_source == "default"
        assert ing.density_alternatives() == []

    def test_db_loaded_egg_has_known_density_source(self, egg: Ingredient) -> None:
        """DB-loaded egg reports a real source and a positive density"""
        assert egg.density_source in ("fdc_derived", "supplementary", "fao")
        assert egg.density > 0
