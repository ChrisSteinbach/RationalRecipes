"""Unit tests for ingredient classes"""

import pytest

from rational_recipes.ingredient import Factory, Ingredient

EGG = Factory.get_by_name("egg")
FLOUR = Factory.get_by_name("flour")


class TestIngredient:
    """Unit tests for ingredient classes"""

    def test_volume2weight(self) -> None:
        """Test conversion from milliliters to grams"""
        assert FLOUR.milliliters2grams(1) == pytest.approx(0.528345)

    def test_weight2volume(self) -> None:
        """Test conversion from grams to milliliters"""
        assert FLOUR.grams2milliliters(0.528345) == pytest.approx(1)

    def test_weight2whole_units_1egg(self) -> None:
        """Test conversion of grams to whole units (1 egg)"""
        assert EGG.grams2wholeunits(44) == pytest.approx(1, abs=1e-2)

    def test_weight2whole_units_2eggs(self) -> None:
        """Test conversion of grams to whole units (2 eggs)"""
        assert EGG.grams2wholeunits(44 * 2.0) == pytest.approx(2, abs=1e-2)

    def test_weight2whole_units_halfegg(self) -> None:
        """Test conversion of grams to whole units (half an egg)"""
        assert EGG.grams2wholeunits(44 / 2.0) == pytest.approx(0.5, abs=1e-2)


class TestDensityTransparency:
    """Tests for density source tracking and alternatives"""

    def test_density_property(self) -> None:
        """density property returns the same value used for conversions"""
        assert FLOUR.density == pytest.approx(0.528345)
        assert FLOUR.density == FLOUR.milliliters2grams(1)

    def test_density_source_is_known(self) -> None:
        """DB-loaded ingredients have a real source, not 'default'"""
        assert FLOUR.density_source in ("fdc_derived", "supplementary", "fao")

    def test_density_alternatives_non_empty(self) -> None:
        """DB-loaded ingredients expose at least one alternative"""
        alts = FLOUR.density_alternatives()
        assert len(alts) >= 1
        # First alternative matches the primary density
        assert alts[0][0] == pytest.approx(FLOUR.density)
        assert alts[0][1] == FLOUR.density_source

    def test_density_fallback_defaults(self) -> None:
        """Ingredient with no DB density gets source='default' and value=1.0"""
        ing = Ingredient(names=["unknown_test"], conversion=1.0)
        assert ing.density == 1.0
        assert ing.density_source == "default"
        assert ing.density_alternatives() == []

    def test_egg_density_source(self) -> None:
        """Egg should have a known density source"""
        assert EGG.density_source in ("fdc_derived", "supplementary", "fao")
        assert EGG.density > 0
