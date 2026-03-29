"""Unit tests for ingredient classes"""

import pytest

from rational_recipes.ingredient import Factory

EGG = Factory.get_by_name("egg")
FLOUR = Factory.get_by_name("flour")


class TestIngredient:
    """Unit tests for ingredient classes"""

    def test_volume2weight(self):
        """Test conversion from milliliters to grams"""
        assert FLOUR.milliliters2grams(1) == pytest.approx(0.528345)

    def test_weight2volume(self):
        """Test conversion from grams to milliliters"""
        assert FLOUR.grams2milliliters(0.528345) == pytest.approx(1)

    def test_weight2whole_units_1egg(self):
        """Test conversion of grams to whole units (1 egg)"""
        assert EGG.grams2wholeunits(44) == pytest.approx(1, abs=1e-2)

    def test_weight2whole_units_2eggs(self):
        """Test conversion of grams to whole units (2 eggs)"""
        assert EGG.grams2wholeunits(44 * 2.0) == pytest.approx(2, abs=1e-2)

    def test_weight2whole_units_halfegg(self):
        """Test conversion of grams to whole units (half an egg)"""
        assert EGG.grams2wholeunits(44 / 2.0) == pytest.approx(0.5, abs=1e-2)
