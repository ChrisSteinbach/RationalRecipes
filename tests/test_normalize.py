"""Tests for data normalization"""

import pytest

from rational_recipes.ingredient import Factory
from rational_recipes.normalize import normalize_to_100g
from rational_recipes.units import (
    DASH,
    DSTSPN,
    GRAM,
    IMP_FLOZ,
    IMP_PINT,
    KG,
    KNOB,
    LARGE,
    LB,
    LITER,
    MEDIUM,
    METRIC_CUP,
    METRIC_TBSP,
    METRIC_TSP,
    ML,
    OZ,
    PINCH,
    SMALL,
    STICK,
    US_FLOZ,
    US_PINT,
    BadUnitException,
)
from tests.test_utils import norm, normalize

BUTTER = Factory.get_by_name("butter")
COCOA = Factory.get_by_name("cocoa")
CORNSTARCH = Factory.get_by_name("cornstarch")
CREAM = Factory.get_by_name("cream")
EGG = Factory.get_by_name("egg")
FLOUR = Factory.get_by_name("flour")
GRATED_CHEESE = Factory.get_by_name("grated cheese")
HONEY = Factory.get_by_name("honey")
MILK = Factory.get_by_name("milk")
POTATO_STARCH = Factory.get_by_name("potato starch")
SALT = Factory.get_by_name("salt")
SUGAR = Factory.get_by_name("sugar")


@pytest.mark.parametrize(
    "unit, expected",
    [
        pytest.param(OZ, 28.3495231, id="ounce"),
        pytest.param(GRAM, 1, id="gram"),
        pytest.param(KG, 1000, id="kilogram"),
        pytest.param(LB, 453.592, id="pound"),
    ],
)
def test_normalize_weight(unit, expected):
    """Convert one unit of weight to grams"""
    assert norm(1, unit) == pytest.approx(expected, abs=0.005)


@pytest.mark.parametrize(
    "unit, expected",
    [
        pytest.param(US_PINT, 473.600, id="us_pint"),
        pytest.param(IMP_PINT, 568.770, id="imperial_pint"),
        pytest.param(IMP_FLOZ, 28.4386, id="imperial_floz"),
        pytest.param(US_FLOZ, 29.600, id="us_floz"),
        pytest.param(LITER, 1000.896, id="liter"),
        pytest.param(METRIC_CUP, 250.224, id="metric_cup"),
        pytest.param(METRIC_TBSP, 15.01344, id="metric_tbsp"),
        pytest.param(METRIC_TSP, 5.00448, id="metric_tsp"),
    ],
)
def test_normalize_volume(unit, expected):
    """Convert one unit of volume to milliliters"""
    assert norm(1, unit) == pytest.approx(expected, abs=0.005)


class TestNormalizeVolumeToWeight:
    """Test normalization of volume based measurements to grams using
    food density information."""

    def test_one_cup_flour(self):
        """Convert one metric cup of flour to grams"""
        grams = norm(1, METRIC_CUP, FLOUR)
        assert grams == pytest.approx(132.086250, abs=1e-2)

    def test_one_tsp_salt(self):
        """Convert one metric teaspoon of salt to grams"""
        grams = norm(1, METRIC_TSP, SALT)
        assert grams == pytest.approx(6.171065, abs=1e-2)

    def test_one_tbsp_butter(self):
        """Convert one metric tablespoon of butter to grams"""
        grams = norm(1, METRIC_TBSP, BUTTER)
        assert grams == pytest.approx(14.392110, abs=1e-2)

    def test_one_liter_grated_cheese(self):
        """Convert one liter of grated cheese to grams"""
        grams = norm(1, LITER, GRATED_CHEESE)
        assert grams == pytest.approx(380.000000, abs=1e-2)

    def test_one_tsp_cocoa(self):
        """Convert one metric teaspoon of cocoa to grams"""
        grams = norm(1, METRIC_TSP, COCOA)
        assert grams == pytest.approx(1.817505, abs=1e-2)

    def test_one_floz_cream(self):
        """Convert one US fluid ounce of cream to grams"""
        grams = norm(1, US_FLOZ, CREAM)
        assert grams == pytest.approx(29.749995, abs=1e-2)

    def test_one_floz_cornstarch(self):
        """Convert one imperial fluid ounce of cornstarch to grams"""
        grams = norm(1, IMP_FLOZ, CORNSTARCH)
        assert grams == pytest.approx(15.372197, abs=1e-2)

    def test_one_dstspn_potatostarch(self):
        """Convert one dessert spoon of potato starch to grams"""
        grams = norm(1, DSTSPN, POTATO_STARCH)
        assert grams == pytest.approx(7.200000, abs=1e-2)

    def test_one_tbsp_honey(self):
        """Convert one metric tablespoon of honey to grams"""
        grams = norm(1, METRIC_TBSP, HONEY)
        assert grams == pytest.approx(21.493065, abs=1e-2)

    def test_one_cup_milk(self):
        """Convert one metric cup of milk to grams"""
        grams = norm(1, METRIC_CUP, MILK)
        assert grams == pytest.approx(257.832250, abs=1e-2)

    def test_one_tbsp_sugar(self):
        """Convert one metric tablespoon of sugar to grams"""
        grams = norm(1, METRIC_TBSP, SUGAR)
        assert grams == pytest.approx(12.680265, abs=1e-2)

    def test_one_medium_egg(self):
        """Convert one medium egg to grams"""
        grams = norm(1, MEDIUM, EGG)
        assert grams == pytest.approx(44, abs=1e-2)

    def test_two_small_eggs(self):
        """Convert two small eggs to grams"""
        grams = norm(2, SMALL, EGG)
        assert grams == pytest.approx(76, abs=1e-2)

    def test_one_large_egg(self):
        """Convert one large egg to grams"""
        grams = norm(1, LARGE, EGG)
        assert grams == pytest.approx(50, abs=1e-2)

    def test_16_pinches_salt(self):
        """Convert 16 pinches of salt to grams"""
        grams = norm(16, PINCH, SALT)
        assert grams == pytest.approx(6.171065, abs=1e-2)

    def test_8_dashes_salt(self):
        """Convert 8 dashes of salt to grams"""
        grams = norm(8, DASH, SALT)
        assert grams == pytest.approx(6.171065, abs=1e-2)

    def test_one_stick_butters(self):
        """Convert one stick of butter to grams"""
        grams = norm(1, STICK, BUTTER, 1)
        assert grams == pytest.approx(113.0, abs=1e-2)

    def test_three_quarter_stick_butter(self):
        """Convert 3/4 stick of butter to grams"""
        grams = norm(0.75, STICK, BUTTER)
        assert grams == pytest.approx(84.75, abs=1e-2)

    def test_half_knob_butter(self):
        """Convert 1/2 knob of butter to grams"""
        grams = norm(0.5, KNOB, BUTTER)
        assert grams == pytest.approx(15, abs=1)

    def test_inapplicable_unit(self):
        """Check that an error is raised when an inapplicable unit of measure
        is specified"""
        with pytest.raises(BadUnitException) as exc_info:
            norm(0.75, STICK, SALT, line_nr=1)
        assert str(exc_info.value) == (
            "Inapplicable unit 'stick' used for ingredient 'salt' at line 1"
        )


class TestNormalizeColumns:
    """Test normalization of multiple rows and columns"""

    def test_three_rows_to_grams(self):
        """Convert three rows of mixed weight and volume measures to grams"""
        ingredients = (FLOUR, SUGAR, BUTTER)
        flour = ((7, OZ), (200, GRAM), (1, ML))
        sugar = ((1, METRIC_CUP), (4, OZ), (1, ML))
        butter = ((1, METRIC_CUP), (4, OZ), (1, ML))
        columns = zip(flour, sugar, butter, strict=False)
        new_columns = normalize(ingredients, columns)

        assert len(new_columns) == 3
        assert len(new_columns[0]) == 3

        assert new_columns[0][1] == pytest.approx(46.86, abs=1e-2)
        assert new_columns[1][1] == pytest.approx(26.569, abs=1e-2)
        assert new_columns[2][1] == pytest.approx(26.569, abs=1e-2)

    def test_normalize_to_100g(self):
        """Normalize a row of weight based measurements (in grams) to 100g"""
        flour = [1.0]
        milk = [3.75]
        egg = [1.01]
        butter = [0.16]
        columns = zip(flour, milk, egg, butter, strict=False)
        new_columns = list(normalize_to_100g(columns))
        assert len(new_columns) == 1
        assert len(new_columns[0]) == 4
        assert new_columns[0][0] == pytest.approx(16.9, abs=1e-1)
        assert new_columns[0][1] == pytest.approx(63.32, abs=1e-1)
        assert new_columns[0][2] == pytest.approx(17.05, abs=1e-1)
        assert new_columns[0][3] == pytest.approx(2.73, abs=1e-1)
