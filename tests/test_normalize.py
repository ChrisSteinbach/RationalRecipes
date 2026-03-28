"""Tests for data normalization"""

import unittest

import pytest

from rational_recipes.ingredient import (
    BUTTER,
    COCOA,
    CORNSTARCH,
    CREAM,
    EGG,
    FLOUR,
    GRATED_CHEESE,
    HONEY,
    MILK,
    POTATO_STARCH,
    SALT,
    SUGAR,
)
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
        pytest.param(US_PINT, 473.176, id="us_pint"),
        pytest.param(IMP_PINT, 568.261, id="imperial_pint"),
        pytest.param(IMP_FLOZ, 28.4131, id="imperial_floz"),
        pytest.param(US_FLOZ, 29.5735, id="us_floz"),
        pytest.param(LITER, 1000, id="liter"),
        pytest.param(METRIC_CUP, 250, id="metric_cup"),
        pytest.param(METRIC_TBSP, 15.0, id="metric_tbsp"),
        pytest.param(METRIC_TSP, 5.0, id="metric_tsp"),
    ],
)
def test_normalize_volume(unit, expected):
    """Convert one unit of volume to milliliters"""
    assert norm(1, unit) == pytest.approx(expected, abs=0.005)


class TestNormalizeVolumeToWeight(unittest.TestCase):
    """Test normalization of volume based measurements to grams using
    food density information."""

    def test_one_cup_flour(self):
        """Convert one metric cup of flour to grams"""
        grams = norm(1, METRIC_CUP, FLOUR)
        self.assertAlmostEqual(grams, 131.856499, 2)

    def test_one_tsp_salt(self):
        """Convert one metric teaspoon of salt to grams"""
        grams = norm(1, METRIC_TSP, SALT)
        self.assertAlmostEqual(grams, 6.329, 2)

    def test_one_tbsp_butter(self):
        """Convert one metric tablespoon of butter to grams"""
        grams = norm(1, METRIC_TBSP, BUTTER)
        self.assertAlmostEqual(grams, 15.1898, 2)

    def test_one_liter_grated_cheese(self):
        """Convert one liter of grated cheese to grams"""
        grams = norm(1, LITER, GRATED_CHEESE)
        self.assertAlmostEqual(grams, 379.7468, 2)

    def test_one_tsp_cocoa(self):
        """Convert one metric teaspoon of cocoa to grams"""
        grams = norm(1, METRIC_TSP, COCOA)
        self.assertAlmostEqual(grams, 6.944440, 2)

    def test_one_floz_cream(self):
        """Convert one US fluid ounce of cream to grams"""
        grams = norm(1, US_FLOZ, CREAM)
        self.assertAlmostEqual(grams, 23.001611, 2)

    def test_one_floz_cornstarch(self):
        """Convert one imperial fluid ounce of cornstarch to grams"""
        grams = norm(1, IMP_FLOZ, CORNSTARCH)
        self.assertAlmostEqual(grams, 18.182400, 2)

    def test_one_dstspn_potatostarch(self):
        """Convert one dessert spoon of potato starch to grams"""
        grams = norm(1, DSTSPN, POTATO_STARCH)
        self.assertAlmostEqual(grams, 7.200000, 2)

    def test_one_tbsp_honey(self):
        """Convert one metric tablespoon of honey to grams"""
        grams = norm(1, METRIC_TBSP, HONEY)
        self.assertAlmostEqual(grams, 19.50, 2)

    def test_one_cup_milk(self):
        """Convert one metric cup of milk to grams"""
        grams = norm(1, METRIC_CUP, MILK)
        self.assertAlmostEqual(grams, 250, 2)

    def test_one_tbsp_sugar(self):
        """Convert one metric tablespoon of sugar to grams"""
        grams = norm(1, METRIC_TBSP, SUGAR)
        self.assertAlmostEqual(grams, 12.658200, 2)

    def test_one_medium_egg(self):
        """Convert one medium egg to grams"""
        grams = norm(1, MEDIUM, EGG)
        self.assertAlmostEqual(grams, 53, 2)

    def test_two_small_eggs(self):
        """Convert two small eggs to grams"""
        grams = norm(2, SMALL, EGG)
        self.assertAlmostEqual(grams, 92, 2)

    def test_one_large_egg(self):
        """Convert one large egg to grams"""
        grams = norm(1, LARGE, EGG)
        self.assertAlmostEqual(grams, 60, 2)

    def test_16_pinches_salt(self):
        """Convert 16 pinches of salt to grams"""
        grams = norm(16, PINCH, SALT)
        self.assertAlmostEqual(grams, 6.329, 2)

    def test_8_dashes_salt(self):
        """Convert 8 dashes of salt to grams"""
        grams = norm(8, DASH, SALT)
        self.assertAlmostEqual(grams, 6.329, 2)

    def test_one_stick_butters(self):
        """Convert one stick of butter to grams"""
        grams = norm(1, STICK, BUTTER, 1)
        self.assertAlmostEqual(grams, 113.398, 2)

    def test_three_quarter_stick_butter(self):
        """Convert 3/4 stick of butter to grams"""
        grams = norm(0.75, STICK, BUTTER)
        self.assertAlmostEqual(grams, 85.0486, 2)

    def test_half_knob_butter(self):
        """Convert 1/2 knob of butter to grams"""
        grams = norm(0.5, KNOB, BUTTER)
        self.assertAlmostEqual(grams, 15, 0)

    def test_inapplicable_unit(self):
        """Check that an error is raised when an inapplicable unit of measure
        is specified"""
        with self.assertRaises(BadUnitException) as cm:
            norm(0.75, STICK, SALT, line_nr=1)
        self.assertEqual(
            "Inapplicable unit 'stick' used for ingredient 'salt' at line 1",
            str(cm.exception),
        )


class TestNormalizeColumns(unittest.TestCase):
    """Test normalization of multiple rows and columns"""

    def test_three_rows_to_grams(self):
        """Convert three rows of mixed weight and volume measures to grams"""
        ingredients = (FLOUR, SUGAR, BUTTER)
        flour = ((7, OZ), (200, GRAM), (1, ML))
        sugar = ((1, METRIC_CUP), (4, OZ), (1, ML))
        butter = ((1, METRIC_CUP), (4, OZ), (1, ML))
        columns = zip(flour, sugar, butter, strict=False)
        new_columns = normalize(ingredients, columns)

        self.assertEqual(len(new_columns), 3)
        self.assertEqual(len(new_columns[0]), 3)

        self.assertAlmostEqual(new_columns[0][1], 46.86, 2)
        self.assertAlmostEqual(new_columns[1][1], 26.569, 2)
        self.assertAlmostEqual(new_columns[2][1], 26.569, 2)

    def test_normalize_to_100g(self):
        """Normalize a row of weight based measurements (in grams) to 100g"""
        flour = [1.0]
        milk = [3.75]
        egg = [1.01]
        butter = [0.16]
        columns = zip(flour, milk, egg, butter, strict=False)
        new_columns = list(normalize_to_100g(columns))
        self.assertEqual(len(new_columns), 1)
        self.assertEqual(len(new_columns[0]), 4)
        self.assertAlmostEqual(new_columns[0][0], 16.9, 1)
        self.assertAlmostEqual(new_columns[0][1], 63.32, 1)
        self.assertAlmostEqual(new_columns[0][2], 17.05, 1)
        self.assertAlmostEqual(new_columns[0][3], 2.73, 1)
