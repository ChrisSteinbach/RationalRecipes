"""Tests for data normalization"""
import unittest
from units import OZ, GRAM, KG, LB, US_PINT, IMP_PINT, IMP_FLOZ, ML
from units import US_FLOZ, LITER, METRIC_CUP, METRIC_TBSP, METRIC_TSP
from units import DSTSPN, MEDIUM, SMALL, LARGE, PINCH, STICK, DASH, KNOB
from units import BadUnitException
from ingredient import SALT, FLOUR, BUTTER, SUGAR, GRATED_CHEESE, COCOA
from ingredient import CREAM, CORNSTARCH, POTATO_STARCH, MILK, EGG, HONEY
from normalize import normalize_to_100g
from tests.test_utils import normalize, norm

class TestNormalizeWeight(unittest.TestCase):
    """Test normalization of weight measurements"""
    
    def test_one_ounce(self):
        """Convert one ounce to grams"""
        grams = norm(1, OZ)
        self.assertAlmostEquals(grams, 28.3495231, 2)

    def test_two_ounces(self):
        """Convert two ounces to grams"""
        grams = norm(2, OZ)
        self.assertAlmostEquals(grams, 56.699, 2)

    def test_one_gram(self):
        """One gram should normalize to...one gram"""
        grams = norm(1, GRAM)
        self.assertEquals(grams, 1)

    def test_two_grams(self):
        """Two grams should remain two grams after normalization"""
        grams = norm(2, GRAM)
        self.assertEquals(grams, 2)

    def test_one_kg(self):
        """Convert one kilogram to grams"""
        grams = norm(1, KG)
        self.assertEquals(grams, 1000)

    def test_two_kg(self):
        """Convert two kilograms to grams"""
        grams = norm(2, KG)
        self.assertEquals(grams, 2000)

    def test_one_pound(self):
        """Convert one pound to grams"""
        grams = norm(1, LB)
        self.assertAlmostEquals(grams, 453.592, 2)

    def test_two_pounds(self):
        """Convert two pounds to grams"""
        grams = norm(2, LB)
        self.assertAlmostEquals(grams, 907.185, 2)

class TestNormalizeVolume(unittest.TestCase):
    """Test normalization of volume based measures to milliliters"""
    
    def test_one_us_pint(self):
        """Convert one US pint to milliliters"""
        milliliters = norm(1, US_PINT)
        self.assertAlmostEquals(milliliters, 473.176, 2)

    def test_two_us_pints(self):
        """Convert two US pints to milliliters"""
        milliliters = norm(2, US_PINT)
        self.assertAlmostEquals(milliliters, 946.353, 2)

    def test_one_imperial_pint(self):
        """Convert one imperial pint to milliliters"""
        milliliters = norm(1, IMP_PINT)
        self.assertAlmostEquals(milliliters, 568.261, 2)

    def test_two_imperial_pints(self):
        """Convert two imperial pints to milliliters"""
        milliliters = norm(2, IMP_PINT)
        self.assertAlmostEquals(milliliters, 1136.52, 2)

    def test_one_imperial_fluid_oz(self):
        """Convert one imperial fluid ounce to milliliters"""
        milliliters = norm(1, IMP_FLOZ)
        self.assertAlmostEquals(milliliters, 28.4131, 2)

    def test_two_imperial_fluid_oz(self):
        """Convert two imperial fluid ounces to milliliters"""
        milliliters = norm(2, IMP_FLOZ)
        self.assertAlmostEquals(milliliters, 56.8261, 2)

    def test_one_us_fluid_oz(self):
        """Convert one US fluid ounce to milliliters"""
        milliliters = norm(1, US_FLOZ)
        self.assertAlmostEquals(milliliters, 29.5735, 2)

    def test_two_us_fluid_oz(self):
        """Convert two US fluid ounces to milliliters"""
        milliliters = norm(2, US_FLOZ)
        self.assertAlmostEquals(milliliters, 59.1471, 2)

    def test_one_liter(self):
        """Convert one liter to milliliters"""
        milliliters = norm(1, LITER)
        self.assertEquals(milliliters, 1000)

    def test_two_liters(self):
        """Convert two liters to milliliters"""
        milliliters = norm(2, LITER)
        self.assertEquals(milliliters, 2000)

    def test_one_metric_cup(self):
        """Convert one metric cup to milliliters"""
        milliliters = norm(1, METRIC_CUP)
        self.assertEquals(milliliters, 250)

    def test_two_metric_cups(self):
        """Convert two metric cups to milliliters"""
        milliliters = norm(2, METRIC_CUP)
        self.assertEquals(milliliters, 500)

    def test_one_metric_tbsp(self):
        """Convert one metric tablespoon to milliliters"""
        milliliters = norm(1, METRIC_TBSP)
        self.assertAlmostEquals(milliliters, 15.0, 2)

    def test_two_metric_tbsps(self):
        """Convert two metric tablespoons to milliliters"""
        milliliters = norm(2, METRIC_TBSP)
        self.assertAlmostEquals(milliliters, 30.0, 2)

    def test_one_metric_tsp(self):
        """Convert one metric teaspoon to milliliters"""
        milliliters = norm(1, METRIC_TSP)
        self.assertAlmostEquals(milliliters, 5.0, 2)

    def test_two_metric_tsp(self):
        """Convert two metric teaspoons to milliliters"""
        milliliters = norm(2, METRIC_TSP)
        self.assertAlmostEquals(milliliters, 10.0, 2)

class TestNormalizeVolumeToWeight(unittest.TestCase):
    """Test normalization of volume based measurements to grams using
       food density information."""
       
    def test_one_cup_flour(self):
        """Convert one metric cup of flour to grams"""
        grams = norm(1, METRIC_CUP, FLOUR)
        self.assertAlmostEquals(grams, 131.856499, 2)

    def test_one_tsp_salt(self):
        """Convert one metric teaspoon of salt to grams"""
        grams = norm(1, METRIC_TSP, SALT)
        self.assertAlmostEquals(grams, 6.329, 2)

    def test_one_tbsp_butter(self):
        """Convert one metric tablespoon of butter to grams"""
        grams = norm(1, METRIC_TBSP, BUTTER)
        self.assertAlmostEquals(grams, 15.1898, 2)

    def test_one_liter_grated_cheese(self):
        """Convert one liter of grated cheese to grams"""
        grams = norm(1, LITER, GRATED_CHEESE)
        self.assertAlmostEquals(grams, 379.7468, 2)

    def test_one_tsp_cocoa(self):
        """Convert one metric teaspoon of cocoa to grams"""
        grams = norm(1, METRIC_TSP, COCOA)
        self.assertAlmostEquals(grams, 6.944440, 2)

    def test_one_floz_cream(self):
        """Convert one US fluid ounce of cream to grams"""
        grams = norm(1, US_FLOZ, CREAM)
        self.assertAlmostEquals(grams, 23.001611, 2)

    def test_one_floz_cornstarch(self):
        """Convert one imperial fluid ounce of cornstarch to grams"""
        grams = norm(1, IMP_FLOZ, CORNSTARCH)
        self.assertAlmostEquals(grams, 18.182400, 2)

    def test_one_dstspn_potatostarch(self):
        """Convert one dessert spoon of potato starch to grams"""
        grams = norm(1, DSTSPN, POTATO_STARCH)
        self.assertAlmostEquals(grams, 7.200000, 2)

    def test_one_tbsp_honey(self):
        """Convert one metric tablespoon of honey to grams"""
        grams = norm(1, METRIC_TBSP, HONEY)
        self.assertAlmostEquals(grams, 19.50, 2)

    def test_one_cup_milk(self):
        """Convert one metric cup of milk to grams"""
        grams = norm(1, METRIC_CUP, MILK)
        self.assertAlmostEquals(grams, 250, 2)

    def test_one_tbsp_sugar(self):
        """Convert one metric tablespoon of sugar to grams"""
        grams = norm(1, METRIC_TBSP, SUGAR)
        self.assertAlmostEquals(grams, 12.658200, 2)

    def test_one_medium_egg(self):
        """Convert one medium egg to grams"""
        grams = norm(1, MEDIUM, EGG)
        self.assertAlmostEquals(grams, 53, 2)

    def test_two_small_eggs(self):
        """Convert two small eggs to grams"""
        grams = norm(2, SMALL, EGG)
        self.assertAlmostEquals(grams, 92, 2)

    def test_one_large_egg(self):
        """Convert one large egg to grams"""
        grams = norm(1, LARGE, EGG)
        self.assertAlmostEquals(grams, 60, 2)

    def test_16_pinches_salt(self):
        """Convert 16 pinches of salt to grams"""
        grams = norm(16, PINCH, SALT)
        self.assertAlmostEquals(grams, 6.329, 2)

    def test_8_dashes_salt(self):
        """Convert 8 dashes of salt to grams"""
        grams = norm(8, DASH, SALT)
        self.assertAlmostEquals(grams, 6.329, 2)

    def test_one_stick_butters(self):
        """Convert one stick of butter to grams"""
        grams = norm(1, STICK, BUTTER, 1)
        self.assertAlmostEquals(grams, 113.398, 2)

    def test_three_quarter_stick_butter(self):
        """Convert 3/4 stick of butter to grams"""
        grams = norm(0.75, STICK, BUTTER)
        self.assertAlmostEquals(grams, 85.0486, 2)

    def test_half_knob_butter(self):
        """Convert 1/2 knob of butter to grams"""
        grams = norm(0.5, KNOB, BUTTER)
        self.assertAlmostEquals(grams, 15, 0)

    def test_inapplicable_unit(self):
        """Check that an error is raised when an inapplicable unit of measure
           is specified"""
        try:
            norm(0.75, STICK, SALT, line_nr=1)
            self.fail("Expected error")
        except BadUnitException, error:
            self.assertEquals(
              "Inapplicable unit 'stick' used for ingredient 'salt' at line 1",
              str(error))
            
class TestNormalizeColumns(unittest.TestCase):
    """Test normalization of multiple rows and columns"""
    
    def test_three_rows_to_grams(self):
        """Convert three rows of mixed weight and volume measures to grams"""
        ingredients = (FLOUR, SUGAR, BUTTER)
        flour = ((7, OZ), (200, GRAM), (1, ML))
        sugar = ((1, METRIC_CUP), (4, OZ), (1, ML))
        butter = ((1, METRIC_CUP), (4, OZ), (1, ML))
        columns = zip(flour, sugar, butter)
        new_columns = normalize(ingredients, columns)
        
        self.assertEquals(len(new_columns), 3)
        self.assertEquals(len(new_columns[0]), 3)

        self.assertAlmostEquals(new_columns[0][1], 46.86, 2)
        self.assertAlmostEquals(new_columns[1][1], 26.569, 2)
        self.assertAlmostEquals(new_columns[2][1], 26.569, 2)

    def test_normalize_to_100g(self):
        """Normalize a row of weight based measurements (in grams) to 100g""" 
        flour = [1.0]
        milk = [3.75]
        egg = [1.01]
        butter = [0.16]
        columns = zip(flour, milk, egg, butter)
        new_columns = list(normalize_to_100g(columns))
        self.assertEquals(len(new_columns), 1)
        self.assertEquals(len(new_columns[0]), 4)
        self.assertAlmostEquals(new_columns[0][0], 16.9, 1)
        self.assertAlmostEquals(new_columns[0][1], 63.32, 1)
        self.assertAlmostEquals(new_columns[0][2], 17.05, 1)
        self.assertAlmostEquals(new_columns[0][3], 2.73, 1)

