"""Unit tests for create_ratio module"""
import unittest
from ratio import calculate_ratio, Ratio, percentage_change
from ratio import percentage_difference, percentage_difference_from_mean
from ingredient import FLOUR, EGG, BUTTER
from numpy import array

def test_data():
    """Shared test data"""
    ingredients = (FLOUR, EGG, BUTTER)
    flour = array([1, 1, 1])
    egg = array([2, 2, 2])
    butter = array([3, 3, 3])
    return ingredients, zip(flour, egg, butter)

def create_ratio(ingredients, proportions):
    """Wrapper for Ratio class creation"""
    return Ratio(ingredients, proportions, [], [])

class TestRatio(unittest.TestCase):
    """Unit test class for ratio module"""
    
    def test_calculate_ratio(self):
        """Calculate simple ratio from three ingredients and three identical
        recipes"""
        ingredients, proportions = test_data()
        ratio = calculate_ratio(ingredients, proportions)
        ratio.set_precision(0)
        self.assertEquals(str(ratio).split()[0], "1:2:3")

    def test_precision(self):
        """Test output precision to two decimal places"""
        ingredients, _ = test_data()
        ratio = create_ratio(ingredients, [1, 2, 3])
        ratio.set_precision(2)
        self.assertEquals(str(ratio).split()[0], "1.00:2.00:3.00")

    def test_scaled_ratio(self):
        """Test scaling of all ratio values by a scalar"""
        ingredients, _ = test_data()
        ratio = create_ratio(ingredients, [1, 2, 3])
        ratio.set_precision(0)
        ratio.set_scale(10)
        self.assertEquals(str(ratio).split()[0], "10:20:30")
        
    def test_describe_value(self):
        """Test ratio output as if in a recipe using weight and volume measures
        """
        ingredients, _ = test_data()
        ratio = create_ratio(ingredients, [1, 2, 3])
        self.assertEquals(ratio[0].describe(),
                          "1.00g or 1.90ml all purpose flour")

    def test_describe_scaled_value(self):
        """Test output of scaled recipe"""
        ingredients, _ = test_data()
        ratio = create_ratio(ingredients, [1, 2, 3])
        ratio.set_scale(10)
        self.assertEquals(ratio[0].describe(),
                          "10.00g or 18.96ml all purpose flour")

    def test_describe_wholeunit_ualue(self):
        """Test descriptive output of whole-unit values (eggs in  this case)"""
        ingredients, _ = test_data()
        ratio = create_ratio(ingredients, [1, 2, 3])
        self.assertEquals(ratio[1].describe(),
                        "2.00g, 1.69ml or 0.00 egg(s) where each egg is 53.00g")

    def test_recipe_by_total_weight(self):
        """Test output of a recipe ratio scaled to a total weight"""
        ingredients, _ = test_data()
        ratio = create_ratio(ingredients, [1, 2, 3])
        ratio.set_precision(0)
        expected_recipe = """33g or 63ml all purpose flour
67g, 56ml or 1 egg(s) where each egg is 53g
100g or 99ml butter"""
        self.assertEquals(ratio.recipe(weight=200), expected_recipe)

    def test_percentage_difference_zero(self):
        """Test difference of two identical ratios"""
        ingredients, _ = test_data()
        ratio_1 = create_ratio(ingredients, [1, 2, 3])
        ratio_2 = create_ratio(ingredients, [1, 2, 3])
        difference, _ = ratio_1.percentage_difference(ratio_2)
        self.assertEquals(0, difference)

    def test_percentage_difference(self):
        """Test per ingredient and over all percentage difference"""
        ingredients, _ = test_data()
        ratio_1 = create_ratio(ingredients, [1, 30, 100])
        ratio_2 = create_ratio(ingredients, [1, 60, 50])
        difference, differences = ratio_1.percentage_difference(ratio_2)
        self.assertAlmostEquals(0.496, difference, 2)
        self.assertAlmostEquals(differences[1][0], 0.81, 2)
        self.assertEquals(differences[1][1], EGG)
        self.assertAlmostEquals(differences[0][0], 0.17, 2)
        self.assertEquals(differences[0][1], FLOUR)

class TestRatioValue(unittest.TestCase):
    """Tests for RatioValue class"""
    
    def test_percentage_difference(self):
        """Percentage difference between two values that are almost equal"""
        value_1 = 3999.9999
        value_2 = 4000
        diff_1 = percentage_difference(value_2, value_1)
        diff_2 = percentage_difference(value_1, value_2)
        self.assertEquals(diff_1, diff_2)
        self.assertAlmostEquals(0.00000, diff_1, 2)

    def test_percentage_diff_from_mean(self):
        """Percentage difference from the mean between two values that are 
           almost equal"""
        value_1 = 3999.9999
        value_2 = 4000
        diff_1 = percentage_difference_from_mean(value_2, value_1)
        diff_2 = percentage_difference_from_mean(value_1, value_2)
        self.assertEquals(diff_1, diff_2)
        self.assertAlmostEquals(0.00000, diff_1, 2)

    def test_percentage_change(self):
        """Percentage change between two values that are almost equal"""
        value_1 = 3999.9999999
        value_2 = 4000
        diff_1 = percentage_change(value_2, value_1)
        diff_2 = percentage_change(value_1, value_2)
        self.assertAlmostEquals(diff_1, diff_2)
        self.assertAlmostEquals(0.00000, diff_1, 2)
        
    def test_percentage_difference_50pc(self):
        """Percentage difference between values where one is 50% of the other"""
        value_1 = 2000
        value_2 = 4000
        diff_1 = percentage_difference(value_2, value_1)
        diff_2 = percentage_difference(value_1, value_2)
        self.assertEquals(diff_1, diff_2)
        self.assertAlmostEquals(0.666, diff_1, 2)

    def test_pc_diff_from_mean_50pc(self):
        """Percentage difference from the mean between values where one is 50%
           of the other"""
        value_1 = 2000
        value_2 = 4000
        diff_1 = percentage_difference_from_mean(value_2, value_1)
        diff_2 = percentage_difference_from_mean(value_1, value_2)
        self.assertEquals(diff_1, diff_2)
        self.assertAlmostEquals(0.333, diff_1, 2)

    def test_percentage_change_50pc(self):
        """Percentage change between values where one is 50% of the other"""
        value_1 = 2000
        value_2 = 4000
        diff_1 = percentage_change(value_2, value_1)
        diff_2 = percentage_change(value_1, value_2)
        self.assertEquals(abs(diff_1), diff_2)
        self.assertAlmostEquals(1, diff_2, 2)

    def test_percentage_difference_99pc(self):
        """Percentage difference between a very small value and a relatively
           large value"""
        value_1 = 0.0000000001
        value_2 = 2**32
        diff_1 = percentage_difference(value_2, value_1)
        diff_2 = percentage_difference(value_1, value_2)
        self.assertEquals(diff_1, diff_2)
        self.assertAlmostEquals(1.999999, diff_1, 2)

    def test_pc_diff_from_mean_99pc(self):
        """Percentage difference from the mean between a very small value and a
           relatively large value"""
        value_1 = 0.0000000001
        value_2 = 2**32
        diff_1 = percentage_difference_from_mean(value_2, value_1)
        diff_2 = percentage_difference_from_mean(value_1, value_2)
        self.assertEquals(diff_1, diff_2)
        self.assertAlmostEquals(0.999999, diff_1, 2)

    def test_percentage_change_99pc(self):
        """Percentage change between a very small value and a relatively large 
           value"""
        value_1 = 1
        value_2 = (2**32) + 1
        diff_1 = percentage_change(value_2, value_1)
        diff_2 = percentage_change(value_1, value_2)
        self.assertAlmostEquals(-0.999999, diff_1, 2)
        self.assertAlmostEquals(2**32, diff_2, 2)
