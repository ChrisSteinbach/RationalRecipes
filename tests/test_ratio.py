"""Unit tests for create_ratio module"""
import unittest
from ratio import calculate_ratio_and_stats, Ratio
from ingredient import FLOUR, EGG, BUTTER
from numpy import array

def calculate_ratio(ingredients, proportions, desired_interval=0.5):
    """Calculate ratio proportions from input data."""
    _, ratio = calculate_ratio_and_stats(ingredients, proportions,
                                         desired_interval)
    return ratio

def test_data():
    """Shared test data"""
    ingredients = (FLOUR, EGG, BUTTER)
    flour = array([1, 1, 1])
    egg = array([2, 2, 2])
    butter = array([3, 3, 3])
    return ingredients, zip(flour, egg, butter)

def create_ratio(ingredients, proportions, restrictions=None):
    """Wrapper for Ratio class creation"""
    ratio = Ratio(ingredients, proportions)
    if restrictions is not None:
        ratio.set_restrictions(restrictions)
    return ratio

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

    def test_recipe_retricted_weight(self):
        """Test output of a recipe ratio scaled to a total weight and then
           restricted on one ingredient"""
        ingredients, _ = test_data()
        ratio = create_ratio(ingredients, [1, 2, 3], [(1, 31.5)])
        ratio.set_precision(0)
        expected_recipe = """16g or 30ml all purpose flour
32g, 27ml or 1 egg(s) where each egg is 53g
47g or 47ml butter"""
        self.assertEquals(ratio.recipe(weight=200), expected_recipe)

    def test_retrict_weight_ny_name(self):
        """Test output of a recipe ratio scaled to a total weight and then
           restricted on one ingredient"""
        ingredients, _ = test_data()
        ratio = create_ratio(ingredients, [1, 2, 3], [("egg", 31.5)])
        ratio.set_precision(0)
        expected_recipe = """16g or 30ml all purpose flour
32g, 27ml or 1 egg(s) where each egg is 53g
47g or 47ml butter"""
        self.assertEquals(ratio.recipe(weight=200), expected_recipe)

    def test_retricted_weight_multiple(self):
        """Test output of a recipe ratio scaled to a total weight and then
           restricted on one ingredient"""
        ingredients, _ = test_data()
        ratio = create_ratio(ingredients, [1, 2, 3],
                             [(0, 17), (1, 31.5), (2, 48)])
        ratio.set_precision(0)
        expected_recipe = """16g or 30ml all purpose flour
32g, 27ml or 1 egg(s) where each egg is 53g
47g or 47ml butter"""
        self.assertEquals(ratio.recipe(weight=200), expected_recipe)