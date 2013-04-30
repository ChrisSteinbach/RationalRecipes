"""Unit tests for percentage difference and change"""
import unittest
from RationalRecipes.difference import calc_percentage_change
from RationalRecipes.difference import calc_percentage_difference
from RationalRecipes.difference import percentage_difference_from_mean
from RationalRecipes.difference import percentage_difference
from RationalRecipes.ingredient import FLOUR, EGG
from tests.test_ratio import create_ratio, test_data


class TestDifference(unittest.TestCase):
    """Tests for RatioValue class"""
    
    def test_percentage_difference(self):
        """Percentage difference between two values that are almost equal"""
        value_1 = 3999.9999
        value_2 = 4000
        diff_1 = calc_percentage_difference(value_2, value_1)
        diff_2 = calc_percentage_difference(value_1, value_2)
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
        diff_1 = calc_percentage_change(value_2, value_1)
        diff_2 = calc_percentage_change(value_1, value_2)
        self.assertAlmostEquals(diff_1, diff_2)
        self.assertAlmostEquals(0.00000, diff_1, 2)
        
    def test_percentage_difference_50pc(self):
        """Percentage difference between values where one is 50% of the other"""
        value_1 = 2000
        value_2 = 4000
        diff_1 = calc_percentage_difference(value_2, value_1)
        diff_2 = calc_percentage_difference(value_1, value_2)
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
        diff_1 = calc_percentage_change(value_2, value_1)
        diff_2 = calc_percentage_change(value_1, value_2)
        self.assertEquals(abs(diff_1), diff_2)
        self.assertAlmostEquals(1, diff_2, 2)

    def test_percentage_difference_99pc(self):
        """Percentage difference between a very small value and a relatively
           large value"""
        value_1 = 0.0000000001
        value_2 = 2**32
        diff_1 = calc_percentage_difference(value_2, value_1)
        diff_2 = calc_percentage_difference(value_1, value_2)
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
        diff_1 = calc_percentage_change(value_2, value_1)
        diff_2 = calc_percentage_change(value_1, value_2)
        self.assertAlmostEquals(-0.999999, diff_1, 2)
        self.assertAlmostEquals(2**32, diff_2, 2)

    def test_ratio_difference_zero(self):
        """Test difference of two identical ratios"""
        ingredients, _ = test_data()
        ratio_1 = create_ratio(ingredients, [1, 2, 3])
        ratio_2 = create_ratio(ingredients, [1, 2, 3])
        difference, _ = percentage_difference(ratio_1, ratio_2)
        self.assertEquals(0, difference)

    def test_ratio_difference(self):
        """Test per ingredient and over all percentage difference"""
        ingredients, _ = test_data()
        ratio_1 = create_ratio(ingredients, [1, 30, 100])
        ratio_2 = create_ratio(ingredients, [1, 60, 50])
        difference, differences = percentage_difference(ratio_1, ratio_2)
        self.assertAlmostEquals(0.496, difference, 2)
        self.assertAlmostEquals(differences[1][0], 0.81, 2)
        self.assertEquals(differences[1][1], EGG)
        self.assertAlmostEquals(differences[0][0], 0.17, 2)
        self.assertEquals(differences[0][1], FLOUR)
