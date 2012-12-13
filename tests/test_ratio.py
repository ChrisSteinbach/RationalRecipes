import unittest
from tests.test_utils import normalize
from ratio import calculate_ratio, Ratio, percentage_difference
from ingredient import FLOUR, EGG, BUTTER
from numpy import array

def test_data():
    ingredients = (FLOUR, EGG, BUTTER)
    flour = array([1,1,1])
    egg = array([2,2,2])
    butter = array([3,3,3])
    return ingredients, zip(flour, egg, butter)

class TestRatio(unittest.TestCase):

    def ratio(self, ingredients, proportions):
        return Ratio(ingredients, proportions, [], [])

    def testCalculateRatio(self):
        ingredients, proportions = test_data()
        ratio = calculate_ratio(ingredients, proportions)
        ratio.set_precision(0)
        self.assertEquals(str(ratio).split()[0], "1:2:3")

    def testPrecision(self):
        ingredients, proportions = test_data()
        ratio = self.ratio(ingredients, [1,2,3])
        ratio.set_precision(2)
        self.assertEquals(str(ratio).split()[0], "1.00:2.00:3.00")

    def testScaledRatio(self):
        ingredients, proportions = test_data()
        ratio = self.ratio(ingredients, [1,2,3])
        ratio.set_precision(0)
        ratio.set_scale(10)
        self.assertEquals(str(ratio).split()[0], "10:20:30")
        
    def testDescribeValue(self):
        ingredients, proportions = test_data()
        ratio = self.ratio(ingredients, [1,2,3])
        self.assertEquals(ratio[0].describe(), "1.00g or 1.90ml all purpose flour")

    def testDescribeScaledValue(self):
        ingredients, proportions = test_data()
        ratio = self.ratio(ingredients, [1,2,3])
        ratio.set_scale(10)
        self.assertEquals(ratio[0].describe(), "10.00g or 18.96ml all purpose flour")

    def testDescribeWholeUnitValue(self):
        ingredients, proportions = test_data()
        ratio = self.ratio(ingredients, [1,2,3])
        self.assertEquals(ratio[1].describe(), "2.00g, 1.69ml or 0.00 medium egg(s) where each medium egg is 53.00g")

    def testRecipeByPortionSize(self):
        ingredients, proportions = test_data()
        ratio = self.ratio(ingredients, [1,2,3])
        ratio.set_precision(0)
        expectedRecipe="""33g or 63ml all purpose flour
67g, 56ml or 1 medium egg(s) where each medium egg is 53g
100g or 99ml butter"""
        self.assertEquals(ratio.recipe(weight=200), expectedRecipe)

    def testPercentageDifferenceZero(self):
        ingredients, proportions = test_data()
        r1 = self.ratio(ingredients, [1,2,3])
        r2 = self.ratio(ingredients, [1,2,3])
        percentage_difference, differences = r1.percentage_difference(r2)
        self.assertEquals(0, percentage_difference)

    def testPercentageDifference(self):
        ingredients, proportions = test_data()
        r1 = self.ratio(ingredients, [1,30,100])
        r2 = self.ratio(ingredients, [1,60,50])
        percentage_difference, differences = r1.percentage_difference(r2)
        self.assertAlmostEquals(0.496, percentage_difference, 2)
        self.assertAlmostEquals(differences[1][0], 0.81, 2)
        self.assertEquals(differences[1][1], EGG)
        self.assertAlmostEquals(differences[0][0], 0.17, 2)
        self.assertEquals(differences[0][1], FLOUR)

class TestRatioValue(unittest.TestCase):
    def testPercentageDifference(self):
        v1 = 3999.9999
        v2 = 4000
        d1 = percentage_difference(v2, v1)
        d2 = percentage_difference(v1, v2)
        self.assertEquals(d1, d2)
        self.assertAlmostEquals(0.00000, d1, 2)

    def testPercentageDifference50pc(self):
        v1 = 2000
        v2 = 4000
        d1 = percentage_difference(v2, v1)
        d2 = percentage_difference(v1, v2)
        self.assertEquals(d1, d2)
        self.assertAlmostEquals(0.666, d1, 2)

    def testPercentageDifference99pc(self):
        v1 = 0.0000000001
        v2 = 4000
        d1 = percentage_difference(v2, v1)
        d2 = percentage_difference(v1, v2)
        self.assertEquals(d1, d2)
        self.assertAlmostEquals(1.999999, d1, 2)

if __name__ == "__main__":
    unittest.main()
