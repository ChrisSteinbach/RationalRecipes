import unittest
from units import *
from ingredient import *
from normalize import normalize_to_100g
from merge import merge_columns
from tests.test_utils import normalize, norm
from numpy import array

class TestNormalizeWeight(unittest.TestCase):

    def testOneOunceToGrams(self):
        grams = norm(1, OZ)
        self.assertAlmostEquals(grams, 28.3495231, 2)

    def testTwoOuncesToGrams(self):
        grams = norm(2, OZ)
        self.assertAlmostEquals(grams, 56.699, 2)

    def testOneGramToGrams(self):
        grams = norm(1, GRAM)
        self.assertEquals(grams, 1)

    def testTwoGramToGrams(self):
        grams = norm(2, GRAM)
        self.assertEquals(grams, 2)

    def testOneKgToGrams(self):
        grams = norm(1, KG)
        self.assertEquals(grams, 1000)

    def testTwoKgToGrams(self):
        grams = norm(2, KG)
        self.assertEquals(grams, 2000)

    def testOnePoundToGrams(self):
        grams = norm(1, LB)
        self.assertAlmostEquals(grams, 453.592, 2)

    def testTwoPoundsToGrams(self):
        grams = norm(2, LB)
        self.assertAlmostEquals(grams, 907.185, 2)

class TestNormalizeVolume(unittest.TestCase):

    def testOneUsPintToMl(self):
        ml = norm(1, US_PINT)
        self.assertAlmostEquals(ml, 473.176, 2)

    def testTwoUsPintsToMl(self):
        ml = norm(2, US_PINT)
        self.assertAlmostEquals(ml, 946.353, 2)

    def testOneImperialPintToMl(self):
        ml = norm(1, IMP_PINT)
        self.assertAlmostEquals(ml, 568.261, 2)

    def testTwoImperialPintsToMl(self):
        ml = norm(2, IMP_PINT)
        self.assertAlmostEquals(ml, 1136.52, 2)

    def testOneImperialFluidOzToMl(self):
        ml = norm(1, IMP_FLOZ)
        self.assertAlmostEquals(ml, 28.4131, 2)

    def testTwoImperialFluidOzToMl(self):
        ml = norm(2, IMP_FLOZ)
        self.assertAlmostEquals(ml, 56.8261, 2)

    def testOneUSFluidOzToMl(self):
        ml = norm(1, US_FLOZ)
        self.assertAlmostEquals(ml, 29.5735, 2)

    def testTwoUSFluidOzToMl(self):
        ml = norm(2, US_FLOZ)
        self.assertAlmostEquals(ml, 59.1471, 2)

    def testOneLiterToMl(self):
        ml = norm(1, LITER)
        self.assertEquals(ml, 1000)

    def testTwoLiterToMl(self):
        ml = norm(2, LITER)
        self.assertEquals(ml, 2000)

    def testOneCupToMl(self):
        ml = norm(1, METRIC_CUP)
        self.assertEquals(ml, 250)

    def testTwoCupsToMl(self):
        ml = norm(2, METRIC_CUP)
        self.assertEquals(ml, 500)

    def testOneTbspToMl(self):
        ml = norm(1, METRIC_TBSP)
        self.assertAlmostEquals(ml, 15.0, 2)

    def testTwoTbspsToMl(self):
        ml = norm(2, METRIC_TBSP)
        self.assertAlmostEquals(ml, 30.0, 2)

    def testOneTspToMl(self):
        ml = norm(1, METRIC_TSP)
        self.assertAlmostEquals(ml, 5.0, 2)

    def testTwoTspToMl(self):
        ml = norm(2, METRIC_TSP)
        self.assertAlmostEquals(ml, 10.0, 2)

class TestNormalizeFoodVolume(unittest.TestCase):

    def testOneCupFlourToGrams(self):
        grams = norm(1, METRIC_CUP, FLOUR)
        self.assertAlmostEquals(grams, 131.856499, 2)

    def testOneTeaspoonSaltToGrams(self):
        grams = norm(1, METRIC_TSP, SALT)
        self.assertAlmostEquals(grams, 6.329, 2)

    def testOneTablespoonButterToGrams(self):
        grams = norm(1, METRIC_TBSP, BUTTER)
        self.assertAlmostEquals(grams, 15.1898, 2)

    def testOneLiterGratedCheeseToGrams(self):
        grams = norm(1, LITER, GRATED_CHEESE)
        self.assertAlmostEquals(grams, 379.7468, 2)

    def testOneTspCocoaToGrams(self):
        grams = norm(1, METRIC_TSP, COCOA)
        self.assertAlmostEquals(grams, 6.944440, 2)

    def testOneUsOzCreamToGrams(self):
        grams = norm(1, US_FLOZ, CREAM)
        self.assertAlmostEquals(grams, 23.001611, 2)

    def testOneImperialOzCornstarchToGrams(self):
        grams = norm(1, IMP_FLOZ, CORNSTARCH)
        self.assertAlmostEquals(grams, 18.182400, 2)

    def testOneImperialOzPotatostarchToGrams(self):
        grams = norm(1, DSTSPN, POTATO_STARCH)
        self.assertAlmostEquals(grams, 7.200000, 2)

    def testOneTablespoonHoneyToGrams(self):
        grams = norm(1, METRIC_TBSP, HONEY)
        self.assertAlmostEquals(grams, 19.50, 2)

    def testOneCupMilkToGrams(self):
        grams = norm(1, METRIC_CUP, MILK)
        self.assertAlmostEquals(grams, 250, 2)

    def testOneTablespoonSugarToGrams(self):
        grams = norm(1, METRIC_TBSP, SUGAR)
        self.assertAlmostEquals(grams, 12.658200, 2)

    def testOneMediumEggToGrams(self):
        grams = norm(1, MEDIUM, EGG)
        self.assertAlmostEquals(grams, 53, 2)

    def testTwoSmallEggToGrams(self):
        grams = norm(2, SMALL, EGG)
        self.assertAlmostEquals(grams, 92, 2)

    def testOneLargeEggToGrams(self):
        grams = norm(1, LARGE, EGG)
        self.assertAlmostEquals(grams, 60, 2)

    def test16PinchesSaltToGrams(self):
        grams = norm(16, PINCH, SALT)
        self.assertAlmostEquals(grams, 6.329, 2)

    def test8DashesSaltToGrams(self):
        grams = norm(8, DASH, SALT)
        self.assertAlmostEquals(grams, 6.329, 2)

    def testOneStickButterToGrams(self):
        grams = norm(1, STICK, BUTTER, 1)
        self.assertAlmostEquals(grams, 113.398, 2)

    def testThreeQuarterStickButterToGrams(self):
        grams = norm(0.75, STICK, BUTTER)
        self.assertAlmostEquals(grams, 85.0486, 2)

    def testHalfKnobOfButterButterToGrams(self):
        grams = norm(0.5, KNOB, BUTTER)
        self.assertAlmostEquals(grams, 15, 0)

    def testInapplicableUnit(self):
        try:
            norm(0.75, STICK, SALT, line_nr=1)
            self.fail("Expected error")
        except Exception, e:
            self.assertEquals("Inapplicable unit 'stick' used for ingredient 'salt' at line 1", str(e))

class TestNormalizeColumns(unittest.TestCase):

    def testThreeRows(self):
        ingredients = (FLOUR, SUGAR, BUTTER)
        flour = ((7, OZ), (200, GRAM), (1, ML))
        sugar = ((1, METRIC_CUP), (4, OZ), (1, ML))
        butter = ((1, METRIC_CUP), (4, OZ), (1, ML))
        columns = zip(flour, sugar, butter)
        new_columns = normalize(ingredients, columns)
        
        self.assertEquals(len(new_columns[0]), 3)
        self.assertAlmostEquals(new_columns[0][1], 46.86, 2)
        self.assertAlmostEquals(new_columns[1][1], 26.569, 2)
        self.assertAlmostEquals(new_columns[2][1], 26.569, 2)

    def testNormalizeTo100g(self):
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

    def testMergeColumns(self):
        ingredients = (FLOUR, SUGAR, BUTTER, SALT, WATER, WATER)
        flour = array([1])
        sugar = array([1])
        butter = array([1])
        salt = array([1])
        water = array([3])
        water = array([3])
        columns = zip(flour, sugar, butter, salt, water, water)
        ingredients, new_columns = merge_columns(ingredients, columns, merge=[((1,1.0),(2,1.0)), ((0,1.0),(4,1.0),(5,1.0))])
        
        self.assertEquals(len(ingredients), 3)
        self.assertEquals(ingredients, (FLOUR, SUGAR, SALT))
        self.assertEquals(len(new_columns[0]), 3)
        self.assertAlmostEquals(new_columns[0][0], 7.0, 2)
        self.assertAlmostEquals(new_columns[0][1], 2.0, 2)
        self.assertAlmostEquals(new_columns[0][2], 1.0, 2)

    def testMergeColumnsUsingNames(self):
        ingredients = (FLOUR, SUGAR, BUTTER, SALT, WATER, WATER)
        flour = array([1])
        sugar = array([1])
        butter = array([1])
        salt = array([1])
        water = array([3])
        water = array([3])
        columns = zip(flour, sugar, butter, salt, water, water)
        ingredients, new_columns = merge_columns(ingredients, columns, merge=[((1,1.0),("butter",1.0)), (("flour",1.0),("water", 1.0))])
        
        self.assertEquals(len(ingredients), 3)
        self.assertEquals(ingredients, (FLOUR, SUGAR, SALT))
        self.assertEquals(len(new_columns[0]), 3)
        self.assertAlmostEquals(new_columns[0][0], 7.0, 2)
        self.assertAlmostEquals(new_columns[0][1], 2.0, 2)
        self.assertAlmostEquals(new_columns[0][2], 1.0, 2)

    def testMergeColumnsUsingPercentages(self):
        ingredients = (FLOUR, SUGAR, BUTTER, SALT, WATER, WATER)
        flour = array([4])
        sugar = array([1])
        butter = array([2])
        salt = array([1])
        water = array([6])
        water = array([6])
        columns = zip(flour, sugar, butter, salt, water, water)
        ingredients, new_columns = merge_columns(ingredients, columns, merge=[(("sugar",1.0),("butter",0.5)), (("flour",0.25),("water", 0.5))])
        
        self.assertEquals(len(ingredients), 3)
        self.assertEquals(ingredients, (FLOUR, SUGAR, SALT))
        self.assertEquals(len(new_columns[0]), 3)
        self.assertAlmostEquals(new_columns[0][0], 7.0, 2)
        self.assertAlmostEquals(new_columns[0][1], 2.0, 2)
        self.assertAlmostEquals(new_columns[0][2], 1.0, 2)

    def testMergeColumnsRetainColumn(self):
        ingredients = (FLOUR, SUGAR, BUTTER, SALT, WATER, WATER)
        flour = array([4])
        sugar = array([1])
        butter = array([2])
        salt = array([1])
        water = array([6])
        water = array([6])
        columns = zip(flour, sugar, butter, salt, water, water)
        ingredients, new_columns = merge_columns(ingredients, columns, merge=[(("sugar",1.0),("butter",0.5), ("flour", 0.0)), (("flour",0.25),("water", 0.5))])
        
        self.assertEquals(len(ingredients), 3)
        self.assertEquals(ingredients, (FLOUR, SUGAR, SALT))
        self.assertEquals(len(new_columns[0]), 3)
        self.assertAlmostEquals(new_columns[0][0], 7.0, 2)
        self.assertAlmostEquals(new_columns[0][1], 2.0, 2)
        self.assertAlmostEquals(new_columns[0][2], 1.0, 2)

    def testMergeColumnsMissingColumn(self):
        ingredients = (FLOUR, SUGAR, BUTTER, SALT, WATER)
        flour = array([1])
        sugar = array([1])
        butter = array([1])
        salt = array([1])
        water = array([3])
        water = array([3])
        columns = zip(flour, sugar, butter, salt, water)
        try:
            ingredients, new_columns = merge_columns(ingredients, columns, merge=[((1,1.0),(2,1.0)), ((0,1.0),(4,1.0),(5,1.0))])
            self.fail("Expected error")
        except Exception, e:
            self.assertEquals(str(e), "Attempted to merge missing column 5", e)

    def testMergeColumnsMissingFirstColumn(self):
        ingredients = (FLOUR, SUGAR, BUTTER, SALT, WATER)
        flour = array([1])
        sugar = array([1])
        butter = array([1])
        salt = array([1])
        water = array([3])
        water = array([3])
        columns = zip(flour, sugar, butter, salt, water)
        try:
            ingredients, new_columns = merge_columns(ingredients, columns, merge=[((1,1.0),(2,1.0)), ((4,1.0),(5,1.0))])
            self.fail("Expected error")
        except Exception, e:
            self.assertEquals(str(e), "Attempted to merge missing column 5")

if __name__ == "__main__":
    unittest.main()
