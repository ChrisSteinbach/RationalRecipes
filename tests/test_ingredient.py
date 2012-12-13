"""Unit tests for ingredient classes"""
import unittest
from ingredient import FLOUR, EGG

class TestIngredientConversion(unittest.TestCase):
    """Unit tests for ingredient classes"""
    
    def test_volume2weight(self):
        """Test conversion from milliliters to grams"""
        self.assertAlmostEquals(FLOUR.milliliters2grams(1), 0.527426)

    def test_weight2volume(self):
        """Test conversion from grams to milliliters"""
        self.assertAlmostEquals(FLOUR.grams2milliliters(0.527426), 1)

    def test_weight2whole_units_1egg(self):
        """Test conversion of grams to whole units (1 egg)"""
        self.assertAlmostEquals(EGG.grams2wholeunits(53), 1, 2)

    def test_weight2whole_units_2eggs(self):
        """Test conversion of grams to whole units (2 eggs)"""
        self.assertAlmostEquals(EGG.grams2wholeunits(53 * 2.0), 2, 2)

    def test_weight2whole_units_halfegg(self):
        """Test conversion of grams to whole units (half an egg)"""
        self.assertAlmostEquals(EGG.grams2wholeunits(53 / 2.0), 0.5, 2)
        
        
if __name__ == "__main__":
    unittest.main()
