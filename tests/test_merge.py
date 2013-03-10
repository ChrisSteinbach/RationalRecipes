"""Test column merge"""
import unittest
from ingredient import SALT, FLOUR, BUTTER, SUGAR, WATER
from merge import merge_columns, MergeConfigError
from numpy import array


class TestMerge(unittest.TestCase):
    """Test class for column merge"""
    
    def test_merge_columns_with_indexes(self):
        """Merge second and third columns. Merge first, fifth and sixth
           columns. Specified using column indexes only."""
        ingredients = (FLOUR, SUGAR, BUTTER, SALT, WATER, WATER)
        flour = array([1])
        sugar = array([1])
        butter = array([1])
        salt = array([1])
        water = array([3])
        columns = zip(flour, sugar, butter, salt, water, water)
        ingredients, new_columns = merge_columns(ingredients, columns,
                merge=[((1, 1.0), (2, 1.0)), ((0, 1.0), (4, 1.0), (5, 1.0))])
        
        self.assertEquals(len(ingredients), 3)
        self.assertEquals(ingredients, (FLOUR, SUGAR, SALT))
        self.assertEquals(len(new_columns[0]), 3)
        self.assertAlmostEquals(new_columns[0][0], 7.0, 2)
        self.assertAlmostEquals(new_columns[0][1], 2.0, 2)
        self.assertAlmostEquals(new_columns[0][2], 1.0, 2)

    def test_merge_named_columns(self):
        """Merge columns using mixed column indexes and named columns"""
        ingredients = (FLOUR, SUGAR, BUTTER, SALT, WATER, WATER)
        flour = array([1])
        sugar = array([1])
        butter = array([1])
        salt = array([1])
        water = array([3])
        columns = zip(flour, sugar, butter, salt, water, water)
        ingredients, new_columns = merge_columns(ingredients, columns,
                    merge=[((1,1.0), ("butter", 1.0)), (("flour", 1.0),
                                                        ("water", 1.0))])
        
        self.assertEquals(len(ingredients), 3)
        self.assertEquals(ingredients, (FLOUR, SUGAR, SALT))
        self.assertEquals(len(new_columns[0]), 3)
        self.assertAlmostEquals(new_columns[0][0], 7.0, 2)
        self.assertAlmostEquals(new_columns[0][1], 2.0, 2)
        self.assertAlmostEquals(new_columns[0][2], 1.0, 2)

    def test_merge_partial_columns(self):
        """Specify that only a percentage of a column be merged"""
        ingredients = (FLOUR, SUGAR, BUTTER, SALT, WATER, WATER)
        flour = array([4])
        sugar = array([1])
        butter = array([2])
        salt = array([1])
        water = array([6])
        columns = zip(flour, sugar, butter, salt, water, water)
        ingredients, new_columns = merge_columns(ingredients, columns,
                    merge=[(("sugar", 1.0), ("butter", 0.5)), (("flour", 0.25),
                                                               ("water", 0.5))])
        
        self.assertEquals(len(ingredients), 3)
        self.assertEquals(ingredients, (FLOUR, SUGAR, SALT))
        self.assertEquals(len(new_columns[0]), 3)
        self.assertAlmostEquals(new_columns[0][0], 7.0, 2)
        self.assertAlmostEquals(new_columns[0][1], 2.0, 2)
        self.assertAlmostEquals(new_columns[0][2], 1.0, 2)

    def test_retain_column(self):
        """When a column is merged into another, that column is removed.
           Make sure we are able to retain merged columns when a column is both
           a source and destination for a merge"""
        ingredients = (FLOUR, SUGAR, BUTTER, SALT, WATER, WATER)
        flour = array([4])
        sugar = array([1])
        butter = array([2])
        salt = array([1])
        water = array([6])
        columns = zip(flour, sugar, butter, salt, water, water)
        ingredients, new_columns = merge_columns(ingredients, columns,
            merge=[(("sugar", 1.0), ("butter", 0.5), ("flour", 0.0)),
                   (("flour", 0.25), ("water", 0.5))])
        
        self.assertEquals(len(ingredients), 3)
        self.assertEquals(ingredients, (FLOUR, SUGAR, SALT))
        self.assertEquals(len(new_columns[0]), 3)
        self.assertAlmostEquals(new_columns[0][0], 7.0, 2)
        self.assertAlmostEquals(new_columns[0][1], 2.0, 2)
        self.assertAlmostEquals(new_columns[0][2], 1.0, 2)

    def test_missing_column_specifed(self):
        """Check that an error is raised with correct message when a missing
           column index is specified"""
        ingredients = (FLOUR, SUGAR, BUTTER, SALT, WATER)
        flour = array([1])
        sugar = array([1])
        butter = array([1])
        salt = array([1])
        water = array([3])
        columns = zip(flour, sugar, butter, salt, water)
        try:
            ingredients, _ = merge_columns(ingredients, columns, 
            merge = [((1, 1.0), (2, 1.0)), ((0, 1.0), (4, 1.0), (5, 1.0))])
            self.fail("Expected error")
        except MergeConfigError, exception:
            self.assertEquals(str(exception),
                              "Attempted to merge missing column 5", exception)

    def test_missing_column_name(self):
        """Check that an error is raised with correct message when a missing
           column name is specified"""
        ingredients = (FLOUR, SUGAR, BUTTER, SALT)
        flour = array([1])
        sugar = array([1])
        butter = array([1])
        salt = array([1])
        columns = zip(flour, sugar, butter, salt)
        try:
            ingredients, _ = merge_columns(ingredients, columns, 
            merge = [((1, 1.0), (2, 1.0)), ((0, 1.0), (3, 1.0),
                                            ("water", 1.0))])
            self.fail("Expected error")
        except MergeConfigError, exception:
            self.assertEquals(str(exception),
                              "Attempted to merge missing column 'water'",
                              exception)

    def test_missing_first_column(self):
        """Test error case when destination column specified is missing"""
        ingredients = (FLOUR, SUGAR, BUTTER, SALT, WATER)
        flour = array([1])
        sugar = array([1])
        butter = array([1])
        salt = array([1])
        water = array([3])
        columns = zip(flour, sugar, butter, salt, water)
        try:
            ingredients, _ = merge_columns(ingredients, columns, 
                        merge=[((1, 1.0), (2, 1.0)), ((5, 1.0), (4, 1.0))])
            self.fail("Expected error")
        except MergeConfigError, exception:
            self.assertEquals(str(exception),
                              "Attempted to merge missing column 5")

    def test_missing_first_ncolumn(self):
        """Test error case when destination column specified is missing"""
        ingredients = (FLOUR, SUGAR, BUTTER, SALT, WATER)
        flour = array([1])
        sugar = array([1])
        butter = array([1])
        salt = array([1])
        water = array([3])
        columns = zip(flour, sugar, butter, salt, water)
        try:
            ingredients, _ = merge_columns(ingredients, columns, 
                        merge=[((1, 1.0), (2, 1.0)),
                               (("error", 1.0), (4, 1.0))])
            self.fail("Expected error")
        except MergeConfigError, exception:
            self.assertEquals(str(exception),
                              "Attempted to merge missing column 'error'")
