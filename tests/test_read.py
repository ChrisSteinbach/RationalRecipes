"""Tests for reading and parsing of input files"""
import unittest
from tests.test_utils import normalize
from ingredient import FLOUR, SUGAR
from units import CUP, GRAM, METRIC_CUP
from read import parse_file_contents, value_and_unit, read_files
from StringIO import StringIO
from errors import InvalidInputException

class TestReadFiles(unittest.TestCase):
    """Unit tests for reading and parsing input files"""
    
    def test_one_file(self):
        """Test reading of one input file with two columns and one row"""
        input_file = StringIO("Flour, Sugar\n1g,2g")
        ingredients, columns = read_files([input_file])
        self.assertEquals((FLOUR, SUGAR), ingredients)
        self.assertEquals(len(columns[0]), 2)
        self.assertEquals(len(columns), 1)

    def test_two_files(self):
        """Test reading of two (identical) input files"""
        input_file_1 = StringIO("Flour, Sugar\n1g,2g")
        input_file_2 = StringIO("Flour, Sugar\n1g,2g")
        ingredients, columns = read_files([input_file_1, input_file_2])
        self.assertEquals((FLOUR, SUGAR), ingredients)
        self.assertEquals(len(columns), 2)
        self.assertEquals(len(columns[0]), 2)

    def test_non_matching_headers(self):
        """Test error condition where two input files are read with differing
           ingredients"""
        input_file_1 = StringIO("Flour, Sugar\n1g,2g")
        input_file_2 = StringIO("Flour, Salt\n1g,2g")
        try:
            _ingredients, _columns = read_files([input_file_1, input_file_2])
            self.fail("Expected error")
        except InvalidInputException, error:
            self.assertEquals(str(error),
                              "All input files must have the same header.")
        
    
class TestReadProportions(unittest.TestCase):
    """Test parsing of proportions from input file"""
    
    def assert_proportions(self, recipes):
        """The tests in this class are designed so that the results are the
           same for each tests despite the inputs being different. This allows
           us to test for correctness with just one method (this one)"""
        ingredients, columns = parse_file_contents(recipes)
        new_columns = normalize(ingredients, columns)
        self.assertAlmostEquals(new_columns[0][0], 30.96, 2)
        self.assertAlmostEquals(new_columns[0][1], 46.860, 2)
        self.assertAlmostEquals(new_columns[1][0], 31.38, 2)
        self.assertAlmostEquals(new_columns[1][1], 26.57, 2)

    def test_read_proportions(self):
        """Test reading of ingredient proportions in ounces, cups and grams"""
        recipes = """Flour, Sugar , Butter
                     7 OZ, 1 CUP, 1 CUP
                     200 GRAM, 4 OZ, 4 OZ"""
        self.assert_proportions(recipes)

    def test_bad_ingredient(self):
        """Test reading of header referencing an unknown ingredient (Blah)"""
        recipes = """Flour, Sugar, Blah
                     7 OZ, 1 CUP, 1 CUP"""
        try:
            self.assert_proportions(recipes)
            self.fail("Expected exception")
        except InvalidInputException, error:
            self.assertEquals(str(error),
                              "No such ingredient as 'blah', line 1")

    def test_read_alternative_format(self):
        """Test parsing using synonyms for units"""
        recipes = """all purpose flour, sugar, butter
                                 7 ounces, 1 c, 1 cup
                                 200 g, 4 oz, 4 oz
                            """
        self.assert_proportions(recipes)

    def test_missing_input_column(self):
        """Test parsing of a row with missing column"""
        recipes = """all purpose flour, sugar, butter
                                 7 ounces, 1c, 1 cup
                                 200g, 4oz
                            """
        try:
            self.assert_proportions(recipes)
            self.fail("Expected exception concerning missing column")
        except InvalidInputException, error:
            self.assertEquals(str(error),
                    "The row on line 3 has 2 columns where 3 were expected")

def parse_measure(measure):
    """Parse a measure into value and unit. Provide dummy values
       for line and column numbers."""
    return value_and_unit(line_nr=1, column_index=1, measure=measure)
    
class TestReadMeasure(unittest.TestCase):
    """Test parsing of ingredient measurements"""
    
    def test_simple_measure(self):
        """Read measure with space between value and unit"""
        value, unit = parse_measure("1 cup")
        assert(unit is CUP)
        self.assertEquals(1, value)

    def test_unit_synonym(self):
        """Read measure using a unit synonym"""
        value, unit = parse_measure("1 c")
        assert(unit is CUP)
        self.assertEquals(1, value)

    def test_read_zero(self):
        """Read a measure of zero. Unit defaults to grams."""
        value, unit = parse_measure("0")
        assert(unit is GRAM)
        self.assertEquals(0, value)

    def test_no_space(self):
        """Parse a measure with no whitespace between the value and the unit"""
        value, unit = parse_measure("1c")
        assert(unit is CUP)
        self.assertEquals(1, value)

    def test_read_float(self):
        """Read a measure where the value is a float"""
        value, unit = parse_measure("1.1c")
        assert(unit is CUP)
        self.assertEquals(1.1, value)

    def test_blank_before_decimal_point(self):
        """Read a value where the zero before the decimal point has been
           omitted"""
        value, unit = parse_measure(".1metric cup ")
        assert(unit is METRIC_CUP)
        self.assertEquals(0.1, value)

    def test_value_missing(self):
        """Test error condition: when the value part of a measurement is
           missing"""
        try:
            _value, _unit = parse_measure("cup")
            self.fail("Expected: Incorrect format of measurement on line 1, "
                      "column 1")
        except InvalidInputException, error:
            self.assertEqual(str(error),
                    "Incorrect format of measurement at line 1, column 1")

    def test_unit_missing(self):
        """Test error condition: when the unit part of a measurement i
           missing"""
        try:
            _value, _unit = parse_measure("1 ")
            self.fail("Expected: Incorrect format of measurement on line 1,"
                      " column 1")
        except InvalidInputException, error:
            self.assertEqual(str(error),
                        "Incorrect format of measurement at line 1, column 1")

    def test_unknown_unit(self):
        """Test error condition: unknown unit name"""
        try:
            _value, _unit = parse_measure("1 blah")
            self.fail("Expected exception")
        except InvalidInputException, error:
            self.assertEqual(str(error),
                             "No unit named 'blah' at line 1, column 1")
