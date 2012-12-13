"""Utility functions and classes for test cases"""
from normalize import to_grams, normalize_to_100g
from ingredient import WATER
from unittest import TestCase

def normalize(ingredients, columns):
    """Tests were originally written with the axis swapped, so
         we use zip to swap back here so that the tests don't need
         to change"""
    columns = to_grams(ingredients, columns)
    return zip(*normalize_to_100g(columns))

def norm(value, unit, ingredient=WATER, line_nr=None):
    """Simplified normalize function which allows tests to normalize without
       needing to explicitly pass an ingredient or line number"""
    return unit.norm(value, ingredient, line_nr)

class ScriptTestCase(TestCase):
    """Base class for script test cases"""
    
    def verify_output(self, output, expected):
        """Check that output is same as expected"""
        output = output.splitlines()
        expected = expected.splitlines()
        self.assertEquals(len(output), len(expected))
        for i in xrange(0, len(output)):
            self.assertEquals(output[i], expected[i], 
                "Output differs as line %d\n"
                "output: %s\n"
                "expected: %s" % 
                (i + 1, output[i], expected[i]))