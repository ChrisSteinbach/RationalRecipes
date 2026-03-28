"""Utility functions and classes for test cases"""

from unittest import TestCase

from rational_recipes.ingredient import WATER
from rational_recipes.normalize import normalize_to_100g, to_grams


def normalize(ingredients, columns):
    """Tests were originally written with the axis swapped, so
    we use zip to swap back here so that the tests don't need
    to change"""
    columns = to_grams(ingredients, columns)
    return list(zip(*normalize_to_100g(columns), strict=False))


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
        self.assertEqual(len(output), len(expected))
        for i in range(0, len(output)):
            self.assertEqual(
                output[i],
                expected[i],
                f"Output differs as line {i + 1}\n"
                f"output: {output[i]}\n"
                f"expected: {expected[i]}",
            )
