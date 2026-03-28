"""Utility functions for test cases"""

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


def verify_output(output, expected):
    """Check that output is same as expected"""
    output_lines = output.splitlines()
    expected_lines = expected.splitlines()
    assert len(output_lines) == len(expected_lines), (
        f"Output has {len(output_lines)} lines, expected {len(expected_lines)}"
    )
    for i in range(len(output_lines)):
        assert output_lines[i] == expected_lines[i], (
            f"Output differs at line {i + 1}\n"
            f"output: {output_lines[i]}\n"
            f"expected: {expected_lines[i]}"
        )
