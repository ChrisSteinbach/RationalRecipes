"""Direct unit tests for statistics module"""

import math
import unittest

import numpy

from rational_recipes.ingredient import Ingredient
from rational_recipes.statistics import (
    Z_VALUE,
    Statistics,
    calculate_confidence_intervals,
    calculate_minimum_sample_sizes,
    calculate_statistics,
    calculate_variables,
    create_zero_filter,
    filter_zero_columns,
    filter_zeros,
)


def make_ingredient(name):
    """Create a minimal Ingredient for testing"""
    return Ingredient([name], 1.0)


class TestCalculateMinimumSampleSizes(unittest.TestCase):
    """Tests for calculate_minimum_sample_sizes"""

    def test_basic(self):
        """ceil((1.96*10 / (50*0.05))^2) = ceil(61.47) = 62"""
        result = list(calculate_minimum_sample_sizes([10.0], [50.0], 0.05))
        self.assertEqual(result, [62])

    def test_zero_mean_yields_zero(self):
        """Zero mean returns 0 to avoid division by zero"""
        result = list(calculate_minimum_sample_sizes([5.0], [0.0], 0.05))
        self.assertEqual(result, [0])

    def test_multiple_ingredients(self):
        """Each ingredient computed independently"""
        result = list(calculate_minimum_sample_sizes([10.0, 20.0], [50.0, 100.0], 0.05))
        # Both: ceil((1.96*std / (mean*0.05))^2) = ceil(61.47) = 62
        self.assertEqual(result, [62, 62])

    def test_small_std_needs_few_samples(self):
        """ceil((1.96*1 / (100*0.05))^2) = ceil(0.154) = 1"""
        result = list(calculate_minimum_sample_sizes([1.0], [100.0], 0.05))
        self.assertEqual(result, [1])


class TestCalculateConfidenceIntervals(unittest.TestCase):
    """Tests for calculate_confidence_intervals"""

    def test_basic(self):
        """interval = Z * std / sqrt(n)"""
        data = [numpy.array([1.0, 2.0, 3.0, 4.0])]  # n=4
        result = calculate_confidence_intervals(data, [1.0])
        expected = Z_VALUE * 1.0 / math.sqrt(4)
        self.assertAlmostEqual(result[0], expected)

    def test_multiple_columns(self):
        """Each column gets its own interval"""
        col1 = numpy.array([1.0, 2.0, 3.0])
        col2 = numpy.array([10.0, 20.0, 30.0])
        result = calculate_confidence_intervals([col1, col2], [2.0, 5.0])
        self.assertAlmostEqual(result[0], Z_VALUE * 2.0 / math.sqrt(3))
        self.assertAlmostEqual(result[1], Z_VALUE * 5.0 / math.sqrt(3))

    def test_single_sample(self):
        """n=1: interval = Z * std"""
        data = [numpy.array([42.0])]
        result = calculate_confidence_intervals(data, [3.0])
        self.assertAlmostEqual(result[0], Z_VALUE * 3.0)


class TestCalculateVariables(unittest.TestCase):
    """Tests for calculate_variables"""

    def test_known_values(self):
        """Verify std, mean, and intervals from known data"""
        col = numpy.array([10.0, 20.0, 30.0])
        intervals, stds, means = calculate_variables([col])
        self.assertAlmostEqual(means[0], 20.0)
        expected_std = col.std()
        self.assertAlmostEqual(stds[0], expected_std)
        expected_interval = Z_VALUE * expected_std / math.sqrt(3)
        self.assertAlmostEqual(intervals[0], expected_interval)


class TestCalculateStatistics(unittest.TestCase):
    """Tests for calculate_statistics end-to-end"""

    def test_equal_rows(self):
        """All rows (50,50) → means [50, 50]"""
        ingredients = [make_ingredient("stat_a"), make_ingredient("stat_b")]
        raw_data = [(50, 50), (50, 50), (50, 50)]
        stats = calculate_statistics(raw_data, ingredients, None)
        self.assertAlmostEqual(stats.means[0], 50.0)
        self.assertAlmostEqual(stats.means[1], 50.0)

    def test_varying_rows(self):
        """(50,50),(60,40),(70,30) → means [60, 40]"""
        ingredients = [make_ingredient("stat_c"), make_ingredient("stat_d")]
        raw_data = [(50, 50), (60, 40), (70, 30)]
        stats = calculate_statistics(raw_data, ingredients, None)
        self.assertAlmostEqual(stats.means[0], 60.0)
        self.assertAlmostEqual(stats.means[1], 40.0)

    def test_normalizes_different_totals(self):
        """(100,100)→(50,50), (30,10)→(75,25) → means [62.5, 37.5]"""
        ingredients = [make_ingredient("stat_e"), make_ingredient("stat_f")]
        raw_data = [(100, 100), (30, 10)]
        stats = calculate_statistics(raw_data, ingredients, None)
        self.assertAlmostEqual(stats.means[0], 62.5)
        self.assertAlmostEqual(stats.means[1], 37.5)

    def test_confidence_intervals(self):
        """Intervals match hand calculation"""
        ingredients = [make_ingredient("stat_g"), make_ingredient("stat_h")]
        raw_data = [(50, 50), (60, 40), (70, 30)]
        stats = calculate_statistics(raw_data, ingredients, None)
        col = numpy.array([50.0, 60.0, 70.0])
        expected_interval = Z_VALUE * col.std() / math.sqrt(3)
        self.assertAlmostEqual(stats.intervals[0], expected_interval)

    def test_stores_ingredients(self):
        """Statistics object preserves ingredient references"""
        ingredients = [make_ingredient("stat_i"), make_ingredient("stat_j")]
        raw_data = [(60, 40)]
        stats = calculate_statistics(raw_data, ingredients, None)
        self.assertIs(stats.ingredients[0], ingredients[0])
        self.assertIs(stats.ingredients[1], ingredients[1])

    def test_empty_zero_columns_same_as_none(self):
        """Empty zero_columns list behaves same as None"""
        ingredients = [make_ingredient("stat_k"), make_ingredient("stat_l")]
        raw_data = [(50, 50), (60, 40)]
        stats_none = calculate_statistics(raw_data, ingredients, None)
        stats_empty = calculate_statistics(raw_data, ingredients, [])
        for a, b in zip(stats_none.means, stats_empty.means, strict=False):
            self.assertAlmostEqual(a, b)


class TestBakersPercentage(unittest.TestCase):
    """Tests for Statistics.bakers_percentage"""

    def test_two_ingredients(self):
        """All means divided by the first mean"""
        stats = Statistics(
            [make_ingredient("bp_a"), make_ingredient("bp_b")],
            [0, 0],
            [0, 0],
            [60.0, 40.0],
        )
        bp = stats.bakers_percentage()
        self.assertAlmostEqual(bp[0], 1.0)
        self.assertAlmostEqual(bp[1], 40.0 / 60.0)

    def test_three_ingredients(self):
        """means [25, 50, 25] → [1.0, 2.0, 1.0]"""
        stats = Statistics(
            [make_ingredient("bp_c"), make_ingredient("bp_d"), make_ingredient("bp_e")],
            [0, 0, 0],
            [0, 0, 0],
            [25.0, 50.0, 25.0],
        )
        bp = stats.bakers_percentage()
        self.assertAlmostEqual(bp[0], 1.0)
        self.assertAlmostEqual(bp[1], 2.0)
        self.assertAlmostEqual(bp[2], 1.0)

    def test_single_ingredient(self):
        """Single ingredient always 1.0"""
        stats = Statistics([make_ingredient("bp_f")], [0], [0], [42.0])
        self.assertEqual(stats.bakers_percentage(), [1.0])


class TestStatisticsDefaults(unittest.TestCase):
    """Tests for Statistics default configuration"""

    def test_default_desired_interval(self):
        stats = Statistics([], [], [], [])
        self.assertAlmostEqual(stats.desired_interval, 0.05)

    def test_default_precision(self):
        stats = Statistics([], [], [], [])
        self.assertEqual(stats._precision, 2)

    def test_set_precision(self):
        stats = Statistics([], [], [], [])
        stats.set_precision(4)
        self.assertEqual(stats._precision, 4)

    def test_set_desired_interval(self):
        stats = Statistics([], [], [], [])
        stats.set_desired_interval(0.10)
        self.assertAlmostEqual(stats.desired_interval, 0.10)


class TestCreateZeroFilter(unittest.TestCase):
    """Tests for create_zero_filter"""

    def test_one_column_marked(self):
        ingredients = [make_ingredient("zf_a"), make_ingredient("zf_b")]
        result = create_zero_filter(ingredients, ["zf_a"])
        self.assertEqual(result, {0: True, 1: False})

    def test_no_columns(self):
        ingredients = [make_ingredient("zf_c"), make_ingredient("zf_d")]
        result = create_zero_filter(ingredients, [])
        self.assertEqual(result, {0: False, 1: False})

    def test_multiple_columns(self):
        ingredients = [
            make_ingredient("zf_e"),
            make_ingredient("zf_f"),
            make_ingredient("zf_g"),
        ]
        result = create_zero_filter(ingredients, ["zf_e", "zf_g"])
        self.assertEqual(result, {0: True, 1: False, 2: True})


class TestFilterZeros(unittest.TestCase):
    """Tests for filter_zeros"""

    def test_removes_zeros_from_marked_columns(self):
        data = [numpy.array([0.0, 10.0, 20.0]), numpy.array([5.0, 0.0, 15.0])]
        filter_map = {0: True, 1: False}
        result = filter_zeros(data, filter_map)
        numpy.testing.assert_array_equal(result[0], [10.0, 20.0])
        numpy.testing.assert_array_equal(result[1], [5.0, 0.0, 15.0])

    def test_no_filtering_preserves_data(self):
        data = [numpy.array([0.0, 10.0])]
        result = filter_zeros(data, {0: False})
        numpy.testing.assert_array_equal(result[0], [0.0, 10.0])


class TestFilterZeroColumns(unittest.TestCase):
    """Tests for filter_zero_columns"""

    def test_no_zeros_rows_sum_to_100(self):
        """Without zeros, rows still sum to 100 after processing"""
        ingredients = [make_ingredient("fzc_a"), make_ingredient("fzc_b")]
        raw_data = [(60, 40), (70, 30), (50, 50)]
        result = filter_zero_columns(raw_data, ingredients, ["fzc_a"])
        for row in result:
            self.assertAlmostEqual(sum(row), 100.0, places=5)

    def test_zeros_get_replaced(self):
        """Zero values in filtered columns become non-zero"""
        ingredients = [make_ingredient("fzc_c"), make_ingredient("fzc_d")]
        raw_data = [(60, 40), (0, 100), (80, 20)]
        result = filter_zero_columns(raw_data, ingredients, ["fzc_c"])
        for row in result:
            self.assertAlmostEqual(sum(row), 100.0, places=5)
            for val in row:
                self.assertGreater(val, 0.0)
