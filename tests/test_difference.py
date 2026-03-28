"""Unit tests for percentage difference and change"""

import pytest

from rational_recipes.difference import (
    calc_percentage_change,
    calc_percentage_difference,
    percentage_difference,
    percentage_difference_from_mean,
)
from rational_recipes.ingredient import EGG, FLOUR
from tests.test_ratio import create_ratio, make_test_data


class TestDifference:
    """Tests for RatioValue class"""

    def test_percentage_difference(self):
        """Percentage difference between two values that are almost equal"""
        value_1 = 3999.9999
        value_2 = 4000
        diff_1 = calc_percentage_difference(value_2, value_1)
        diff_2 = calc_percentage_difference(value_1, value_2)
        assert diff_1 == diff_2
        assert diff_1 == pytest.approx(0.00000, abs=1e-2)

    def test_percentage_diff_from_mean(self):
        """Percentage difference from the mean between two values that are
        almost equal"""
        value_1 = 3999.9999
        value_2 = 4000
        diff_1 = percentage_difference_from_mean(value_2, value_1)
        diff_2 = percentage_difference_from_mean(value_1, value_2)
        assert diff_1 == diff_2
        assert diff_1 == pytest.approx(0.00000, abs=1e-2)

    def test_percentage_change(self):
        """Percentage change between two values that are almost equal"""
        value_1 = 3999.9999999
        value_2 = 4000
        diff_1 = calc_percentage_change(value_2, value_1)
        diff_2 = calc_percentage_change(value_1, value_2)
        assert diff_1 == pytest.approx(diff_2, abs=5e-8)
        assert diff_1 == pytest.approx(0.00000, abs=1e-2)

    def test_percentage_difference_50pc(self):
        """Percentage difference between values where one is 50% of the other"""
        value_1 = 2000
        value_2 = 4000
        diff_1 = calc_percentage_difference(value_2, value_1)
        diff_2 = calc_percentage_difference(value_1, value_2)
        assert diff_1 == diff_2
        assert diff_1 == pytest.approx(0.666, abs=1e-2)

    def test_pc_diff_from_mean_50pc(self):
        """Percentage difference from the mean between values where one is 50%
        of the other"""
        value_1 = 2000
        value_2 = 4000
        diff_1 = percentage_difference_from_mean(value_2, value_1)
        diff_2 = percentage_difference_from_mean(value_1, value_2)
        assert diff_1 == diff_2
        assert diff_1 == pytest.approx(0.333, abs=1e-2)

    def test_percentage_change_50pc(self):
        """Percentage change between values where one is 50% of the other"""
        value_1 = 2000
        value_2 = 4000
        diff_1 = calc_percentage_change(value_2, value_1)
        diff_2 = calc_percentage_change(value_1, value_2)
        assert diff_1 == pytest.approx(-0.5, abs=1e-2)
        assert diff_2 == pytest.approx(1.0, abs=1e-2)

    def test_percentage_difference_99pc(self):
        """Percentage difference between a very small value and a relatively
        large value"""
        value_1 = 0.0000000001
        value_2 = 2**32
        diff_1 = calc_percentage_difference(value_2, value_1)
        diff_2 = calc_percentage_difference(value_1, value_2)
        assert diff_1 == diff_2
        assert diff_1 == pytest.approx(1.999999, abs=1e-2)

    def test_pc_diff_from_mean_99pc(self):
        """Percentage difference from the mean between a very small value and a
        relatively large value"""
        value_1 = 0.0000000001
        value_2 = 2**32
        diff_1 = percentage_difference_from_mean(value_2, value_1)
        diff_2 = percentage_difference_from_mean(value_1, value_2)
        assert diff_1 == diff_2
        assert diff_1 == pytest.approx(0.999999, abs=1e-2)

    def test_percentage_change_99pc(self):
        """Percentage change between a very small value and a relatively large
        value"""
        value_1 = 1
        value_2 = (2**32) + 1
        diff_1 = calc_percentage_change(value_2, value_1)
        diff_2 = calc_percentage_change(value_1, value_2)
        assert diff_1 == pytest.approx(-0.999999, abs=1e-2)
        assert diff_2 == pytest.approx(2**32, abs=1e-2)

    def test_ratio_difference_zero(self):
        """Test difference of two identical ratios"""
        ingredients, _ = make_test_data()
        ratio_1 = create_ratio(ingredients, [1, 2, 3])
        ratio_2 = create_ratio(ingredients, [1, 2, 3])
        difference, _ = percentage_difference(ratio_1, ratio_2)
        assert difference == 0

    def test_ratio_difference(self):
        """Test per ingredient and over all percentage difference"""
        ingredients, _ = make_test_data()
        ratio_1 = create_ratio(ingredients, [1, 30, 100])
        ratio_2 = create_ratio(ingredients, [1, 60, 50])
        difference, differences = percentage_difference(ratio_1, ratio_2)
        assert difference == pytest.approx(0.496, abs=1e-2)
        assert differences[1][0] == pytest.approx(0.81, abs=1e-2)
        assert differences[1][1] == EGG
        assert differences[0][0] == pytest.approx(0.17, abs=1e-2)
        assert differences[0][1] == FLOUR
