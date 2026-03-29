"""Unit tests for diff script"""

import pytest

import rational_recipes.utils as utils
from rational_recipes import DiffMain
from tests.test_utils import verify_output

PERCENT_CHANGE_EXPECTED_OUTPUT = """
Ratio for data set 1 in units of weight is 1.00:2.42:0.97:0.21 (flour:milk:egg:butter)
Ratio for data set 2 in units of weight is 1.00:2.02:0.82:0.17 (flour:milk:egg:butter)

The flour proportion has increased by 15% from data set 1 to 2
The butter proportion has decreased by 7% from data set 1 to 2
The milk proportion has decreased by 4% from data set 1 to 2
The egg proportion has decreased by 3% from data set 1 to 2

Overall percentage difference = 7%
"""

PERCENT_DIFF_EXPECTED_OUTPUT = """
Ratio for data set 1 in units of weight is 1.00:2.42:0.97:0.21 (flour:milk:egg:butter)
Ratio for data set 2 in units of weight is 1.00:2.02:0.82:0.17 (flour:milk:egg:butter)

Percentage difference between flour proportions 13.65%
Percentage difference between butter proportions 7.43%
Percentage difference between milk proportions 4.34%
Percentage difference between egg proportions 2.94%

Overall percentage difference = 7.09%
"""


def script_instance():
    """Run the script from the command line"""
    script = DiffMain(
        ["tests/test_diff_a.csv"],
        ["tests/test_diff_b.csv"],
        distinct=True,
        merge=utils.parse_column_merge("1+2:0+5"),
    )
    return script


class TestDiff:
    """Unit tests for diff script"""

    def test_percentage_change_output(self):
        """Integration test: verify percentage change output format"""
        script = script_instance()
        result = script.main(show_percentage_change=True, precision=0)
        verify_output(str(result), PERCENT_CHANGE_EXPECTED_OUTPUT)

    def test_percentage_difference_output(self):
        """Integration test: verify percentage difference output format"""
        script = script_instance()
        result = script.main(show_percentage_change=False, precision=2)
        verify_output(str(result), PERCENT_DIFF_EXPECTED_OUTPUT)

    def test_percentage_change_values(self):
        """Percentage change values per ingredient"""
        script = script_instance()
        result = script.main(show_percentage_change=True, precision=0)
        changes = {name: value for value, name in result.percentage_changes}
        assert changes["flour"] * 100 == pytest.approx(15, abs=1)
        assert changes["butter"] * 100 == pytest.approx(-7, abs=1)
        assert changes["milk"] * 100 == pytest.approx(-4, abs=1)
        assert changes["egg"] * 100 == pytest.approx(-3, abs=1)

    def test_percentage_difference_values(self):
        """Percentage difference values per ingredient"""
        script = script_instance()
        result = script.main(show_percentage_change=False, precision=2)
        diffs = {name: value for value, name in result.percentage_differences}
        assert diffs["flour"] * 100 == pytest.approx(13.65, abs=1e-2)
        assert diffs["butter"] * 100 == pytest.approx(7.43, abs=1e-2)
        assert diffs["milk"] * 100 == pytest.approx(4.34, abs=1e-2)
        assert diffs["egg"] * 100 == pytest.approx(2.94, abs=1e-2)

    def test_overall_percentage_difference(self):
        """Overall mean percentage difference"""
        script = script_instance()
        result = script.main(show_percentage_change=False, precision=2)
        assert result.mean_difference * 100 == pytest.approx(7.09, abs=1e-2)

    def test_ingredients(self):
        """Ingredient names are reported correctly"""
        script = script_instance()
        result = script.main(show_percentage_change=False, precision=2)
        assert result.ingredients == ["flour", "milk", "egg", "butter"]
