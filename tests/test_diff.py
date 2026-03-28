"""Unit tests for diff script"""

import pytest

import rational_recipes.utils as utils
from rational_recipes import DiffMain
from tests.test_utils import verify_output

PERCENT_CHANGE_EXPECTED_OUTPUT = """
Ratio for data set 1 in units of weight is 1.00:2.36:1.16:0.22 (all purpose flour:milk:egg:butter)
Ratio for data set 2 in units of weight is 1.00:1.97:0.75:0.17 (all purpose flour:milk:egg:butter)

The all purpose flour proportion has increased by 22% from data set 1 to 2
The egg proportion has decreased by 21% from data set 1 to 2
The butter proportion has decreased by 5% from data set 1 to 2
The milk proportion has increased by 2% from data set 1 to 2

Overall percentage difference = 13%
"""

PERCENT_DIFF_EXPECTED_OUTPUT = """
Ratio for data set 1 in units of weight is 1.00:2.36:1.16:0.22 (all purpose flour:milk:egg:butter)
Ratio for data set 2 in units of weight is 1.00:1.97:0.75:0.17 (all purpose flour:milk:egg:butter)

Percentage difference between egg proportions 23.97%
Percentage difference between all purpose flour proportions 19.86%
Percentage difference between butter proportions 4.92%
Percentage difference between milk proportions 1.60%

Overall percentage difference = 12.59%
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
        assert changes["all purpose flour"] * 100 == pytest.approx(22, abs=1)
        assert changes["egg"] * 100 == pytest.approx(-21, abs=1)
        assert changes["butter"] * 100 == pytest.approx(-5, abs=1)
        assert changes["milk"] * 100 == pytest.approx(2, abs=1)

    def test_percentage_difference_values(self):
        """Percentage difference values per ingredient"""
        script = script_instance()
        result = script.main(show_percentage_change=False, precision=2)
        diffs = {name: value for value, name in result.percentage_differences}
        assert diffs["egg"] * 100 == pytest.approx(23.97, abs=1e-2)
        assert diffs["all purpose flour"] * 100 == pytest.approx(19.86, abs=1e-2)
        assert diffs["butter"] * 100 == pytest.approx(4.92, abs=1e-2)
        assert diffs["milk"] * 100 == pytest.approx(1.60, abs=1e-2)

    def test_overall_percentage_difference(self):
        """Overall mean percentage difference"""
        script = script_instance()
        result = script.main(show_percentage_change=False, precision=2)
        assert result.mean_difference * 100 == pytest.approx(12.59, abs=1e-2)

    def test_ingredients(self):
        """Ingredient names are reported correctly"""
        script = script_instance()
        result = script.main(show_percentage_change=False, precision=2)
        assert result.ingredients == ["all purpose flour", "milk", "egg", "butter"]
