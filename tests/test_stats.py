"""Unit tests for stats script"""

import pytest

import rational_recipes.utils as utils
from rational_recipes import StatsMain
from tests.test_utils import verify_output

EXPECTED_OUTPUT = """
Recipe ratio in units of weight is 1.00:2.02:0.82:0.17 (flour:milk:egg:butter)

Recipe ratio with confidence intervals (confidence level is 95%)
----------------------------------------------------------------
The flour proportion is between 23.66% and 26.06% (the interval is 5% of the mean proportion: 24.86%)
The milk proportion is between 48.18% and 52.49% (the interval is 4% of the mean proportion: 50.34%)
The egg proportion is between 18.95% and 22.01% (the interval is 7% of the mean proportion: 20.48%)
The butter proportion is between 3.70% and 4.94% (the interval is 14% of the mean proportion: 4.32%)

Minimum sample sizes needed for confidence interval with 5% difference and confidence level of 95%
--------------------------------------------------------------------------------------------------
Minimum sample size for flour proportion: 112
Minimum sample size for milk proportion: 88
Minimum sample size for egg proportion: 266
Minimum sample size for butter proportion: 979

450g Recipe
-----------
112g or 212ml flour
227g or 220ml milk
92g, 90ml or 2 egg(s) where each egg is 44g
19g or 20ml butter

Note: these calculations are based on 119 distinct recipe proportions. Duplicates have been removed.
"""


def script_instance(merge_spec, restrictions=""):
    """Create instance of StatsMain in order to test stats script"""
    distinct = True
    confidence = 0.05
    merge = utils.parse_column_merge(merge_spec)
    restrictions = utils.parse_restrictions(restrictions)
    inst = StatsMain(["tests/test.csv"], distinct, merge, [])
    inst.set_desired_interval(confidence)
    inst.set_restrictions(restrictions)
    return inst


class TestStats:
    """Unit tests for stats script"""

    def get_result(
        self, merge_spec="milk+water:flour+salt", restrictions="", total_weight=450
    ):
        """Get structured result for testing"""
        script = script_instance(merge_spec, restrictions)
        return script.main(
            ratio_precision=2,
            recipe_precision=0,
            total_recipe_weight=total_weight,
            verbose=True,
        )

    def test_output_format(self):
        """Integration test: verify full formatted output"""
        result = self.get_result()
        verify_output(str(result), EXPECTED_OUTPUT)

    def test_merge_by_index_matches_by_name(self):
        """Merge by column index produces same values as merge by name"""
        by_name = self.get_result("milk+water:flour+salt")
        by_index = self.get_result("1+2:0+5")
        for a, b in zip(by_name.ratio_values, by_index.ratio_values, strict=False):
            assert a == pytest.approx(b)
        for a, b in zip(by_name.proportions, by_index.proportions, strict=False):
            assert a == pytest.approx(b)
        assert by_name.sample_size == by_index.sample_size

    def test_ratio_values(self):
        """Baker's percentage ratio values"""
        result = self.get_result()
        expected = [1.0, 2.02, 0.82, 0.17]
        for actual, exp in zip(result.ratio_values, expected, strict=False):
            assert actual == pytest.approx(exp, abs=1e-2)

    def test_proportions(self):
        """Mean ingredient proportions"""
        result = self.get_result()
        expected = [24.86, 50.34, 20.48, 4.32]
        for actual, exp in zip(result.proportions, expected, strict=False):
            assert actual == pytest.approx(exp, abs=1e-2)

    def test_confidence_intervals(self):
        """Confidence interval bounds"""
        result = self.get_result()
        expected_bounds = [
            (23.66, 26.06),
            (48.18, 52.49),
            (18.95, 22.01),
            (3.70, 4.94),
        ]
        for (lower, upper), (exp_lo, exp_hi) in zip(
            result.intervals, expected_bounds, strict=False
        ):
            assert lower == pytest.approx(exp_lo, abs=1e-2)
            assert upper == pytest.approx(exp_hi, abs=1e-2)

    def test_min_sample_sizes(self):
        """Minimum sample size calculations"""
        result = self.get_result()
        assert result.min_sample_sizes == [112, 88, 266, 979]

    def test_recipe_weights(self):
        """Recipe weight calculations"""
        result = self.get_result()
        expected = [112, 227, 92, 19]
        for actual, exp in zip(result.recipe_weights, expected, strict=False):
            assert actual == pytest.approx(exp, abs=1)
        assert result.total_recipe_weight == pytest.approx(450, abs=1)

    def test_sample_size(self):
        """Sample size reported correctly"""
        result = self.get_result()
        assert result.sample_size == 119

    def test_ingredients(self):
        """Ingredient names in correct order"""
        result = self.get_result()
        assert result.ingredients == ["flour", "milk", "egg", "butter"]

    def test_restrictions(self):
        """Weight restrictions limit recipe total below requested weight"""
        result = self.get_result(
            "1+2:0+5", "flour=116,milk=228,egg=86.27,butter=20.4", total_weight=500
        )
        assert result.total_recipe_weight == pytest.approx(421, abs=1)
        expected = [105, 212, 86, 18]
        for actual, exp in zip(result.recipe_weights, expected, strict=False):
            assert actual == pytest.approx(exp, abs=1)
