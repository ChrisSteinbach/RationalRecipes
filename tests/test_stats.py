"""Unit tests for stats script"""
from rational_recipes import StatsMain
import rational_recipes.utils as utils
import tests.test_utils as test_utils

EXPECTED_OUTPUT = """
Recipe ratio in units of weight is 1.00:1.97:0.75:0.17 (all purpose flour:milk:egg:butter)

Recipe ratio with confidence intervals (confidence level is 95%)
----------------------------------------------------------------
The all purpose flour proportion is between 24.47% and 26.96% (the interval is 5% of the mean proportion: 25.72%)
The milk proportion is between 48.46% and 52.77% (the interval is 4% of the mean proportion: 50.61%)
The egg proportion is between 17.65% and 20.69% (the interval is 8% of the mean proportion: 19.17%)
The butter proportion is between 3.86% and 5.14% (the interval is 14% of the mean proportion: 4.50%)

Minimum sample sizes needed for confidence interval with 5% difference and confidence level of 95%
--------------------------------------------------------------------------------------------------
Minimum sample size for all purpose flour proportion: 112
Minimum sample size for milk proportion: 87
Minimum sample size for egg proportion: 299
Minimum sample size for butter proportion: 963

450g Recipe
-----------
116g or 219ml all purpose flour
228g or 228ml milk
86g, 73ml or 2 egg(s) where each egg is 53g
20g or 20ml butter

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


class TestStats(test_utils.ScriptTestCase):
    """Unit tests for stats script"""

    def get_result(self, merge_spec="milk+water:flour+salt",
                   restrictions="", total_weight=450):
        """Get structured result for testing"""
        script = script_instance(merge_spec, restrictions)
        return script.main(ratio_precision=2, recipe_precision=0,
                           total_recipe_weight=total_weight, verbose=True)

    def test_output_format(self):
        """Integration test: verify full formatted output"""
        result = self.get_result()
        self.verify_output(str(result), EXPECTED_OUTPUT)

    def test_merge_by_index_matches_by_name(self):
        """Merge by column index produces same values as merge by name"""
        by_name = self.get_result("milk+water:flour+salt")
        by_index = self.get_result("1+2:0+5")
        for a, b in zip(by_name.ratio_values, by_index.ratio_values):
            self.assertAlmostEqual(a, b)
        for a, b in zip(by_name.proportions, by_index.proportions):
            self.assertAlmostEqual(a, b)
        self.assertEqual(by_name.sample_size, by_index.sample_size)

    def test_ratio_values(self):
        """Baker's percentage ratio values"""
        result = self.get_result()
        expected = [1.0, 1.97, 0.75, 0.17]
        for actual, exp in zip(result.ratio_values, expected):
            self.assertAlmostEqual(actual, exp, places=2)

    def test_proportions(self):
        """Mean ingredient proportions"""
        result = self.get_result()
        expected = [25.72, 50.61, 19.17, 4.50]
        for actual, exp in zip(result.proportions, expected):
            self.assertAlmostEqual(actual, exp, places=2)

    def test_confidence_intervals(self):
        """Confidence interval bounds"""
        result = self.get_result()
        expected_bounds = [
            (24.47, 26.96),
            (48.46, 52.77),
            (17.65, 20.69),
            (3.86, 5.14),
        ]
        for (lower, upper), (exp_lo, exp_hi) in zip(result.intervals,
                                                     expected_bounds):
            self.assertAlmostEqual(lower, exp_lo, places=2)
            self.assertAlmostEqual(upper, exp_hi, places=2)

    def test_min_sample_sizes(self):
        """Minimum sample size calculations"""
        result = self.get_result()
        self.assertEqual(result.min_sample_sizes, [112, 87, 299, 963])

    def test_recipe_weights(self):
        """Recipe weight calculations"""
        result = self.get_result()
        expected = [116, 228, 86, 20]
        for actual, exp in zip(result.recipe_weights, expected):
            self.assertAlmostEqual(actual, exp, places=0)
        self.assertAlmostEqual(result.total_recipe_weight, 450, places=0)

    def test_sample_size(self):
        """Sample size reported correctly"""
        result = self.get_result()
        self.assertEqual(result.sample_size, 119)

    def test_ingredients(self):
        """Ingredient names in correct order"""
        result = self.get_result()
        self.assertEqual(result.ingredients,
                         ["all purpose flour", "milk", "egg", "butter"])

    def test_restrictions(self):
        """Weight restrictions limit recipe total below requested weight"""
        result = self.get_result("1+2:0+5",
                                 "flour=116,milk=228,egg=86.27,butter=20.4",
                                 total_weight=500)
        self.assertAlmostEqual(result.total_recipe_weight, 450, places=0)
        expected = [116, 228, 86, 20]
        for actual, exp in zip(result.recipe_weights, expected):
            self.assertAlmostEqual(actual, exp, places=0)
