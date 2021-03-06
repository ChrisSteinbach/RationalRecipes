"""Unit tests for stats script"""
from RationalRecipes import StatsMain
import RationalRecipes.utils as utils
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

    def verify_script_output(self, script, total_weight=450):
        """Check that script output conforms to expected output"""
        output = script.main(ratio_precision=2, recipe_precision=0,
                             total_recipe_weight=total_weight, verbose=True)
        self.verify_output(output, EXPECTED_OUTPUT)

    def test_run_merge_using_names(self):
        """Test run of script using ingredient names to 
           specify column merge"""
        script = script_instance("milk+water:flour+salt")
        self.verify_script_output(script)
            
    def test_run_merge_using_indexes(self):
        """Test run of script using column indexes to specify
           column merge"""
        script = script_instance("1+2:0+5")
        self.verify_script_output(script)
                 
    def test_restrictions(self):
        """Test run of script using column indexes to specify
           column merge"""
        script = script_instance("1+2:0+5",
                                 "flour=116,milk=228,egg=86.27,butter=20.4")
        self.verify_script_output(script, 500)
