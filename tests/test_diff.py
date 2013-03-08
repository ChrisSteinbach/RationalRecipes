"""Unit tests for diff script"""
import utils
from diff import DiffMain
import tests.test_utils as test_utils

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
    script = DiffMain(["tests/test_diff_a.csv"], ["tests/test_diff_b.csv"], 
                      distinct=True,
                      merge=utils.parse_column_merge("1+2:0+5"))
    return script 

class TestDiff(test_utils.ScriptTestCase):
    """Unit tests for diff script"""

    def test_percentage_change(self):
        """Test diff script showing percentage change"""
        script = script_instance()
        output = script.main(show_percentage_change=True, precision=0)
        self.verify_output(output, PERCENT_CHANGE_EXPECTED_OUTPUT)

    def test_percentage_difference(self):
        """Test diff script showing percentage difference"""
        script = script_instance()
        output = script.main(show_percentage_change=False, precision=2)
        self.verify_output(output, PERCENT_DIFF_EXPECTED_OUTPUT)

