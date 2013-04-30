"""Statistical analysis of multiple recipes of the same type."""

import RationalRecipes.utils as utils
from RationalRecipes.output import Output

class StatsMain(object):
    """Defines entry point and supporting methods for stats script"""

    def __init__(self, filenames, distinct, merge, zero_columns):
        self.distinct = distinct
        self.confidence = 0.05
        self.restrictions = []
        _, self.ratio, self.stats, self.sample_size = utils.get_ratio_and_stats(
                                                    filenames, distinct, merge,
                                                    zero_columns=zero_columns)

    def set_restrictions(self, restrictions):
        """Set per ingredient weight restrictions""" 
        self.ratio.set_restrictions(restrictions)

    def set_desired_interval(self, interval):
        """Set desired confidence interval"""
        self.confidence = interval
        self.stats.set_desired_interval(interval)

    def main(self, ratio_precision, recipe_precision, total_recipe_weight,
             verbose):
        """Entry method for script"""
        self.ratio.set_precision(ratio_precision)
        self.stats.set_precision(ratio_precision)
        output = Output()
        self.print_ratio(output)
        if verbose:
            self.print_confidence_intervals(output, self.confidence)
        self.print_recipe(output, recipe_precision, total_recipe_weight)
        self.print_footer(output)
        return str(output)
    
    def print_footer(self, output):
        """Print note on sample data at end of input"""
        text = "recipe proportions. The data may contain duplicates."
        if self.distinct:
            text = "distinct recipe proportions. Duplicates have been removed."
        output.line("Note: these calculations are based on %d %s" % \
            (self.sample_size, text))
    
    def print_recipe(self, output, recipe_precision, total_recipe_weight):
        """Print recipe with a specified total weight"""
        self.ratio.set_precision(recipe_precision)
        weight, text = self.ratio.recipe(total_recipe_weight)
        output.title("%dg Recipe" % weight)
        output.line(text)
        output.line()
    
    def print_ratio(self, output):
        """Print calculated ingredient ratio"""
        output.line()
        output.line("Recipe ratio in units of weight is %s" % self.ratio)
        output.line()
       
    def print_confidence_intervals(self, output, confidence):
        """Print confidence intervals for each ingredient proportion"""
        if self.sample_size < 2:
            output.line()
            output.line("Too little data available to provide statistics.")
            output.line()
            return
        output.title("Recipe ratio with confidence intervals "
                     "(confidence level is 95%)")
        self.stats.print_confidence_intervals(output)
        output.line()
        output.title("Minimum sample sizes needed for confidence "
                    "interval with %d%% difference and confidence level "
                    "of 95%%" % int(confidence * 100))
        self.stats.print_min_sample_sizes(output)
        output.line()