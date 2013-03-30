#!/usr/bin/python
"""Statistical analysis of multiple recipes of the same type."""

import utils
from optparse import OptionParser
from output import Output

def parse_command_line():
    """Parse command line arguments"""
    usage = "usage: %prog [options] csv-file"
    parser = OptionParser(usage=usage)
    parser.add_option("-p", "--precision", type="int",
        dest="ratio_precision", help="number of DIGITS to show after "
        "decimal point for ratio values (default is %default)", default=2,
        metavar="DIGITS")
    parser.add_option("-r", "--recipe-precision", type="int",
        dest="recipe_precision", help="number of DIGITS to show after "
        "decimal point for recipe values (default is %default)", default=0,
        metavar="DIGITS")
    parser.add_option("-w", "--weight", type="int", dest="total_recipe_weight",
        help="total weight to use for example recipe in GRAMS "
        "(default is 100)", default=None, metavar="GRAMS")
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose",
        default=False, help="output extra information")
    utils.add_include_option(parser)
    parser.add_option("-c", "--confidence-interval", type="float",
        dest="confidence", default=0.05,
        help="desired confidence interval expressed as a percentage "
        "difference from zero to the mean, default is %default")
    utils.add_merge_option(parser)
    parser.add_option("-t", "--restrict", type="string",
        dest="restrictions", default=None,
        help="restrict ingredients by weight, per ingredient where restrictions"
           " is col=weight[,col=weight]", metavar="RESTRICTIONS")
    options, filenames = parser.parse_args()
    merge = utils.parse_column_merge(options.merge)
    restrictions = utils.parse_restrictions(options.restrictions)
    if len(filenames) < 1:
        parser.error("no input file provided")
    return filenames, options, merge, restrictions
 

class StatsMain(object):
    """Defines entry point and supporting methods for stats script"""

    def __init__(self, filenames, distinct, merge, confidence):
        self.distinct = distinct
        self.confidence = confidence
        self.restrictions = []
        _, self.ratio, self.stats, self.sample_size = utils.get_ratio_and_stats(
                                                    filenames, distinct, merge, 
                                                    desired_interval=confidence)

    def set_restrictions(self, restrictions):
        """Set per ingredient weight restrictions""" 
        self.ratio.set_restrictions(restrictions)

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
        self.stats.print_confidence_intervals(self.ratio, output)
        output.line()
        output.title("Minimum sample sizes needed for confidence "
                    "interval with %d%% difference and confidence level "
                    "of 95%%" % int(confidence * 100))
        self.stats.print_min_sample_sizes(self.ratio, output)
        output.line()

def run():
    """Run the script from the command line"""
    import sys
    filenames, options, merge, restrictions = parse_command_line()
    distinct = options.distinct
    confidence = options.confidence
    script = StatsMain(filenames, distinct, merge, confidence)
    
    total_recipe_weight = options.total_recipe_weight
    ratio_precision = options.ratio_precision
    recipe_precision = options.recipe_precision
    verbose = options.verbose

    if restrictions:
        script.set_restrictions(restrictions)
        if total_recipe_weight is None:
            total_recipe_weight = sys.maxint
    else:
        if total_recipe_weight is None:
            total_recipe_weight = 100

    print script.main(ratio_precision, recipe_precision, total_recipe_weight,
                      verbose)

if __name__ == "__main__":
    run()
