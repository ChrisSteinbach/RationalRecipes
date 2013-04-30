#!/usr/bin/python
"""Compare recipe ratios showing percentage change or percentage difference"""

import RationalRecipes.utils as utils
from RationalRecipes.output import Output
from RationalRecipes.difference import percentage_change, percentage_difference
import sys

def get_ratios_to_compare(first_filename, remaining_filenames, distinct, merge):
    """Get ratios to compare from input files"""
    ingredients1, ratio1 = utils.get_ratio(first_filename, distinct, merge)
    ingredients2, ratio2 = utils.get_ratio(remaining_filenames, distinct, merge)
    if ingredients1 != ingredients2:
        print "Ingredients for input files do not match: unable to compare"
        sys.exit(1)
    return ratio1, ratio2
      
class DiffMain(object):
    """Defines entry point and supporting methods for diff script"""
    
    def __init__(self, first_filename, remaining_filenames, distinct, merge):
        self.number_template = "%%0.%df"
        self.ratio1, self.ratio2 = get_ratios_to_compare(first_filename,
                                        remaining_filenames, distinct, merge)

    def main(self, show_percentage_change, precision):
        """Entry method for script"""
        self.number_template = "%%0.%df" % precision
        output = Output()
        self.print_ratios(output)
        diff_info = percentage_difference(self.ratio1, self.ratio2)
        mean_difference, differences = diff_info
        if show_percentage_change is False:
            self.print_percentage_difference(output, differences)
        else:
            self.print_percentage_change(output)
        self.print_overall_percentage_diff(output, mean_difference)
        return str(output)
    
    def print_overall_percentage_diff(self, output, mean_difference):
        """Print overall percentage difference between ratios. This is
           calculated as the mean value of the percentage change for all
           ingredients"""
        output.line()
        output.line(("Overall percentage difference = " + \
               self.number_template + "%%") % (mean_difference * 100))
        output.line()
    
    def print_percentage_change(self, output):
        """Print percentage change between ratios for each ingredient"""
        changes = percentage_change(self.ratio1, self.ratio2)
        for change, ingredient in sorted(changes, cmp=lambda x, y:cmp(abs(x[0]),
                                         abs(y[0])), reverse=True):
            direction = "increased"
            if change < 0.0:
                change = abs(change)
                direction = "decreased"
            output.line(("The %s proportion has %s by " + \
                    self.number_template + "%% from data set 1 to 2") % \
                    (ingredient, direction, change * 100))
    
    def print_percentage_difference(self, output, differences):
        """Print percentage difference between ratios for each ingredient"""
        for difference, ingredient in sorted(differences, reverse=True):
            output.line(("Percentage difference between %s proportions " + \
                self.number_template + "%%") % (ingredient, difference * 100))
    
    def print_ratios(self, output):
        """Print ratios to be compared"""
        output.line()
        output.line("Ratio for data set 1 in units of weight is %s" % \
                    self.ratio1)
        output.line("Ratio for data set 2 in units of weight is %s" % \
                    self.ratio2)
        output.line()