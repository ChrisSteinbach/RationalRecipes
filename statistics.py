"""Calculation and formatting of statistics"""

from difference import percentage_difference_from_mean
import math

Z_VALUE = 1.96 # represents a confidence level of 95%

def calculate_minimum_sample_sizes(std_deviations, means, desired_interval):
    """Calculate minimum sample size needed for a confidence interval 
       of 5% difference from the mean with 95% confidence level"""
    for std, mean in zip(std_deviations, means):
        yield math.ceil(((Z_VALUE * std) / (mean * desired_interval)) ** 2)

def calculate_confidence_intervals(data, std_deviations):
    """Calculate confidence intervals for each ingredient"""
    sample_size = len(data)
    std_errors = tuple(std / math.sqrt(sample_size) for std in std_deviations)
    return tuple(stderr * Z_VALUE for stderr in std_errors)

def calculate_statistics(data, desired_interval=0.05):
    """Calculate mean, confidence interval and minimum sample size for each
       ingredient. The "minimum sample size" is the sample size required to
       achieve a confidence interval that is within a certain percentage
       difference of the mean value (controlled by the 'desired_interval'
       argument) with a confidence level of 95%.
    """
    std_deviations = data.std(axis=0)
    intervals = calculate_confidence_intervals(data, std_deviations)
    means = data.mean(axis=0)
    minimum_sample_sizes = tuple(calculate_minimum_sample_sizes(std_deviations,
                                 means, desired_interval))
    return means, Statistics(intervals, minimum_sample_sizes)


class Statistics:
    """Calculate statistics"""
    
    def __init__(self, intervals, min_sample_sizes):
        self.intervals = intervals
        self.min_sample_sizes = min_sample_sizes
        self._precision = 2
        
    def set_precision(self, precision):
        """Set precision (i.e. number of digits shown after decimal point)
           for floating point values."""
        self._precision = precision
        
    def _float_format(self):
        """String format for floats with correct precision"""
        return "%1." + "%df" % self._precision
    
    def print_min_sample_sizes(self, ratio, output):
        """Print (pre-calculated) minimum samples size for each ingredient
           proportion mean"""
        for i in range(0, ratio.len()):
            ingredient = str(ratio.ingredients[i])
            output.line("Minimum sample size for %s proportion: %d" % \
              (ingredient, self.min_sample_sizes[i]))

    def print_confidence_intervals(self, ratio, output):
        """Print confidence intervals for mean of each ingredient proportion"""
        percentages = ratio.values()
        for percentage, interval, ingredient in zip(percentages, self.intervals,
                                                    ratio.ingredients):
            upper_value = percentage + interval
            upper = self._float_format() % upper_value
            lower_value = percentage - interval
            lower = self._float_format() % lower_value
            mean = self._float_format() % percentage
            text = "The " + str(ingredient) + " proportion "
            if interval == 0.0:
                difference = 0.0
            else:
                difference = percentage_difference_from_mean(lower_value,
                                upper_value) * 100
            if difference > 0.01:
                diff_text = ("%% (the interval is %0.0f%% of the mean "
                             "proportion: %s%%)") % (difference, mean)
                output.line(text + "is between " + lower + "% and " +\
                             upper + diff_text)
            else:
                output.line(text + lower)
