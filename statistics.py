"""Calculation and formatting of statistics"""

from difference import percentage_difference_from_mean
import math
import numpy
from columns import ColumnTranslator


Z_VALUE = 1.96 # represents a confidence level of 95%

def calculate_minimum_sample_sizes(std_deviations, means, desired_interval):
    """Calculate minimum sample size needed for a confidence interval 
       of 5% difference from the mean with 95% confidence level"""
    for std, mean in zip(std_deviations, means):
        if mean == 0:
            yield 0
        else:
            yield math.ceil(((Z_VALUE * std) / (mean * desired_interval)) ** 2)

def calculate_confidence_intervals(data, std_deviations):
    """Calculate confidence intervals for each ingredient"""
    intervals = []
    for column, std in zip(data, std_deviations):
        sample_size = len(column)
        std_error = std / math.sqrt(sample_size)
        intervals.append(std_error * Z_VALUE)
    return intervals

def create_zero_filter(ingredients, zero_columns):
    """Convert column id list into specification for which columns should be
       filtered for zeros"""
    filter_map = {}
    for i in range(len(ingredients)):
        filter_map[i] = False
    if zero_columns is None:
        return filter_map
    column_translator = ColumnTranslator(ingredients)
    for column_id in zero_columns:
        for index in column_translator.id_to_indexes(column_id):
            filter_map[index] = True
    return filter_map


def filter_zeros(data, ingredients, zero_columns):
    """Filter zero values according to specification"""
    filter_map = create_zero_filter(ingredients, zero_columns)
    new_data = []
    for i in range(len(data)):
        column = data[i]
        if filter_map[i]:
            column = list(value for value in column if float(value) != 0.0)
            new_data.append(numpy.array(column))
        else:
            new_data.append(column)

    return new_data

def calculate_statistics(data, ingredients, desired_interval,
                        zero_columns):
    """Calculate mean, confidence interval and minimum sample size for each
       ingredient. The "minimum sample size" is the sample size required to
       achieve a confidence interval that is within a certain percentage
       difference of the mean value (controlled by the 'desired_interval'
       argument) with a confidence level of 95%.
    """
    data = numpy.array(data).transpose()
    data = filter_zeros(data, ingredients, zero_columns)
    std_deviations = []
    means = []
    for column in data:
        std_deviations.append(column.std())
        means.append(column.mean())
    intervals = calculate_confidence_intervals(data, std_deviations)
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
           for floating point as_percentages."""
        self._precision = precision
        
    def float_format(self):
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
        percentages = ratio.as_percentages()
        for percentage, interval, ingredient in zip(percentages, self.intervals,
                                                    ratio.ingredients):
            upper_value = percentage + interval
            upper = self.float_format() % upper_value
            lower_value = percentage - interval
            lower = self.float_format() % lower_value
            mean = self.float_format() % percentage
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
