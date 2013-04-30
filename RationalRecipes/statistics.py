"""Calculation and formatting of statistics"""

from RationalRecipes.difference import percentage_difference_from_mean
from RationalRecipes.normalize import normalize_to_100g
from RationalRecipes.columns import ColumnTranslator
import math
import numpy


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
    column_translator = ColumnTranslator(ingredients)
    for column_id in zero_columns:
        for index in column_translator.id_to_indexes(column_id):
            filter_map[index] = True
    return filter_map


def filter_zeros(data, filter_map):
    """Filter zero values according to specification"""
    new_data = []
    for i in range(len(data)):
        column = data[i]
        if filter_map[i]:
            column = list(value for value in column if float(value) != 0.0)
            new_data.append(numpy.array(column))
        else:
            new_data.append(column)

    return new_data

def apply_defaults(data, defaults, filter_map):
    """Apply default values to zero columns according to settings"""
    new_data = []
    total = sum(defaults)
    percentages = [default/total for default in defaults]
    col_range = range(len(data[0]))
    for row in data:
        for i in col_range:
            if filter_map[i] and row[i] == 0:
                row = [column - (column * percentages[i]) for column in row]
        for i in col_range:
            if filter_map[i] and row[i] == 0:
                row[i] = percentages[i] * 100
        new_data.append(row)
    return new_data

def calculate_variables(data):
    """Calculate standard deviation, mean and confidence interval vectors"""
    std_deviations = []
    means = []
    for column in data:
        std_deviations.append(column.std())
        means.append(column.mean())
    intervals = calculate_confidence_intervals(data, std_deviations)
    return intervals, std_deviations, means

def calculate_statistics(raw_data, ingredients, zero_columns):
    """Calculate mean, confidence interval and minimum sample size for each
       ingredient.
    """
    raw_data = list(normalize_to_100g(raw_data))
    data = numpy.array(raw_data).transpose()
    if zero_columns is not None and len(zero_columns) > 0:
        filter_map = create_zero_filter(ingredients, zero_columns)
        data = filter_zeros(data, filter_map)
        _, _, defaults = calculate_variables(data)
        data = apply_defaults(raw_data, defaults, filter_map)
        return calculate_statistics(data, ingredients, None)
    else:
        intervals, std_deviations, means = calculate_variables(data)
        return Statistics(ingredients, intervals, std_deviations, means)


class Statistics:
    """Calculate statistics"""
    
    def __init__(self, ingredients, intervals, std_deviations, means):
        self.ingredients = ingredients
        self.intervals = intervals
        self.std_deviations = std_deviations
        self.desired_interval = 0.05
        self.means = means
        self._precision = 2
        
    def _float_format(self):
        """String format for floats with correct precision"""
        return "%1." + "%df" % self._precision

    def set_precision(self, precision):
        """Set precision (i.e. number of digits shown after decimal point)
           for floating point as_percentages."""
        self._precision = precision
        
    def set_desired_interval(self, desired_interval):
        """Set desired confidence interval"""
        self.desired_interval = desired_interval

    def bakers_percentage(self):
        """Express mean values as bakers percentage"""
        return [mean / self.means[0] for mean in self.means]

    
    def print_min_sample_sizes(self, output):
        """Print (pre-calculated) minimum samples size for each ingredient
           proportion mean"""
        min_sample_sizes = tuple(calculate_minimum_sample_sizes(
                                        self.std_deviations,
                                        self.means, self.desired_interval))
        for i in range(0, len(self.means)):
            ingredient = str(self.ingredients[i])
            output.line("Minimum sample size for %s proportion: %d" % \
              (ingredient, min_sample_sizes[i]))


    def _print_interval(self, output, percentage, interval, ingredient):
        """Output confidence interval for one ingredient"""
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
            output.line(text + "is between " + lower + "% and " + upper \
                        + diff_text)
        else:
            output.line(text + lower)

    def print_confidence_intervals(self, output):
        """Print confidence intervals for mean of each ingredient proportion"""
        total = sum(self.means)
        percentages = [(mean/total)*100 for mean in self.means]
        for percentage, interval, ingredient in zip(percentages, self.intervals,
                                                    self.ingredients):
            self._print_interval(output, percentage, interval, ingredient)
