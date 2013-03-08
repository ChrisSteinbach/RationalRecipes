"""Classes and functions for calculating and presenting the mean recipe ratio 
   and related information and statistics: ingredient proportions for recipe of
   a given total weight, confidence intervals and more.
"""
from normalize import normalize_to_100g
import math
import numpy

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
    return means, intervals, minimum_sample_sizes

class RatioValue(object):
    """Formats an ingredient proportion for output"""
    
    def __init__(self, value, ingredient, value_template):
        self.value = value
        self.ingredient = ingredient
        self.value_template = value_template
    

    def describe_grams_and_milliliters(self):
        """Describe an ingredient proportion in grams and milliliters"""
        value = self.value
        ingredient = self.ingredient
        grams = self.value_template % value
        milliliters = self.value_template % ingredient.grams2milliliters(value)
        return grams + "g or " + milliliters + "ml %s" % ingredient.name()

    def format_number(self, number):
        """Format float according to precision setting"""
        return self.value_template % number
        
    def describe_wholeunits(self):
        """Describe an ingredient proportion in grams, milliliters and whole
           units"""
        value = self.value
        ingredient = self.ingredient
        template = "%sg, %sml or %s %s(s) where each %s is %sg"
        wholeunits = self.format_number(ingredient.grams2wholeunits(value))
        grams_per_wholeunit = \
            self.format_number(ingredient.default_wholeunit_weight())
        name = ingredient.name()
        grams = self.format_number(value)
        milliliters = self.format_number(ingredient.grams2milliliters(value))
        return template % (grams, milliliters, wholeunits, name, name,
                           grams_per_wholeunit)

    def describe(self):
        """Describe an ingredient proportion"""
        if self.ingredient.default_wholeunit_weight() == None:
            return self.describe_grams_and_milliliters()
        else:
            return self.describe_wholeunits()

class Ratio(object):
    """Provides formatting for ingredient ratios and related statistics"""
    
    def __init__(self, ingredients, ratio, intervals, min_sample_sizes):
        self.ratio = ratio
        self.intervals = intervals
        self.min_sample_sizes = min_sample_sizes
        self.ingredients = ingredients
        self._precision = 2
        self._scale = 1
    
    def __delitem__(self):
        """Definition provided purely to appease pylint"""
        raise AssertionError("attempt to delete from read only container")
    
    def __setitem__(self):
        """Definition provided purely to appease pylint"""
        raise AssertionError("attempt to delete from read only container")

    def __len__(self):
        return len(self.ratio)
            
    def set_precision(self, precision):
        """Set precision (i.e. number of digits shown after decimal point)
           for floating point values."""
        self._precision = precision
    
    def set_scale(self, scale):
        """Set a scale for the ratio values."""
        self._scale = scale
    
    def _float_format(self):
        """String format for floats with correct precision"""
        return "%1." + "%df" % self._precision
    
    def describe_ingredients(self):
        """List the ingredients in the same order as they will appear in the
           ratio."""
        return " (" + ":".join(str(c) for c in self.ingredients) + ")"
    
    def __str__(self):
        return (":".join(self._float_format() % (value * self._scale) \
                    for value in self.ratio)) + self.describe_ingredients()
    
    def print_confidence_intervals(self, output):
        """Print confidence intervals for mean of each ingredient proportion"""
        percentages = [v.value for v in self.scaled_ratio(100)]
        for percentage, interval, ingredient in zip(percentages, self.intervals,
                                                    self.ingredients):
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
    
    def print_min_sample_sizes(self, output):
        """Print (pre-calculated) minimum samples size for each ingredient
           proportion mean"""
        for i in range(0, len(self)):
            ingredient = str(self.ingredients[i])
            output.line("Minimum sample size for %s proportion: %d" % \
              (ingredient, self.min_sample_sizes[i]))
    
    def __getitem__(self, index):
        return RatioValue(self.ratio[index] * self._scale,
                          self.ingredients[index], self._float_format())
    
    def scaled_ratio(self, weight=100):
        """Yield ratio proportions with specified scaling"""
        saved_scaling = self._scale
        total_grams = sum(self.ratio)
        self.set_scale(weight / float(total_grams))
        try:
            for proportion in self:
                yield proportion
        finally:
            self.set_scale(saved_scaling)
    
    def recipe(self, weight=100):
        """Format the ingredient proportions as if for a recipe ingredient
           list."""
        ingredients_list = self.scaled_ratio(weight)
        return "\n".join(ingredient.describe() \
                         for ingredient in ingredients_list)
    
    def percentage_difference(self, other):
        """Return the mean percentage difference and the percentage difference
           for individual ingredient proportions between two ratios."""
        differences = []
        lhs_pc = [v.value for v in self.scaled_ratio(100)]
        rhs_pc = [v.value for v in other.scaled_ratio(100)]
        for i in range(0, len(self)):
            difference = percentage_difference(lhs_pc[i], rhs_pc[i])
            differences.append((difference, self.ingredients[i]))
        total = sum(difference for difference, unused in differences)
        mean_difference = total / len(self)
        return mean_difference, differences
    
    def percentage_change(self, other):
        """Return the percentage change for individual ingredient proportions
           between two ratios."""
        differences = []
        lhs_pc = [v.value for v in self.scaled_ratio(100)]
        rhs_pc = [v.value for v in other.scaled_ratio(100)]
        for i in range(0, len(self)):
            change = percentage_change(lhs_pc[i], rhs_pc[i])
            differences.append((change, self.ingredients[i]))
        return differences
    
def calculate_ratio(ingredients, proportions, desired_interval=0.5):
    """Calculate ratio proportions and related statistics (confidence intervals
       and minimum sample sizes) from input data."""
    relative_proportions = list(normalize_to_100g(proportions))
    relative_proportions = numpy.array(relative_proportions)
    means, intervals, min_sample_sizes = \
        calculate_statistics(relative_proportions, desired_interval)
    # Normalize relative to 100% of first ingredient
    ratio = [means[i] / means[0] for i in xrange(0, len(ingredients))]
    return Ratio(ingredients, ratio, intervals, min_sample_sizes)

def percentage_change(src, dest):
    """Calculate percentage change from one value (src) to another (dest)"""
    return (dest - src) / src

def percentage_difference(value1, value2):
    """Calculate percentage difference between two values. Used for comparing
       two different ratios. Percentage difference calculated this way can
       exceed 100%."""
    mean = (value1 + value2) / 2.0
    mean_diff = abs(value2 - value1)
    return mean_diff / mean

def percentage_difference_from_mean(value1, value2):
    """Calculate percentage difference between two values. Used to help make
       confidence interval sizes more intuitive by keeping the percentage
       difference under 100%"""
    mean = (value1 + value2) / 2.0
    mean_diff = abs(mean - value1)
    return mean_diff / mean
