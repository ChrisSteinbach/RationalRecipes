"""Classes and functions for calculating and presenting the mean recipe ratio 
   and related information and statistics: ingredient proportions for recipe of
   a given total weight, confidence intervals and more.
"""
from normalize import normalize_to_100g
from columns import ColumnTranslator
from statistics import calculate_statistics

class RatioElement(object):
    """Formats an ingredient proportion for output"""
    
    def __init__(self, value, ingredient, ratio):
        self.value = float(value)
        self.ingredient = ingredient
        self.ratio = ratio
        
    def _float_format(self):
        """Return float output format set for ratio"""
        return self.ratio.float_format()
    
    def _describe_grams_and_milliliters(self, scale):
        """Describe an ingredient proportion in grams and milliliters"""
        value = self.value * scale
        ingredient = self.ingredient
        grams = self._float_format() % value
        milliliters = self._float_format() % ingredient.grams2milliliters(value)
        return grams + "g or " + milliliters + "ml %s" % ingredient.name()

    def _format_number(self, number):
        """Format float according to precision setting"""
        return self._float_format() % number

    def _describe_wholeunits(self, scale):
        """Describe an ingredient proportion in grams, milliliters and whole
           units"""
        value = self.value * scale
        ingredient = self.ingredient
        template = "%sg, %sml or %s %s(s) where each %s is %sg"
        wholeunits = self._format_number(ingredient.grams2wholeunits(value))
        grams_per_wholeunit = \
            self._format_number(ingredient.default_wholeunit_weight())
        name = ingredient.name()
        grams = self._format_number(value)
        milliliters = self._format_number(ingredient.grams2milliliters(value))
        return template % (grams, milliliters, wholeunits, name, name,
                           grams_per_wholeunit)

    def __str__(self):
        """Return the value as a formatted string"""
        return self._format_number(self.value)

    def scaled(self, scale):
        """Scaled value"""
        return self.value * scale
      
    def describe(self, scale):
        """Describe an ingredient proportion"""
        if self.ingredient.default_wholeunit_weight() == None:
            return self._describe_grams_and_milliliters(scale)
        else:
            return self._describe_wholeunits(scale)


class Ratio(object):
    """Provides formatting for ingredient ratios and related statistics"""
    
    def __init__(self, ingredients, values):
        self.ingredients = ingredients
        self._precision = 2
        self._restrictions = []
        self._column_translator = ColumnTranslator(self.ingredients)
        self._elements = [RatioElement(values[i], ingredients[i],
                                       self) for i in range(len(values))]

    def _column_id_to_indexes(self, column_identifier):
        """Normalize column identifier to a column index"""
        return self._column_translator.id_to_indexes(column_identifier)

    def _values(self, scale=1):
        """Return raw ratio values"""
        for element in self._elements:
            yield element.scaled(scale)
            
    def _restrict_total_weight(self, weight):
        """Yield ratio proportions with specific total weight. Returns scale
           applied."""
        total_grams = sum(self._values())
        return weight / float(total_grams)

    def _restrict_by_ingredient(self, scale):
        """Restrict a recipe based on individual ingredient/weight-limit
           specifications"""
        for column_indexes, weight_limit in self._restrictions:
            scaled_weight = sum(self._elements[index].scaled(scale) for \
                                index in column_indexes)
            unscaled_weight = sum(self._elements[index].value for \
                                index in column_indexes)
            if scaled_weight > weight_limit:
                new_scale = weight_limit / unscaled_weight
                if new_scale < scale:
                    scale = new_scale
        return scale

    def float_format(self):
        """String format for floats with correct precision"""
        return "%1." + "%df" % self._precision

    def set_restrictions(self, restrictions):
        """Individual ingredient weight restrictions"""
        _restrictions = []
        for column_id, weight in restrictions:
            indexes = self._column_id_to_indexes(column_id)
            _restrictions.append((indexes, weight))
        self._restrictions = _restrictions

    def len(self):
        """Return number of ratio elements"""
        return len(self._elements)
            
    def set_precision(self, precision):
        """Set precision (i.e. number of digits shown after decimal point)
           for floating point as_percentages."""
        self._precision = precision
    
    def list_ingredients(self):
        """List the ingredients in the same order as they will appear in the
           ratio."""
        return " (" + ":".join(str(c) for c in self.ingredients) + ")"
    
    def __str__(self):
        return (":".join(str(element) \
                    for element in self._elements)) + self.list_ingredients()
                    
    def describe_ingredient(self, column_id):
        """Describe individual ingredients"""
        return "\n".join(self._elements[index].describe(scale=1) \
                         for index in self._column_id_to_indexes(column_id))
    
    def recipe(self, weight):
        """Format the ingredient proportions as if for a recipe ingredient
           list. Also return total weight."""
        scale = self._restrict_total_weight(weight)
        scale = self._restrict_by_ingredient(scale)
        total_weight = sum(self._values(scale))
        return total_weight, "\n".join(element.describe(scale) \
                         for element in self._elements)
    
    def as_percentages(self):
        """Return ratio values as percentages"""
        scale =  self._restrict_total_weight(100)
        return list(self._values(scale))
     
def calculate_ratio_and_stats(ingredients, proportions, zero_columns=None):
    """Calculate ratio proportions and related statistics (confidence intervals
       and minimum sample sizes) from input data."""
    relative_proportions = list(normalize_to_100g(proportions))
    means, statistics = \
        calculate_statistics(relative_proportions, ingredients, zero_columns)
    # Normalize relative to 100% of first ingredient
    ratio = [means[i] / means[0] for i in xrange(0, len(ingredients))]
    return statistics, Ratio(ingredients, ratio)

