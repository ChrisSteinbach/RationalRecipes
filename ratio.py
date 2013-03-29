"""Classes and functions for calculating and presenting the mean recipe ratio 
   and related information and statistics: ingredient proportions for recipe of
   a given total weight, confidence intervals and more.
"""
from normalize import normalize_to_100g
from columns import ColumnTranslator
from errors import InvalidInputException
from statistics import calculate_statistics

class RatioElement(object):
    """Formats an ingredient proportion for output"""
    
    def __init__(self, value, ingredient, value_template):
        self.value = value
        self.ingredient = ingredient
        self.value_template = value_template
    
    def _describe_grams_and_milliliters(self):
        """Describe an ingredient proportion in grams and milliliters"""
        value = self.value
        ingredient = self.ingredient
        grams = self.value_template % value
        milliliters = self.value_template % ingredient.grams2milliliters(value)
        return grams + "g or " + milliliters + "ml %s" % ingredient.name()

    def _format_number(self, number):
        """Format float according to precision setting"""
        return self.value_template % number
        
    def _describe_wholeunits(self):
        """Describe an ingredient proportion in grams, milliliters and whole
           units"""
        value = self.value
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

    def describe(self):
        """Describe an ingredient proportion"""
        if self.ingredient.default_wholeunit_weight() == None:
            return self._describe_grams_and_milliliters()
        else:
            return self._describe_wholeunits()


class Ratio(object):
    """Provides formatting for ingredient ratios and related statistics"""
    
    def __init__(self, ingredients, values):
        self._values = values
        self.ingredients = ingredients
        self._precision = 2
        self._scale = 1
        self._restrictions = []
        self._column_translator = ColumnTranslator(self.ingredients)

    def _column_id_to_indexes(self, column_identifier):
        """Normalize column identifier to a column index"""
        indexes = list(self._column_translator.id_to_indexes(column_identifier))
        if len(indexes) == 0:
            raise InvalidInputException(
                "Attempted to restrict missing column '%s'" % column_identifier)
        return indexes

    def _float_format(self):
        """String format for floats with correct precision"""
        return "%1." + "%df" % self._precision

    def _restrict_total_weight(self, weight):
        """Yield ratio proportions with specific total weight. Returns scale
           applied."""
        total_grams = sum(self._values)
        scale = weight / float(total_grams)
        return list(self._scaled_ratio(scale)), scale

    def _restrict_by_ingredient(self, ingredients_list, scale):
        """Restrict a recipe based on individual ingredient/weight-limit
           specifications"""
        for column_index, weight_limit in self._restrictions:
            if ingredients_list[column_index] > weight_limit:
                new_scale = weight_limit / self._values[column_index]
                if new_scale < scale:
                    scale = new_scale
        return list(self._scaled_ratio(scale))

    def _element(self, index):
        """Return a single _element of the ratio"""
        return RatioElement(self._values[index] * self._scale,
                          self.ingredients[index], self._float_format())
    
    def _scaled_ratio(self, scale):
        """Yield ratio proportions with a specific scale applied"""
        saved_scaling = self._scale
        self.set_scale(scale)
        try:
            for i in range(0, len(self._values)):
                yield self._element(i)
        finally:
            self.set_scale(saved_scaling)

    def set_restrictions(self, restrictions):
        """Individual ingredient weight restrictions"""
        _restrictions = []
        for column_id, weight in restrictions:
            for column_index in self._column_id_to_indexes(column_id):
                _restrictions.append((column_index, weight))
        self._restrictions = _restrictions

    def len(self):
        """Return number of ratio elements"""
        return len(self._values)
            
    def set_precision(self, precision):
        """Set precision (i.e. number of digits shown after decimal point)
           for floating point values."""
        self._precision = precision
    
    def set_scale(self, scale):
        """Set a scale for the ratio values."""
        self._scale = scale
        
    def list_ingredients(self):
        """List the ingredients in the same order as they will appear in the
           ratio."""
        return " (" + ":".join(str(c) for c in self.ingredients) + ")"
    
    def __str__(self):
        return (":".join(self._float_format() % value \
                    for value in self._values)) + self.list_ingredients()
                    
    def describe_ingredient(self, column_id):
        """Describe individual ingredients"""
        return "\n".join(self._element(index).describe() \
                         for index in self._column_id_to_indexes(column_id))
                   
    def recipe(self, weight):
        """Format the ingredient proportions as if for a recipe ingredient
           list. Also return total weight."""
        ingredients_list, scale = self._restrict_total_weight(weight)
        ingredients_list = self._restrict_by_ingredient(ingredients_list, scale)
        total_weight = sum(ingredient.value for ingredient in ingredients_list)
        return total_weight, "\n".join(ingredient.describe() \
                         for ingredient in ingredients_list)
    
    def values(self):
        """Return ratio numeric values"""
        return [v.value for v in self._restrict_total_weight(100)[0]]
     
def calculate_ratio_and_stats(ingredients, proportions, desired_interval=0.5):
    """Calculate ratio proportions and related statistics (confidence intervals
       and minimum sample sizes) from input data."""
    relative_proportions = list(normalize_to_100g(proportions))
    means, statistics = \
        calculate_statistics(relative_proportions, desired_interval)
    # Normalize relative to 100% of first ingredient
    ratio = [means[i] / means[0] for i in xrange(0, len(ingredients))]
    return statistics, Ratio(ingredients, ratio)

