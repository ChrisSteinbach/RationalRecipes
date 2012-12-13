"""Volume, weight and whole units of measure. Each unit is capable of conversion
   to grams.
"""

class Factory(object):
    """Registry and factory for all units of measure."""
    _UNITS = {}

    @classmethod
    def register(cls, unit):
        """Register unit name and synonyms"""
        for name in unit.synonyms():
            cls._UNITS[name.lower().strip()] = unit

    @classmethod
    def get_by_name(cls, name):
        """Lookup a Unit instance by name"""
        try:
            return cls._UNITS[name.lower()]
        except KeyError:
            return None

class Unit(object):
    """Abstract unit of measure"""
    
    def __init__(self, names):
        self._names = names
        Factory.register(self)

    def synonyms(self):
        """List synonyms for a unit"""
        return self._names
    
    def norm(self, value, ingredient, line_nr=None):
        """Normalize an ingredient measure to grams"""
        raise NotImplementedError("Unit.norm() must be implemented in derived " 
                 "class")
    
class WeightUnit(Unit):
    """Units of measure by weight"""
    def __init__(self, names, conversion):
        Unit.__init__(self, names)
        self._conversion = conversion

    def norm(self, value, ingredient, line_nr=None):
        """Normalizes any weight unit to grams"""
        return value * self._conversion

GRAM = WeightUnit(["gram", "grams", "g"], 1)
HG = WeightUnit(["hg", "hectogram", "hectograms"], 100)
KG = WeightUnit(["kg", "kilos", "kilograms"], 1000)
OZ = WeightUnit(["oz", "ounces"], 28.3495231)
LB = WeightUnit(["lbs", "lb", "pounds"], 453.592)

class VolumeUnit(Unit):
    """Units of measure by volume"""
    def __init__(self, names, conversion):
        Unit.__init__(self, names)
        self._conversion = conversion

    def norm(self, value, ingredient, line_nr=None):
        """Normalizes any volume unit to milliliters and then converts to grams
        """
        milliliters = value * self._conversion
        return ingredient.milliliters2grams(milliliters)

QUART = VolumeUnit(["quart", "quarts"], 946.353)
US_PINT = VolumeUnit(["US pint", "pint", "pints", "pt", "us_pint"], 473.176)
IMP_PINT = VolumeUnit(["imperial pint", "uk pint", "imp_pint"], 568.261)
US_FLOZ = VolumeUnit(["US fluid ounce", "us_floz", "fluid ounce", "fl oz",
                      "fl. oz"], 29.5735)
IMP_FLOZ = VolumeUnit(["imperial fluid ounce", "uk fluid ounce", "imp_floz"],
                      28.4131)
ML = VolumeUnit(["ml", "milliliter", "milliliters"], 1)
KRM = VolumeUnit(["krm"], 1)
CL = VolumeUnit(["cl", "centiliter", "centiliters"], 10)
DL = VolumeUnit(["dl", "deciliter", "deciliters"], 100)
LITER = VolumeUnit(["l", "liter", "liters"], 1000)
# This cup size is taken from a bit of amateur market research where online
# measuring cup sizes were split between 30% legal size, 30% metric and 30%
# traditional. Taking into account figures from Wikipedia saying that the online
# market is 8% of all retail we get the ~238.33ml measure below since measuring
# cups bought in US shops most likely exclude metric cups.
CUP = VolumeUnit(["c", "cups", "cup", "us cup", "us cups"], 238.337210755)
METRIC_CUP = VolumeUnit(["metric cup", "metric cups"], 250)
METRIC_TBSP = VolumeUnit(["metric tbsp", "metric tb", "mtb",
                          "metric tablespoon", "metric tablespoons", "msk",
                          "cas"], 15.0)
TBSP = VolumeUnit(["tbsp", "tb", "tblsp", "tablespoon", "tablespoons"], 14.7868)
DESSERT_SPOON = DSP = DSTSPN = VolumeUnit(["dessert spoon", "dsp", "dstspn"],
                                          10.0)
TSP = VolumeUnit(["tsp", "teaspoon", "teaspoons"], 4.92892)
METRIC_TSP = VolumeUnit(["metric tsp", "metric teaspoon", "metric teaspoons",
                         "tsk", "mtsp", "cac"], 5.0)
PINCH = VolumeUnit(["pinch", "pinches"], 0.3125)
DASH = VolumeUnit(["dash", "dashes"], 0.625)


class WholeUnit(Unit):
    """Whole units of measure"""
    
    def __init__(self, sizes):
        Unit.__init__(self, sizes)
        self._size = sizes[0]

    def norm(self, value, ingredient, line_nr=None):
        """Normalizes sizeable food stuffs to grams"""
        conversion = ingredient.wholeunits2grams(self._size)
        if conversion is None:
            raise Exception("Inapplicable unit '%s' used for ingredient '%s' at"
                            " line %d" % (self._size, ingredient.name(),
                                          line_nr))
        return value * conversion

XL = WholeUnit(["XL"])
MEDIUM = WholeUnit(["MEDIUM", "US MEDIUM"])
EU_MEDIUM = WholeUnit(["EU MEDIUM"])
LARGE = WholeUnit(["LARGE", "US LARGE"])
EU_LARGE = WholeUnit(["EU LARGE"])
SMALL = WholeUnit(["SMALL", "US SMALL"])
EU_SMALL = WholeUnit(["EU SMALL"])
STICK = WholeUnit(["stick", "sticks"])
CUBE = WholeUnit(["cube", "cubes"])
KNOB = WholeUnit(["knob", "knobs"])
