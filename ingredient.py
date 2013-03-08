"""Ingredient conversions from milliliters to grams"""

class Factory(object):
    """Factory and registry for ingredient instances"""
    _INGREDIENTS = {}

    @classmethod
    def register(cls, ingredient):
        """Register ingredient name and synonyms"""
        for name in ingredient.synonyms():
            cls._INGREDIENTS[name.lower().strip()] = ingredient

    @classmethod
    def get_by_name(cls, name):
        """Lookup a Ingredient instance by name"""
        return cls._INGREDIENTS[name.lower()]
    
class Ingredient(object):
    """Ingredient class converts between volume, weight and whole unit
       measurements"""
       
    def __init__(self, names, conversion, wholeunits2weight=None,
                 default_wholeunit_weight=None):
        self._conversion = conversion
        self._name = names[0]
        self._names = names
        self._wholeunits2grams = {}
        if wholeunits2weight is not None:
            for unit, weight in wholeunits2weight.items():
                self._wholeunits2grams[unit.lower()] = weight
        if default_wholeunit_weight is not None:
            self._default_wholeunit_weight = default_wholeunit_weight.lower()
        else:
            self._default_wholeunit_weight = None
        Factory.register(self)

    def name(self):
        """Returns ingredient name"""
        return self._name
    
    def synonyms(self):
        """Returns a list of ingredient synonyms"""
        return self._names
    
    def milliliters2grams(self, milliliters):
        """Convert milliliter measure to grams"""
        return milliliters * self._conversion

    def grams2milliliters(self, grams):
        """Convert measure in grams to milliliters"""
        return grams / self._conversion

    def wholeunits2grams(self, wholeunit):
        """Convert whole unit measurement to grams"""
        if self._wholeunits2grams is None:
            return None
        try:
            return self._wholeunits2grams[wholeunit.lower()]
        except KeyError:
            return None

    def grams2wholeunits(self, grams):
        """Convert measure in grams to the default wholeunit
           (if such exists)"""
        if self.default_wholeunit_weight() != None:
            return grams / self.default_wholeunit_weight()
        else:
            return None

    def default_wholeunit_weight(self):
        """Returns a standard weight for an ingredient, or None if there is no 
           such weight"""
        if self._default_wholeunit_weight:
            return self._wholeunits2grams[self._default_wholeunit_weight]
        else:
            return None

    def __repr__(self):
        return self.name()

    def __str__(self):
        return self.name()
    
MILK = Ingredient(["milk"], 1)
WATER = Ingredient(["water"], 1)
EGG = Ingredient(["medium egg", "egg"], 1.181592, {"XL":67, "LARGE":60,
                "MEDIUM":53, "SMALL":46, "EU LARGE":59, "EU MEDIUM":52,
                "EU SMALL":45, "EU XL":66}, "MEDIUM")
FLOUR = Ingredient(["all purpose flour", "plain flour", "flour"], 0.527426)
SALT = Ingredient(["salt"], 1.2658)
BUTTER = Ingredient(["butter"], 1.012658, {"STICK":113.398, "CUBE":56.699,
                "KNOB":30.37974})
GRATED_CHEESE = Ingredient(["grated cheese"], 0.379747)
COCOA = Ingredient(["cocoa"], 1.388888)
CREAM = Ingredient(["cream"], 0.777777)
SOUR_CREAM = Ingredient(["sour cream"], 0.811537)
CORNSTARCH = Ingredient(["cornstarch"], 0.640000)
POTATO_STARCH = Ingredient(["potato starch"], 0.72)
HONEY = Ingredient(["honey"], 1.3)
SUGAR = Ingredient(["granulated sugar", "sugar"], 0.843880)
ICING_SUGAR = Ingredient(["icing sugar", " powder sugar",
   "confectioner's sugar"], 0.506329)
CORN_SYRUP = Ingredient(["corn syrup"], 1.3688)
CHOCOLATE_70_PERCENT = Ingredient(["chocolate 70 percent"], 0.731228)
VANILLA_EXTRACT = Ingredient(["vanilla extract"], 0.879165)
BLUEBERRIES = Ingredient(["blueberries"], 0.625559)
BAKING_SODA = Ingredient(["baking soda", "bicarbonate", "bicarbonate of soda"],
                         0.934112)
BAKING_POWDER = Ingredient(["baking powder"], 0.934112)
BUTTER_MILK = Ingredient(["buttermilk", "butter milk"], 1.035554)
CARDAMOM = Ingredient(["ground cardamom", "cardamom"], 0.39)
CINNAMON = Ingredient(["ground cinnamon", "cinnamon"], 0.53)

# Milliliter conversion for potatoes assumes shredded (i.e grated) potatoes
POTATO = Ingredient(["potato", "potatoes", "shredded potato", "grated potato"],
      1.3796292, {"medium":184, "large":283, "large baking":340,
                  "medium baking":283, "small baking":226}, "medium")

ONION = Ingredient(["onion", "onions"], 1.04, {"large":340, "medium":227,
      "small":113}, "medium")
