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
RUM = Ingredient(["rum"], 1)
JUICE = Ingredient(["juice"], 1)
EGG = Ingredient(["egg", "eggs"], 1.181592, {"XL":67, "LARGE":60,
                "MEDIUM":53, "SMALL":46, "EU LARGE":59, "EU MEDIUM":52,
                "EU SMALL":45, "EU XL":66}, "MEDIUM")
PC_YOLK = 0.31
EGG_YOLK = Ingredient(["egg yolk", "yolk"], 1.03, {"XL":67*0.31, "LARGE":60*0.31,
                "MEDIUM":53*0.31, "SMALL":46*0.31, "EU LARGE":59*0.31, "EU MEDIUM":52*0.31,
                "EU SMALL":45*0.31, "EU XL":66*0.31}, "MEDIUM")
PC_WHITE = 0.58
EGG_YOLK = Ingredient(["egg white"], 1.03, {"XL":67*0.58, "LARGE":60*0.58,
                "MEDIUM":53*0.58, "SMALL":46*0.58, "EU LARGE":59*0.58, "EU MEDIUM":52*0.58,
                "EU SMALL":45*0.58, "EU XL":66*0.58}, "MEDIUM")
APPLE = Ingredient(["apple", "apples"], 1.181592)
RAISIN = Ingredient(["raisin", "raisins"], 1.181592)
PEEL = Ingredient(["peel"], 1.181592)
FLOUR = Ingredient(["all purpose flour", "plain flour", "flour"], 0.527426)
SALT = Ingredient(["salt"], 1.2658)
BUTTER = Ingredient(["butter"], 1.012658, {"STICK":113.398, "CUBE":56.699,
                "KNOB":30.37974})
GRATED_CHEESE = Ingredient(["grated cheese"], 0.379747)
GRATED_PARMESAN = Ingredient(["grated parmesan"], 0.42)
RICOTTA = Ingredient(["ricotta", "ricotta cheese"], 0.93)
COCOA = Ingredient(["cocoa"], 1.388888)
CREAM = Ingredient(["cream"], 0.777777)
SOUR_CREAM = Ingredient(["sour cream"], 0.811537)
CORNSTARCH = Ingredient(["cornstarch"], 0.640000)
POTATO_STARCH = Ingredient(["potato starch"], 0.72)
HONEY = Ingredient(["honey"], 1.3)
SUGAR = Ingredient(["granulated sugar", "sugar"], 0.843880)
MOLASSES = Ingredient(["molasses", "black treacle"], 1.42)
BROWN_SUGAR = Ingredient(["brown sugar"], 0.93)
ICING_SUGAR = Ingredient(["icing sugar", " powder sugar",
   "confectioner's sugar"], 0.506329)
CORN_SYRUP = Ingredient(["corn syrup"], 1.3688)
MALT_EXTRACT = Ingredient(["malt extract", "malt syrup"], 1.403281939)
CHOCOLATE_70_PERCENT = Ingredient(["chocolate 70 percent", "dark chocolate"],
                                  0.731228)
VANILLA_EXTRACT = Ingredient(["vanilla extract"], 0.879165)
BLUEBERRIES = Ingredient(["blueberries"], 0.625559)
BAKING_SODA = Ingredient(["baking soda", "bicarbonate", "bicarbonate of soda"],
                         0.934112)
VEG_SHORTENING = Ingredient(["vegetable shortening", "crisco"], 0.87)
BAKING_POWDER = Ingredient(["baking powder"], 0.934112)
BUTTER_MILK = Ingredient(["buttermilk", "butter milk"], 1.035554)
CARDAMOM = Ingredient(["ground cardamom", "cardamom"], 0.39)
CARDAMOM_SEED = Ingredient(["cardamom seed", "cardamom seeds"], 0.6509)
CINNAMON = Ingredient(["ground cinnamon", "cinnamon"], 0.53)
CLOVES = Ingredient(["ground cloves", "cloves"], 0.53)
GINGER = Ingredient(["ground ginger"], 0.53)
ALL_SPICE = Ingredient(["all spice"], 0.53)
NUTMEG = Ingredient(["ground nutmeg", "nutmeg"], 0.47)
RICE = Ingredient(["rice"], 0.801688)
YEAST = Ingredient(["fresh yeast"], 1.0354)

# Milliliter conversion for potatoes assumes shredded (i.e grated) potatoes
POTATO = Ingredient(["potato", "potatoes", "shredded potato", "grated potato"],
      1.3796292, {"medium":184, "large":283, "large baking":340,
                  "medium baking":283, "small baking":226}, "medium")

ONION = Ingredient(["onion", "onions"], 1.04, {"large":340, "medium":227,
      "small":113}, "medium")
