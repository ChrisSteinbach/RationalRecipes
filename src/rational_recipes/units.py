"""Volume, weight and whole units of measure. Each unit is capable of conversion
to grams.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from rational_recipes.errors import InvalidInputException

if TYPE_CHECKING:
    from rational_recipes.ingredient import Ingredient


class Factory:
    """Registry and factory for units of measure.

    Each ``Factory()`` instance owns its own registry. Tests instantiate
    ``Factory()`` directly to register units in isolation. The module-level
    default registry is populated at import time with the built-in units
    (GRAM, CUP, etc.); the ``Factory.get_by_name(name)`` classmethod looks
    names up in that default registry, preserving the call-site shape that
    production code already uses (``UnitFactory.get_by_name(...)``).

    ``Unit.__init__`` does *not* auto-register. Callers register explicitly
    via ``factory.register(unit)``, which keeps test setup free of import-time
    side effects.
    """

    _default: Factory  # populated below class definition

    def __init__(self) -> None:
        self._units: dict[str, Unit] = {}

    def register(self, unit: Unit) -> None:
        """Register unit name and synonyms in this Factory's registry."""
        for name in unit.synonyms():
            self._units[name.lower().strip()] = unit

    def lookup(self, name: str) -> Unit | None:
        """Look up a unit by name in this Factory's registry."""
        return self._units.get(name.lower())

    @classmethod
    def get_by_name(cls, name: str) -> Unit | None:
        """Look up a unit by name in the module-level default registry."""
        return cls._default.lookup(name)


Factory._default = Factory()


class Unit:
    """Abstract unit of measure"""

    def __init__(self, names: list[str]) -> None:
        self._names = names

    def synonyms(self) -> list[str]:
        """List synonyms for a unit"""
        return self._names

    def norm(
        self, value: float, ingredient: Ingredient, line_nr: int | None = None
    ) -> float:
        """Normalize an ingredient measure to grams"""
        raise NotImplementedError("Unit.norm() must be implemented in derived class")


class WeightUnit(Unit):
    """Units of measure by weight"""

    def __init__(self, names: list[str], conversion: float) -> None:
        Unit.__init__(self, names)
        self._conversion = conversion

    def norm(
        self, value: float, ingredient: Ingredient, line_nr: int | None = None
    ) -> float:
        """Normalizes any weight unit to grams"""
        return value * self._conversion


class VolumeUnit(Unit):
    """Units of measure by volume"""

    def __init__(self, names: list[str], conversion: float) -> None:
        Unit.__init__(self, names)
        self._conversion = conversion

    def norm(
        self, value: float, ingredient: Ingredient, line_nr: int | None = None
    ) -> float:
        """Normalizes any volume unit to milliliters and then converts to grams"""
        milliliters = value * self._conversion
        return ingredient.milliliters2grams(milliliters)


class BadUnitException(InvalidInputException):
    """Thrown when a unit is specified that cannot be used"""

    pass


class WholeUnit(Unit):
    """Whole units of measure"""

    def __init__(self, sizes: list[str]) -> None:
        Unit.__init__(self, sizes)
        self._size = sizes[0]

    def norm(
        self, value: float, ingredient: Ingredient, line_nr: int | None = None
    ) -> float:
        """Normalizes sizeable food stuffs to grams"""
        conversion = ingredient.wholeunits2grams(self._size)
        if conversion is None:
            raise BadUnitException(
                f"Inapplicable unit '{self._size}' used for"
                f" ingredient '{ingredient.name()}' at line {line_nr}"
            )
        return value * conversion


def _builtin(unit: Unit) -> Unit:
    """Register a unit in the module-level default Factory and return it."""
    Factory._default.register(unit)
    return unit


GRAM = _builtin(WeightUnit(["gram", "grams", "g"], 1))
HG = _builtin(WeightUnit(["hg", "hectogram", "hectograms"], 100))
KG = _builtin(WeightUnit(["kg", "kilos", "kilograms"], 1000))
OZ = _builtin(WeightUnit(["oz", "ounces"], 28.3495231))
LB = _builtin(WeightUnit(["lbs", "lb", "pounds"], 453.592))

QUART = _builtin(VolumeUnit(["quart", "quarts"], 946.353))
US_PINT = _builtin(VolumeUnit(["US pint", "pint", "pints", "pt", "us_pint"], 473.176))
IMP_PINT = _builtin(VolumeUnit(["imperial pint", "uk pint", "imp_pint"], 568.261))
US_FLOZ = _builtin(
    VolumeUnit(
        ["US fluid ounce", "us_floz", "fluid ounce", "fl oz", "fl. oz"], 29.5735
    )
)
IMP_FLOZ = _builtin(
    VolumeUnit(["imperial fluid ounce", "uk fluid ounce", "impfloz"], 28.4131)
)
ML = _builtin(VolumeUnit(["ml", "milliliter", "milliliters"], 1))
KRM = _builtin(VolumeUnit(["krm"], 1))
CL = _builtin(VolumeUnit(["cl", "centiliter", "centiliters"], 10))
DL = _builtin(VolumeUnit(["dl", "deciliter", "deciliters"], 100))
LITER = _builtin(VolumeUnit(["l", "liter", "liters"], 1000))
# This cup size is taken from a bit of amateur market research where online
# measuring cup sizes were split between 30% legal size, 30% metric and 30%
# traditional. Taking into account figures from Wikipedia saying that the online
# market is 8% of all retail we get the ~238.33ml measure below since measuring
# cups bought in US shops most likely exclude metric cups.
CUP = _builtin(VolumeUnit(["c", "cups", "cup", "us cup", "us cups"], 238.337210755))
METRIC_CUP = _builtin(VolumeUnit(["metric cup", "metric cups"], 250))
METRIC_TBSP = _builtin(
    VolumeUnit(
        [
            "metric tbsp",
            "metric tb",
            "mtb",
            "metric tablespoon",
            "metric tablespoons",
            "msk",
            "cas",
        ],
        15.0,
    )
)
TBSP = _builtin(
    VolumeUnit(["tbsp", "tb", "tblsp", "tablespoon", "tablespoons"], 14.7868)
)
DESSERT_SPOON = DSP = DSTSPN = _builtin(
    VolumeUnit(["dessert spoon", "dsp", "dstspn"], 10.0)
)
TSP = _builtin(VolumeUnit(["tsp", "teaspoon", "teaspoons"], 4.92892))
METRIC_TSP = _builtin(
    VolumeUnit(
        ["metric tsp", "metric teaspoon", "metric teaspoons", "tsk", "mtsp", "cac"],
        5.0,
    )
)
PINCH = _builtin(VolumeUnit(["pinch", "pinches"], 0.3125))
DASH = _builtin(VolumeUnit(["dash", "dashes"], 0.625))

XL = _builtin(WholeUnit(["XL"]))
MEDIUM = _builtin(WholeUnit(["MEDIUM", "US MEDIUM"]))
EU_MEDIUM = _builtin(WholeUnit(["EU MEDIUM"]))
LARGE = _builtin(WholeUnit(["LARGE", "US LARGE"]))
EU_LARGE = _builtin(WholeUnit(["EU LARGE"]))
SMALL = _builtin(WholeUnit(["SMALL", "US SMALL"]))
EU_SMALL = _builtin(WholeUnit(["EU SMALL"]))
STICK = _builtin(WholeUnit(["stick", "sticks"]))
CUBE = _builtin(WholeUnit(["cube", "cubes"]))
KNOB = _builtin(WholeUnit(["knob", "knobs"]))
