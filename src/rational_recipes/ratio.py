"""Classes and functions for calculating and presenting the mean recipe ratio
and related information and statistics: ingredient proportions for recipe of
a given total weight, confidence intervals and more.
"""

from collections.abc import Generator

from rational_recipes.columns import ColumnTranslator
from rational_recipes.ingredient import Ingredient


class RatioElement:
    """Formats an ingredient proportion for output"""

    def __init__(
        self, value: float, ingredient: Ingredient, settings: dict[str, str]
    ) -> None:
        self._value: float = float(value)
        self._ingredient = ingredient
        self._settings = settings

    def _float_format(self) -> str:
        """Return float output format set for ratio"""
        return self._settings["float_format"]

    def _describe_grams_and_milliliters(self, scale: float) -> str:
        """Describe an ingredient proportion in grams and milliliters"""
        value = self.value(scale)
        ingredient = self._ingredient
        grams = self._float_format() % value
        milliliters = self._float_format() % ingredient.grams2milliliters(value)
        return grams + "g or " + milliliters + f"ml {ingredient.name()}"

    def _format_number(self, number: float) -> str:
        """Format float according to precision setting"""
        return self._float_format() % number

    def _describe_wholeunits(self, scale: float) -> str:
        """Describe an ingredient proportion in grams, milliliters and whole
        units"""
        value = self.value(scale)
        ingredient = self._ingredient
        template = "%sg, %sml or %s %s(s) where each %s is %sg"
        wholeunits_value = ingredient.grams2wholeunits(value)
        assert wholeunits_value is not None
        wholeunits = self._format_number(wholeunits_value)
        default_weight = ingredient.default_wholeunit_weight()
        assert default_weight is not None
        grams_per_wholeunit = self._format_number(default_weight)
        name = ingredient.name()
        grams = self._format_number(value)
        milliliters = self._format_number(ingredient.grams2milliliters(value))
        return template % (
            grams,
            milliliters,
            wholeunits,
            name,
            name,
            grams_per_wholeunit,
        )

    def __str__(self) -> str:
        """Return the value as a formatted string"""
        return self._format_number(self._value)

    def value(self, scale: float = 1) -> float:
        """Scaled value"""
        return self._value * scale

    def describe(self, scale: float) -> str:
        """Describe an ingredient proportion"""
        if self._ingredient.default_wholeunit_weight() is None:
            return self._describe_grams_and_milliliters(scale)
        else:
            return self._describe_wholeunits(scale)


class Ratio:
    """Provides formatting for ingredient ratios and related statistics"""

    def __init__(
        self, ingredients: tuple[Ingredient, ...], values: list[float]
    ) -> None:
        self.ingredients = ingredients
        self._settings: dict[str, str] = {}
        self.set_precision(2)
        self._restrictions: list[tuple[list[int], float]] = []
        self._column_translator = ColumnTranslator(self.ingredients)
        self._elements = [
            RatioElement(values[i], ingredients[i], self._settings)
            for i in range(len(values))
        ]

    def _column_id_to_indexes(self, column_identifier: str | int) -> list[int]:
        """Normalize column identifier to a column index"""
        return self._column_translator.id_to_indexes(column_identifier)

    def _values(self, scale: float = 1) -> Generator[float, None, None]:
        """Return raw ratio values"""
        for element in self._elements:
            yield element.value(scale)

    def _restrict_total_weight(self, weight: float) -> float:
        """Yield ratio proportions with specific total weight. Returns scale
        applied."""
        total_grams = sum(self._values())
        return weight / float(total_grams)

    def _restrict_by_ingredient(self, scale: float) -> float:
        """Restrict a recipe based on individual ingredient/weight-limit
        specifications"""
        for column_indexes, weight_limit in self._restrictions:
            scaled_weight = sum(
                self._elements[index].value(scale) for index in column_indexes
            )
            unscaled_weight = sum(
                self._elements[index].value() for index in column_indexes
            )
            if scaled_weight > weight_limit:
                new_scale = weight_limit / unscaled_weight
                if new_scale < scale:
                    scale = new_scale
        return scale

    def set_restrictions(self, restrictions: list[tuple[str | int, float]]) -> None:
        """Individual ingredient weight restrictions"""
        _restrictions: list[tuple[list[int], float]] = []
        for column_id, weight in restrictions:
            indexes = self._column_id_to_indexes(column_id)
            _restrictions.append((indexes, weight))
        self._restrictions = _restrictions

    def len(self) -> int:
        """Return number of ratio elements"""
        return len(self._elements)

    def set_precision(self, precision: int) -> None:
        """Set precision (i.e. number of digits shown after decimal point)
        for floating point percentages."""
        self._settings["float_format"] = f"%1.{precision}f"

    def list_ingredients(self) -> str:
        """List the ingredients in the same order as they will appear in the
        ratio."""
        return " (" + ":".join(str(c) for c in self.ingredients) + ")"

    def __str__(self) -> str:
        return (
            ":".join(str(element) for element in self._elements)
        ) + self.list_ingredients()

    def describe_ingredient(self, column_id: str | int) -> str:
        """Describe individual ingredients"""
        return "\n".join(
            self._elements[index].describe(scale=1)
            for index in self._column_id_to_indexes(column_id)
        )

    def recipe(self, weight: float) -> tuple[float, str]:
        """Format the ingredient proportions as if for a recipe ingredient
        list. Also return total weight."""
        scale = self._restrict_total_weight(weight)
        scale = self._restrict_by_ingredient(scale)
        total_weight = sum(self._values(scale))
        return total_weight, "\n".join(
            element.describe(scale) for element in self._elements
        )

    def as_percentages(self) -> list[float]:
        """Return ratio values as percentages"""
        scale = self._restrict_total_weight(100)
        return list(self._values(scale))
