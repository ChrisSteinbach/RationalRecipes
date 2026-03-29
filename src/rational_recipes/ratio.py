"""Data model for ingredient ratios.

Holds proportions, performs scaling and restriction math, and returns
numeric values. Presentation is handled by ratio_format.RatioFormatter.
"""

from rational_recipes.columns import ColumnTranslator
from rational_recipes.ingredient import Ingredient


class RatioElement:
    """Holds a single ingredient proportion value."""

    def __init__(self, value: float, ingredient: Ingredient) -> None:
        self._value: float = float(value)
        self._ingredient = ingredient

    @property
    def ingredient(self) -> Ingredient:
        return self._ingredient

    def value(self, scale: float = 1) -> float:
        """Scaled value"""
        return self._value * scale


class Ratio:
    """Pure data model for ingredient ratios.

    Holds proportions, does scaling math, handles restrictions, and
    returns numeric values.
    """

    def __init__(
        self, ingredients: tuple[Ingredient, ...], values: list[float]
    ) -> None:
        self.ingredients = ingredients
        self._restrictions: list[tuple[list[int], float]] = []
        self._column_translator = ColumnTranslator(self.ingredients)
        self._elements = [
            RatioElement(values[i], ingredients[i]) for i in range(len(values))
        ]

    def _column_id_to_indexes(self, column_identifier: str | int) -> list[int]:
        """Normalize column identifier to a column index"""
        return self._column_translator.id_to_indexes(column_identifier)

    def values(self, scale: float = 1) -> list[float]:
        """Return ratio values, optionally scaled."""
        return [element.value(scale) for element in self._elements]

    def _restrict_total_weight(self, weight: float) -> float:
        """Yield ratio proportions with specific total weight. Returns scale
        applied."""
        total_grams = sum(self.values())
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

    def recipe(self, weight: float) -> tuple[float, list[float]]:
        """Compute a scaled recipe. Returns (total_weight, per_ingredient_grams)."""
        scale = self._restrict_total_weight(weight)
        scale = self._restrict_by_ingredient(scale)
        scaled = self.values(scale)
        return sum(scaled), scaled

    def as_percentages(self) -> list[float]:
        """Return ratio values as percentages"""
        scale = self._restrict_total_weight(100)
        return self.values(scale)

    def ingredient_values(
        self, column_id: str | int, scale: float = 1
    ) -> list[tuple[float, Ingredient]]:
        """Return (grams, ingredient) pairs for the given column identifier."""
        return [
            (self._elements[index].value(scale), self._elements[index].ingredient)
            for index in self._column_id_to_indexes(column_id)
        ]
