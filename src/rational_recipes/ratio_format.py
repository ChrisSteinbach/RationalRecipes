"""Formatting layer for ratio data.

Translates numeric ratio/recipe data into human-readable text output.
"""

from rational_recipes.ingredient import Ingredient
from rational_recipes.ratio import Ratio


class RatioFormatter:
    """Formats Ratio data for text output."""

    def __init__(self, precision: int = 2) -> None:
        self._float_format = f"%1.{precision}f"

    def set_precision(self, precision: int) -> None:
        """Set number of digits shown after decimal point."""
        self._float_format = f"%1.{precision}f"

    def _format_number(self, number: float) -> str:
        return self._float_format % number

    def format_ratio(self, ratio: Ratio) -> str:
        """Format ratio as '1.00:2.00:3.00 (flour:egg:butter)'."""
        values_str = ":".join(self._format_number(v) for v in ratio.values())
        ingredients_str = ":".join(str(c) for c in ratio.ingredients)
        return values_str + " (" + ingredients_str + ")"

    def format_recipe(self, ratio: Ratio, weight: float) -> tuple[float, str]:
        """Format recipe ingredient list. Returns (total_weight, text)."""
        total_weight, scaled_values = ratio.recipe(weight)
        lines = [
            self._describe_ingredient_value(grams, ingredient)
            for grams, ingredient in zip(scaled_values, ratio.ingredients, strict=False)
        ]
        return total_weight, "\n".join(lines)

    def format_ingredient(self, ratio: Ratio, column_id: str | int) -> str:
        """Format description of ingredients matching column_id."""
        return "\n".join(
            self._describe_ingredient_value(grams, ingredient)
            for grams, ingredient in ratio.ingredient_values(column_id)
        )

    def _describe_ingredient_value(self, grams: float, ingredient: Ingredient) -> str:
        if ingredient.default_wholeunit_weight() is None:
            return self._describe_grams_and_milliliters(grams, ingredient)
        else:
            return self._describe_wholeunits(grams, ingredient)

    def _describe_grams_and_milliliters(
        self, grams: float, ingredient: Ingredient
    ) -> str:
        grams_str = self._format_number(grams)
        ml_str = self._format_number(ingredient.grams2milliliters(grams))
        return f"{grams_str}g or {ml_str}ml {ingredient.name()}"

    def _describe_wholeunits(self, grams: float, ingredient: Ingredient) -> str:
        template = "%sg, %sml or %s %s(s) where each %s is %sg"
        wholeunits_value = ingredient.grams2wholeunits(grams)
        assert wholeunits_value is not None
        default_weight = ingredient.default_wholeunit_weight()
        assert default_weight is not None
        name = ingredient.name()
        return template % (
            self._format_number(grams),
            self._format_number(ingredient.grams2milliliters(grams)),
            self._format_number(wholeunits_value),
            name,
            name,
            self._format_number(default_weight),
        )
