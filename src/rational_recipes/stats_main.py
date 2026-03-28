"""Statistical analysis of multiple recipes of the same type."""

from dataclasses import dataclass

import rational_recipes.utils as utils
from rational_recipes.output import Output
from rational_recipes.statistics import calculate_minimum_sample_sizes


@dataclass
class StatsResult:
    """Structured result from statistical analysis."""

    output: str
    ratio_values: list[float]
    ingredients: list[str]
    proportions: list[float]
    intervals: list[tuple[float, float]]
    min_sample_sizes: list[int]
    recipe_weights: list[float]
    total_recipe_weight: float
    sample_size: int

    def __str__(self) -> str:
        return self.output


class StatsMain:
    """Defines entry point and supporting methods for stats script"""

    def __init__(
        self,
        filenames: list[str],
        distinct: bool,
        merge: list[list[tuple[str | int, float]]],
        zero_columns: list[str],
    ) -> None:
        self.distinct = distinct
        self.confidence: float = 0.05
        self.restrictions: list[tuple[str | int, float]] = []
        result = utils.get_ratio_and_stats(
            filenames, distinct, merge, zero_columns=zero_columns
        )
        _: object
        _, self.ratio, self.stats, self.sample_size = result

    def set_restrictions(self, restrictions: list[tuple[str | int, float]]) -> None:
        """Set per ingredient weight restrictions"""
        self.ratio.set_restrictions(restrictions)

    def set_desired_interval(self, interval: float) -> None:
        """Set desired confidence interval"""
        self.confidence = interval
        self.stats.set_desired_interval(interval)

    def main(
        self,
        ratio_precision: int,
        recipe_precision: int,
        total_recipe_weight: float,
        verbose: bool,
    ) -> StatsResult:
        """Entry method for script"""
        self.ratio.set_precision(ratio_precision)
        self.stats.set_precision(ratio_precision)
        output = Output()
        self.print_ratio(output)
        if verbose:
            self.print_confidence_intervals(output, self.confidence)
        self.print_recipe(output, recipe_precision, total_recipe_weight)
        self.print_footer(output)
        return self._build_result(str(output), total_recipe_weight)

    def _build_result(
        self, output_text: str, total_recipe_weight: float
    ) -> StatsResult:
        """Build structured result from computed data"""
        ingredients = [str(i) for i in self.ratio.ingredients]
        ratio_values = self.stats.bakers_percentage()
        total = sum(self.stats.means)
        proportions = [(m / total) * 100 for m in self.stats.means]
        intervals = [
            (p - iv, p + iv)
            for p, iv in zip(proportions, self.stats.intervals, strict=False)
        ]
        min_sample_sizes = list(
            calculate_minimum_sample_sizes(
                self.stats.std_deviations, self.stats.means, self.confidence
            )
        )
        weight, _ = self.ratio.recipe(total_recipe_weight)
        recipe_weights = [p / 100 * weight for p in proportions]
        return StatsResult(
            output=output_text,
            ratio_values=ratio_values,
            ingredients=ingredients,
            proportions=proportions,
            intervals=intervals,
            min_sample_sizes=min_sample_sizes,
            recipe_weights=recipe_weights,
            total_recipe_weight=weight,
            sample_size=self.sample_size,
        )

    def print_footer(self, output: Output) -> None:
        """Print note on sample data at end of input"""
        text = "recipe proportions. The data may contain duplicates."
        if self.distinct:
            text = "distinct recipe proportions. Duplicates have been removed."
        output.line(f"Note: these calculations are based on {self.sample_size} {text}")

    def print_recipe(
        self, output: Output, recipe_precision: int, total_recipe_weight: float
    ) -> None:
        """Print recipe with a specified total weight"""
        self.ratio.set_precision(recipe_precision)
        weight, text = self.ratio.recipe(total_recipe_weight)
        output.title(f"{weight:g}g Recipe")
        output.line(text)
        output.line()

    def print_ratio(self, output: Output) -> None:
        """Print calculated ingredient ratio"""
        output.line()
        output.line(f"Recipe ratio in units of weight is {self.ratio}")
        output.line()

    def print_confidence_intervals(self, output: Output, confidence: float) -> None:
        """Print confidence intervals for each ingredient proportion"""
        if self.sample_size < 2:
            output.line()
            output.line("Too little data available to provide statistics.")
            output.line()
            return
        output.title("Recipe ratio with confidence intervals (confidence level is 95%)")
        self.stats.print_confidence_intervals(output)
        output.line()
        output.title(
            f"Minimum sample sizes needed for confidence "
            f"interval with {int(confidence * 100)}% difference"
            f" and confidence level of 95%"
        )
        self.stats.print_min_sample_sizes(output)
        output.line()
