"""Calculation and formatting of statistics"""

import math
from collections.abc import Generator, Sequence
from typing import Any

import numpy
import numpy.typing as npt

from rational_recipes.columns import ColumnTranslator
from rational_recipes.difference import percentage_difference_from_mean
from rational_recipes.ingredient import Ingredient
from rational_recipes.normalize import normalize_to_100g
from rational_recipes.output import Output

Z_VALUE = 1.96  # represents a confidence level of 95%


def calculate_minimum_sample_sizes(
    std_deviations: list[float], means: list[float], desired_interval: float
) -> Generator[int, None, None]:
    """Calculate minimum sample size needed for a confidence interval
    of 5% difference from the mean with 95% confidence level"""
    for std, mean in zip(std_deviations, means, strict=False):
        if mean == 0:
            yield 0
        else:
            yield math.ceil(((Z_VALUE * std) / (mean * desired_interval)) ** 2)


def calculate_confidence_intervals(
    data: Any, std_deviations: list[float]
) -> list[float]:
    """Calculate confidence intervals for each ingredient"""
    intervals: list[float] = []
    for column, std in zip(data, std_deviations, strict=False):
        sample_size = len(column)
        std_error = std / math.sqrt(sample_size)
        intervals.append(std_error * Z_VALUE)
    return intervals


def create_zero_filter(
    ingredients: tuple[Ingredient, ...], zero_columns: list[str]
) -> dict[int, bool]:
    """Convert column id list into specification for which columns should be
    filtered for zeros"""
    filter_map: dict[int, bool] = {}
    for i in range(len(ingredients)):
        filter_map[i] = False
    column_translator = ColumnTranslator(ingredients)
    for column_id in zero_columns:
        for index in column_translator.id_to_indexes(column_id):
            filter_map[index] = True
    return filter_map


def filter_zeros(
    data: npt.NDArray[numpy.floating[Any]], filter_map: dict[int, bool]
) -> list[Any]:
    """Filter zero values according to specification"""
    new_data: list[Any] = []
    for i in range(len(data)):
        column = data[i]
        if filter_map[i]:
            column = list(value for value in column if float(value) != 0.0)
            new_data.append(numpy.array(column))
        else:
            new_data.append(column)

    return new_data


def apply_defaults(
    data: Sequence[Sequence[float]],
    defaults: list[float],
    filter_map: dict[int, bool],
) -> list[list[float]]:
    """Apply default values to zero columns according to settings"""
    new_data: list[list[float]] = []
    total = sum(defaults)
    percentages = [default / total for default in defaults]
    col_range = range(len(data[0]))
    for original_row in data:
        row = list(original_row)
        for i in col_range:
            if filter_map[i] and row[i] == 0:
                row = [column - (column * percentages[i]) for column in row]
        for i in col_range:
            if filter_map[i] and row[i] == 0:
                row[i] = percentages[i] * 100
        new_data.append(row)
    return new_data


def calculate_variables(
    data: Any,
) -> tuple[list[float], list[float], list[float]]:
    """Calculate standard deviation, mean and confidence interval vectors"""
    std_deviations: list[float] = []
    means: list[float] = []
    for column in data:
        std_deviations.append(float(column.std()))
        means.append(float(column.mean()))
    intervals = calculate_confidence_intervals(data, std_deviations)
    return intervals, std_deviations, means


def filter_zero_columns(
    raw_data: list[tuple[float, ...]],
    ingredients: tuple[Ingredient, ...],
    zero_columns: list[str],
) -> list[list[float]]:
    """Filter zero values from specified columns and apply defaults.

    Normalizes data to 100g proportions, filters zeros from the specified
    columns, computes default values from the filtered data, and applies
    them back. Returns row-major data ready for further processing.
    """
    normalized = list(normalize_to_100g(raw_data))
    data = numpy.array(normalized).transpose()
    filter_map = create_zero_filter(ingredients, zero_columns)
    data_filtered = filter_zeros(data, filter_map)
    _, _, defaults = calculate_variables(data_filtered)
    return apply_defaults(normalized, defaults, filter_map)


def calculate_statistics(
    raw_data: Sequence[Sequence[float]],
    ingredients: tuple[Ingredient, ...],
    zero_columns: list[str] | None,
) -> "Statistics":
    """Calculate mean, confidence interval and minimum sample size for each
    ingredient.
    """
    processed: Sequence[Sequence[float]]
    if zero_columns is not None and len(zero_columns) > 0:
        processed = filter_zero_columns(
            [tuple(row) for row in raw_data],
            ingredients,
            zero_columns,
        )
    else:
        processed = raw_data
    normalized = list(normalize_to_100g(processed))
    data = numpy.array(normalized).transpose()
    intervals, std_deviations, means = calculate_variables(data)
    return Statistics(ingredients, intervals, std_deviations, means)


class Statistics:
    """Calculate statistics"""

    def __init__(
        self,
        ingredients: tuple[Ingredient, ...],
        intervals: list[float],
        std_deviations: list[float],
        means: list[float],
    ) -> None:
        self.ingredients = ingredients
        self.intervals = intervals
        self.std_deviations = std_deviations
        self.desired_interval: float = 0.05
        self.means = means
        self._precision: int = 2

    def _float_format(self) -> str:
        """String format for floats with correct precision"""
        return f"%1.{self._precision}f"

    def set_precision(self, precision: int) -> None:
        """Set precision (i.e. number of digits shown after decimal point)
        for floating point as_percentages."""
        self._precision = precision

    def set_desired_interval(self, desired_interval: float) -> None:
        """Set desired confidence interval"""
        self.desired_interval = desired_interval

    def bakers_percentage(self) -> list[float]:
        """Express mean values as bakers percentage"""
        return [mean / self.means[0] for mean in self.means]

    def print_min_sample_sizes(self, output: Output) -> None:
        """Print (pre-calculated) minimum samples size for each ingredient
        proportion mean"""
        min_sample_sizes = tuple(
            calculate_minimum_sample_sizes(
                self.std_deviations, self.means, self.desired_interval
            )
        )
        for i in range(0, len(self.means)):
            ingredient = str(self.ingredients[i])
            output.line(
                f"Minimum sample size for {ingredient}"
                f" proportion: {min_sample_sizes[i]}"
            )

    def _print_interval(
        self,
        output: Output,
        percentage: float,
        interval: float,
        ingredient: Ingredient,
    ) -> None:
        """Output confidence interval for one ingredient"""
        upper_value = percentage + interval
        upper = self._float_format() % upper_value
        lower_value = percentage - interval
        lower = self._float_format() % lower_value
        mean = self._float_format() % percentage
        text = "The " + str(ingredient) + " proportion "
        if interval == 0.0:
            difference = 0.0
        else:
            difference = percentage_difference_from_mean(lower_value, upper_value) * 100
        if difference > 0.01:
            diff_text = (
                f"% (the interval is {difference:0.0f}%"
                f" of the mean proportion: {mean}%)"
            )
            output.line(text + "is between " + lower + "% and " + upper + diff_text)
        else:
            output.line(text + lower)

    def print_confidence_intervals(self, output: Output) -> None:
        """Print confidence intervals for mean of each ingredient proportion"""
        total = sum(self.means)
        percentages = [(mean / total) * 100 for mean in self.means]
        for percentage, interval, ingredient in zip(
            percentages, self.intervals, self.ingredients, strict=False
        ):
            self._print_interval(output, percentage, interval, ingredient)
