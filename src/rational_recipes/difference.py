"""Functions for calculating difference between two ratios"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rational_recipes.ingredient import Ingredient
    from rational_recipes.ratio import Ratio


def diff(
    lhs: Ratio,
    rhs: Ratio,
    diff_func: Callable[[float, float], float],
) -> list[tuple[float, Ingredient]]:
    """Return the mean percentage difference and the percentage difference
    for individual ingredient proportions between two ratios."""
    ingredients = lhs.ingredients
    lhs_values = lhs.as_percentages()
    rhs_values = rhs.as_percentages()
    differences: list[tuple[float, Ingredient]] = []
    for i in range(0, len(lhs_values)):
        difference = diff_func(lhs_values[i], rhs_values[i])
        differences.append((difference, ingredients[i]))
    return differences


def percentage_difference(
    lhs: Ratio, rhs: Ratio
) -> tuple[float, list[tuple[float, Ingredient]]]:
    """Return the mean percentage difference and the percentage difference
    for individual ingredient proportions between two ratios."""
    differences = diff(lhs, rhs, calc_percentage_difference)
    total = sum(difference for difference, _ in differences)
    mean_difference = total / lhs.len()
    return mean_difference, differences


def percentage_change(lhs: Ratio, rhs: Ratio) -> list[tuple[float, Ingredient]]:
    """Calculate percentage difference between two percentages. Used for
    comparing two different ratios. Percentage difference calculated this way
    can exceed 100%."""
    return diff(lhs, rhs, calc_percentage_change)


def calc_percentage_change(src: float, dest: float) -> float:
    """Calculate percentage change from one value (src) to another (dest)"""
    return (dest - src) / src


def calc_percentage_difference(value1: float, value2: float) -> float:
    """Calculate percentage difference between two percentages. Used for
    comparing two different ratios. Percentage difference calculated this way
    can exceed 100%."""
    mean = (value1 + value2) / 2.0
    mean_diff = abs(value2 - value1)
    return mean_diff / mean


def percentage_difference_from_mean(value1: float, value2: float) -> float:
    """Calculate percentage difference between two percentages. Used to help
    make confidence interval sizes more intuitive by keeping the percentage
    difference under 100%"""
    mean = (value1 + value2) / 2.0
    mean_diff = abs(mean - value1)
    return mean_diff / mean
