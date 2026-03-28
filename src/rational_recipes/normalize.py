"""Normalize input data."""

from __future__ import annotations

from collections.abc import Generator, Iterable, Sequence
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rational_recipes.ingredient import Ingredient
    from rational_recipes.units import Unit


def to_grams(
    ingredients: tuple[Ingredient, ...],
    rows: Iterable[Iterable[tuple[float, Unit]]],
) -> Generator[tuple[float, ...], None, None]:
    """Normalize input ingredient measures to grams (from volume based and
    non-gram based weight units."""
    rows = rows
    line_nr = 2
    for row in rows:
        yield tuple(
            unit.norm(value, ingredient, line_nr)
            for (value, unit), ingredient in zip(row, ingredients, strict=False)
        )
        line_nr += 1


def normalize_to_100g(
    rows: Iterable[Sequence[float]],
) -> Generator[tuple[float, ...], None, None]:
    """Normalize to 100g recipe"""
    for row in rows:
        multiplier = 100 / float(sum(row))
        yield tuple(value * multiplier for value in row)
