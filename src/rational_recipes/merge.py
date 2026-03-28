"""Merge ingredient measures.

Sometimes it's interesting to analyze ingredient ratios with alternative
ingredients, either because,

 - It is difficult to identify a 'core' set of ingredients
 - A particular substitution occurs regularly
 - We want to learn how often a substitution is made (perhaps with a view to
   mixing ingredients, e.g. water and milk in pancake batter)

When we do this we might want to combine column data so that, say all liquid
ingredient appears as one column. Or we may want the fat content to be combined
with one column and the remainder with another.

Equally, when a single ingredient appears multiple times in a recipe it is
useful to combine the columns so that we can measure out the total amount of
that ingredient at the start of cooking.

This module allows column merging for these situations.
"""

from collections.abc import Callable, Generator, Iterable, Sequence
from typing import TypeVar

from rational_recipes.columns import ColumnTranslator
from rational_recipes.errors import InvalidArgumentException
from rational_recipes.ingredient import Ingredient

_T = TypeVar("_T")


class MergeConfigError(InvalidArgumentException):
    """Exception triggered by invalid merge configuration or input argument"""

    pass


class Merge:
    """Responsible for applying a column merge specification to return a new
    set of merged columns.
    """

    def _convert_spec_to_indexes(
        self, merge_specification: list[list[tuple[str | int, float]]]
    ) -> list[list[tuple[int, float]]]:
        """Normalize merge specification so that it only uses column indexes
        and not mixed indexes and ingredient names."""
        new_merge_specification: list[list[tuple[int, float]]] = []
        for combine_spec in merge_specification:
            new_combine_spec: list[tuple[int, float]] = []
            for column_identifier, percentage in combine_spec:
                for column_index in self.column_translator.id_to_indexes(
                    column_identifier
                ):
                    new_combine_spec.append((column_index, percentage))
            new_merge_specification.append(new_combine_spec)
        return new_merge_specification

    def map_column_indexes(
        self,
        merge_specification: list[list[tuple[int, float]]],
        ingredients: tuple[Ingredient, ...],
    ) -> None:
        """Map column indexes to combination of columns to merge. For
        columns that will be removed, the column index maps to None. Column
        combinations are a list of tuples where each tuple has two elements:
        the column index to merge followed by the percentage of that column's
        value to add."""
        last_column = len(ingredients) - 1
        accumulating: dict[int, list[tuple[int, float]]] = {}
        remove: set[int] = set()
        # default behavior, no column merge
        for column_index in range(0, last_column + 1):
            self.column_index_to_columns[column_index] = [(column_index, 1.0)]

        for columns in merge_specification:
            accumulating_column = columns[0][0]
            if accumulating_column > last_column or accumulating_column < 0:
                raise MergeConfigError(
                    f"Attempted to merge missing column {accumulating_column}"
                )
            # specifies which columns should be merged into this one
            accumulating[accumulating_column] = columns
            for column_index, _ in columns[1:]:
                column_index = column_index
                if column_index > last_column or column_index < 0:
                    raise MergeConfigError(
                        f"Attempted to merge missing column {column_index}"
                    )
                # drop this column; it will be merged into another
                remove.add(column_index)

        # drop columns first so that any columns both specified as
        # accumulating *and* merged columns do not get dropped
        for column_index in remove:
            self.column_index_to_columns[column_index] = None

        for column_index, columns in accumulating.items():
            self.column_index_to_columns[column_index] = columns

    def __init__(
        self,
        merge_specification: list[list[tuple[str | int, float]]],
        ingredients: tuple[Ingredient, ...],
    ) -> None:
        self.column_translator = ColumnTranslator(ingredients)
        self.column_index_to_columns: dict[int, list[tuple[int, float]] | None] = {}
        merge_specification_indexed = self._convert_spec_to_indexes(merge_specification)
        self.map_column_indexes(merge_specification_indexed, ingredients)

    def merge_one_row(
        self,
        row: Sequence[_T],
        combine: Callable[[Sequence[_T], list[tuple[int, float]]], _T],
    ) -> Generator[_T, None, None]:
        """Yield a new row by combining columns"""
        for index in range(0, len(row)):
            columns_to_combine = self.column_index_to_columns[index]
            if columns_to_combine is not None:
                yield combine(row, columns_to_combine)

    def merge_rows(
        self, rows: Iterable[Sequence[float]]
    ) -> Generator[tuple[float, ...], None, None]:
        """Merge all rows of measurements"""
        for row in rows:
            yield tuple(self.merge_one_row(row, combine_measurements))

    def merge_ingredients(
        self, ingredients: tuple[Ingredient, ...]
    ) -> tuple[Ingredient, ...]:
        """Merge ingredients"""
        return tuple(self.merge_one_row(ingredients, combine_ingredients))


def combine_measurements(
    row: Sequence[float], columns_to_combine: list[tuple[int, float]]
) -> float:
    """Combine columns in a row of measurements in grams according to
    specification."""
    return sum(
        row[column_index] * percentage
        for column_index, percentage in columns_to_combine
    )


def combine_ingredients(
    ingredients: Sequence[Ingredient], columns_to_combine: list[tuple[int, float]]
) -> Ingredient:
    """Combine columns in a row of ingredients according to specification."""
    return ingredients[columns_to_combine[0][0]]


def merge_columns(
    ingredients: tuple[Ingredient, ...],
    rows: list[tuple[float, ...]],
    merge: list[list[tuple[str | int, float]]] | None = None,
) -> tuple[tuple[Ingredient, ...], list[tuple[float, ...]]]:
    """Merge columns of input data according to specification."""
    if merge is None or len(merge) == 0:
        return ingredients, rows
    merger = Merge(merge, ingredients)
    new_rows = list(merger.merge_rows(rows))
    new_ingredients = merger.merge_ingredients(ingredients)
    return new_ingredients, new_rows
