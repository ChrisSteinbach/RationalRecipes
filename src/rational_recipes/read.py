"""Read and parse input files"""

import re

from rational_recipes.errors import InvalidInputException
from rational_recipes.ingredient import Factory as IngredientFactory
from rational_recipes.units import GRAM
from rational_recipes.units import Factory as UnitFactory


def read_ingredients_from_header(header):
    """The header line of each input file is a comma separated string of
    ingredient names. Each ingredient represents a column in the input file.
    """
    try:
        return tuple(
            IngredientFactory.get_by_name(ingredient.strip())
            for ingredient in header.split(",")
        )
    except KeyError as error:
        raise InvalidInputException(
            f"No such ingredient as {str(error)}, line 1"
        ) from error


def split_header_and_rows(file_contents):
    """Split file_contents file into header and rows"""
    lines = [
        line.strip() for line in file_contents.splitlines() if len(line.strip()) > 0
    ]
    return (lines[0], lines[1:])


def ingredient_measures_from_row(line_nr, row, nr_columns):
    """Parse measures from one row"""
    measures = row.split(",")
    if len(measures) != nr_columns:
        raise InvalidInputException(
            f"The row on line {line_nr} has {len(measures)} columns"
            f" where {nr_columns} were expected"
        )
    for column_index in range(0, nr_columns):
        yield column_index, measures[column_index].strip()


MEASURE_PATTERN = re.compile(
    r"((?P<value>([0-9]+)|([0-9]*\.[0-9]+)) "
    "*(?P<unit>[a-zA-Z][a-zA-Z ]*))|(?P<zero>0)"
)


def value_and_unit(line_nr, column_index, measure):
    """Parse measure value and unit. The general form is a number followed by
    the unit, for example "1g" will be read as 1 gram. Any unit synonym
    may be used, so "1gram" or "1grams" will have the same parse result.
    A space may be used between value and unit, or not. A single zero, '0',
    without a unit specified will parse to zero grams.
    """
    match = MEASURE_PATTERN.match(measure)
    if match is not None:
        if match.group("zero") == "0":
            return 0, GRAM
        value = float(match.group("value"))
        unit = UnitFactory.get_by_name(match.group("unit").strip())
        if unit is None:
            raise InvalidInputException(
                f"No unit named '{match.group('unit')}' at line"
                f" {line_nr}, column {column_index}"
            )
        return value, unit
    else:
        raise InvalidInputException(
            f"Incorrect format of measurement at line {line_nr}, column {column_index}"
        )


def read_files(input_files):
    """Read one or more input files. For multiple files, columns
    (i.e. ingredients) must be identical between files. Rows from
    multiple files will be concatenated.
    """
    all_columns = []
    ingredients = None
    for input_file in input_files:
        file_contents = input_file.read()
        input_file.close()
        tmp_ingredients, columns = parse_file_contents(file_contents)
        if len(all_columns) == 0:
            ingredients = tmp_ingredients
        else:
            if ingredients != tmp_ingredients:
                raise InvalidInputException(
                    "All input files must have the same header."
                )
        all_columns += list(columns)
    return ingredients, all_columns


def read_rows(rows, nr_columns):
    """Parse the rows of ingredient measurements from one file"""
    line_nr = 2
    for row in rows:
        new_row = [
            value_and_unit(line_nr, column_index, measure)
            for column_index, measure in ingredient_measures_from_row(
                line_nr, row, nr_columns
            )
        ]
        yield new_row
        line_nr += 1


def parse_file_contents(file_contents):
    """Parse the contents of one file returning the ingredients and rows
    of measurements.
    """
    header, rows = split_header_and_rows(file_contents)
    ingredients = read_ingredients_from_header(header)
    new_rows = read_rows(rows, len(ingredients))
    return ingredients, new_rows
