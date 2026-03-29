"""Functions for adding and parsing command line options"""

from optparse import OptionParser
from typing import TextIO

from rational_recipes.errors import InvalidInputException
from rational_recipes.ingredient import Ingredient
from rational_recipes.merge import merge_columns
from rational_recipes.normalize import to_grams
from rational_recipes.ratio import Ratio
from rational_recipes.read import read_files
from rational_recipes.statistics import Statistics, calculate_statistics


def get_ratio_and_stats(
    filenames: list[str],
    distinct: bool,
    merge: list[list[tuple[str | int, float]]],
    zero_columns: list[str] | None = None,
) -> tuple[tuple[Ingredient, ...], Ratio, Statistics, int]:
    """Parse input files to produce mean recipe ratio and related statistics"""
    files: list[TextIO] = [open(filename) for filename in filenames]
    ingredients, proportions = read_files(files)
    proportions_grams = list(to_grams(ingredients, proportions))
    if distinct:
        proportions_grams = list(set(proportions_grams))
    ingredients, proportions_merged = merge_columns(
        ingredients, proportions_grams, merge
    )
    statistics = calculate_statistics(proportions_merged, ingredients, zero_columns)
    ratio = Ratio(ingredients, statistics.bakers_percentage())
    return ingredients, ratio, statistics, len(proportions_merged)


def get_ratio(
    filenames: list[str],
    distinct: bool,
    merge: list[list[tuple[str | int, float]]],
) -> tuple[tuple[Ingredient, ...], Ratio]:
    """Parse input files to produce mean recipe ratio"""
    ingredients, ratio, _, _ = get_ratio_and_stats(filenames, distinct, merge)
    return ingredients, ratio


def add_merge_option(parser: OptionParser) -> None:
    """Add option used to specify column merge"""
    parser.add_option(
        "-m",
        "--merge",
        type="string",
        dest="merge",
        help="merge columns where MAPPING is <col>[.percent][+<col>[.percent]]"
        "[:<col>[.percent][+<col>[.percent]]...",
        default=None,
        metavar="MAPPING",
    )


def add_include_option(parser: OptionParser) -> None:
    """Add option to choose whether duplicate input rows are removed"""
    parser.add_option(
        "-i",
        "--include",
        action="store_false",
        dest="distinct",
        default=True,
        help="include duplicate proportions in ratio calculation",
    )


def parse_column_merge(
    merge_option: str | None,
) -> list[list[tuple[str | int, float]]]:
    """Parse specification of column merge"""
    merge: list[list[tuple[str | int, float]]] = []
    if merge_option is not None:
        for mappings in merge_option.split(":"):
            mapping: list[tuple[str | int, float]] = []
            for column_spec_str in mappings.split("+"):
                column_spec = column_spec_str.split(".")
                column_id: str | int = column_spec[0]
                percentage = 1.0
                if len(column_spec) == 2:
                    if not column_spec[1].isdigit():
                        raise InvalidInputException(
                            "Expected percentage after period in merge specification"
                        )
                    percentage = float("0." + column_spec[1])
                if column_spec[0].isdigit():
                    mapping.append((int(column_spec[0]), percentage))
                else:
                    mapping.append((column_id, percentage))
            merge.append(mapping)
    return merge


def parse_restrictions(
    options: str | None,
) -> list[tuple[str | int, float]]:
    """Parse specification of column merge"""
    column_options: list[tuple[str | int, float]] = []
    if options is not None and len(options) > 0:
        for mappings in options.split(","):
            column_spec = mappings.split("=")
            column_id_str = column_spec[0]
            if len(column_spec) != 2:
                raise InvalidInputException("Expected column option")
            weight = float(column_spec[1])
            if column_id_str.isdigit():
                column_options.append((int(column_id_str), weight))
            else:
                column_options.append((column_id_str, weight))
    return column_options
