"""CLI entry point for recipe diff."""

import sys
from optparse import OptionParser, Values

import rational_recipes.errors
import rational_recipes.utils as utils
from rational_recipes import DiffMain


def parse_command_line() -> tuple[
    list[str],
    list[str],
    Values,
    list[list[tuple[str | int, float]]],
]:
    """Parse command line arguments"""
    usage = "usage: %prog [options] csv-file1 csv-file2 [csv-file3]"
    parser = OptionParser(usage=usage)
    parser.add_option(
        "-p",
        "--precision",
        type="int",
        dest="precision",
        help="number of DIGITS to show after decimal "
        "point for percentage values (default is %default)",
        default=0,
        metavar="DIGITS",
    )
    utils.add_include_option(parser)
    parser.add_option(
        "-c",
        "--change",
        action="store_true",
        dest="show_percentage_change",
        default=False,
        help="show percentage change instead of percentage difference",
    )
    utils.add_merge_option(parser)
    options, args = parser.parse_args()
    merge = utils.parse_column_merge(options.merge)
    if len(args) < 2:
        parser.error("no input file provided")
    first_filename = args[0:1]
    remaining_filenames = args[1:]
    return first_filename, remaining_filenames, options, merge


def run() -> None:
    """Run the diff tool from the command line."""
    first_filename, remaining_filenames, options, merge = parse_command_line()
    try:
        script = DiffMain(first_filename, remaining_filenames, options.distinct, merge)
        print(script.main(options.show_percentage_change, options.precision))
    except rational_recipes.errors.InvalidInputException as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
