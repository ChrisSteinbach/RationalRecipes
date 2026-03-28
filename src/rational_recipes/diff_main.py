"""Compare recipe ratios showing percentage change or percentage difference"""

import sys
from dataclasses import dataclass

import rational_recipes.utils as utils
from rational_recipes.difference import percentage_change, percentage_difference
from rational_recipes.ingredient import Ingredient
from rational_recipes.output import Output
from rational_recipes.ratio import Ratio


@dataclass
class DiffResult:
    """Structured result from ratio comparison."""

    output: str
    ratio1_percentages: list[float]
    ratio2_percentages: list[float]
    ingredients: list[str]
    percentage_differences: list[tuple[float, str]]
    mean_difference: float
    percentage_changes: list[tuple[float, str]]

    def __str__(self) -> str:
        return self.output


def get_ratios_to_compare(
    first_filename: list[str],
    remaining_filenames: list[str],
    distinct: bool,
    merge: list[list[tuple[str | int, float]]],
) -> tuple[Ratio, Ratio]:
    """Get ratios to compare from input files"""
    ingredients1, ratio1 = utils.get_ratio(first_filename, distinct, merge)
    ingredients2, ratio2 = utils.get_ratio(remaining_filenames, distinct, merge)
    if ingredients1 != ingredients2:
        print("Ingredients for input files do not match: unable to compare")
        sys.exit(1)
    return ratio1, ratio2


class DiffMain:
    """Defines entry point and supporting methods for diff script"""

    def __init__(
        self,
        first_filename: list[str],
        remaining_filenames: list[str],
        distinct: bool,
        merge: list[list[tuple[str | int, float]]],
    ) -> None:
        self.number_template = "%%0.%df"
        self.ratio1, self.ratio2 = get_ratios_to_compare(
            first_filename, remaining_filenames, distinct, merge
        )

    def main(self, show_percentage_change: bool, precision: int) -> DiffResult:
        """Entry method for script"""
        self.number_template = f"%0.{precision}f"
        output = Output()
        self.print_ratios(output)
        diff_info = percentage_difference(self.ratio1, self.ratio2)
        mean_difference, differences = diff_info
        if show_percentage_change is False:
            self.print_percentage_difference(output, differences)
        else:
            self.print_percentage_change(output)
        self.print_overall_percentage_diff(output, mean_difference)
        changes = percentage_change(self.ratio1, self.ratio2)
        return DiffResult(
            output=str(output),
            ratio1_percentages=self.ratio1.as_percentages(),
            ratio2_percentages=self.ratio2.as_percentages(),
            ingredients=[str(i) for i in self.ratio1.ingredients],
            percentage_differences=[(d, str(i)) for d, i in differences],
            mean_difference=mean_difference,
            percentage_changes=[(c, str(i)) for c, i in changes],
        )

    def print_overall_percentage_diff(
        self, output: Output, mean_difference: float
    ) -> None:
        """Print overall percentage difference between ratios. This is
        calculated as the mean value of the percentage change for all
        ingredients"""
        output.line()
        output.line(
            ("Overall percentage difference = " + self.number_template + "%%")
            % (mean_difference * 100)
        )
        output.line()

    def print_percentage_change(self, output: Output) -> None:
        """Print percentage change between ratios for each ingredient"""
        changes = percentage_change(self.ratio1, self.ratio2)
        for change, ingredient in sorted(
            changes, key=lambda x: abs(x[0]), reverse=True
        ):
            direction = "increased"
            if change < 0.0:
                change = abs(change)
                direction = "decreased"
            output.line(
                (
                    "The %s proportion has %s by "
                    + self.number_template
                    + "%% from data set 1 to 2"
                )
                % (ingredient, direction, change * 100)
            )

    def print_percentage_difference(
        self,
        output: Output,
        differences: list[tuple[float, Ingredient]],
    ) -> None:
        """Print percentage difference between ratios for each ingredient"""
        for difference, ingredient in sorted(differences, reverse=True):
            output.line(
                (
                    "Percentage difference between %s proportions "
                    + self.number_template
                    + "%%"
                )
                % (ingredient, difference * 100)
            )

    def print_ratios(self, output: Output) -> None:
        """Print ratios to be compared"""
        output.line()
        output.line(f"Ratio for data set 1 in units of weight is {self.ratio1}")
        output.line(f"Ratio for data set 2 in units of weight is {self.ratio2}")
        output.line()
