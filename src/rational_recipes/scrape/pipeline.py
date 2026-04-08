"""End-to-end pipeline: load corpus → group → parse → normalize → CSV.

This module wires the scraping stages together and produces CSV files
compatible with the existing rr-stats/rr-diff tools.
"""

from __future__ import annotations

import csv
import logging
from dataclasses import dataclass
from io import StringIO
from pathlib import Path

from rational_recipes.ingredient import Factory as IngredientFactory
from rational_recipes.scrape.grouping import (
    IngredientGroup,
    group_by_ingredients,
    group_by_title,
)
from rational_recipes.scrape.parse import ParsedIngredient, parse_ingredient_lines
from rational_recipes.scrape.recipenlg import Recipe, RecipeNLGLoader
from rational_recipes.units import Factory as UnitFactory

logger = logging.getLogger(__name__)


@dataclass
class ParsedRecipe:
    """A recipe with all ingredient lines parsed to structured fields."""

    source_recipe: Recipe
    parsed_ingredients: list[ParsedIngredient]

    @property
    def ingredient_names(self) -> list[str]:
        return [p.ingredient for p in self.parsed_ingredients]


@dataclass
class NormalizedRow:
    """One recipe's ingredients as value-unit strings for CSV output."""

    source_recipe: Recipe
    cells: dict[str, str]  # ingredient name → "value unit" string
    skipped_ingredients: list[str]  # ingredients that couldn't be resolved


@dataclass
class PipelineResult:
    """Result of running the pipeline on a group of recipes."""

    group_title: str
    ingredient_group: IngredientGroup
    normalized_rows: list[NormalizedRow]
    header_ingredients: list[str]
    parse_failures: int
    ingredient_db_misses: dict[str, int]  # unknown ingredient → count
    total_recipes_in: int

    @property
    def parse_success_rate(self) -> float:
        total = self.total_recipes_in
        return (total - self.parse_failures) / total if total > 0 else 0.0

    def to_csv(self) -> str:
        """Produce CSV compatible with rr-stats."""
        buf = StringIO()
        writer = csv.writer(buf)
        writer.writerow(self.header_ingredients)
        for row in self.normalized_rows:
            cells = []
            for ing in self.header_ingredients:
                cells.append(row.cells.get(ing, "0"))
            writer.writerow(cells)
        return buf.getvalue()

    def write_csv(self, path: Path) -> None:
        """Write CSV file."""
        path.write_text(self.to_csv(), encoding="utf-8")


def _resolve_unit_string(parsed: ParsedIngredient) -> str | None:
    """Convert a parsed ingredient to a 'value unit' string for CSV output.

    Returns None if the unit or ingredient can't be resolved in the
    existing registries.
    """
    unit_name = parsed.unit.strip()
    quantity = parsed.quantity

    # Check if the unit exists in our registry
    unit = UnitFactory.get_by_name(unit_name)
    if unit is not None:
        if quantity == 0:
            return "0"
        return f"{quantity:g} {unit_name}"

    # Common LLM output normalizations
    aliases = {
        "cup": "cup",
        "cups": "cup",
        "tablespoon": "tbsp",
        "tablespoons": "tbsp",
        "teaspoon": "tsp",
        "teaspoons": "tsp",
        "ounce": "oz",
        "ounces": "oz",
        "pound": "lb",
        "pounds": "lb",
    }
    resolved = aliases.get(unit_name.lower())
    if resolved and UnitFactory.get_by_name(resolved):
        if quantity == 0:
            return "0"
        return f"{quantity:g} {resolved}"

    return None


def normalize_recipe(
    recipe: Recipe,
    parsed_ingredients: list[ParsedIngredient],
) -> NormalizedRow:
    """Convert parsed ingredients to value-unit strings, tracking DB misses."""
    cells: dict[str, str] = {}
    skipped: list[str] = []

    for parsed in parsed_ingredients:
        # Check if ingredient is known to the DB
        try:
            IngredientFactory.get_by_name(parsed.ingredient)
        except KeyError:
            skipped.append(parsed.ingredient)
            continue

        unit_str = _resolve_unit_string(parsed)
        if unit_str is None:
            skipped.append(f"{parsed.ingredient} (unknown unit: {parsed.unit})")
            continue

        cells[parsed.ingredient] = unit_str

    return NormalizedRow(
        source_recipe=recipe,
        cells=cells,
        skipped_ingredients=skipped,
    )


def run_pipeline(
    corpus_path: Path,
    title_query: str,
    *,
    l1_min_group_size: int = 3,
    l2_similarity_threshold: float = 0.6,
    l2_min_group_size: int = 3,
    llm_model: str = "gemma4:e4b",
) -> list[PipelineResult]:
    """Run the full pipeline for a title query.

    1. Load recipes matching the title query from RecipeNLG
    2. Level 1: group by normalized title
    3. Level 2: cluster each L1 group by ingredient sets
    4. Parse ingredient lines via LLM
    5. Normalize to value-unit strings
    6. Return results (one per L2 group)
    """
    loader = RecipeNLGLoader(path=corpus_path)

    # Step 1: Load matching recipes
    logger.info("Searching for recipes matching %r...", title_query)
    matching = list(loader.search_title(title_query))
    logger.info("Found %d recipes matching %r", len(matching), title_query)

    if not matching:
        return []

    # Step 2: Level 1 grouping
    l1_groups = group_by_title(matching, min_group_size=l1_min_group_size)
    logger.info(
        "Level 1: %d title groups (min size %d)",
        len(l1_groups),
        l1_min_group_size,
    )

    results: list[PipelineResult] = []

    for title_key, group_recipes in l1_groups.items():
        # Step 3: Level 2 grouping
        l2_groups = group_by_ingredients(
            group_recipes,
            similarity_threshold=l2_similarity_threshold,
            min_group_size=l2_min_group_size,
        )
        logger.info(
            "  %r: %d recipes → %d L2 groups",
            title_key,
            len(group_recipes),
            len(l2_groups),
        )

        for l2_group in l2_groups:
            # Step 4: Parse ingredient lines
            parse_failures = 0
            parsed_recipes: list[tuple[Recipe, list[ParsedIngredient]]] = []
            db_misses: dict[str, int] = {}

            for recipe in l2_group.recipes:
                raw_parsed = parse_ingredient_lines(
                    list(recipe.ingredients), model=llm_model
                )
                valid = [p for p in raw_parsed if p is not None]
                if len(valid) < len(raw_parsed):
                    parse_failures += len(raw_parsed) - len(valid)
                if valid:
                    parsed_recipes.append((recipe, valid))

            # Step 5: Normalize
            # Determine common ingredients across this group
            all_ingredient_names: dict[str, int] = {}
            normalized_rows: list[NormalizedRow] = []

            for recipe, recipe_parsed in parsed_recipes:
                row = normalize_recipe(recipe, recipe_parsed)
                normalized_rows.append(row)
                for ing_name in row.cells:
                    all_ingredient_names[ing_name] = (
                        all_ingredient_names.get(ing_name, 0) + 1
                    )
                for skip in row.skipped_ingredients:
                    # Track the base ingredient name
                    base = skip.split(" (")[0]
                    db_misses[base] = db_misses.get(base, 0) + 1

            # Header: ingredients that appear in at least half the recipes
            min_appearance = max(1, len(normalized_rows) // 2)
            header = sorted(
                name
                for name, count in all_ingredient_names.items()
                if count >= min_appearance
            )

            if not header or not normalized_rows:
                continue

            # Filter rows: keep only rows that have at least half the header ingredients
            min_cols = max(1, len(header) // 2)
            good_rows = [
                r
                for r in normalized_rows
                if sum(1 for h in header if h in r.cells) >= min_cols
            ]

            results.append(
                PipelineResult(
                    group_title=title_key,
                    ingredient_group=l2_group,
                    normalized_rows=good_rows,
                    header_ingredients=header,
                    parse_failures=parse_failures,
                    ingredient_db_misses=db_misses,
                    total_recipes_in=len(l2_group.recipes),
                )
            )

    return results
