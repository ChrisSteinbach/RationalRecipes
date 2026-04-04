#!/usr/bin/env python3
"""Export curated recipe data from sample_input CSVs to the artifact JSON.

Runs the statistics pipeline on each configured recipe and writes a
CuratedRecipeCatalog JSON file matching schema/curated_recipes.schema.json.

Usage:
    python3 scripts/export_curated_recipes.py
    python3 scripts/export_curated_recipes.py --output path/to/out.json
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from rational_recipes.ingredient import Ingredient
from rational_recipes.statistics import (
    Statistics,
    calculate_minimum_sample_sizes,
)
from rational_recipes.utils import get_ratio_and_stats

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT = REPO_ROOT / "artifacts" / "curated_recipes.json"
SCHEMA_PATH = REPO_ROOT / "schema" / "curated_recipes.schema.json"

DESIRED_INTERVAL = 0.05  # 5% — used for min_sample_size calculation
CONFIDENCE_LEVEL = 0.95

# Each entry drives one recipe in the output catalog. CSV paths are relative
# to the repo root. `csv_files` may list multiple files to pool samples.
RECIPE_CONFIGS: list[dict[str, Any]] = [
    {
        "id": "swedish-pancakes",
        "title": "Swedish Pancakes (Pannkakor)",
        "category": "crepes",
        "description": (
            "Thin, eggy Scandinavian pancakes with a high milk-to-flour "
            "ratio. Served with jam and whipped cream."
        ),
        "csv_files": ["sample_input/crepes/swedish_recipe_pannkisar.csv"],
        "sources": [
            {
                "type": "text",
                "title": "Aggregated Swedish recipes",
                "ref": (
                    "Swedish pannkakor recipes collected from Swedish-language "
                    "cooking websites and cookbooks."
                ),
            }
        ],
    },
    {
        "id": "english-pannkakor",
        "title": "Pannkakor (English sources)",
        "category": "crepes",
        "description": (
            "Swedish-style thin pancakes as documented in English-language "
            "cooking sources."
        ),
        "csv_files": ["sample_input/crepes/english_recipe_pannkisar.csv"],
        "sources": [
            {
                "type": "text",
                "title": "Aggregated English-language pannkakor recipes",
                "ref": (
                    "Pannkakor recipes collected from English-language "
                    "cooking websites and cookbooks."
                ),
            }
        ],
    },
    {
        "id": "french-crepes",
        "title": "French Crêpes",
        "category": "crepes",
        "description": (
            "Classic French crêpes — thin pancakes made with flour, milk, "
            "eggs, and butter."
        ),
        "csv_files": ["sample_input/crepes/french_recipe_crepes.csv"],
        "sources": [
            {
                "type": "text",
                "title": "Aggregated French recipes",
                "ref": (
                    "Crêpe recipes collected from French-language cooking "
                    "websites and cookbooks."
                ),
            }
        ],
    },
    {
        "id": "english-crepes",
        "title": "English Crepes",
        "category": "crepes",
        "description": (
            "Crepes as documented in English-language cooking sources — "
            "typically a bit thicker than French crêpes."
        ),
        "csv_files": ["sample_input/crepes/english_recipe_crepes.csv"],
        "sources": [
            {
                "type": "text",
                "title": "Aggregated English-language crepe recipes",
                "ref": (
                    "Crepe recipes collected from English-language cooking "
                    "websites and cookbooks."
                ),
            }
        ],
    },
]


def _whole_unit_for(ingredient: Ingredient) -> dict[str, Any] | None:
    """Build the whole_unit field for an ingredient, or None if not applicable."""
    # The Ingredient class stores the default whole-unit name as a private
    # field; reach in directly since there's no public accessor yet.
    name = ingredient._default_wholeunit_weight  # noqa: SLF001
    grams = ingredient.default_wholeunit_weight()
    if name is None or grams is None:
        return None
    return {"name": name, "grams": round(float(grams), 4)}


def _density_for(ingredient: Ingredient) -> float | None:
    """Return density in g/ml, or None if only a default (1.0) was available."""
    if ingredient.density_source == "default":
        return None
    return round(float(ingredient.density), 4)


def _build_ingredient_stats(
    ingredients: tuple[Ingredient, ...],
    stats: Statistics,
) -> list[dict[str, Any]]:
    """Convert the pipeline's Statistics object into per-ingredient dicts
    shaped for the CuratedRecipeCatalog schema."""
    bakers = stats.bakers_percentage()
    min_sample_sizes = list(
        calculate_minimum_sample_sizes(
            stats.std_deviations, stats.means, DESIRED_INTERVAL
        )
    )

    # Means are in "grams per 100g of recipe", so dividing by 100 gives the
    # 0-1 proportion scale the schema expects. Stddev and CI half-widths
    # are in the same units, so they scale the same way.
    result: list[dict[str, Any]] = []
    for i, ing in enumerate(ingredients):
        proportion = stats.means[i] / 100.0
        half_width = stats.intervals[i] / 100.0
        # Clamp CI lower bound to 0 — the schema requires ci_lower >= 0,
        # and for very sparse ingredients the naive lower bound can dip
        # slightly negative.
        ci_lower = max(0.0, proportion - half_width)
        ci_upper = proportion + half_width
        result.append(
            {
                "name": ing.name(),
                "ratio": round(bakers[i], 4),
                "proportion": round(proportion, 4),
                "std_deviation": round(stats.std_deviations[i] / 100.0, 4),
                "ci_lower": round(ci_lower, 4),
                "ci_upper": round(ci_upper, 4),
                "min_sample_size": int(min_sample_sizes[i]),
                "density_g_per_ml": _density_for(ing),
                "whole_unit": _whole_unit_for(ing),
            }
        )
    return result


def _build_recipe(config: dict[str, Any]) -> dict[str, Any]:
    """Run the pipeline for one recipe config and build its JSON dict."""
    csv_paths = [str(REPO_ROOT / f) for f in config["csv_files"]]
    ingredients, _ratio, stats, sample_size = get_ratio_and_stats(
        csv_paths, distinct=True, merge=[], zero_columns=None
    )
    base_name = ingredients[0].name()
    recipe: dict[str, Any] = {
        "id": config["id"],
        "title": config["title"],
        "category": config["category"],
    }
    if "description" in config:
        recipe["description"] = config["description"]
    recipe.update(
        {
            "base_ingredient": base_name,
            "sample_size": sample_size,
            "confidence_level": CONFIDENCE_LEVEL,
            "ingredients": _build_ingredient_stats(ingredients, stats),
            "sources": config["sources"],
        }
    )
    return recipe


def _validate(catalog: dict[str, Any]) -> None:
    """Validate the catalog against the JSON schema if jsonschema is available."""
    try:
        import jsonschema
    except ImportError:
        print("jsonschema not installed — skipping validation")
        return
    schema = json.loads(SCHEMA_PATH.read_text())
    jsonschema.validate(catalog, schema)
    print(f"Validated {len(catalog['recipes'])} recipes against schema")


def build_catalog() -> dict[str, Any]:
    """Run the pipeline for every configured recipe and return the catalog."""
    return {
        "version": 1,
        "recipes": [_build_recipe(cfg) for cfg in RECIPE_CONFIGS],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"output path (default: {DEFAULT_OUTPUT.relative_to(REPO_ROOT)})",
    )
    args = parser.parse_args()

    catalog = build_catalog()
    _validate(catalog)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(catalog, indent=2, ensure_ascii=False) + "\n")
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
