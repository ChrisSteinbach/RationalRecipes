"""Export the v1 catalog from recipes.db to a static JSON manifest.

Reads `output/catalog/recipes.db`, applies the v1 filter
``WHERE n_recipes >= MIN_RECIPES AND (review_status IS NULL OR
review_status != 'drop')``, and writes a JSON file matching the
``Catalog`` TypeScript type the PWA already declares
(``web/src/catalog.ts``). The shape is identical to what
``CatalogRepo.toCatalog()`` produced under the previous sql.js path,
so no PWA-side schema changes are needed.

Default ``--min-recipes`` is 100 (the v1 cut at ~hundreds of variants).
``rebuild-catalog.sh --smoke`` overrides this to 1 so the smoke run's
small variant population still ends up in the output.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from rational_recipes.catalog_db import (
    CatalogDB,
    IngredientStatsRow,
    ListFilters,
    VariantRow,
    VariantSourceRow,
)


def _ingredient_to_dict(stat: IngredientStatsRow) -> dict[str, Any]:
    """Render one ingredient row into the CatalogIngredient JSON shape."""
    mean = stat.mean_proportion
    out: dict[str, Any] = {
        "name": stat.canonical_name,
        "ratio": stat.ratio if stat.ratio is not None else 0.0,
        "proportion": mean,
        "std_deviation": stat.stddev if stat.stddev is not None else 0.0,
        "ci_lower": stat.ci_lower if stat.ci_lower is not None else mean,
        "ci_upper": stat.ci_upper if stat.ci_upper is not None else mean,
        "min_sample_size": stat.min_sample_size,
        "density_g_per_ml": stat.density_g_per_ml,
    }
    if stat.whole_unit_name is not None and stat.whole_unit_grams is not None:
        out["whole_unit"] = {
            "name": stat.whole_unit_name,
            "grams": stat.whole_unit_grams,
        }
    else:
        out["whole_unit"] = None
    return out


def _source_to_dict(source: VariantSourceRow) -> dict[str, Any]:
    """Render one source row into the CatalogSource JSON shape."""
    out: dict[str, Any] = {"type": source.source_type, "ref": source.ref}
    if source.title:
        out["title"] = source.title
    return out


def _variant_to_recipe(
    db: CatalogDB,
    variant: VariantRow,
) -> dict[str, Any]:
    """Hydrate one variant into the CuratedRecipe JSON shape."""
    stats = db.get_ingredient_stats(variant.variant_id)
    sources = db.get_variant_sources(variant.variant_id)

    base_ingredient = variant.base_ingredient
    if not base_ingredient and stats:
        base_ingredient = stats[0].canonical_name

    recipe: dict[str, Any] = {
        "id": variant.normalized_title,
        "title": variant.display_title or variant.normalized_title,
        "category": variant.category or "uncategorized",
        "base_ingredient": base_ingredient or "",
        "sample_size": variant.n_recipes,
        "ingredients": [_ingredient_to_dict(s) for s in stats],
    }
    if variant.description:
        recipe["description"] = variant.description
    if variant.confidence_level is not None:
        recipe["confidence_level"] = variant.confidence_level
    if sources:
        recipe["sources"] = [_source_to_dict(s) for s in sources]
    return recipe


def export(
    db_path: Path,
    out_path: Path,
    *,
    min_recipes: int = 100,
    indent: int | None = None,
) -> int:
    """Read variants from ``db_path`` and write the JSON manifest.

    Returns the number of variants written. An empty result is fine —
    the smoke path expects to potentially produce zero variants when
    its filter is narrow.
    """
    db = CatalogDB.open(db_path)
    try:
        filters = ListFilters(min_sample_size=min_recipes)
        variants = db.list_variants(filters)
        recipes = [_variant_to_recipe(db, v) for v in variants]
    finally:
        db.close()

    catalog: dict[str, Any] = {"version": 1, "recipes": recipes}
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(catalog, indent=indent, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return len(recipes)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        type=Path,
        default=Path("output/catalog/recipes.db"),
        help="Path to the source recipes.db (default: output/catalog/recipes.db)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("output/catalog/catalog.json"),
        help="Path to write catalog.json (default: output/catalog/catalog.json)",
    )
    parser.add_argument(
        "--min-recipes",
        type=int,
        default=100,
        help=(
            "Minimum n_recipes for a variant to ship in the catalog. "
            "Default 100 (v1 cut). Smoke runs override to 1."
        ),
    )
    parser.add_argument(
        "--indent",
        type=int,
        default=None,
        help="Pretty-print JSON with this indent. Default: compact (no indent).",
    )
    args = parser.parse_args(argv)

    if not args.db.exists():
        print(f"Source DB not found: {args.db}")
        return 1

    n = export(
        args.db,
        args.output,
        min_recipes=args.min_recipes,
        indent=args.indent,
    )
    print(f"Wrote {n} variant(s) (min_recipes={args.min_recipes}) → {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
