"""Build CuratedRecipeCatalog JSON from pipeline output.

The PWA (epic ``RationalRecipes-f85``) consumes this JSON contract —
per-recipe baker's percentages plus statistical metadata plus
ingredient-DB facts (density, whole-unit) needed to display alternative
units client-side. Schema lives at ``schema/curated_recipes.schema.json``.

Two entry points build catalogs from different sources:

- ``build_recipe_entry`` runs ``get_ratio_and_stats`` on a list of CSV
  paths. The hand-curated path (``scripts/export_curated_recipes.py``)
  uses this with a static per-recipe config.

- ``catalog_from_manifest`` reads a merged-pipeline ``manifest.json``,
  iterates per-variant CSVs, and emits one catalog entry per variant.
  This is the handoff between the collection epic
  (``RationalRecipes-b7t``) and the PWA epic (``RationalRecipes-f85``)
  — the bead ``5ub``.

The ``CuratedRecipeCatalog`` format intentionally stays JSON (not
SQLite) per the ``ntm`` decision: dataset size is small, JSON is native
to the PWA consumer, easy to version in git, no client-side query
layer required.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from rational_recipes.ingredient import Ingredient
from rational_recipes.scrape.manifest import Manifest, VariantManifestEntry
from rational_recipes.statistics import (
    Statistics,
    calculate_minimum_sample_sizes,
)
from rational_recipes.utils import get_ratio_and_stats

CATALOG_VERSION = 1
CONFIDENCE_LEVEL = 0.95
DESIRED_INTERVAL = 0.05  # 5% — used for min_sample_size calculation


def slugify(text: str) -> str:
    """URL-safe slug matching the schema's ``^[a-z0-9]+(-[a-z0-9]+)*$`` pattern.

    Spaces and non-alphanumerics collapse to dashes; empty result
    returns ``"recipe"`` so the constraint always satisfies.
    """
    s = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return s or "recipe"


def whole_unit_for(ingredient: Ingredient) -> dict[str, Any] | None:
    """Build the ``whole_unit`` field for an ingredient, or None if not applicable."""
    name = ingredient._default_wholeunit_weight  # noqa: SLF001
    grams = ingredient.default_wholeunit_weight()
    if name is None or grams is None:
        return None
    return {"name": name, "grams": round(float(grams), 4)}


def density_for(ingredient: Ingredient) -> float | None:
    """Density in g/ml, or None if only the default (1.0) was available."""
    if ingredient.density_source == "default":
        return None
    return round(float(ingredient.density), 4)


def build_ingredient_stats(
    ingredients: tuple[Ingredient, ...],
    stats: Statistics,
) -> list[dict[str, Any]]:
    """Per-ingredient dicts shaped for the CuratedRecipeCatalog schema."""
    bakers = stats.bakers_percentage()
    min_sample_sizes = list(
        calculate_minimum_sample_sizes(
            stats.std_deviations, stats.means, DESIRED_INTERVAL
        )
    )

    # Means are in "grams per 100g", so ÷100 gives the 0-1 proportion
    # scale the schema expects. Stddev and CI half-widths are in the
    # same units and scale the same way.
    result: list[dict[str, Any]] = []
    for i, ing in enumerate(ingredients):
        proportion = stats.means[i] / 100.0
        half_width = stats.intervals[i] / 100.0
        # Clamp CI lower bound to 0 — schema requires ci_lower >= 0, and
        # for sparse ingredients the naive lower bound can dip slightly
        # negative.
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
                "density_g_per_ml": density_for(ing),
                "whole_unit": whole_unit_for(ing),
            }
        )
    return result


def build_recipe_entry(
    *,
    recipe_id: str,
    title: str,
    category: str,
    csv_paths: list[str],
    description: str | None = None,
    sources: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Run get_ratio_and_stats on the CSV(s), build one schema-shaped dict."""
    ingredients, _ratio, stats, sample_size = get_ratio_and_stats(
        csv_paths, distinct=True, merge=[], zero_columns=None
    )
    base_name = ingredients[0].name()
    recipe: dict[str, Any] = {
        "id": recipe_id,
        "title": title,
        "category": category,
    }
    if description:
        recipe["description"] = description
    recipe.update(
        {
            "base_ingredient": base_name,
            "sample_size": sample_size,
            "confidence_level": CONFIDENCE_LEVEL,
            "ingredients": build_ingredient_stats(ingredients, stats),
            "sources": sources or [],
        }
    )
    return recipe


def _sources_from_entry(entry: VariantManifestEntry) -> list[dict[str, Any]]:
    """Manifest's ``source_urls`` map straight to ``type=url`` sources."""
    return [{"type": "url", "ref": url} for url in entry.source_urls if url]


def catalog_from_manifest(
    manifest_path: Path,
    *,
    default_category: str = "uncategorized",
    category_overrides: dict[str, str] | None = None,
    description_overrides: dict[str, str] | None = None,
    title_overrides: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Build a CuratedRecipeCatalog from a merged-pipeline manifest.

    ``category_overrides`` / ``description_overrides`` / ``title_overrides``
    are keyed by ``variant_id`` and let a caller tag results
    out-of-band (the pipeline doesn't yet infer category from variant
    metadata). Variants not in the override maps fall back to
    ``default_category`` and the manifest's ``title``.

    Variant-level outlier scores in the manifest are informational for
    reviewer tooling (bead ``eco``); they do not enter this catalog,
    which is aggregated stats only.
    """
    category_overrides = category_overrides or {}
    description_overrides = description_overrides or {}
    title_overrides = title_overrides or {}

    manifest = Manifest.read(manifest_path)
    manifest_dir = manifest_path.parent

    recipes: list[dict[str, Any]] = []
    for entry in manifest.variants:
        if entry.n_recipes < 1:
            continue
        csv_path = manifest_dir / entry.csv_path
        if not csv_path.exists():
            raise FileNotFoundError(
                f"Variant {entry.variant_id} references missing CSV {csv_path}"
            )

        title = title_overrides.get(entry.variant_id, entry.title)
        recipe = build_recipe_entry(
            recipe_id=f"{slugify(title)}-{entry.variant_id}",
            title=title,
            category=category_overrides.get(entry.variant_id, default_category),
            csv_paths=[str(csv_path)],
            description=description_overrides.get(entry.variant_id),
            sources=_sources_from_entry(entry),
        )
        recipes.append(recipe)

    return {"version": CATALOG_VERSION, "recipes": recipes}


def validate_catalog(catalog: dict[str, Any], schema_path: Path) -> None:
    """Validate against the JSON schema if jsonschema is available.

    Silent no-op when jsonschema isn't installed — the schema lives in
    the repo and is authoritative; this check is defense in depth for
    tests and CI, not a runtime requirement.
    """
    try:
        import jsonschema
    except ImportError:
        return
    schema = json.loads(schema_path.read_text())
    jsonschema.validate(catalog, schema)
