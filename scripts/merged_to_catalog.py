#!/usr/bin/env python3
"""Build a CuratedRecipeCatalog JSON from a merged-pipeline manifest.

Bridges the collection epic (``RationalRecipes-b7t``, scrape pipeline
output as ``manifest.json`` + per-variant CSVs) and the PWA epic
(``RationalRecipes-f85``, consumes ``CuratedRecipeCatalog`` JSON
shaped by ``schema/curated_recipes.schema.json``).

Optional override files let you tag variants with a category /
description / display title without modifying the manifest itself.
Each is JSON keyed by ``variant_id``::

    # categories.json
    {
      "3fa8c91d7e42": "crepes",
      "8bcd441e0913": "muffins"
    }

Usage:
    python3 scripts/merged_to_catalog.py output/merged/manifest.json \\
        -o artifacts/pipeline_recipes.json \\
        --default-category crepes \\
        --categories categories.json \\
        --descriptions descriptions.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from rational_recipes.catalog import (
    catalog_from_manifest,
    validate_catalog,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT = REPO_ROOT / "artifacts" / "pipeline_recipes.json"
SCHEMA_PATH = REPO_ROOT / "schema" / "curated_recipes.schema.json"


def _load_overrides(path: Path | None) -> dict[str, str]:
    if path is None:
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise SystemExit(f"Override file {path} must contain a JSON object")
    return {str(k): str(v) for k, v in data.items()}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "manifest",
        type=Path,
        help="Path to manifest.json emitted by the merged pipeline",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Output JSON path (default: {DEFAULT_OUTPUT.relative_to(REPO_ROOT)})",
    )
    parser.add_argument(
        "--default-category",
        default="uncategorized",
        help="Category for variants not in the categories override file",
    )
    parser.add_argument(
        "--categories",
        type=Path,
        help="JSON file: {variant_id: category} overrides",
    )
    parser.add_argument(
        "--descriptions",
        type=Path,
        help="JSON file: {variant_id: description} overrides",
    )
    parser.add_argument(
        "--titles",
        type=Path,
        help="JSON file: {variant_id: title} overrides",
    )
    args = parser.parse_args()

    if not args.manifest.exists():
        print(f"Manifest not found: {args.manifest}", file=sys.stderr)
        return 1

    catalog = catalog_from_manifest(
        args.manifest,
        default_category=args.default_category,
        category_overrides=_load_overrides(args.categories),
        description_overrides=_load_overrides(args.descriptions),
        title_overrides=_load_overrides(args.titles),
    )
    validate_catalog(catalog, SCHEMA_PATH)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(catalog, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(f"Wrote {args.output} ({len(catalog['recipes'])} recipes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
