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
from datetime import date
from pathlib import Path
from typing import Any

from rational_recipes.catalog import (
    CATALOG_VERSION,
    attach_metadata,
    build_metadata,
    build_recipe_entry,
    detect_pipeline_revision,
    validate_catalog,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT = REPO_ROOT / "artifacts" / "curated_recipes.json"
SCHEMA_PATH = REPO_ROOT / "schema" / "curated_recipes.schema.json"

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


def _build_recipe(config: dict[str, Any]) -> dict[str, Any]:
    """Run the pipeline for one recipe config and build its JSON dict."""
    return build_recipe_entry(
        recipe_id=config["id"],
        title=config["title"],
        category=config["category"],
        csv_paths=[str(REPO_ROOT / f) for f in config["csv_files"]],
        description=config.get("description"),
        sources=config["sources"],
    )


def build_catalog(metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    """Run the pipeline for every configured recipe and return the catalog.

    ``metadata`` attaches a dataset-release block (dataset_version,
    released date, pipeline_revision, notes) for tagged releases; leave
    it ``None`` for ad-hoc rebuilds during development.
    """
    catalog = {
        "version": CATALOG_VERSION,
        "recipes": [_build_recipe(cfg) for cfg in RECIPE_CONFIGS],
    }
    return attach_metadata(catalog, metadata)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"output path (default: {DEFAULT_OUTPUT.relative_to(REPO_ROOT)})",
    )
    parser.add_argument(
        "--dataset-version",
        help=(
            "Release version to record in metadata (e.g. 2026.04.24)."
            " Omit for untagged builds."
        ),
    )
    parser.add_argument(
        "--released",
        help=(
            "Release date in ISO YYYY-MM-DD."
            " Defaults to today's date when --dataset-version is set."
        ),
    )
    parser.add_argument(
        "--notes",
        help="Short human-readable release notes.",
    )
    parser.add_argument(
        "--pipeline-revision",
        help=(
            "Git revision to record."
            " Defaults to 'git rev-parse --short HEAD' when --dataset-version is set."
        ),
    )
    parser.add_argument(
        "--no-metadata",
        action="store_true",
        help="Emit the catalog without any metadata block (explicit opt-out).",
    )
    args = parser.parse_args()

    metadata: dict[str, Any] | None = None
    if not args.no_metadata and (
        args.dataset_version or args.released or args.notes or args.pipeline_revision
    ):
        metadata = build_metadata(
            dataset_version=args.dataset_version,
            released=args.released or (date.today() if args.dataset_version else None),
            pipeline_revision=args.pipeline_revision
            or (detect_pipeline_revision(REPO_ROOT) if args.dataset_version else None),
            notes=args.notes,
        )

    catalog = build_catalog(metadata=metadata)
    validate_catalog(catalog, SCHEMA_PATH)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(catalog, indent=2, ensure_ascii=False) + "\n")
    tag = (
        f" [{metadata['dataset_version']}]"
        if metadata and "dataset_version" in metadata
        else ""
    )
    print(f"Wrote {args.output} ({len(catalog['recipes'])} recipes){tag}")


if __name__ == "__main__":
    main()
