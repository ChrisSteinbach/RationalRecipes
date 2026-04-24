#!/usr/bin/env python3
"""Migrate artifacts/curated_recipes.json into a fresh recipes.db.

One-shot seed: the 4 hand-curated crêpe variants live in a JSON file
today; this script writes them into the same SQLite schema the
pipeline will target, so the PWA can read variants via sql.js
regardless of whether they came from the hand-curated seed or the
pipeline.

The JSON stays on disk as a historical seed (per vwt.6 acceptance).
Pipeline-produced variants come later via scripts/scrape_catalog.py
(vwt.2) calling CatalogDB.upsert_variant directly.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from rational_recipes.catalog_db import CatalogDB
from rational_recipes.scrape.manifest import compute_variant_id


def _synthetic_recipe_id(ref: str, title: str) -> str:
    payload = f"{ref}|{title}".encode()
    return hashlib.sha1(payload, usedforsecurity=False).hexdigest()[:12]


def _variant_id_for_curated(recipe: dict[str, Any]) -> str:
    """Stable id matching the pipeline's variant_id scheme.

    Hand-curated recipes have no cookingMethod signal, so the method
    set stays empty. The canonical ingredient set comes from the
    ``ingredients[].name`` list in the JSON.
    """
    canonical_set = frozenset(str(i["name"]) for i in recipe["ingredients"])
    return cast(
        str,
        compute_variant_id(
            normalized_title=str(recipe["id"]),
            canonical_ingredients=canonical_set,
            cooking_methods=frozenset(),
        ),
    )


def _insert_variant(db: CatalogDB, recipe: dict[str, Any], *, now: str) -> None:
    variant_id = _variant_id_for_curated(recipe)
    canonical_set = sorted(i["name"] for i in recipe["ingredients"])

    conn: sqlite3.Connection = db.connection
    with conn:
        conn.execute("DELETE FROM variant_members WHERE variant_id = ?", (variant_id,))
        conn.execute(
            "DELETE FROM variant_ingredient_stats WHERE variant_id = ?",
            (variant_id,),
        )
        conn.execute("DELETE FROM variant_sources WHERE variant_id = ?", (variant_id,))
        conn.execute("DELETE FROM variants WHERE variant_id = ?", (variant_id,))

        conn.execute(
            """
            INSERT INTO variants (
              variant_id, normalized_title, display_title, category,
              description, base_ingredient, cooking_methods,
              canonical_ingredient_set, n_recipes, confidence_level
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                variant_id,
                recipe["id"],
                recipe["title"],
                recipe.get("category"),
                recipe.get("description"),
                recipe["base_ingredient"],
                "",
                ",".join(canonical_set),
                int(recipe["sample_size"]),
                recipe.get("confidence_level"),
            ),
        )

        for ordinal, ing in enumerate(recipe["ingredients"]):
            whole = ing.get("whole_unit") or {}
            conn.execute(
                """
                INSERT INTO variant_ingredient_stats (
                  variant_id, canonical_name, ordinal, mean_proportion,
                  stddev, ci_lower, ci_upper, ratio, min_sample_size,
                  density_g_per_ml, whole_unit_name, whole_unit_grams
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    variant_id,
                    ing["name"],
                    ordinal,
                    ing["proportion"],
                    ing["std_deviation"],
                    ing["ci_lower"],
                    ing["ci_upper"],
                    ing["ratio"],
                    int(ing.get("min_sample_size", 0)),
                    ing.get("density_g_per_ml"),
                    whole.get("name"),
                    whole.get("grams"),
                ),
            )

        for ordinal, source in enumerate(recipe.get("sources") or []):
            recipe_id = _synthetic_recipe_id(source["ref"], recipe["title"])
            source_type = source.get("type", "url")
            url_value = source["ref"] if source_type == "url" else None
            title_value = source.get("title") or recipe["title"]
            conn.execute(
                """
                INSERT OR REPLACE INTO recipes (
                  recipe_id, url, title, corpus, language, source_type,
                  extracted_at
                ) VALUES (?, ?, ?, 'curated', NULL, ?, ?)
                """,
                (recipe_id, url_value, title_value, source_type, now),
            )
            conn.execute(
                """
                INSERT INTO variant_members (variant_id, recipe_id, outlier_score)
                VALUES (?, ?, NULL)
                """,
                (variant_id, recipe_id),
            )
            conn.execute(
                """
                INSERT INTO variant_sources (
                  variant_id, ordinal, source_type, title, ref
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    variant_id,
                    ordinal,
                    source_type,
                    source.get("title"),
                    source["ref"],
                ),
            )


def migrate(curated_json: Path, db_path: Path) -> int:
    """Fresh-build ``db_path`` from ``curated_json``. Returns variant count."""
    if db_path.exists():
        db_path.unlink()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    data = json.loads(curated_json.read_text(encoding="utf-8"))
    recipes: Iterable[dict[str, Any]] = data.get("recipes", [])
    now = datetime.now(tz=UTC).isoformat(timespec="seconds")

    db = CatalogDB.open(db_path)
    try:
        count = 0
        for recipe in recipes:
            _insert_variant(db, recipe, now=now)
            count += 1
    finally:
        db.close()
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source",
        type=Path,
        default=Path("artifacts/curated_recipes.json"),
        help="Path to curated_recipes.json (default: artifacts/curated_recipes.json)",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("artifacts/recipes.db"),
        help="Path to write recipes.db (default: artifacts/recipes.db)",
    )
    args = parser.parse_args()

    n = migrate(args.source, args.out)
    print(f"Wrote {n} variant(s) from {args.source} → {args.out}")


if __name__ == "__main__":
    main()
