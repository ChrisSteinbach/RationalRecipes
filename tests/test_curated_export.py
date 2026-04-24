"""Smoke test for the curated recipe export script.

Loads the script as a module, builds the catalog from the real sample CSVs,
and validates the result against schema/curated_recipes.schema.json.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import ModuleType
from typing import Any

import jsonschema
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT_PATH = REPO_ROOT / "scripts" / "export_curated_recipes.py"
SCHEMA_PATH = REPO_ROOT / "schema" / "curated_recipes.schema.json"

Catalog = dict[str, Any]


def _load_export_module() -> ModuleType:
    """Load scripts/export_curated_recipes.py as a module for in-process tests."""
    spec = importlib.util.spec_from_file_location(
        "_export_curated_recipes", SCRIPT_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def catalog() -> Catalog:
    built: Catalog = _load_export_module().build_catalog()
    return built


def test_catalog_validates_against_schema(catalog: Catalog) -> None:
    schema = json.loads(SCHEMA_PATH.read_text())
    jsonschema.validate(catalog, schema)


def test_catalog_has_expected_crepes_recipes(catalog: Catalog) -> None:
    ids = {r["id"] for r in catalog["recipes"]}
    assert ids == {
        "swedish-pancakes",
        "english-pannkakor",
        "french-crepes",
        "english-crepes",
    }


def test_swedish_pancakes_ratios(catalog: Catalog) -> None:
    """Spot-check that the Swedish pancake stats still match the canonical
    numbers from schema/example.json — these changing means either the input
    data or the pipeline's normalization has shifted."""
    recipe = next(r for r in catalog["recipes"] if r["id"] == "swedish-pancakes")
    assert recipe["base_ingredient"] == "flour"
    assert recipe["sample_size"] == 200

    by_name = {i["name"]: i for i in recipe["ingredients"]}
    flour = by_name["flour"]
    assert flour["ratio"] == pytest.approx(1.0)
    assert flour["proportion"] == pytest.approx(0.1673, abs=0.0001)

    milk = by_name["milk"]
    assert milk["ratio"] == pytest.approx(3.5976, abs=0.0001)
    assert milk["proportion"] == pytest.approx(0.6019, abs=0.0001)


def test_ingredient_proportions_sum_to_one(catalog: Catalog) -> None:
    for recipe in catalog["recipes"]:
        total = sum(i["proportion"] for i in recipe["ingredients"])
        assert total == pytest.approx(1.0, abs=0.001), (
            f"Proportions for {recipe['id']} sum to {total}, expected ~1.0"
        )


def test_base_ingredient_has_ratio_one(catalog: Catalog) -> None:
    for recipe in catalog["recipes"]:
        base_name = recipe["base_ingredient"]
        base = next(i for i in recipe["ingredients"] if i["name"] == base_name)
        assert base["ratio"] == pytest.approx(1.0)


def test_build_catalog_with_metadata_attaches_block() -> None:
    """Release-tagged catalogs carry a schema-valid metadata block."""
    module = _load_export_module()
    metadata = {
        "dataset_version": "2026.04.24",
        "released": "2026-04-24",
        "pipeline_revision": "abc1234",
        "notes": "Hand-curated crepes baseline",
    }
    catalog: Catalog = module.build_catalog(metadata=metadata)
    assert catalog["metadata"]["dataset_version"] == "2026.04.24"
    assert catalog["metadata"]["recipe_count"] == len(catalog["recipes"])
    schema = json.loads(SCHEMA_PATH.read_text())
    jsonschema.validate(catalog, schema)


def test_build_catalog_without_metadata_omits_block() -> None:
    """Untagged builds produce exactly the v1 shape with no metadata key."""
    module = _load_export_module()
    catalog: Catalog = module.build_catalog()
    assert "metadata" not in catalog
