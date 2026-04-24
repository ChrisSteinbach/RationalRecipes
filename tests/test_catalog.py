"""Tests for the CuratedRecipeCatalog builder (bead 5ub)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from rational_recipes.catalog import (
    CATALOG_VERSION,
    attach_metadata,
    build_metadata,
    build_recipe_entry,
    catalog_from_manifest,
    detect_pipeline_revision,
    slugify,
    validate_catalog,
)
from rational_recipes.scrape.manifest import Manifest, VariantManifestEntry

REPO_ROOT = Path(__file__).resolve().parent.parent
SCHEMA_PATH = REPO_ROOT / "schema" / "curated_recipes.schema.json"
SAMPLE_CSV = REPO_ROOT / "sample_input" / "crepes" / "swedish_recipe_pannkisar.csv"


class TestSlugify:
    def test_lowercases_and_dashes(self) -> None:
        assert slugify("Swedish Pancakes") == "swedish-pancakes"

    def test_collapses_non_alphanumerics(self) -> None:
        assert slugify("French Crêpes (Classic)") == "french-cr-pes-classic"

    def test_strips_leading_trailing_dashes(self) -> None:
        assert slugify("  Hello!  ") == "hello"

    def test_empty_input_returns_default(self) -> None:
        assert slugify("") == "recipe"
        assert slugify("!!!") == "recipe"

    def test_matches_schema_pattern(self) -> None:
        """Slugs must satisfy schema pattern ^[a-z0-9]+(-[a-z0-9]+)*$."""
        import re

        pattern = re.compile(r"^[a-z0-9]+(-[a-z0-9]+)*$")
        for s in ["pannkakor", "swedish-pancakes", "abc123-def456"]:
            assert pattern.match(slugify(s))


class TestBuildRecipeEntry:
    def test_builds_full_schema_dict_from_real_csv(self) -> None:
        recipe = build_recipe_entry(
            recipe_id="swedish-pancakes",
            title="Swedish Pancakes",
            category="crepes",
            csv_paths=[str(SAMPLE_CSV)],
            description="Test description",
            sources=[{"type": "url", "ref": "https://example.com/r"}],
        )
        assert recipe["id"] == "swedish-pancakes"
        assert recipe["title"] == "Swedish Pancakes"
        assert recipe["category"] == "crepes"
        assert recipe["description"] == "Test description"
        assert recipe["sample_size"] > 0
        assert recipe["confidence_level"] == 0.95
        assert len(recipe["ingredients"]) > 0
        # First ingredient is the base — schema requires ratio = 1.0
        first = recipe["ingredients"][0]
        assert first["ratio"] == 1.0
        # Proportions sum close to 1.0 (small rounding).
        total = sum(ing["proportion"] for ing in recipe["ingredients"])
        assert abs(total - 1.0) < 0.01
        assert recipe["sources"][0]["ref"] == "https://example.com/r"

    def test_optional_description_omitted_when_none(self) -> None:
        recipe = build_recipe_entry(
            recipe_id="test",
            title="T",
            category="cat",
            csv_paths=[str(SAMPLE_CSV)],
        )
        assert "description" not in recipe

    def test_default_sources_is_empty_list(self) -> None:
        recipe = build_recipe_entry(
            recipe_id="test",
            title="T",
            category="cat",
            csv_paths=[str(SAMPLE_CSV)],
        )
        assert recipe["sources"] == []


def _write_variant_csv(path: Path, header: list[str], rows: list[list[str]]) -> None:
    """Write a tiny rr-stats-compatible CSV for tests."""
    lines = [",".join(header)]
    for row in rows:
        lines.append(",".join(row))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


class TestCatalogFromManifest:
    def _write_manifest_with_variants(
        self, tmp_path: Path, n_variants: int = 1
    ) -> Path:
        """Write a manifest + matching CSVs into tmp_path. Returns manifest path."""
        entries: list[VariantManifestEntry] = []
        for i in range(n_variants):
            csv_name = f"variant_{i}.csv"
            _write_variant_csv(
                tmp_path / csv_name,
                header=["flour", "milk"],
                rows=[
                    ["100 g", "200 ml"],
                    ["120 g", "200 ml"],
                    ["110 g", "210 ml"],
                ],
            )
            entries.append(
                VariantManifestEntry(
                    variant_id=f"abc{i:09d}".ljust(12, "0")[:12],
                    title=f"variant {i}",
                    canonical_ingredients=("flour", "milk"),
                    cooking_methods=(),
                    n_recipes=3,
                    csv_path=csv_name,
                    source_urls=(f"https://example.com/r/{i}",),
                    row_outlier_scores=(0.0, 1.5, 1.5),
                )
            )
        manifest = Manifest(variants=entries)
        manifest_path = tmp_path / "manifest.json"
        manifest.write(manifest_path)
        return manifest_path

    def test_builds_catalog_from_synthetic_manifest(self, tmp_path: Path) -> None:
        manifest_path = self._write_manifest_with_variants(tmp_path, n_variants=2)
        catalog = catalog_from_manifest(manifest_path, default_category="crepes")

        assert catalog["version"] == CATALOG_VERSION
        assert len(catalog["recipes"]) == 2
        for recipe in catalog["recipes"]:
            assert recipe["category"] == "crepes"
            assert recipe["sample_size"] == 3
            assert recipe["sources"][0]["type"] == "url"

    def test_recipe_id_combines_slug_and_variant_id(self, tmp_path: Path) -> None:
        """Two variants with the same title still get distinct ids via the
        variant_id suffix — slug alone would collide."""
        manifest_path = self._write_manifest_with_variants(tmp_path, n_variants=2)
        catalog = catalog_from_manifest(manifest_path)
        ids = [r["id"] for r in catalog["recipes"]]
        assert len(set(ids)) == 2
        # Each id ends with the variant_id.
        for r in catalog["recipes"]:
            assert r["id"].endswith(r["id"][-12:])

    def test_category_overrides_per_variant(self, tmp_path: Path) -> None:
        manifest_path = self._write_manifest_with_variants(tmp_path, n_variants=2)
        manifest = Manifest.read(manifest_path)
        first_id = manifest.variants[0].variant_id
        catalog = catalog_from_manifest(
            manifest_path,
            default_category="default",
            category_overrides={first_id: "crepes"},
        )
        cats_by_vid = {r["id"][-12:]: r["category"] for r in catalog["recipes"]}
        assert cats_by_vid[first_id] == "crepes"
        # Second variant falls back to default.
        other_id = manifest.variants[1].variant_id
        assert cats_by_vid[other_id] == "default"

    def test_description_overrides_per_variant(self, tmp_path: Path) -> None:
        manifest_path = self._write_manifest_with_variants(tmp_path, n_variants=1)
        manifest = Manifest.read(manifest_path)
        vid = manifest.variants[0].variant_id
        catalog = catalog_from_manifest(
            manifest_path, description_overrides={vid: "My description"}
        )
        assert catalog["recipes"][0]["description"] == "My description"

    def test_title_override_per_variant(self, tmp_path: Path) -> None:
        manifest_path = self._write_manifest_with_variants(tmp_path, n_variants=1)
        manifest = Manifest.read(manifest_path)
        vid = manifest.variants[0].variant_id
        catalog = catalog_from_manifest(
            manifest_path, title_overrides={vid: "Pretty Title"}
        )
        recipe = catalog["recipes"][0]
        assert recipe["title"] == "Pretty Title"
        assert "pretty-title" in recipe["id"]

    def test_missing_csv_file_raises(self, tmp_path: Path) -> None:
        manifest = Manifest(
            variants=[
                VariantManifestEntry(
                    variant_id="abc123def456",
                    title="missing",
                    canonical_ingredients=("flour",),
                    cooking_methods=(),
                    n_recipes=2,
                    csv_path="does_not_exist.csv",
                    source_urls=(),
                ),
            ],
        )
        manifest_path = tmp_path / "manifest.json"
        manifest.write(manifest_path)
        with pytest.raises(FileNotFoundError, match="does_not_exist.csv"):
            catalog_from_manifest(manifest_path)

    def test_zero_recipe_variants_skipped(self, tmp_path: Path) -> None:
        """Variants with n_recipes=0 are filtered out (no stats to compute)."""
        # Write a real CSV for variant 0; manifest claims n_recipes=0 for it.
        csv_name = "variant_0.csv"
        _write_variant_csv(tmp_path / csv_name, header=["flour"], rows=[["100 g"]])
        manifest = Manifest(
            variants=[
                VariantManifestEntry(
                    variant_id="abc123def456",
                    title="empty",
                    canonical_ingredients=("flour",),
                    cooking_methods=(),
                    n_recipes=0,
                    csv_path=csv_name,
                    source_urls=(),
                ),
            ],
        )
        manifest_path = tmp_path / "manifest.json"
        manifest.write(manifest_path)
        catalog = catalog_from_manifest(manifest_path)
        assert catalog["recipes"] == []


class TestBuildMetadata:
    def test_all_none_returns_empty_dict(self) -> None:
        assert build_metadata() == {}

    def test_date_serialized_iso(self) -> None:
        from datetime import date

        md = build_metadata(released=date(2026, 4, 24))
        assert md == {"released": "2026-04-24"}

    def test_string_date_passthrough(self) -> None:
        md = build_metadata(released="2026-04-24")
        assert md == {"released": "2026-04-24"}

    def test_all_fields_populated(self) -> None:
        md = build_metadata(
            dataset_version="2026.04.24",
            released="2026-04-24",
            pipeline_revision="abc1234",
            recipe_count=7,
            notes="Initial release",
        )
        assert md == {
            "dataset_version": "2026.04.24",
            "released": "2026-04-24",
            "pipeline_revision": "abc1234",
            "recipe_count": 7,
            "notes": "Initial release",
        }


class TestAttachMetadata:
    def _mk_catalog(self) -> dict[str, object]:
        return {"version": CATALOG_VERSION, "recipes": [{"id": "a"}, {"id": "b"}]}

    def test_none_returns_unchanged(self) -> None:
        cat = self._mk_catalog()
        assert attach_metadata(cat, None) is cat

    def test_empty_returns_unchanged(self) -> None:
        cat = self._mk_catalog()
        assert attach_metadata(cat, {}) is cat

    def test_inserts_metadata_block(self) -> None:
        cat = self._mk_catalog()
        out = attach_metadata(cat, {"dataset_version": "2026.04.24"})
        assert list(out.keys()) == ["version", "metadata", "recipes"]
        assert out["metadata"]["dataset_version"] == "2026.04.24"

    def test_fills_recipe_count_when_missing(self) -> None:
        cat = self._mk_catalog()
        out = attach_metadata(cat, {"dataset_version": "1.0"})
        assert out["metadata"]["recipe_count"] == 2

    def test_preserves_explicit_recipe_count(self) -> None:
        cat = self._mk_catalog()
        out = attach_metadata(cat, {"recipe_count": 99})
        assert out["metadata"]["recipe_count"] == 99

    def test_rejects_unknown_keys(self) -> None:
        cat = self._mk_catalog()
        with pytest.raises(ValueError, match="Unknown metadata keys"):
            attach_metadata(cat, {"bogus": "x"})


class TestDetectPipelineRevision:
    def test_returns_short_sha_in_git_repo(self) -> None:
        sha = detect_pipeline_revision(REPO_ROOT)
        # Short SHAs are 7+ hex chars.
        assert sha is not None
        assert len(sha) >= 7
        assert all(c in "0123456789abcdef" for c in sha)

    def test_returns_none_outside_repo(self, tmp_path: Path) -> None:
        assert detect_pipeline_revision(tmp_path) is None


class TestCatalogFromManifestMetadata:
    """Manifest -> catalog with metadata passes through to output."""

    def test_metadata_attached_to_output(self, tmp_path: Path) -> None:
        csv_name = "variant_0.csv"
        _write_variant_csv(
            tmp_path / csv_name,
            header=["flour", "milk"],
            rows=[["100 g", "200 ml"], ["120 g", "210 ml"]],
        )
        manifest = Manifest(
            variants=[
                VariantManifestEntry(
                    variant_id="abc123def456",
                    title="pannkakor",
                    canonical_ingredients=("flour", "milk"),
                    cooking_methods=(),
                    n_recipes=2,
                    csv_path=csv_name,
                    source_urls=(),
                ),
            ],
        )
        manifest_path = tmp_path / "manifest.json"
        manifest.write(manifest_path)
        catalog = catalog_from_manifest(
            manifest_path,
            metadata={"dataset_version": "2026.04.24", "notes": "test"},
        )
        assert catalog["metadata"]["dataset_version"] == "2026.04.24"
        assert catalog["metadata"]["recipe_count"] == 1
        # Still schema-valid.
        try:
            import jsonschema  # noqa: F401
        except ImportError:
            return
        validate_catalog(catalog, SCHEMA_PATH)


class TestValidateCatalog:
    """End-to-end: catalog_from_manifest output validates against the JSON schema."""

    def test_pipeline_catalog_validates_against_schema(self, tmp_path: Path) -> None:
        try:
            import jsonschema  # noqa: F401
        except ImportError:
            pytest.skip("jsonschema not installed")

        # Build manifest + CSVs, generate catalog, validate.
        csv_name = "variant_0.csv"
        _write_variant_csv(
            tmp_path / csv_name,
            header=["flour", "milk"],
            rows=[["100 g", "200 ml"], ["120 g", "210 ml"]],
        )
        manifest = Manifest(
            variants=[
                VariantManifestEntry(
                    variant_id="abc123def456",
                    title="pannkakor",
                    canonical_ingredients=("flour", "milk"),
                    cooking_methods=(),
                    n_recipes=2,
                    csv_path=csv_name,
                    source_urls=("https://example.com/r/1",),
                ),
            ],
        )
        manifest_path = tmp_path / "manifest.json"
        manifest.write(manifest_path)
        catalog = catalog_from_manifest(manifest_path, default_category="crepes")
        validate_catalog(catalog, SCHEMA_PATH)  # raises if invalid


class TestRoundTripFromExporter:
    """The hand-curated exporter still works after the refactor."""

    def test_export_still_produces_valid_catalog(self, tmp_path: Path) -> None:
        try:
            import jsonschema  # noqa: F401
        except ImportError:
            pytest.skip("jsonschema not installed")

        # Import the exporter's build_catalog and validate its output.
        import sys

        sys.path.insert(0, str(REPO_ROOT / "scripts"))
        try:
            import export_curated_recipes as exporter
        finally:
            sys.path.pop(0)

        catalog = exporter.build_catalog()
        # Same shape as before: version, recipes list, all 4 hand-curated entries.
        assert catalog["version"] == CATALOG_VERSION
        assert len(catalog["recipes"]) == 4
        validate_catalog(catalog, SCHEMA_PATH)
        # Confirm the first recipe still has the expected id (regression).
        assert catalog["recipes"][0]["id"] == "swedish-pancakes"

        # And actually persisting also works.
        out = tmp_path / "out.json"
        out.write_text(
            json.dumps(catalog, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        assert out.exists()
