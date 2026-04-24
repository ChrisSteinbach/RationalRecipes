"""Tests for the merged-pipeline manifest contract."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from rational_recipes.scrape.manifest import (
    MANIFEST_VERSION,
    Manifest,
    VariantManifestEntry,
    compute_variant_id,
)


class TestComputeVariantId:
    def test_produces_12_hex_chars(self) -> None:
        vid = compute_variant_id("pannkakor", ["flour", "milk"], ["stekt"])
        assert len(vid) == 12
        assert all(c in "0123456789abcdef" for c in vid)

    def test_stable_across_ingredient_order(self) -> None:
        a = compute_variant_id("pannkakor", ["flour", "milk", "egg"], ["stekt"])
        b = compute_variant_id("pannkakor", ["egg", "milk", "flour"], ["stekt"])
        assert a == b

    def test_stable_across_method_order(self) -> None:
        a = compute_variant_id("pannkakor", ["flour"], ["stekt", "i ugn"])
        b = compute_variant_id("pannkakor", ["flour"], ["i ugn", "stekt"])
        assert a == b

    def test_different_titles_differ(self) -> None:
        a = compute_variant_id("pannkakor", ["flour"], [])
        b = compute_variant_id("crepes", ["flour"], [])
        assert a != b

    def test_different_ingredients_differ(self) -> None:
        a = compute_variant_id("pannkakor", ["flour", "milk"], [])
        b = compute_variant_id("pannkakor", ["flour", "buttermilk"], [])
        assert a != b

    def test_different_methods_differ(self) -> None:
        a = compute_variant_id("pannkakor", ["flour"], ["stekt"])
        b = compute_variant_id("pannkakor", ["flour"], ["i ugn"])
        assert a != b

    def test_empty_methods_hashes_stably(self) -> None:
        a = compute_variant_id("pannkakor", ["flour"], [])
        b = compute_variant_id("pannkakor", ["flour"], [])
        assert a == b

    def test_empty_methods_differs_from_non_empty(self) -> None:
        a = compute_variant_id("pannkakor", ["flour"], [])
        b = compute_variant_id("pannkakor", ["flour"], ["stekt"])
        assert a != b

    def test_set_inputs_supported(self) -> None:
        """Callers pass frozenset-derived iterables; accept any iterable."""
        a = compute_variant_id("pannkakor", frozenset({"flour", "milk"}), set())
        b = compute_variant_id("pannkakor", ("flour", "milk"), [])
        assert a == b


class TestManifestRoundtrip:
    def test_variant_entry_roundtrip(self) -> None:
        entry = VariantManifestEntry(
            variant_id="abc123def456",
            title="pannkakor",
            canonical_ingredients=("butter", "egg", "flour", "milk", "salt"),
            cooking_methods=("stekt",),
            n_recipes=42,
            csv_path="pannkakor_abc123def456.csv",
            source_urls=("https://example.com/r/1", "https://example.com/r/2"),
        )
        restored = VariantManifestEntry.from_json_dict(entry.to_json_dict())
        assert restored == entry

    def test_manifest_roundtrip_via_file(self, tmp_path: Path) -> None:
        manifest = Manifest(
            variants=[
                VariantManifestEntry(
                    variant_id="abc123def456",
                    title="pannkakor",
                    canonical_ingredients=("flour", "milk"),
                    cooking_methods=(),
                    n_recipes=10,
                    csv_path="pannkakor_abc123def456.csv",
                    source_urls=("https://example.com/r/1",),
                ),
            ],
        )
        manifest_path = tmp_path / "manifest.json"
        manifest.write(manifest_path)

        reloaded = Manifest.read(manifest_path)
        assert reloaded.manifest_version == MANIFEST_VERSION
        assert reloaded.variants == manifest.variants

    def test_manifest_file_is_valid_json(self, tmp_path: Path) -> None:
        manifest = Manifest(variants=[])
        manifest_path = tmp_path / "manifest.json"
        manifest.write(manifest_path)
        data = json.loads(manifest_path.read_text(encoding="utf-8"))
        assert data["manifest_version"] == MANIFEST_VERSION
        assert data["variants"] == []

    def test_manifest_preserves_non_ascii(self, tmp_path: Path) -> None:
        """Non-ASCII ingredient names (kept post-canonicalization on
        unresolved items) must survive the JSON round-trip."""
        manifest = Manifest(
            variants=[
                VariantManifestEntry(
                    variant_id="abc123def456",
                    title="saffranspannkaka",
                    canonical_ingredients=("egg", "mjölk"),
                    cooking_methods=("i ugn",),
                    n_recipes=5,
                    csv_path="saffranspannkaka.csv",
                    source_urls=(),
                ),
            ],
        )
        manifest_path = tmp_path / "manifest.json"
        manifest.write(manifest_path)

        # Must be valid UTF-8 JSON, not \uXXXX-escaped
        raw = manifest_path.read_text(encoding="utf-8")
        assert "mjölk" in raw
        reloaded = Manifest.read(manifest_path)
        assert reloaded.variants[0].canonical_ingredients == ("egg", "mjölk")

    def test_unsupported_version_rejected(self, tmp_path: Path) -> None:
        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(
            json.dumps({"manifest_version": 999, "variants": []}),
            encoding="utf-8",
        )
        with pytest.raises(ValueError, match="manifest_version"):
            Manifest.read(manifest_path)


class TestOutlierScoresField:
    """Bead 0g3 adds optional row_outlier_scores to the manifest entry."""

    def test_scores_roundtrip(self) -> None:
        entry = VariantManifestEntry(
            variant_id="abc123def456",
            title="pannkakor",
            canonical_ingredients=("flour", "milk"),
            cooking_methods=(),
            n_recipes=3,
            csv_path="pannkakor_abc123def456.csv",
            source_urls=(),
            row_outlier_scores=(0.0, 1.5, 2.3),
        )
        restored = VariantManifestEntry.from_json_dict(entry.to_json_dict())
        assert restored.row_outlier_scores == (0.0, 1.5, 2.3)

    def test_empty_scores_omitted_from_json(self) -> None:
        """Emit-when-empty would pollute the v1 schema; the field is optional."""
        entry = VariantManifestEntry(
            variant_id="abc123def456",
            title="pannkakor",
            canonical_ingredients=("flour",),
            cooking_methods=(),
            n_recipes=1,
            csv_path="x.csv",
            source_urls=(),
        )
        data = entry.to_json_dict()
        assert "row_outlier_scores" not in data

    def test_v1_manifest_without_scores_reads_as_empty(self) -> None:
        """Backward compat: manifests written before bead 0g3 have no
        row_outlier_scores key. Reader must default to () without raising."""
        legacy_data: dict[str, object] = {
            "variant_id": "abc123def456",
            "title": "pannkakor",
            "canonical_ingredients": ["flour"],
            "cooking_methods": [],
            "n_recipes": 5,
            "csv_path": "x.csv",
            "source_urls": [],
        }
        entry = VariantManifestEntry.from_json_dict(legacy_data)
        assert entry.row_outlier_scores == ()

    def test_non_list_scores_rejected(self, tmp_path: Path) -> None:
        bad = {
            "variant_id": "abc123def456",
            "title": "pannkakor",
            "canonical_ingredients": ["flour"],
            "cooking_methods": [],
            "n_recipes": 1,
            "csv_path": "x.csv",
            "source_urls": [],
            "row_outlier_scores": "not a list",
        }
        with pytest.raises(ValueError, match="row_outlier_scores"):
            VariantManifestEntry.from_json_dict(bad)

    def test_manifest_with_scores_full_roundtrip(self, tmp_path: Path) -> None:
        manifest = Manifest(
            variants=[
                VariantManifestEntry(
                    variant_id="abc123def456",
                    title="pannkakor",
                    canonical_ingredients=("flour", "milk"),
                    cooking_methods=(),
                    n_recipes=3,
                    csv_path="x.csv",
                    source_urls=(),
                    row_outlier_scores=(0.0, 1.5, 2.3),
                ),
            ],
        )
        path = tmp_path / "manifest.json"
        manifest.write(path)
        reloaded = Manifest.read(path)
        assert reloaded.variants[0].row_outlier_scores == (0.0, 1.5, 2.3)
