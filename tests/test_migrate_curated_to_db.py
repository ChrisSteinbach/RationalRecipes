"""End-to-end test for the curated → SQLite migration (vwt.6)."""

from __future__ import annotations

import json
from pathlib import Path

from migrate_curated_to_db import migrate

from rational_recipes.catalog_db import CatalogDB


def _minimal_curated() -> dict[str, object]:
    return {
        "version": 1,
        "recipes": [
            {
                "id": "french-crepes",
                "title": "French Crêpes",
                "category": "crepes",
                "description": "Classic thin crêpes.",
                "base_ingredient": "flour",
                "sample_size": 119,
                "confidence_level": 0.95,
                "ingredients": [
                    {
                        "name": "flour",
                        "ratio": 1.0,
                        "proportion": 0.2473,
                        "std_deviation": 0.0668,
                        "ci_lower": 0.2353,
                        "ci_upper": 0.2594,
                        "min_sample_size": 113,
                        "density_g_per_ml": 0.5283,
                        "whole_unit": None,
                    },
                    {
                        "name": "egg",
                        "ratio": 0.83,
                        "proportion": 0.2048,
                        "std_deviation": 0.0852,
                        "ci_lower": 0.1895,
                        "ci_upper": 0.2201,
                        "min_sample_size": 266,
                        "density_g_per_ml": 1.0271,
                        "whole_unit": {"name": "medium", "grams": 44.0},
                    },
                ],
                "sources": [
                    {
                        "type": "text",
                        "title": "Aggregated French recipes",
                        "ref": "Crêpe recipes collected from French-language sources.",
                    }
                ],
            }
        ],
    }


class TestMigrate:
    def test_full_migration_round_trip(self, tmp_path: Path) -> None:
        src = tmp_path / "curated.json"
        src.write_text(json.dumps(_minimal_curated()), encoding="utf-8")
        out = tmp_path / "recipes.db"

        count = migrate(src, out)
        assert count == 1

        db = CatalogDB.open(out)
        variants = db.list_variants()
        assert len(variants) == 1
        v = variants[0]
        assert v.normalized_title == "french-crepes"
        assert v.display_title == "French Crêpes"
        assert v.category == "crepes"
        assert v.base_ingredient == "flour"
        assert v.confidence_level == 0.95
        assert v.n_recipes == 119
        assert v.canonical_ingredient_set == ("egg", "flour")

        stats = db.get_ingredient_stats(v.variant_id)
        assert [s.canonical_name for s in stats] == ["flour", "egg"]
        assert stats[0].ratio == 1.0
        assert stats[0].density_g_per_ml == 0.5283
        assert stats[1].whole_unit_name == "medium"
        assert stats[1].whole_unit_grams == 44.0

        members = db.get_variant_members(v.variant_id)
        assert len(members) == 1
        assert members[0].corpus == "curated"
        assert members[0].source_type == "text"

        sources = db.get_variant_sources(v.variant_id)
        assert len(sources) == 1
        assert sources[0].source_type == "text"
        assert sources[0].title == "Aggregated French recipes"
        db.close()

    def test_rerun_replaces_existing(self, tmp_path: Path) -> None:
        src = tmp_path / "curated.json"
        src.write_text(json.dumps(_minimal_curated()), encoding="utf-8")
        out = tmp_path / "recipes.db"
        migrate(src, out)
        migrate(src, out)
        db = CatalogDB.open(out)
        assert len(db.list_variants()) == 1
        db.close()

    def test_real_fixture_yields_four_variants(self, tmp_path: Path) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        curated = repo_root / "artifacts" / "curated_recipes.json"
        if not curated.exists():
            import pytest

            pytest.skip("curated_recipes.json not present")
        out = tmp_path / "recipes.db"
        count = migrate(curated, out)
        assert count == 4
        db = CatalogDB.open(out)
        titles = {v.normalized_title for v in db.list_variants()}
        assert titles == {
            "swedish-pancakes",
            "english-pannkakor",
            "french-crepes",
            "english-crepes",
        }
        db.close()
