"""Smoke tests for the substitution-provenance spike (RationalRecipes-4rgy)."""

from __future__ import annotations

import csv
import sys
from pathlib import Path

import pytest

# Allow importing scripts/inspect_variant_provenance.py directly.
SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import inspect_variant_provenance as ivp  # noqa: E402

from rational_recipes.catalog_db import CatalogDB  # noqa: E402
from rational_recipes.scrape.pipeline_merged import (  # noqa: E402
    MergedNormalizedRow,
    MergedVariantResult,
)


class TestBuildVariantCanonicalMap:
    def test_single_form_canonicals_map_to_themselves(self) -> None:
        out = ivp.build_variant_canonical_map(["flour", "egg"])
        assert out["flour"] == "flour"
        assert out["egg"] == "egg"

    def test_fold_family_keeper_absorbs_siblings(self) -> None:
        # Variant has 'margarine' but not 'butter' — margarine is the keeper
        # of the butter family for this variant, so all sibling forms map
        # back to margarine.
        out = ivp.build_variant_canonical_map(["margarine", "flour"])
        assert out["margarine"] == "margarine"
        assert out["butter"] == "margarine"
        assert out["unsalted butter"] == "margarine"
        assert out["salted butter"] == "margarine"
        assert out["sweet butter"] == "margarine"
        # Outside-family canonical stays direct.
        assert out["flour"] == "flour"

    def test_brand_fold_crisco_to_shortening(self) -> None:
        out = ivp.build_variant_canonical_map(["shortening"])
        assert out["shortening"] == "shortening"
        assert out["crisco"] == "shortening"


class TestExtractFormKey:
    @pytest.mark.parametrize(
        "line,expected_form",
        [
            ("1 c. 70% cacao chocolate chips", "70% cacao chocolate chips"),
            ("2 cups Crisco shortening", "crisco shortening"),
            ("1/2 tsp. salt", "salt"),
            ("1 1/2 tsp. baking soda", "baking soda"),
            ("12 oz. semisweet chocolate chips", "semisweet chocolate chips"),
            # Trailing prep is stripped at the first comma.
            ("1 c. brown sugar, packed", "brown sugar"),
            # Parenthetical stripped before form extraction.
            ("1 c. nuts (optional)", "nuts"),
        ],
    )
    def test_form_key_preserves_specifiers(
        self, line: str, expected_form: str
    ) -> None:
        _qty, _unit, form = ivp.extract_form_key(line)
        assert form == expected_form


class TestAggregateRawObservations:
    """End-to-end: synthetic variant + raw lines, no LLM, no cache."""

    def test_aggregates_and_buckets_by_form(self) -> None:
        db = CatalogDB.in_memory()

        # Build a minimal variant with two recipes, two canonical ingredients.
        rows = [
            MergedNormalizedRow(
                url="http://example.com/r1",
                title="ccc",
                corpus="recipenlg",
                cells={"chocolate chips": "1 cup", "flour": "2 cups"},
                proportions={"chocolate chips": 33.0, "flour": 67.0},
            ),
            MergedNormalizedRow(
                url="http://example.com/r2",
                title="ccc",
                corpus="recipenlg",
                cells={"chocolate chips": "2 cups", "flour": "3 cups"},
                proportions={"chocolate chips": 40.0, "flour": 60.0},
            ),
            MergedNormalizedRow(
                url="http://example.com/r3",
                title="ccc",
                corpus="recipenlg",
                cells={"chocolate chips": "1 cup", "flour": "1.5 cups"},
                proportions={"chocolate chips": 35.0, "flour": 65.0},
            ),
        ]
        variant = MergedVariantResult(
            variant_title="chocolate chip cookies",
            canonical_ingredients=frozenset({"chocolate chips", "flour"}),
            cooking_methods=frozenset(),
            normalized_rows=rows,
            header_ingredients=["chocolate chips", "flour"],
        )
        db.upsert_variant(variant, l1_key="chocolate chip cookies")

        # Stitch synthetic raw lines into the test bucket. We bypass the
        # corpus join and call aggregate_raw_observations directly so the
        # smoke test doesn't depend on the RecipeNLG dataset.
        member_raw_lines = {
            "r1": [
                "1 cup 70% cacao chocolate chips",
                "2 cups all-purpose flour",
            ],
            "r2": [
                "2 cups milk chocolate chips",
                "3 cups flour",
            ],
            "r3": [
                "1 cup 70% cacao chocolate chips",
                "1 1/2 cups flour",
            ],
        }
        canonical_map = ivp.build_variant_canonical_map(
            ["chocolate chips", "flour"]
        )
        observations, _unmatched = ivp.aggregate_raw_observations(
            member_raw_lines, canonical_map, db=db
        )
        db.close()

        # 'chocolate chips' should have observations bucketed by raw form.
        cc = observations.get("chocolate chips", [])
        cc_forms = {o.form_key for o in cc}
        assert "70% cacao chocolate chips" in cc_forms
        assert "milk chocolate chips" in cc_forms

        # Flour rows should land under the 'flour' canonical regardless of
        # specifier ('all-purpose', plain).
        fl = observations.get("flour", [])
        fl_forms = {o.form_key for o in fl}
        assert "all-purpose flour" in fl_forms
        assert "flour" in fl_forms

    def test_unmatched_lines_recorded(self) -> None:
        db = CatalogDB.in_memory()
        canonical_map = ivp.build_variant_canonical_map(["flour"])
        member_raw_lines = {
            "r1": ["2 cups flour", "1 tsp. cinnamon"],
        }
        observations, unmatched = ivp.aggregate_raw_observations(
            member_raw_lines, canonical_map, db=db
        )
        db.close()
        # 'cinnamon' isn't in the variant — it's reported as unmatched.
        assert "r1" in unmatched
        assert any("cinnamon" in line for line in unmatched["r1"])
        assert any(o.form_key == "flour" for o in observations.get("flour", []))


class TestInspectVariantEndToEnd:
    """Full flow with a tiny synthetic recipes.db + RecipeNLG CSV."""

    def test_smoke(self, tmp_path: Path) -> None:
        # Tiny synthetic recipes.db with one variant, two members.
        db_path = tmp_path / "recipes.db"
        db = CatalogDB.open(db_path)
        rows = [
            MergedNormalizedRow(
                url="www.example.com/r1",
                title="cookies",
                corpus="recipenlg",
                cells={"chocolate chips": "1 cup", "flour": "2 cups"},
                proportions={"chocolate chips": 33.0, "flour": 67.0},
            ),
            MergedNormalizedRow(
                url="www.example.com/r2",
                title="cookies",
                corpus="recipenlg",
                cells={"chocolate chips": "2 cups", "flour": "3 cups"},
                proportions={"chocolate chips": 40.0, "flour": 60.0},
            ),
        ]
        variant = MergedVariantResult(
            variant_title="cookies",
            canonical_ingredients=frozenset({"chocolate chips", "flour"}),
            cooking_methods=frozenset(),
            normalized_rows=rows,
            header_ingredients=["chocolate chips", "flour"],
        )
        db.upsert_variant(variant, l1_key="cookies")
        # Pull the variant id back so we can ask the script to inspect it.
        (variant_id,) = db.connection.execute(
            "SELECT variant_id FROM variants"
        ).fetchone()
        db.close()

        # Tiny synthetic RecipeNLG CSV — same shape as the real dataset.
        csv_path = tmp_path / "rnlg.csv"
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                ["", "title", "ingredients", "directions", "link", "source", "NER"]
            )
            writer.writerow(
                [
                    "0",
                    "Cookies",
                    str(["1 cup 70% cacao chocolate chips", "2 cups flour"]),
                    "[]",
                    "www.example.com/r1",
                    "Synthetic",
                    str(["chocolate chips", "flour"]),
                ]
            )
            writer.writerow(
                [
                    "1",
                    "Cookies",
                    str(["2 cups milk chocolate chips", "3 cups flour"]),
                    "[]",
                    "www.example.com/r2",
                    "Synthetic",
                    str(["chocolate chips", "flour"]),
                ]
            )

        out = ivp.inspect_variant(
            variant_id, db_path=db_path, recipenlg_path=csv_path
        )
        # Output should mention the canonicals and at least one raw form.
        assert "chocolate chips" in out
        assert "flour" in out
        assert "70% cacao chocolate chips" in out
        assert "milk chocolate chips" in out
