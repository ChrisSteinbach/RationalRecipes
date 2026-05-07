"""Tests for the shared provenance module (RationalRecipes-xekj).

The domain logic was extracted from ``scripts/inspect_variant_provenance.py``
(RationalRecipes-4rgy spike) so the maintainer editor can surface the same
breakdown without depending on a script-relative ``sys.path`` insert.
``tests/test_inspect_variant_provenance.py`` keeps the script-CLI smoke test;
this file targets the shared module directly.
"""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

from rational_recipes.catalog_db import CatalogDB
from rational_recipes.provenance import (
    FormSummary,
    RawObservation,
    aggregate_raw_observations,
    build_variant_canonical_map,
    extract_form_key,
    load_variant_provenance,
    summarize_observations,
)
from rational_recipes.scrape.pipeline_merged import (
    MergedNormalizedRow,
    MergedVariantResult,
)


class TestBuildVariantCanonicalMap:
    def test_single_form_canonicals_map_to_themselves(self) -> None:
        out = build_variant_canonical_map(["flour", "egg"])
        assert out["flour"] == "flour"
        assert out["egg"] == "egg"

    def test_fold_family_keeper_absorbs_siblings(self) -> None:
        out = build_variant_canonical_map(["margarine", "flour"])
        assert out["margarine"] == "margarine"
        assert out["butter"] == "margarine"
        assert out["flour"] == "flour"


class TestExtractFormKey:
    @pytest.mark.parametrize(
        "line,expected_form",
        [
            ("1 c. 70% cacao chocolate chips", "70% cacao chocolate chips"),
            ("12 oz. semisweet chocolate chips", "semisweet chocolate chips"),
            ("1 c. brown sugar, packed", "brown sugar"),
        ],
    )
    def test_form_key_preserves_specifiers(
        self, line: str, expected_form: str
    ) -> None:
        _qty, _unit, form = extract_form_key(line)
        assert form == expected_form


class TestAggregateRawObservations:
    def test_observations_carry_recipe_id(self) -> None:
        # The xekj editor needs the per-source recipe_id on each observation
        # so the canonical-reassignment UI can wire (recipe_id, raw_text)
        # straight into add_canonical_reassign_override. The 4rgy spike
        # discarded recipe_id; this test pins the new behavior.
        db = CatalogDB.in_memory()
        canonical_map = build_variant_canonical_map(["flour"])
        member_raw_lines = {
            "r1": ["2 cups all-purpose flour"],
            "r2": ["1 cup flour"],
        }
        observations, unmatched = aggregate_raw_observations(
            member_raw_lines, canonical_map, db=db
        )
        db.close()
        assert unmatched == {}
        flour_obs = observations.get("flour", [])
        recipe_ids = {o.recipe_id for o in flour_obs}
        assert recipe_ids == {"r1", "r2"}

    def test_unmatched_lines_recorded(self) -> None:
        db = CatalogDB.in_memory()
        canonical_map = build_variant_canonical_map(["flour"])
        observations, unmatched = aggregate_raw_observations(
            {"r1": ["2 cups flour", "1 tsp. cinnamon"]},
            canonical_map,
            db=db,
        )
        db.close()
        assert "r1" in unmatched
        assert any("cinnamon" in line for line in unmatched["r1"])
        assert any(
            o.form_key == "flour" for o in observations.get("flour", [])
        )


class TestSummarizeObservations:
    def test_groups_by_form_key_and_sorts_by_count_desc(self) -> None:
        observations = {
            "chocolate chips": [
                RawObservation("r1", "1 c chocolate chips", "chocolate chips", 175.0),
                RawObservation("r2", "2 c chocolate chips", "chocolate chips", 350.0),
                RawObservation("r3", "1 c chocolate chips", "chocolate chips", None),
                RawObservation(
                    "r4", "1 c milk chocolate chips", "milk chocolate chips", 175.0
                ),
            ],
        }
        out = summarize_observations(observations, ["chocolate chips"])
        assert len(out) == 1
        canon = out[0]
        assert canon.canonical == "chocolate chips"
        assert canon.total_observations == 4
        # Sorted by count desc: 'chocolate chips' (3) before 'milk chocolate chips' (1)
        assert [f.form_key for f in canon.forms] == [
            "chocolate chips",
            "milk chocolate chips",
        ]
        first = canon.forms[0]
        assert isinstance(first, FormSummary)
        assert first.count == 3
        assert first.n_with_grams == 2
        # Mean is over the 2 obs with grams (175 + 350) / 2.
        assert first.mean_grams == pytest.approx(262.5)
        assert set(first.recipe_ids) == {"r1", "r2", "r3"}

    def test_canonical_with_no_observations_present_with_empty_forms(self) -> None:
        out = summarize_observations({}, ["flour", "milk"])
        assert [c.canonical for c in out] == ["flour", "milk"]
        assert all(c.total_observations == 0 for c in out)
        assert all(c.forms == [] for c in out)

    def test_unspecified_form_key_relabeled(self) -> None:
        observations = {
            "salt": [
                RawObservation("r1", "salt to taste", "", None),
            ]
        }
        out = summarize_observations(observations, ["salt"])
        assert out[0].forms[0].form_key == "(unspecified)"


class TestLoadVariantProvenance:
    """End-to-end synthetic flow: db + tiny RecipeNLG CSV."""

    def _seed_variant(self, db: CatalogDB) -> str:
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
        (vid,) = db.connection.execute(
            "SELECT variant_id FROM variants"
        ).fetchone()
        return vid

    def _write_csv(self, csv_path: Path) -> None:
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

    def test_returns_none_for_unknown_variant(self, tmp_path: Path) -> None:
        db = CatalogDB.open(tmp_path / "recipes.db")
        try:
            assert load_variant_provenance(
                db, "no-such-id", tmp_path / "absent.csv"
            ) is None
        finally:
            db.close()

    def test_full_flow_buckets_by_form_and_canonical(
        self, tmp_path: Path
    ) -> None:
        db = CatalogDB.open(tmp_path / "recipes.db")
        try:
            vid = self._seed_variant(db)
            csv_path = tmp_path / "rnlg.csv"
            self._write_csv(csv_path)
            prov = load_variant_provenance(db, vid, csv_path)
        finally:
            db.close()

        assert prov is not None
        assert prov.variant_id == vid
        assert prov.n_recipenlg_members == 2
        assert prov.n_recipenlg_hit == 2
        assert prov.n_corpus_members == 2

        canonicals = {c.canonical: c for c in prov.canonicals}
        cc = canonicals["chocolate chips"]
        cc_forms = {f.form_key for f in cc.forms}
        assert "70% cacao chocolate chips" in cc_forms
        assert "milk chocolate chips" in cc_forms

        flour = canonicals["flour"]
        flour_forms = {f.form_key for f in flour.forms}
        assert "flour" in flour_forms

    def test_missing_corpus_returns_empty_provenance_not_crash(
        self, tmp_path: Path
    ) -> None:
        # Editor surface must show an empty state when full_dataset.csv is
        # gitignored / absent rather than crashing the page.
        db = CatalogDB.open(tmp_path / "recipes.db")
        try:
            vid = self._seed_variant(db)
            prov = load_variant_provenance(
                db, vid, tmp_path / "does-not-exist.csv"
            )
        finally:
            db.close()
        assert prov is not None
        assert prov.n_recipenlg_hit == 0
        assert prov.n_corpus_members == 0
        assert all(c.total_observations == 0 for c in prov.canonicals)
