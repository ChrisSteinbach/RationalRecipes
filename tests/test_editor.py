"""Tests for the maintainer-editor helper layer (RationalRecipes-1t8x + xekj).

Targets the ``rational_recipes.editor.operations`` helpers — they're
where the editor's behaviour lives, and they have no Streamlit dependency
so the tests can run in the standard pytest env.

The Streamlit shell in ``scripts/editor.py`` is presentation only: it
calls these helpers, then renders the result. Anything verifiable about
the editor's correctness — that filter/substitute/canonical_reassign
writes go through the same CatalogDB helpers as the CLI, that stats
refresh, that overrides list, that errors surface as
``OperationResult(ok=False)`` — is testable here.
"""

from __future__ import annotations

import csv
from pathlib import Path

from rational_recipes.catalog_db import CatalogDB
from rational_recipes.editor import operations as ops
from rational_recipes.scrape.pipeline_merged import (
    MergedNormalizedRow,
    MergedVariantResult,
)


def _row(url: str, title: str, i: int) -> MergedNormalizedRow:
    return MergedNormalizedRow(
        url=url,
        title=title,
        corpus="recipenlg",
        cells={"flour": "100 g", "milk": "250 ml"},
        proportions={"flour": 28.5 + i * 0.01, "milk": 71.5 - i * 0.01},
    )


def _variant(title: str, n: int = 3) -> MergedVariantResult:
    rows = [_row(f"https://example.com/{title}/{i}", title, i) for i in range(n)]
    return MergedVariantResult(
        variant_title=title,
        canonical_ingredients=frozenset({"flour", "milk"}),
        cooking_methods=frozenset(),
        normalized_rows=rows,
        header_ingredients=["flour", "milk"],
    )


def _seed(db: CatalogDB, *titles: str) -> dict[str, str]:
    ids: dict[str, str] = {}
    for t in titles:
        v = _variant(t)
        db.upsert_variant(v, l1_key=t, base_ingredient="flour")
        ids[t] = v.variant_id
    return ids


class TestListVariantSummaries:
    def test_lists_all_seeded_variants(self) -> None:
        db = CatalogDB.in_memory()
        ids = _seed(db, "pannkakor", "crepes")
        summaries = ops.list_variant_summaries(db)
        assert {s.variant_id for s in summaries} == set(ids.values())
        for s in summaries:
            assert s.n_recipes == 3
            assert "flour" in s.canonical_ingredients
            assert "milk" in s.canonical_ingredients

    def test_includes_dropped_by_default(self) -> None:
        # Editor wants to see every variant including review='drop' rows
        # whose decisions may need revisiting.
        db = CatalogDB.in_memory()
        ids = _seed(db, "pannkakor")
        db.update_review_status(ids["pannkakor"], "drop")
        assert {s.variant_id for s in ops.list_variant_summaries(db)} == {
            ids["pannkakor"]
        }
        # Caller can still filter dropped out if they want.
        without = ops.list_variant_summaries(db, include_dropped=False)
        assert without == []


class TestLoadVariantDetail:
    def test_returns_none_for_unknown_variant(self) -> None:
        db = CatalogDB.in_memory()
        assert ops.load_variant_detail(db, "no-such-variant") is None

    def test_bundles_variant_stats_members_overrides(self) -> None:
        db = CatalogDB.in_memory()
        ids = _seed(db, "pannkakor")
        vid = ids["pannkakor"]
        detail = ops.load_variant_detail(db, vid)
        assert detail is not None
        assert detail.variant.variant_id == vid
        assert {s.canonical_name for s in detail.stats} == {"flour", "milk"}
        assert len(detail.members) == 3
        assert detail.overrides == []
        assert detail.excluded_recipe_ids == frozenset()

    def test_excluded_recipe_ids_reflect_active_filter_overrides(self) -> None:
        db = CatalogDB.in_memory()
        ids = _seed(db, "pannkakor")
        vid = ids["pannkakor"]
        members = db.get_variant_members(vid)
        target = members[0].recipe_id
        db.add_filter_override(vid, target, reason="outlier")
        detail = ops.load_variant_detail(db, vid)
        assert detail is not None
        assert target in detail.excluded_recipe_ids
        assert len(detail.overrides) == 1


class TestApplyFilter:
    def test_drops_source_and_recomputes(self) -> None:
        db = CatalogDB.in_memory()
        ids = _seed(db, "pannkakor")
        vid = ids["pannkakor"]
        target = db.get_variant_members(vid)[0].recipe_id

        result = ops.apply_filter(db, vid, target, reason="bad units")
        assert result.ok is True
        assert result.override_id is not None

        # n_recipes recomputes to 2 (original 3 minus the dropped one).
        v = db.get_variant(vid)
        assert v is not None
        assert v.n_recipes == 2
        # variant_members rows are preserved (filter is reversible).
        assert len(db.get_variant_members(vid)) == 3

        overrides = db.list_overrides(vid)
        assert len(overrides) == 1
        assert overrides[0].override_type == "filter"
        assert overrides[0].payload == {
            "recipe_id": target,
            "reason": "bad units",
        }

    def test_unknown_recipe_returns_error_no_override(self) -> None:
        db = CatalogDB.in_memory()
        ids = _seed(db, "pannkakor")
        vid = ids["pannkakor"]
        result = ops.apply_filter(db, vid, "ghost-recipe-id", reason="")
        assert result.ok is False
        assert "ghost-recipe-id" in result.message
        assert db.list_overrides(vid) == []

    def test_unknown_variant_returns_error(self) -> None:
        db = CatalogDB.in_memory()
        _seed(db, "pannkakor")
        result = ops.apply_filter(db, "no-such-variant", "x", reason="")
        assert result.ok is False


class TestApplySubstitute:
    def test_folds_canonical_and_recomputes(self) -> None:
        db = CatalogDB.in_memory()
        ids = _seed(db, "pannkakor")
        vid = ids["pannkakor"]
        result = ops.apply_substitute(db, vid, "milk", "buttermilk")
        assert result.ok is True
        stats = {s.canonical_name for s in db.get_ingredient_stats(vid)}
        assert "milk" not in stats
        assert "buttermilk" in stats

    def test_same_name_returns_error(self) -> None:
        db = CatalogDB.in_memory()
        ids = _seed(db, "pannkakor")
        vid = ids["pannkakor"]
        result = ops.apply_substitute(db, vid, "flour", "flour")
        assert result.ok is False
        assert db.list_overrides(vid) == []

    def test_empty_name_returns_error(self) -> None:
        db = CatalogDB.in_memory()
        ids = _seed(db, "pannkakor")
        vid = ids["pannkakor"]
        result = ops.apply_substitute(db, vid, "", "milk")
        assert result.ok is False


class TestClearOneOverride:
    def test_clear_existing_restores_baseline(self) -> None:
        db = CatalogDB.in_memory()
        ids = _seed(db, "pannkakor")
        vid = ids["pannkakor"]
        sub = ops.apply_substitute(db, vid, "milk", "buttermilk")
        assert sub.override_id is not None

        cleared = ops.clear_one_override(db, sub.override_id)
        assert cleared.ok is True

        assert db.list_overrides(vid) == []
        stats = {s.canonical_name for s in db.get_ingredient_stats(vid)}
        # Substitution was reversed — milk is back.
        assert "milk" in stats
        assert "buttermilk" not in stats

    def test_clear_unknown_returns_error(self) -> None:
        db = CatalogDB.in_memory()
        _seed(db, "pannkakor")
        result = ops.clear_one_override(db, 999_999)
        assert result.ok is False


class TestDescribeOverride:
    def test_describes_filter(self) -> None:
        db = CatalogDB.in_memory()
        ids = _seed(db, "pannkakor")
        vid = ids["pannkakor"]
        target = db.get_variant_members(vid)[0].recipe_id
        db.add_filter_override(vid, target, reason="outlier")
        ov = db.list_overrides(vid)[0]
        s = ops.describe_override(ov)
        assert "filter" in s
        assert target in s
        assert "outlier" in s

    def test_describes_substitute(self) -> None:
        db = CatalogDB.in_memory()
        ids = _seed(db, "pannkakor")
        vid = ids["pannkakor"]
        db.add_substitute_override(vid, "milk", "buttermilk")
        ov = db.list_overrides(vid)[0]
        s = ops.describe_override(ov)
        assert "substitute" in s
        assert "milk" in s and "buttermilk" in s

    def test_describes_canonical_reassign(self) -> None:
        db = CatalogDB.in_memory()
        ids = _seed(db, "pannkakor")
        vid = ids["pannkakor"]
        target = db.get_variant_members(vid)[0].recipe_id
        db.add_canonical_reassign_override(vid, target, "milk", "buttermilk")
        ov = db.list_overrides(vid)[0]
        s = ops.describe_override(ov)
        assert "canonical_reassign" in s
        assert "milk" in s
        assert "buttermilk" in s


class TestApplyCanonicalReassign:
    """Per-source canonical reassignment via the editor operations layer (xekj)."""

    def test_records_override_and_recomputes_stats(self) -> None:
        db = CatalogDB.in_memory()
        ids = _seed(db, "pannkakor")
        vid = ids["pannkakor"]
        target = db.get_variant_members(vid)[0].recipe_id

        result = ops.apply_canonical_reassign(
            db, vid, target, "milk", "buttermilk"
        )
        assert result.ok is True
        assert result.override_id is not None
        assert "buttermilk" in result.message

        overrides = db.list_overrides(vid)
        assert len(overrides) == 1
        assert overrides[0].override_type == "canonical_reassign"
        assert overrides[0].payload == {
            "recipe_id": target,
            "raw_text": "milk",
            "new_canonical": "buttermilk",
        }
        # Stats recomputed: the targeted recipe's milk grams now contribute
        # to a 'buttermilk' canonical, so 'buttermilk' is now in the stat set.
        stats = {s.canonical_name for s in db.get_ingredient_stats(vid)}
        assert "buttermilk" in stats

    def test_unknown_variant_returns_error(self) -> None:
        db = CatalogDB.in_memory()
        _seed(db, "pannkakor")
        result = ops.apply_canonical_reassign(
            db, "no-such-variant", "x", "milk", "buttermilk"
        )
        assert result.ok is False
        assert "no-such-variant" in result.message

    def test_recipe_not_a_member_returns_error(self) -> None:
        db = CatalogDB.in_memory()
        ids = _seed(db, "pannkakor")
        vid = ids["pannkakor"]
        result = ops.apply_canonical_reassign(
            db, vid, "ghost-recipe", "milk", "buttermilk"
        )
        assert result.ok is False
        assert "ghost-recipe" in result.message
        assert db.list_overrides(vid) == []

    def test_raw_text_unresolvable_returns_error(self) -> None:
        # 'salt' isn't in any of this variant's parsed_ingredients rows
        # (the seed only writes 'flour' and 'milk'), so the override is
        # rejected before insert.
        db = CatalogDB.in_memory()
        ids = _seed(db, "pannkakor")
        vid = ids["pannkakor"]
        target = db.get_variant_members(vid)[0].recipe_id
        result = ops.apply_canonical_reassign(
            db, vid, target, "salt", "kosher salt"
        )
        assert result.ok is False
        assert db.list_overrides(vid) == []

    def test_empty_raw_text_returns_error(self) -> None:
        db = CatalogDB.in_memory()
        ids = _seed(db, "pannkakor")
        vid = ids["pannkakor"]
        target = db.get_variant_members(vid)[0].recipe_id
        result = ops.apply_canonical_reassign(db, vid, target, "", "milk")
        assert result.ok is False


class TestLoadProvenance:
    """Smoke test for the editor's provenance reader on synthetic corpus data."""

    def _write_synthetic_csv(self, path: Path, urls: list[str]) -> None:
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                ["", "title", "ingredients", "directions", "link", "source", "NER"]
            )
            for i, url in enumerate(urls):
                writer.writerow(
                    [
                        str(i),
                        "Pannkakor",
                        str(["2 cups all-purpose flour", "1 cup milk"]),
                        "[]",
                        url,
                        "Synthetic",
                        str(["flour", "milk"]),
                    ]
                )

    def test_returns_none_for_unknown_variant(self, tmp_path: Path) -> None:
        db = CatalogDB.in_memory()
        prov = ops.load_provenance(db, "no-such", tmp_path / "absent.csv")
        assert prov is None

    def test_returns_provenance_bundle_for_known_variant(
        self, tmp_path: Path
    ) -> None:
        db = CatalogDB.in_memory()
        ids = _seed(db, "pannkakor")
        vid = ids["pannkakor"]
        urls = [m.url for m in db.get_variant_members(vid) if m.url]
        csv_path = tmp_path / "rnlg.csv"
        self._write_synthetic_csv(csv_path, urls)

        prov = ops.load_provenance(db, vid, csv_path)
        assert prov is not None
        assert prov.variant_id == vid
        assert prov.n_recipenlg_hit > 0
        # 'flour' and 'milk' are the variant's canonicals — both surface.
        canonicals = {c.canonical for c in prov.canonicals}
        assert "flour" in canonicals
        assert "milk" in canonicals

    def test_missing_corpus_returns_empty_provenance(
        self, tmp_path: Path
    ) -> None:
        # full_dataset.csv is gitignored. The editor must render an empty
        # state, not crash, when the corpus isn't on disk.
        db = CatalogDB.in_memory()
        ids = _seed(db, "pannkakor")
        vid = ids["pannkakor"]
        prov = ops.load_provenance(db, vid, tmp_path / "missing.csv")
        assert prov is not None
        assert prov.n_recipenlg_hit == 0
        assert all(c.total_observations == 0 for c in prov.canonicals)
