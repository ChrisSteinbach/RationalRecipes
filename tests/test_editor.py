"""Tests for the maintainer-editor helper layer (RationalRecipes-1t8x).

Targets the ``rational_recipes.editor.operations`` helpers — they're
where the editor's behaviour lives, and they have no Streamlit dependency
so the tests can run in the standard pytest env.

The Streamlit shell in ``scripts/editor.py`` is presentation only: it
calls these helpers, then renders the result. Anything verifiable about
the editor's correctness — that filter/substitute writes go through the
same CatalogDB helpers as the CLI, that stats refresh, that overrides
list, that errors surface as ``OperationResult(ok=False)`` — is testable
here.
"""

from __future__ import annotations

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
