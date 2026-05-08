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


class TestApplyComponentAssign:
    """Per-drop component grouping via the editor operations layer (kfp3)."""

    def test_labels_canonical_and_recomputes_stats(self) -> None:
        db = CatalogDB.in_memory()
        ids = _seed(db, "pannkakor")
        vid = ids["pannkakor"]

        result = ops.apply_component_assign(db, vid, "flour", "dough")
        assert result.ok is True
        assert "flour" in result.message
        assert "dough" in result.message
        assert result.override_id is not None

        stats = {s.canonical_name: s for s in db.get_ingredient_stats(vid)}
        assert stats["flour"].component == "dough"
        # The other canonical stays unlabeled — partial grouping is the
        # natural intermediate state during editor work.
        assert stats["milk"].component is None

    def test_unknown_canonical_returns_error_no_override(self) -> None:
        db = CatalogDB.in_memory()
        ids = _seed(db, "pannkakor")
        vid = ids["pannkakor"]

        result = ops.apply_component_assign(db, vid, "ghost-ingredient", "x")
        assert result.ok is False
        assert "not in stats" in result.message
        # No override was recorded.
        assert db.list_overrides(vid) == []

    def test_duplicate_label_returns_error(self) -> None:
        db = CatalogDB.in_memory()
        ids = _seed(db, "pannkakor")
        vid = ids["pannkakor"]
        first = ops.apply_component_assign(db, vid, "flour", "dough")
        assert first.ok is True

        # Re-labels must go through clear+add — we don't silently overwrite.
        second = ops.apply_component_assign(db, vid, "flour", "topping")
        assert second.ok is False
        assert "already" in second.message

    def test_empty_component_returns_error(self) -> None:
        db = CatalogDB.in_memory()
        ids = _seed(db, "pannkakor")
        vid = ids["pannkakor"]
        result = ops.apply_component_assign(db, vid, "flour", "")
        assert result.ok is False
        assert "component" in result.message

    def test_unknown_variant_returns_error(self) -> None:
        db = CatalogDB.in_memory()
        result = ops.apply_component_assign(db, "ghost-vid", "flour", "dough")
        assert result.ok is False
        assert "variant_id" in result.message


class TestDescribeComponentAssign:
    def test_describes_component_assign(self) -> None:
        db = CatalogDB.in_memory()
        ids = _seed(db, "pannkakor")
        vid = ids["pannkakor"]
        db.add_component_assign_override(vid, "flour", "dough")
        ov = db.list_overrides(vid)[0]
        s = ops.describe_override(ov)
        assert "component_assign" in s
        assert "flour" in s
        assert "dough" in s

    def test_describes_component_split(self) -> None:
        """1jbk: multi-component shape renders each component with its weight."""
        db = CatalogDB.in_memory()
        ids = _seed(db, "pannkakor")
        vid = ids["pannkakor"]
        db.add_component_split_override(
            vid, "flour", [("crumble", 0.7), ("filling", 0.3)]
        )
        ov = db.list_overrides(vid)[0]
        s = ops.describe_override(ov)
        assert "component_assign" in s
        assert "flour" in s
        assert "crumble" in s
        assert "filling" in s
        # Both weights surface so the maintainer sees the proportion at
        # a glance in the active-overrides panel.
        assert "0.7" in s
        assert "0.3" in s


class TestApplyComponentSplit:
    """1jbk: split one canonical's mass across multiple components."""

    def test_split_recomputes_with_two_rows(self) -> None:
        db = CatalogDB.in_memory()
        ids = _seed(db, "pannkakor")
        vid = ids["pannkakor"]

        result = ops.apply_component_split(
            db, vid, "flour", [("crumble", 0.6), ("filling", 0.4)]
        )
        assert result.ok is True
        assert "flour" in result.message
        assert "crumble" in result.message
        assert "filling" in result.message

        stats = db.get_ingredient_stats(vid)
        flour_rows = [s for s in stats if s.canonical_name == "flour"]
        assert len(flour_rows) == 2
        assert {s.component for s in flour_rows} == {"crumble", "filling"}

    def test_bad_weights_return_error_no_override(self) -> None:
        db = CatalogDB.in_memory()
        ids = _seed(db, "pannkakor")
        vid = ids["pannkakor"]
        result = ops.apply_component_split(
            db, vid, "flour", [("crumble", 0.5), ("filling", 0.6)]
        )
        assert result.ok is False
        assert "sum to 1.0" in result.message
        assert db.list_overrides(vid) == []

    def test_unknown_canonical_returns_error(self) -> None:
        db = CatalogDB.in_memory()
        ids = _seed(db, "pannkakor")
        vid = ids["pannkakor"]
        result = ops.apply_component_split(
            db, vid, "ghost", [("a", 0.5), ("b", 0.5)]
        )
        assert result.ok is False
        assert "not in stats" in result.message


class TestListCanonicalContributors:
    """RecipeNLG-only provenance can leave non-RecipeNLG-sourced
    canonicals with no raw-line forms to surface. The editor falls back
    to listing contributing recipes via parsed_ingredients so the
    maintainer at least knows where the canonical came from."""

    def test_lists_recipes_with_titles_and_urls(self) -> None:
        db = CatalogDB.in_memory()
        ids = _seed(db, "pannkakor")
        vid = ids["pannkakor"]
        # _seed builds a 3-recipe variant with flour + milk in every
        # row; both canonicals show up in parsed_ingredients with one
        # row per (recipe, canonical).
        contribs = ops.list_canonical_contributors(db, vid, "flour")
        assert len(contribs) == 3
        for c in contribs:
            assert c.title == "pannkakor"
            assert c.url is not None
            assert c.corpus == "recipenlg"

    def test_unknown_canonical_returns_empty_list(self) -> None:
        db = CatalogDB.in_memory()
        ids = _seed(db, "pannkakor")
        vid = ids["pannkakor"]
        assert ops.list_canonical_contributors(db, vid, "ghost") == []

    def test_member_ingredients_lists_with_shares(self) -> None:
        """list_member_ingredients returns per-canonical (grams, fraction)
        for one source recipe, sorted by share descending."""
        db = CatalogDB.in_memory()
        ids = _seed(db, "pannkakor")
        vid = ids["pannkakor"]
        # Pick the first member; the seed gives every recipe flour=100g
        # (cells column) — but ``parsed_ingredients`` is what we read,
        # and the seed populates that with proportions only via the
        # MergedNormalizedRow.proportions path. Read what's there.
        member = db.get_variant_members(vid)[0]
        ingredients = ops.list_member_ingredients(db, member.recipe_id)
        assert len(ingredients) == 2
        # Sorted by share descending: milk (~71%) before flour (~29%).
        assert ingredients[0].canonical_name == "milk"
        assert ingredients[0].fraction is not None
        assert ingredients[0].fraction > ingredients[1].fraction
        # Fractions sum to ~1.0.
        total = sum(i.fraction for i in ingredients if i.fraction is not None)
        assert abs(total - 1.0) < 1e-6

    def test_member_ingredients_handles_null_quantity(self) -> None:
        """Rows with NULL quantity are surfaced last with fraction=None."""
        db = CatalogDB.in_memory()
        ids = _seed(db, "pannkakor")
        vid = ids["pannkakor"]
        member = db.get_variant_members(vid)[0]
        # Hand-insert a NULL-quantity parsed_ingredient.
        with db.connection:
            db.connection.execute(
                "INSERT INTO parsed_ingredients (recipe_id, canonical_name, "
                "quantity) VALUES (?, ?, ?)",
                (member.recipe_id, "ghost ingredient", None),
            )
        ingredients = ops.list_member_ingredients(db, member.recipe_id)
        assert ingredients[-1].canonical_name == "ghost ingredient"
        assert ingredients[-1].fraction is None
        assert ingredients[-1].grams is None

    def test_member_ingredients_unknown_recipe_empty(self) -> None:
        db = CatalogDB.in_memory()
        assert ops.list_member_ingredients(db, "no-such-recipe") == []

    def test_directions_text_carried_through_when_cached(self) -> None:
        """cw2u: ``directions_text`` is surfaced when cached on the
        recipe row so the editor's split form can render it with the
        canonical name highlighted."""
        db = CatalogDB.in_memory()
        ids = _seed(db, "pannkakor")
        vid = ids["pannkakor"]
        # The seed seeded a 3-recipe variant with placeholder titles
        # but no directions_text (recipes table column is nullable
        # and _seed doesn't populate it). Backfill one row's
        # directions to assert the helper carries it through.
        members = db.get_variant_members(vid)
        with db.connection:
            db.connection.execute(
                "UPDATE recipes SET directions_text = ? WHERE recipe_id = ?",
                (
                    "Mix the flour with milk to form a batter.",
                    members[0].recipe_id,
                ),
            )
        contribs = ops.list_canonical_contributors(db, vid, "flour")
        directed = [c for c in contribs if c.directions_text]
        # Exactly one of the three contributors has directions; the
        # other two return None.
        assert len(directed) == 1
        assert "flour" in directed[0].directions_text.lower()
        assert sum(1 for c in contribs if c.directions_text is None) == 2

    def test_only_returns_variant_members(self) -> None:
        """Two variants share a canonical name; the helper returns only
        the recipes that are members of the requested variant."""
        db = CatalogDB.in_memory()
        ids = _seed(db, "pannkakor", "crepes")
        # Each variant has its own 3 recipes with flour, so the
        # contributor list is scoped per-variant.
        for label in ("pannkakor", "crepes"):
            contribs = ops.list_canonical_contributors(
                db, ids[label], "flour"
            )
            assert len(contribs) == 3
            for c in contribs:
                # Each variant's recipes carry that variant's title; if
                # the helper leaked across variants we'd see mixed
                # titles here.
                assert c.title == label
