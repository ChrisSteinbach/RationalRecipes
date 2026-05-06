"""Tests for the DB-backed variant review shell (bead vwt.9)."""

from __future__ import annotations

from pathlib import Path

import pytest
from rich.console import Console

from rational_recipes.catalog_db import (
    CatalogDB,
    ListFilters,
)
from rational_recipes.cli import review_variants as rv
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
    """Seed variants and return {l1_key → variant_id}. Order-independent."""
    ids: dict[str, str] = {}
    for t in titles:
        v = _variant(t)
        db.upsert_variant(v, l1_key=t, base_ingredient="flour")
        ids[t] = v.variant_id
    return ids


def _silent_console() -> Console:
    """Keep test output quiet without suppressing real assertion errors."""
    return Console(quiet=True, record=False)


class TestReviewLoopDBWrites:
    def test_accept_writes_accept_status(self) -> None:
        db = CatalogDB.in_memory()
        ids = _seed(db, "pannkakor", "crepes")

        inputs = [
            rv.ReviewInput(action=rv.ReviewAction.ACCEPT),
            rv.ReviewInput(action=rv.ReviewAction.ACCEPT),
        ]
        writes = rv.review_loop(
            db,
            console=_silent_console(),
            input_source=rv.iter_input_source(inputs),
        )
        assert writes == 2

        for vid in ids.values():
            v = db.get_variant(vid)
            assert v is not None
            assert v.review_status == "accept"
            assert v.reviewed_at is not None

    def test_drop_writes_drop_and_hides_from_default_list(self) -> None:
        db = CatalogDB.in_memory()
        ids = _seed(db, "pannkakor", "crepes")

        # Drive explicitly-targeted actions rather than trusting iteration order.
        decisions: dict[str, rv.ReviewInput] = {
            ids["pannkakor"]: rv.ReviewInput(
                action=rv.ReviewAction.DROP, note="category bleed"
            ),
            ids["crepes"]: rv.ReviewInput(action=rv.ReviewAction.ACCEPT),
        }

        def targeted(v: object) -> rv.ReviewInput:
            return decisions[v.variant_id]  # type: ignore[attr-defined]

        rv.review_loop(db, console=_silent_console(), input_source=targeted)

        dropped = db.get_variant(ids["pannkakor"])
        assert dropped is not None
        assert dropped.review_status == "drop"
        assert dropped.review_note == "category bleed"

        # Default catalog filter hides the dropped variant (PWA contract).
        visible = db.list_variants()
        assert {v.variant_id for v in visible} == {ids["crepes"]}

    def test_annotate_captures_note(self) -> None:
        db = CatalogDB.in_memory()
        vid = _seed(db, "pannkakor")["pannkakor"]

        inputs = [
            rv.ReviewInput(
                action=rv.ReviewAction.ANNOTATE,
                note="needs re-scrape with stricter filter",
            ),
        ]
        rv.review_loop(
            db,
            console=_silent_console(),
            input_source=rv.iter_input_source(inputs),
        )

        v = db.get_variant(vid)
        assert v is not None
        assert v.review_status == "annotate"
        assert v.review_note == "needs re-scrape with stricter filter"

    def test_defer_does_not_write(self) -> None:
        db = CatalogDB.in_memory()
        vid = _seed(db, "pannkakor")["pannkakor"]

        inputs = [rv.ReviewInput(action=rv.ReviewAction.DEFER)]
        writes = rv.review_loop(
            db,
            console=_silent_console(),
            input_source=rv.iter_input_source(inputs),
        )
        assert writes == 0
        v = db.get_variant(vid)
        assert v is not None
        assert v.review_status is None
        assert v.reviewed_at is None

    def test_quit_halts_loop_without_writing_remaining(self) -> None:
        db = CatalogDB.in_memory()
        _seed(db, "pannkakor", "crepes", "waffles")

        inputs = [
            rv.ReviewInput(action=rv.ReviewAction.ACCEPT),
            rv.ReviewInput(action=rv.ReviewAction.QUIT),
            rv.ReviewInput(action=rv.ReviewAction.DROP),  # should not be reached
        ]
        writes = rv.review_loop(
            db,
            console=_silent_console(),
            input_source=rv.iter_input_source(inputs),
        )
        assert writes == 1

        # Exactly one variant got a decision; the other two stay NULL.
        statuses = [v.review_status for v in db.list_variants()]
        assert statuses.count(None) == 2
        assert statuses.count("accept") == 1


class TestPendingFilter:
    def test_reviewed_variants_skipped_on_next_session(self) -> None:
        db = CatalogDB.in_memory()
        ids = _seed(db, "pannkakor", "crepes")

        # Session 1: drop whichever the loop surfaces first, defer the other.
        rv.review_loop(
            db,
            console=_silent_console(),
            input_source=rv.iter_input_source(
                [
                    rv.ReviewInput(action=rv.ReviewAction.DROP),
                    rv.ReviewInput(action=rv.ReviewAction.DEFER),
                ]
            ),
        )
        dropped = {
            v.variant_id for v in db.list_variants(ListFilters(include_dropped=True))
            if v.review_status == "drop"
        }
        assert len(dropped) == 1
        assert dropped.issubset(set(ids.values()))

        # Session 2: only the still-pending variant should be offered.
        offered: list[str] = []

        def recorder(v: object) -> rv.ReviewInput:
            offered.append(v.variant_id)  # type: ignore[attr-defined]
            return rv.ReviewInput(action=rv.ReviewAction.ACCEPT)

        rv.review_loop(db, console=_silent_console(), input_source=recorder)
        # The dropped variant must not reappear; the deferred one does.
        assert len(offered) == 1
        assert offered[0] not in dropped


class TestPersistenceAcrossOpen:
    def test_drop_persists_file_roundtrip(self, tmp_path: Path) -> None:
        path = tmp_path / "recipes.db"
        db = CatalogDB.open(path)
        vid = _seed(db, "pannkakor")["pannkakor"]
        rv.review_loop(
            db,
            console=_silent_console(),
            input_source=rv.iter_input_source(
                [rv.ReviewInput(action=rv.ReviewAction.DROP, note="bad data")]
            ),
        )
        db.close()

        reopened = CatalogDB.open(path)
        try:
            # Default filter hides the dropped variant.
            assert reopened.list_variants() == []
            # And it doesn't reappear in the pending-only queue.
            pending = reopened.list_variants(ListFilters(pending_only=True))
            assert pending == []
            # But it's still present under include_dropped=True.
            all_rows = reopened.list_variants(ListFilters(include_dropped=True))
            assert len(all_rows) == 1
            assert all_rows[0].variant_id == vid
            assert all_rows[0].review_status == "drop"
            assert all_rows[0].review_note == "bad data"
        finally:
            reopened.close()


class TestApplyInput:
    def test_unknown_action_raises(self) -> None:
        db = CatalogDB.in_memory()
        vid = _seed(db, "pannkakor")["pannkakor"]
        variant = db.get_variant(vid)
        assert variant is not None
        with pytest.raises(AssertionError):
            rv._apply_input(
                db, variant, rv.ReviewInput(action=rv.ReviewAction.QUIT)
            )


class TestCLIEntrypoint:
    def test_missing_db_exits_nonzero(self, tmp_path: Path) -> None:
        missing = tmp_path / "nope.db"
        assert rv.main(["--db", str(missing)]) == 1


def _seed_persisted(path: Path, *titles: str) -> dict[str, str]:
    """Write seed variants to a real on-disk DB and close it."""
    db = CatalogDB.open(path)
    try:
        ids = _seed(db, *titles)
    finally:
        db.close()
    return ids


class TestCLISubstituteSubcommand:
    def test_substitute_records_override_and_recomputes(
        self, tmp_path: Path
    ) -> None:
        path = tmp_path / "recipes.db"
        ids = _seed_persisted(path, "pannkakor")
        vid = ids["pannkakor"]

        exit_code = rv.main(
            ["--db", str(path), "substitute", vid, "milk", "buttermilk"]
        )
        assert exit_code == 0

        db = CatalogDB.open(path)
        try:
            overrides = db.list_overrides(vid)
            assert len(overrides) == 1
            assert overrides[0].override_type == "substitute"
            assert overrides[0].payload == {"from": "milk", "to": "buttermilk"}
            stats = {s.canonical_name for s in db.get_ingredient_stats(vid)}
            assert "milk" not in stats
            assert "buttermilk" in stats
        finally:
            db.close()

    def test_substitute_with_unknown_variant_returns_error(
        self, tmp_path: Path
    ) -> None:
        path = tmp_path / "recipes.db"
        _seed_persisted(path, "pannkakor")
        exit_code = rv.main(
            ["--db", str(path), "substitute", "nope", "milk", "buttermilk"]
        )
        assert exit_code == 2


class TestCLIFilterSubcommand:
    def test_filter_excludes_recipe_and_recomputes(self, tmp_path: Path) -> None:
        path = tmp_path / "recipes.db"
        ids = _seed_persisted(path, "pannkakor")
        vid = ids["pannkakor"]

        db = CatalogDB.open(path)
        try:
            members = db.get_variant_members(vid)
            assert len(members) == 3
            target = members[0].recipe_id
        finally:
            db.close()

        exit_code = rv.main(
            ["--db", str(path), "filter", vid, target, "--reason", "outlier"]
        )
        assert exit_code == 0

        db = CatalogDB.open(path)
        try:
            v = db.get_variant(vid)
            assert v is not None
            assert v.n_recipes == 2
            # variant_members rows are preserved (excluded, not deleted).
            assert len(db.get_variant_members(vid)) == 3
            overrides = db.list_overrides(vid)
            assert overrides[0].payload["recipe_id"] == target
            assert overrides[0].payload["reason"] == "outlier"
        finally:
            db.close()

    def test_filter_unknown_recipe_returns_error(self, tmp_path: Path) -> None:
        path = tmp_path / "recipes.db"
        ids = _seed_persisted(path, "pannkakor")
        exit_code = rv.main(
            ["--db", str(path), "filter", ids["pannkakor"], "ghost"]
        )
        assert exit_code == 2


class TestCLIOverridesSubcommand:
    def test_overrides_lists_active_rows(self, tmp_path: Path) -> None:
        path = tmp_path / "recipes.db"
        ids = _seed_persisted(path, "pannkakor")
        vid = ids["pannkakor"]
        rv.main(["--db", str(path), "substitute", vid, "milk", "buttermilk"])
        # Should not error and should print a table; just assert exit 0.
        assert rv.main(["--db", str(path), "overrides", vid]) == 0

    def test_overrides_empty_returns_zero(self, tmp_path: Path) -> None:
        path = tmp_path / "recipes.db"
        ids = _seed_persisted(path, "pannkakor")
        assert rv.main(["--db", str(path), "overrides", ids["pannkakor"]]) == 0


class TestCLIClearOverrideSubcommand:
    def test_clear_existing_override(self, tmp_path: Path) -> None:
        path = tmp_path / "recipes.db"
        ids = _seed_persisted(path, "pannkakor")
        vid = ids["pannkakor"]

        db = CatalogDB.open(path)
        try:
            override_id = db.add_substitute_override(vid, "milk", "buttermilk")
        finally:
            db.close()

        exit_code = rv.main(
            ["--db", str(path), "clear-override", str(override_id)]
        )
        assert exit_code == 0

        db = CatalogDB.open(path)
        try:
            assert db.list_overrides(vid) == []
            stats = {s.canonical_name for s in db.get_ingredient_stats(vid)}
            assert "milk" in stats
            assert "buttermilk" not in stats
        finally:
            db.close()

    def test_clear_unknown_override_returns_error(self, tmp_path: Path) -> None:
        path = tmp_path / "recipes.db"
        _seed_persisted(path, "pannkakor")
        assert rv.main(["--db", str(path), "clear-override", "9999"]) == 1
