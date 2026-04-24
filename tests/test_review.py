"""Tests for the variant-level review shell's pure layer (beads eco + 4lf)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from rational_recipes.review import (
    DECISIONS_SCHEMA_VERSION,
    MIN_ROWS_FOR_SPLIT,
    Decision,
    ReviewAction,
    ReviewDecisions,
    format_variant_status,
    pending_variants,
    progress_summary,
    propose_split,
    short_ingredient_list,
    summarize_variant,
)
from rational_recipes.scrape.manifest import Manifest, VariantManifestEntry


def _entry(
    vid: str = "abc123def456",
    title: str = "pannkakor",
    n: int = 3,
    csv_path: str = "pannkakor_abc123def456.csv",
    ingredients: tuple[str, ...] = ("flour", "milk"),
    cooking_methods: tuple[str, ...] = (),
    outlier_scores: tuple[float, ...] = (),
    urls: tuple[str, ...] = (),
) -> VariantManifestEntry:
    return VariantManifestEntry(
        variant_id=vid,
        title=title,
        canonical_ingredients=ingredients,
        cooking_methods=cooking_methods,
        n_recipes=n,
        csv_path=csv_path,
        source_urls=urls,
        row_outlier_scores=outlier_scores,
    )


class TestDecision:
    def test_minimal_roundtrip(self) -> None:
        d = Decision(action=ReviewAction.ACCEPT)
        restored = Decision.from_json_dict(d.to_json_dict())
        assert restored == d

    def test_full_roundtrip_preserves_note_and_timestamp(self) -> None:
        d = Decision(
            action=ReviewAction.DROP,
            note="category contamination",
            decided_at="2026-04-25T10:00:00+00:00",
        )
        restored = Decision.from_json_dict(d.to_json_dict())
        assert restored == d

    def test_empty_note_omitted_from_json(self) -> None:
        d = Decision(action=ReviewAction.ACCEPT)
        data = d.to_json_dict()
        assert "note" not in data
        assert "decided_at" not in data


class TestReviewDecisions:
    def test_record_creates_decision_with_timestamp(self) -> None:
        decisions = ReviewDecisions()
        fixed_now = datetime(2026, 4, 25, 12, 0, tzinfo=UTC)
        d = decisions.record("abc", ReviewAction.ACCEPT, now=fixed_now)
        assert d.action == ReviewAction.ACCEPT
        assert d.decided_at == "2026-04-25T12:00:00+00:00"
        assert decisions.decisions["abc"].action == ReviewAction.ACCEPT

    def test_record_note_preserved(self) -> None:
        decisions = ReviewDecisions()
        decisions.record("abc", ReviewAction.DROP, note="outlier proportions")
        assert decisions.decisions["abc"].note == "outlier proportions"

    def test_record_overwrites_previous(self) -> None:
        decisions = ReviewDecisions()
        decisions.record("abc", ReviewAction.DEFER)
        decisions.record("abc", ReviewAction.ACCEPT)
        assert decisions.decisions["abc"].action == ReviewAction.ACCEPT

    def test_is_decided_false_when_absent(self) -> None:
        assert not ReviewDecisions().is_decided("xyz")

    def test_is_decided_true_after_record(self) -> None:
        d = ReviewDecisions()
        d.record("abc", ReviewAction.ACCEPT)
        assert d.is_decided("abc")

    def test_file_roundtrip(self, tmp_path: Path) -> None:
        d = ReviewDecisions()
        d.record("abc", ReviewAction.ACCEPT, note="ok")
        d.record("def", ReviewAction.DROP)
        path = tmp_path / "decisions.json"
        d.write(path)
        reloaded = ReviewDecisions.read(path)
        assert reloaded.decisions == d.decisions

    def test_read_missing_file_returns_empty(self, tmp_path: Path) -> None:
        d = ReviewDecisions.read(tmp_path / "nonexistent.json")
        assert d.decisions == {}
        assert d.schema_version == DECISIONS_SCHEMA_VERSION

    def test_read_rejects_unknown_schema_version(self, tmp_path: Path) -> None:
        path = tmp_path / "decisions.json"
        path.write_text(
            json.dumps({"schema_version": 999, "decisions": {}}), encoding="utf-8"
        )
        with pytest.raises(ValueError, match="schema_version"):
            ReviewDecisions.read(path)

    def test_read_rejects_non_object_root(self, tmp_path: Path) -> None:
        path = tmp_path / "decisions.json"
        path.write_text("[]", encoding="utf-8")
        with pytest.raises(ValueError, match="object"):
            ReviewDecisions.read(path)

    def test_json_keys_sorted_for_stable_diffs(self, tmp_path: Path) -> None:
        d = ReviewDecisions()
        d.record("z", ReviewAction.ACCEPT)
        d.record("a", ReviewAction.DROP)
        path = tmp_path / "decisions.json"
        d.write(path)
        raw = path.read_text(encoding="utf-8")
        # "a" key appears before "z" in the serialized output.
        assert raw.index('"a"') < raw.index('"z"')


class TestPendingVariants:
    def test_excludes_decided(self) -> None:
        manifest = Manifest(
            variants=[_entry(vid="a"), _entry(vid="b"), _entry(vid="c")],
        )
        decisions = ReviewDecisions()
        decisions.record("a", ReviewAction.ACCEPT)
        pending = pending_variants(manifest, decisions)
        ids = [v.variant_id for v in pending]
        assert ids == ["b", "c"]

    def test_defer_counts_as_decided(self) -> None:
        """Defer is a deliberate act — the variant shouldn't re-appear."""
        manifest = Manifest(variants=[_entry(vid="a")])
        decisions = ReviewDecisions()
        decisions.record("a", ReviewAction.DEFER)
        assert pending_variants(manifest, decisions) == []

    def test_preserves_manifest_order(self) -> None:
        """Pending list retains the manifest's emission order (not sorted)."""
        manifest = Manifest(
            variants=[_entry(vid="z"), _entry(vid="a"), _entry(vid="m")],
        )
        decisions = ReviewDecisions()
        pending = pending_variants(manifest, decisions)
        assert [v.variant_id for v in pending] == ["z", "a", "m"]


class TestFormatVariantStatus:
    def test_pending_when_no_decision(self) -> None:
        entry = _entry(vid="x")
        assert format_variant_status(entry, ReviewDecisions()) == "pending"

    def test_plain_action_when_no_note(self) -> None:
        entry = _entry(vid="x")
        d = ReviewDecisions()
        d.record("x", ReviewAction.ACCEPT)
        assert format_variant_status(entry, d) == "accept"

    def test_note_indicator_when_present(self) -> None:
        entry = _entry(vid="x")
        d = ReviewDecisions()
        d.record("x", ReviewAction.DROP, note="contaminated")
        assert format_variant_status(entry, d) == "drop+note"


class TestShortIngredientList:
    def test_returns_all_when_under_max(self) -> None:
        entry = _entry(ingredients=("flour", "milk", "egg"))
        assert short_ingredient_list(entry, max_items=5) == "flour, milk, egg"

    def test_truncates_with_overflow_count(self) -> None:
        entry = _entry(
            ingredients=("flour", "milk", "egg", "butter", "salt", "sugar", "yeast")
        )
        assert short_ingredient_list(entry, max_items=3) == "flour, milk, egg, +4"


class TestProgressSummary:
    def test_counts_by_action(self) -> None:
        manifest = Manifest(variants=[_entry(vid=chr(ord("a") + i)) for i in range(4)])
        d = ReviewDecisions()
        d.record("a", ReviewAction.ACCEPT)
        d.record("b", ReviewAction.ACCEPT)
        d.record("c", ReviewAction.DROP)
        decided, total, breakdown = progress_summary(manifest, d)
        assert decided == 3
        assert total == 4
        assert breakdown == {"accept": 2, "drop": 1}

    def test_empty(self) -> None:
        manifest = Manifest(variants=[_entry(vid="a")])
        assert progress_summary(manifest, ReviewDecisions()) == (0, 1, {})


class TestSummarizeVariant:
    def test_computes_mean_and_stddev_per_column(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "v.csv"
        csv_path.write_text(
            "flour,milk\n100 g,200 ml\n110 g,210 ml\n90 g,190 ml\n",
            encoding="utf-8",
        )
        entry = _entry(csv_path="v.csv")
        summaries = summarize_variant(entry, csv_path)
        names = {s.name: s for s in summaries}
        assert abs(names["flour"].mean - 100.0) < 1e-9
        assert names["flour"].stddev > 0
        assert abs(names["milk"].mean - 200.0) < 1e-9

    def test_single_row_stddev_is_zero(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "v.csv"
        csv_path.write_text("flour\n100 g\n", encoding="utf-8")
        entry = _entry(csv_path="v.csv")
        summaries = summarize_variant(entry, csv_path)
        assert summaries[0].stddev == 0.0

    def test_empty_csv_returns_empty_list(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "v.csv"
        csv_path.write_text("", encoding="utf-8")
        entry = _entry(csv_path="v.csv")
        assert summarize_variant(entry, csv_path) == []

    def test_zero_cell_counts_as_zero(self, tmp_path: Path) -> None:
        """Pipeline writes '0' for ingredients absent from a recipe; treat
        that as the numeric value 0, not skipped."""
        csv_path = tmp_path / "v.csv"
        csv_path.write_text("flour,sugar\n100 g,0\n100 g,10 g\n", encoding="utf-8")
        entry = _entry(csv_path="v.csv")
        summaries = summarize_variant(entry, csv_path)
        names = {s.name: s for s in summaries}
        assert abs(names["sugar"].mean - 5.0) < 1e-9


class TestProposeSplit:
    """Bead 4lf: 2-medoid split proposal on a variant's rows."""

    def _write_csv(self, tmp_path: Path, header: str, rows: list[str]) -> Path:
        csv_path = tmp_path / "v.csv"
        csv_path.write_text(header + "\n" + "\n".join(rows) + "\n", encoding="utf-8")
        return csv_path

    def test_splits_two_clear_clusters(self, tmp_path: Path) -> None:
        """Three thin-crêpe rows (low flour) + three American-pancake rows
        (high flour) should split along that axis."""
        rows = [
            "100 g,500 ml",  # thin cluster
            "105 g,490 ml",
            "95 g,510 ml",
            "300 g,200 ml",  # thick cluster
            "290 g,210 ml",
            "310 g,190 ml",
        ]
        csv_path = self._write_csv(tmp_path, "flour,milk", rows)
        proposal = propose_split(csv_path)
        assert proposal is not None
        # Six rows partitioned into two 3-row groups.
        assert len(proposal.group_a) == 3
        assert len(proposal.group_b) == 3
        assert set(proposal.group_a) | set(proposal.group_b) == set(range(6))
        assert set(proposal.group_a) & set(proposal.group_b) == set()
        # The split should separate rows 0-2 from rows 3-5 (up to group label).
        a = set(proposal.group_a)
        b = set(proposal.group_b)
        expected_partition = [{0, 1, 2}, {3, 4, 5}]
        assert [a, b] == expected_partition or [b, a] == expected_partition

    def test_below_min_rows_returns_none(self, tmp_path: Path) -> None:
        rows = ["100 g,500 ml"] * (MIN_ROWS_FOR_SPLIT - 1)
        csv_path = self._write_csv(tmp_path, "flour,milk", rows)
        assert propose_split(csv_path) is None

    def test_all_identical_rows_returns_none(self, tmp_path: Path) -> None:
        rows = ["100 g,500 ml"] * 6
        csv_path = self._write_csv(tmp_path, "flour,milk", rows)
        assert propose_split(csv_path) is None

    def test_outlier_in_a_tight_cluster_rejected_as_singleton(
        self, tmp_path: Path
    ) -> None:
        """Five near-identical rows + one outlier: the split would be
        5+1, but 1 < MIN_SUB_GROUP_SIZE, so None is returned."""
        rows = [
            "100 g,500 ml",
            "101 g,499 ml",
            "100 g,501 ml",
            "99 g,500 ml",
            "102 g,498 ml",
            "300 g,200 ml",  # lone outlier
        ]
        csv_path = self._write_csv(tmp_path, "flour,milk", rows)
        assert propose_split(csv_path) is None

    def test_separation_reported(self, tmp_path: Path) -> None:
        rows = [
            "100 g,500 ml",
            "100 g,500 ml",
            "300 g,200 ml",
            "300 g,200 ml",
        ]
        csv_path = self._write_csv(tmp_path, "flour,milk", rows)
        proposal = propose_split(csv_path)
        assert proposal is not None
        assert proposal.separation > 0

    def test_empty_csv_returns_none(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "v.csv"
        csv_path.write_text("", encoding="utf-8")
        assert propose_split(csv_path) is None

    def test_groups_are_sorted_for_stable_diffs(self, tmp_path: Path) -> None:
        rows = [
            "100 g,500 ml",
            "300 g,200 ml",
            "100 g,500 ml",
            "300 g,200 ml",
        ]
        csv_path = self._write_csv(tmp_path, "flour,milk", rows)
        proposal = propose_split(csv_path)
        assert proposal is not None
        # Within each group, indexes are in ascending order.
        assert list(proposal.group_a) == sorted(proposal.group_a)
        assert list(proposal.group_b) == sorted(proposal.group_b)


class TestDecisionWithSplitGroups:
    """Decision's split_groups field rides along with the accept_split action."""

    def test_accept_split_roundtrip(self) -> None:
        d = Decision(
            action=ReviewAction.ACCEPT_SPLIT,
            split_groups=((0, 2, 4), (1, 3, 5)),
            decided_at="2026-04-25T10:00:00+00:00",
        )
        restored = Decision.from_json_dict(d.to_json_dict())
        assert restored.split_groups == ((0, 2, 4), (1, 3, 5))
        assert restored.action == ReviewAction.ACCEPT_SPLIT

    def test_empty_split_groups_omitted(self) -> None:
        d = Decision(action=ReviewAction.ACCEPT)
        data = d.to_json_dict()
        assert "split_groups" not in data

    def test_legacy_decision_without_field_defaults_to_empty(self) -> None:
        legacy: dict[str, object] = {
            "action": "accept",
            "note": "",
            "decided_at": "",
        }
        d = Decision.from_json_dict(legacy)
        assert d.split_groups == ()

    def test_non_list_split_groups_rejected(self) -> None:
        bad: dict[str, object] = {
            "action": "accept_split",
            "split_groups": "not a list",
        }
        with pytest.raises(ValueError, match="split_groups"):
            Decision.from_json_dict(bad)

    def test_record_with_split_groups_persists(self, tmp_path: Path) -> None:
        decisions = ReviewDecisions()
        decisions.record(
            "abc",
            ReviewAction.ACCEPT_SPLIT,
            split_groups=((0, 1), (2, 3)),
        )
        path = tmp_path / "decisions.json"
        decisions.write(path)
        reloaded = ReviewDecisions.read(path)
        assert reloaded.decisions["abc"].split_groups == ((0, 1), (2, 3))
