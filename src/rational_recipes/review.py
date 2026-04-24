"""Variant-level review decisions for the merged-pipeline output (bead eco).

Persistence + filtering layer for the terminal review shell. The
script in ``scripts/review_variants.py`` is the interactive UI; this
module owns the pure pieces that are unit-testable: the decisions
JSON sidecar, the pending-variants filter, and the per-variant
summary that drives the drill-in display.

Decision file format (versioned, JSON, keyed by variant_id)::

    {
      "schema_version": 1,
      "decisions": {
        "3fa8c91d7e42": {
          "action": "accept",         # accept | drop | defer
          "note": "validated 2026-04-25",
          "decided_at": "2026-04-25T10:30:00Z"
        },
        ...
      }
    }

A variant_id present in ``decisions`` with action ``defer`` still
counts as decided for skip purposes — defer is a deliberate act, not
the absence of one. ``annotate`` is not a separate action: any action
may carry a free-text note.
"""

from __future__ import annotations

import json
import math
import statistics as py_stats
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Literal, cast

from rational_recipes.scrape.manifest import Manifest, VariantManifestEntry

DECISIONS_SCHEMA_VERSION = 1


class ReviewAction(StrEnum):
    ACCEPT = "accept"
    DROP = "drop"
    DEFER = "defer"
    ACCEPT_SPLIT = "accept_split"


ActionLiteral = Literal["accept", "drop", "defer", "accept_split"]


@dataclass(frozen=True, slots=True)
class Decision:
    action: ReviewAction
    note: str = ""
    decided_at: str = ""
    # For ``accept_split``: list of row-index groups (0-based) describing how
    # the variant's rows should be partitioned into sub-variants. Empty tuple
    # for non-split actions. A later materialization stage re-emits each
    # group as its own variant with a fresh variant_id.
    split_groups: tuple[tuple[int, ...], ...] = ()

    def to_json_dict(self) -> dict[str, object]:
        out: dict[str, object] = {"action": self.action.value}
        if self.note:
            out["note"] = self.note
        if self.decided_at:
            out["decided_at"] = self.decided_at
        if self.split_groups:
            out["split_groups"] = [list(g) for g in self.split_groups]
        return out

    @classmethod
    def from_json_dict(cls, data: dict[str, object]) -> Decision:
        raw_groups = data.get("split_groups", [])
        if not isinstance(raw_groups, list):
            raise ValueError("split_groups must be a list of lists when present")
        split_groups = tuple(
            tuple(cast(int, i) for i in cast(list[object], g)) for g in raw_groups
        )
        return cls(
            action=ReviewAction(str(data["action"])),
            note=str(data.get("note", "")),
            decided_at=str(data.get("decided_at", "")),
            split_groups=split_groups,
        )


@dataclass
class ReviewDecisions:
    """All review decisions for a manifest, keyed by variant_id."""

    decisions: dict[str, Decision] = field(default_factory=dict)
    schema_version: int = DECISIONS_SCHEMA_VERSION

    def is_decided(self, variant_id: str) -> bool:
        return variant_id in self.decisions

    def record(
        self,
        variant_id: str,
        action: ReviewAction,
        note: str = "",
        *,
        split_groups: tuple[tuple[int, ...], ...] = (),
        now: datetime | None = None,
    ) -> Decision:
        ts = (now or datetime.now(UTC)).isoformat()
        decision = Decision(
            action=action,
            note=note,
            decided_at=ts,
            split_groups=split_groups,
        )
        self.decisions[variant_id] = decision
        return decision

    def to_json_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "decisions": {
                vid: d.to_json_dict() for vid, d in sorted(self.decisions.items())
            },
        }

    @classmethod
    def from_json_dict(cls, data: dict[str, object]) -> ReviewDecisions:
        version = cast(int, data.get("schema_version", 0))
        if version != DECISIONS_SCHEMA_VERSION:
            raise ValueError(
                f"Unsupported decisions schema_version {version}; "
                f"expected {DECISIONS_SCHEMA_VERSION}"
            )
        raw = data.get("decisions", {})
        if not isinstance(raw, dict):
            raise ValueError("decisions must be an object keyed by variant_id")
        return cls(
            decisions={
                str(vid): Decision.from_json_dict(cast(dict[str, object], v))
                for vid, v in raw.items()
            },
            schema_version=version,
        )

    def write(self, path: Path) -> None:
        path.write_text(
            json.dumps(self.to_json_dict(), indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

    @classmethod
    def read(cls, path: Path) -> ReviewDecisions:
        if not path.exists():
            return cls()
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError("decisions file root must be an object")
        return cls.from_json_dict(data)


def pending_variants(
    manifest: Manifest,
    decisions: ReviewDecisions,
) -> list[VariantManifestEntry]:
    """Variants in manifest order, excluding any with a recorded decision."""
    return [v for v in manifest.variants if not decisions.is_decided(v.variant_id)]


@dataclass(frozen=True, slots=True)
class IngredientSummary:
    name: str
    mean: float
    stddev: float


def summarize_variant(
    entry: VariantManifestEntry, csv_path: Path
) -> list[IngredientSummary]:
    """Per-ingredient mean + stddev (proportion, g per 100g) for the drill-in.

    Reads the per-variant CSV directly rather than re-running the full
    rr-stats pipeline, because at this stage we need a quick at-a-glance
    summary, not normalized ratios. The CSV's "value unit" cells are
    parsed numerically — units within a column are typically consistent
    on this stage's output, but mixed units are tolerated by extracting
    the numeric value only (an approximation suitable for triage view).
    """
    import csv as _csv

    with csv_path.open(encoding="utf-8") as f:
        reader = _csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            return []
        rows: list[list[str]] = list(reader)

    summaries: list[IngredientSummary] = []
    for col_idx, ingredient in enumerate(header):
        values: list[float] = []
        for row in rows:
            if col_idx >= len(row):
                continue
            cell = row[col_idx].strip()
            if not cell or cell == "0":
                values.append(0.0)
                continue
            try:
                values.append(float(cell.split()[0]))
            except (ValueError, IndexError):
                continue
        if not values:
            continue
        mean = py_stats.fmean(values)
        stddev = py_stats.stdev(values) if len(values) > 1 else 0.0
        summaries.append(IngredientSummary(name=ingredient, mean=mean, stddev=stddev))
    return summaries


def format_variant_status(
    entry: VariantManifestEntry, decisions: ReviewDecisions
) -> str:
    """One-word status for the list view."""
    if entry.variant_id not in decisions.decisions:
        return "pending"
    d = decisions.decisions[entry.variant_id]
    if d.note:
        return f"{d.action.value}+note"
    return d.action.value


def short_ingredient_list(entry: VariantManifestEntry, max_items: int = 5) -> str:
    """Trimmed ingredient summary for the list view."""
    items = list(entry.canonical_ingredients[:max_items])
    if len(entry.canonical_ingredients) > max_items:
        items.append(f"+{len(entry.canonical_ingredients) - max_items}")
    return ", ".join(items)


def progress_summary(
    manifest: Manifest, decisions: ReviewDecisions
) -> tuple[int, int, dict[str, int]]:
    """(decided_count, total_count, action_breakdown)."""
    breakdown: dict[str, int] = {}
    for entry in manifest.variants:
        d = decisions.decisions.get(entry.variant_id)
        if d:
            breakdown[d.action.value] = breakdown.get(d.action.value, 0) + 1
    decided_count = sum(breakdown.values())
    return decided_count, len(manifest.variants), breakdown


# --- L3 split proposal (bead 4lf) ---


MIN_ROWS_FOR_SPLIT = 4
"""Don't propose splits for variants with fewer than 4 rows — 2+2 is the
smallest split where each sub-variant has enough signal to aggregate,
and anything smaller is better served by individual keep/drop decisions."""

MIN_SUB_GROUP_SIZE = 2
"""Sub-groups below this count are rejected. A singleton sub-variant
would itself fail L3's min-variant-size policy downstream."""


@dataclass(frozen=True, slots=True)
class SplitProposal:
    """Two disjoint sub-groups of row indexes proposed for a variant."""

    group_a: tuple[int, ...]
    group_b: tuple[int, ...]
    separation: float
    """Euclidean distance between the two sub-group centroids (higher =
    more proportion-distinct) — informational, for the reviewer's
    judgement."""

    @property
    def groups(self) -> tuple[tuple[int, ...], tuple[int, ...]]:
        return (self.group_a, self.group_b)


def _proportion_vectors_from_csv(csv_path: Path) -> list[list[float]]:
    """Read variant CSV and return per-row proportion vectors.

    Assumes consistent units within each column (generally true for
    pipeline output). Treats missing/unparseable cells as 0. Each row is
    normalized to sum-to-100 so absolute scale differences (small vs big
    recipe) don't drown out proportion differences.
    """
    import csv as _csv

    with csv_path.open(encoding="utf-8") as f:
        reader = _csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            return []
        raw_rows = list(reader)

    ncols = len(header)
    vectors: list[list[float]] = []
    for row in raw_rows:
        vec = [0.0] * ncols
        for i in range(min(ncols, len(row))):
            cell = row[i].strip()
            if not cell or cell == "0":
                continue
            try:
                vec[i] = float(cell.split()[0])
            except (ValueError, IndexError):
                continue
        total = sum(vec)
        if total > 0:
            vectors.append([v * 100.0 / total for v in vec])
    return vectors


def _euclidean(a: Sequence[float], b: Sequence[float]) -> float:
    return math.sqrt(sum((x - y) * (x - y) for x, y in zip(a, b, strict=True)))


def _centroid(
    vectors: Sequence[Sequence[float]], indexes: Sequence[int]
) -> list[float]:
    if not indexes:
        return []
    ncols = len(vectors[indexes[0]])
    totals = [0.0] * ncols
    for i in indexes:
        for j, v in enumerate(vectors[i]):
            totals[j] += v
    return [t / len(indexes) for t in totals]


def propose_split(csv_path: Path) -> SplitProposal | None:
    """Propose a 2-way split of a variant's rows using 2-medoid
    clustering in proportion space.

    Algorithm:
    1. Build proportion vectors from the CSV (sum-to-100 per row).
    2. If fewer than ``MIN_ROWS_FOR_SPLIT`` rows, return None.
    3. Find the farthest-apart pair of rows — these are the two
       medoid seeds.
    4. Assign each remaining row to whichever seed it is closer to.
    5. If either sub-group has fewer than ``MIN_SUB_GROUP_SIZE``
       rows, return None — the split isn't viable.
    6. Return a SplitProposal with the two row-index groups and the
       distance between their centroids.

    The function is a heuristic, not a classifier — it surfaces a
    candidate split for the reviewer to accept or reject. A ``None``
    return does not assert the variant is homogeneous; it means this
    simple method did not find a viable 2-way split.
    """
    vectors = _proportion_vectors_from_csv(csv_path)
    n = len(vectors)
    if n < MIN_ROWS_FOR_SPLIT:
        return None

    # Farthest pair search — O(n²), fine at review-time scale (N << 100).
    best_i, best_j = 0, 1
    best_d = -1.0
    for i in range(n):
        for j in range(i + 1, n):
            d = _euclidean(vectors[i], vectors[j])
            if d > best_d:
                best_d = d
                best_i, best_j = i, j

    if best_d <= 0:
        return None  # all identical → no split

    group_a_list: list[int] = [best_i]
    group_b_list: list[int] = [best_j]
    for k in range(n):
        if k == best_i or k == best_j:
            continue
        d_a = _euclidean(vectors[k], vectors[best_i])
        d_b = _euclidean(vectors[k], vectors[best_j])
        if d_a <= d_b:
            group_a_list.append(k)
        else:
            group_b_list.append(k)

    if len(group_a_list) < MIN_SUB_GROUP_SIZE or len(group_b_list) < MIN_SUB_GROUP_SIZE:
        return None

    centroid_a = _centroid(vectors, group_a_list)
    centroid_b = _centroid(vectors, group_b_list)
    separation = _euclidean(centroid_a, centroid_b)

    return SplitProposal(
        group_a=tuple(sorted(group_a_list)),
        group_b=tuple(sorted(group_b_list)),
        separation=separation,
    )


def parse_iterable_actions(
    seq: Iterable[str],
) -> list[ReviewAction]:
    """Used by the CLI script when an action queue is preset (testing)."""
    out: list[ReviewAction] = []
    for raw in seq:
        out.append(ReviewAction(raw))
    return out
