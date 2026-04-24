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
import statistics as py_stats
from collections.abc import Iterable
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


ActionLiteral = Literal["accept", "drop", "defer"]


@dataclass(frozen=True, slots=True)
class Decision:
    action: ReviewAction
    note: str = ""
    decided_at: str = ""

    def to_json_dict(self) -> dict[str, object]:
        out: dict[str, object] = {"action": self.action.value}
        if self.note:
            out["note"] = self.note
        if self.decided_at:
            out["decided_at"] = self.decided_at
        return out

    @classmethod
    def from_json_dict(cls, data: dict[str, object]) -> Decision:
        return cls(
            action=ReviewAction(str(data["action"])),
            note=str(data.get("note", "")),
            decided_at=str(data.get("decided_at", "")),
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
        now: datetime | None = None,
    ) -> Decision:
        ts = (now or datetime.now(UTC)).isoformat()
        decision = Decision(action=action, note=note, decided_at=ts)
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


def parse_iterable_actions(
    seq: Iterable[str],
) -> list[ReviewAction]:
    """Used by the CLI script when an action queue is preset (testing)."""
    out: list[ReviewAction] = []
    for raw in seq:
        out.append(ReviewAction(raw))
    return out
