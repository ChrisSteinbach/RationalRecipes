"""Maintainer-editor helpers: thin, testable wrappers around CatalogDB.

The Streamlit shell in ``scripts/editor.py`` uses these helpers for every
read and write. Keeping the data plumbing here (and out of the Streamlit
module) means the editor's logic can be unit-tested without spinning up
the streamlit runtime, and any future surface (FastAPI+HTMX, Tauri, etc.)
can re-use the same helpers.

Write paths are pass-throughs onto ``CatalogDB.add_filter_override`` /
``CatalogDB.add_substitute_override`` / ``CatalogDB.clear_override``, so
filter / substitute decisions made via the editor produce the same
``variant_overrides`` rows and trigger the same ``_recompute_stats_for_variant``
as the CLI in ``scripts/review_variants.py``. No parallel implementation,
no recompute drift.
"""

from __future__ import annotations

from dataclasses import dataclass

from rational_recipes.catalog_db import (
    CatalogDB,
    IngredientStatsRow,
    ListFilters,
    VariantMemberRow,
    VariantOverrideRow,
    VariantRow,
)


@dataclass(frozen=True, slots=True)
class VariantSummary:
    """One row in the editor's variant-list table."""

    variant_id: str
    title: str
    n_recipes: int
    canonical_ingredients: tuple[str, ...]
    review_status: str | None


@dataclass(frozen=True, slots=True)
class VariantDetail:
    """Everything the detail view renders for one variant."""

    variant: VariantRow
    stats: list[IngredientStatsRow]
    members: list[VariantMemberRow]
    overrides: list[VariantOverrideRow]
    excluded_recipe_ids: frozenset[str]


@dataclass(frozen=True, slots=True)
class OperationResult:
    """Outcome of a write — surfaced to the UI as a status message."""

    ok: bool
    message: str
    override_id: int | None = None


def list_variant_summaries(
    db: CatalogDB, *, include_dropped: bool = True
) -> list[VariantSummary]:
    """Variant rows for the sidebar list.

    Includes dropped variants by default — the maintainer editor wants
    to see everything, including review='drop' rows whose decisions may
    need re-litigation. The CLI's review loop hides them; this surface
    doesn't.
    """
    filters = ListFilters(include_dropped=include_dropped)
    rows = db.list_variants(filters)
    return [
        VariantSummary(
            variant_id=r.variant_id,
            title=r.display_title or r.normalized_title,
            n_recipes=r.n_recipes,
            canonical_ingredients=r.canonical_ingredient_set,
            review_status=r.review_status,
        )
        for r in rows
    ]


def load_variant_detail(
    db: CatalogDB, variant_id: str
) -> VariantDetail | None:
    """Bundle every read needed to render the detail view."""
    variant = db.get_variant(variant_id)
    if variant is None:
        return None
    overrides = db.list_overrides(variant_id)
    excluded = frozenset(
        ov.payload["recipe_id"]
        for ov in overrides
        if ov.override_type == "filter"
    )
    return VariantDetail(
        variant=variant,
        stats=db.get_ingredient_stats(variant_id),
        members=db.get_variant_members(variant_id),
        overrides=overrides,
        excluded_recipe_ids=excluded,
    )


def apply_filter(
    db: CatalogDB,
    variant_id: str,
    recipe_id: str,
    reason: str,
) -> OperationResult:
    """Drop one source recipe via add_filter_override + recompute."""
    try:
        override_id = db.add_filter_override(
            variant_id, recipe_id, reason=reason
        )
    except ValueError as exc:
        return OperationResult(ok=False, message=str(exc))
    detail = f" — {reason}" if reason else ""
    return OperationResult(
        ok=True,
        message=f"Excluded {recipe_id}{detail}.",
        override_id=override_id,
    )


def apply_substitute(
    db: CatalogDB,
    variant_id: str,
    from_name: str,
    to_name: str,
) -> OperationResult:
    """Fold canonical X into Y via add_substitute_override + recompute."""
    try:
        override_id = db.add_substitute_override(
            variant_id, from_name, to_name
        )
    except ValueError as exc:
        return OperationResult(ok=False, message=str(exc))
    return OperationResult(
        ok=True,
        message=f"Folded {from_name} → {to_name}.",
        override_id=override_id,
    )


def clear_one_override(db: CatalogDB, override_id: int) -> OperationResult:
    """Remove a single override row + recompute the affected variant."""
    if db.clear_override(override_id):
        return OperationResult(
            ok=True,
            message=f"Cleared override {override_id}.",
            override_id=override_id,
        )
    return OperationResult(
        ok=False,
        message=f"No override with id {override_id}.",
    )


def describe_override(override: VariantOverrideRow) -> str:
    """One-line human-readable summary for the active-overrides panel."""
    p = override.payload
    if override.override_type == "filter":
        reason = p.get("reason") or ""
        suffix = f" ({reason})" if reason else ""
        return f"filter: drop {p.get('recipe_id', '?')}{suffix}"
    if override.override_type == "substitute":
        return f"substitute: {p.get('from', '?')} → {p.get('to', '?')}"
    if override.override_type == "canonical_reassign":
        return (
            f"canonical_reassign: recipe={p.get('recipe_id', '?')} "
            f"{p.get('raw_text', '?')!r} → {p.get('new_canonical', '?')}"
        )
    return f"{override.override_type}: {p}"
