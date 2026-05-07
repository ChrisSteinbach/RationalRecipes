"""Streamlit maintainer editor for ``recipes.db`` (RationalRecipes-1t8x).

Localhost, single-maintainer workbench for filter / substitute operations
on extracted variants. Reads + writes ``recipes.db`` directly via
``CatalogDB`` — no HTTP layer, no JSON cache, no sql.js.

Launch:

    streamlit run scripts/editor.py -- --db output/catalog/recipes.db

Filter / substitute writes go through the same ``add_filter_override``
and ``add_substitute_override`` helpers as ``scripts/review_variants.py``,
so an override applied here is indistinguishable from one applied via
the CLI. ``_recompute_stats_for_variant`` runs eagerly inside each helper
— after a write the in-process ``CatalogDB`` already holds the updated
``variant_ingredient_stats``; the UI just needs to re-read.

This module is the **presentation layer only**. The data plumbing — list
the variants, load a detail, apply / clear overrides — lives in
``rational_recipes.editor.operations`` and is tested separately so the
core logic doesn't depend on the Streamlit runtime.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

from rational_recipes.catalog_db import CatalogDB
from rational_recipes.editor import operations as ops

DEFAULT_DB_PATH = Path("output/catalog/recipes.db")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Maintainer editor for recipes.db")
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB_PATH,
        help=f"Path to recipes.db (default: {DEFAULT_DB_PATH})",
    )
    return parser.parse_args(argv)


def _streamlit_argv() -> list[str]:
    """Args after the ``--`` separator that ``streamlit run`` passes through."""
    if "--" in sys.argv:
        idx = sys.argv.index("--")
        return sys.argv[idx + 1 :]
    # Fallback when invoked as a plain script (e.g. ``python scripts/editor.py``):
    # treat everything after the script name as our args.
    return sys.argv[1:]


def _open_db(path: Path) -> CatalogDB:
    if not path.exists():
        msg = f"Catalog DB not found: {path}"
        raise SystemExit(msg)
    return CatalogDB.open(path)


def _format_summary_label(s: ops.VariantSummary) -> str:
    status = f" [{s.review_status}]" if s.review_status else ""
    return f"{s.title}  ·  n={s.n_recipes}{status}"


def _render_stats(st: Any, detail: ops.VariantDetail) -> None:
    if not detail.stats:
        st.info("No ingredient stats yet — recompute may be pending.")
        return
    rows = [
        {
            "ingredient": s.canonical_name,
            "mean": round(s.mean_proportion, 4),
            "stddev": None if s.stddev is None else round(s.stddev, 4),
            "ratio": None if s.ratio is None else round(s.ratio, 3),
            "n": s.min_sample_size,
        }
        for s in detail.stats
    ]
    st.dataframe(rows, use_container_width=True, hide_index=True)


def _render_filter_panel(
    st: Any, db: CatalogDB, detail: ops.VariantDetail
) -> None:
    st.subheader("Source recipes")
    st.caption(
        "Drop a source to exclude it from the average. The override is "
        "reversible from the panel below."
    )
    excluded = detail.excluded_recipe_ids
    for member in detail.members:
        is_excluded = member.recipe_id in excluded
        cols = st.columns([4, 2, 2, 1])
        title = member.title or "(no title)"
        prefix = "~~" if is_excluded else ""
        suffix = "~~" if is_excluded else ""
        cols[0].markdown(f"{prefix}**{title}**{suffix}")
        cols[1].caption(member.corpus)
        score = (
            "—"
            if member.outlier_score is None
            else f"{member.outlier_score:.2f}"
        )
        cols[2].caption(f"outlier {score}")
        if is_excluded:
            cols[3].caption("dropped")
            continue
        with cols[3].popover("drop"):
            reason = st.text_input(
                "reason",
                key=f"reason::{detail.variant.variant_id}::{member.recipe_id}",
            )
            if st.button(
                "Confirm drop",
                key=f"drop::{detail.variant.variant_id}::{member.recipe_id}",
            ):
                result = ops.apply_filter(
                    db,
                    detail.variant.variant_id,
                    member.recipe_id,
                    reason,
                )
                if result.ok:
                    st.success(result.message)
                    st.rerun()
                else:
                    st.error(result.message)


def _render_substitute_panel(
    st: Any, db: CatalogDB, detail: ops.VariantDetail
) -> None:
    st.subheader("Substitute (fold X into Y)")
    canonicals = [s.canonical_name for s in detail.stats]
    if len(canonicals) < 2:
        st.info("Need at least two canonical ingredients to substitute.")
        return
    cols = st.columns([3, 3, 2])
    from_name = cols[0].selectbox(
        "From (X)",
        canonicals,
        key=f"sub_from::{detail.variant.variant_id}",
    )
    to_options = [c for c in canonicals if c != from_name]
    to_name = cols[1].selectbox(
        "Into (Y)",
        to_options,
        key=f"sub_to::{detail.variant.variant_id}",
    )
    if cols[2].button(
        "Fold",
        key=f"fold::{detail.variant.variant_id}",
    ):
        result = ops.apply_substitute(
            db, detail.variant.variant_id, from_name, to_name
        )
        if result.ok:
            st.success(result.message)
            st.rerun()
        else:
            st.error(result.message)


def _render_overrides_panel(
    st: Any, db: CatalogDB, detail: ops.VariantDetail
) -> None:
    st.subheader("Active overrides")
    if not detail.overrides:
        st.caption("No overrides on this variant.")
        return
    for ov in detail.overrides:
        cols = st.columns([5, 2, 1])
        cols[0].text(ops.describe_override(ov))
        cols[1].caption(ov.created_at)
        if cols[2].button("Clear", key=f"clear::{ov.override_id}"):
            result = ops.clear_one_override(db, ov.override_id)
            if result.ok:
                st.success(result.message)
                st.rerun()
            else:
                st.error(result.message)


def _render_detail(
    st: Any, db: CatalogDB, variant_id: str
) -> None:
    detail = ops.load_variant_detail(db, variant_id)
    if detail is None:
        st.error(f"Variant {variant_id} not found.")
        return
    title = detail.variant.display_title or detail.variant.normalized_title
    st.header(title)
    st.caption(
        f"variant_id: `{detail.variant.variant_id}`  ·  "
        f"n_recipes: **{detail.variant.n_recipes}**  ·  "
        f"review: {detail.variant.review_status or 'pending'}"
    )

    st.subheader("Per-ingredient stats")
    _render_stats(st, detail)

    _render_substitute_panel(st, db, detail)
    _render_overrides_panel(st, db, detail)
    _render_filter_panel(st, db, detail)


def _render_sidebar(
    st: Any, db: CatalogDB, db_path: Path
) -> str | None:
    st.sidebar.title("Maintainer editor")
    st.sidebar.caption(f"DB: `{db_path}`")
    summaries = ops.list_variant_summaries(db)
    if not summaries:
        st.sidebar.warning("No variants in this DB.")
        return None
    query = st.sidebar.text_input("Filter by title", "").strip().lower()
    if query:
        summaries = [s for s in summaries if query in s.title.lower()]
    if not summaries:
        st.sidebar.caption("No matches.")
        return None
    options = [s.variant_id for s in summaries]
    labels = {s.variant_id: _format_summary_label(s) for s in summaries}
    return st.sidebar.radio(
        f"Variants ({len(summaries)})",
        options,
        format_func=lambda vid: labels[vid],
        key="selected_variant_id",
    )


def main() -> None:
    """Streamlit entry point — invoked once per page render."""
    import streamlit as st  # noqa: PLC0415

    args = _parse_args(_streamlit_argv())
    st.set_page_config(
        page_title="RationalRecipes editor",
        layout="wide",
    )
    db = _open_db(args.db)
    try:
        selected = _render_sidebar(st, db, args.db)
        if selected is None:
            st.title("RationalRecipes maintainer editor")
            st.write(
                "Pick a variant on the left to start. Operations available "
                "in the detail view: drop a source recipe (filter), fold "
                "canonical X into Y (substitute), and clear active "
                "overrides. All writes go through `CatalogDB` so the CLI "
                "review tool sees them immediately."
            )
            return
        _render_detail(st, db, selected)
    finally:
        db.close()


if __name__ == "__main__":
    main()
