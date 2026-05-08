"""Streamlit maintainer editor for ``recipes.db`` (RationalRecipes-1t8x + xekj).

Localhost, single-maintainer workbench for filter / substitute /
canonical-reassign operations on extracted variants. Reads + writes
``recipes.db`` directly via ``CatalogDB`` — no HTTP layer, no JSON cache,
no sql.js.

Launch:

    streamlit run scripts/editor.py -- --db output/catalog/recipes.db \\
        --recipenlg dataset/full_dataset.csv

Override writes go through the same ``add_filter_override``,
``add_substitute_override``, and ``add_canonical_reassign_override``
helpers as ``scripts/review_variants.py``, so an override applied here
is indistinguishable from one applied via the CLI.
``_recompute_stats_for_variant`` runs eagerly inside each helper — after
a write the in-process ``CatalogDB`` already holds the updated
``variant_ingredient_stats``; the UI just needs to re-read.

This module is the **presentation layer only**. The data plumbing — list
the variants, load a detail, apply / clear overrides, load provenance —
lives in ``rational_recipes.editor.operations`` and is tested separately
so the core logic doesn't depend on the Streamlit runtime.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Any

from rational_recipes.catalog_db import CatalogDB
from rational_recipes.editor import operations as ops

DEFAULT_DB_PATH = Path("output/catalog/recipes.db")
DEFAULT_RECIPENLG_PATH = Path("dataset/full_dataset.csv")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Maintainer editor for recipes.db")
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB_PATH,
        help=f"Path to recipes.db (default: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--recipenlg",
        type=Path,
        default=DEFAULT_RECIPENLG_PATH,
        help=(
            "Path to RecipeNLG full_dataset.csv for provenance breakdown "
            f"(default: {DEFAULT_RECIPENLG_PATH}; gitignored — provenance "
            "panels render an empty state when missing)"
        ),
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
    # 1jbk: split canonicals produce one row per (canonical, component);
    # surface the component column so the editor can tell them apart.
    rows = [
        {
            "ingredient": s.canonical_name,
            "component": s.component or "",
            "mean": round(s.mean_proportion, 4),
            "stddev": None if s.stddev is None else round(s.stddev, 4),
            "ratio": None if s.ratio is None else round(s.ratio, 3),
            "n": s.min_sample_size,
        }
        for s in detail.stats
    ]
    st.dataframe(rows, use_container_width=True, hide_index=True)


# Top-level streamlit import is needed by the module-scope cache_data
# decorator below — Streamlit only caches when the @cache_data is applied
# at definition time, not inside a render function. The rest of the file
# uses the ``st`` parameter passed to the render functions so the
# helpers can be unit-tested with a fake (see tests/test_editor.py).
import streamlit as _st  # noqa: E402


@_st.cache_data(show_spinner="Loading source provenance…")
def _cached_load_provenance(
    _db: CatalogDB,
    variant_id: str,
    override_count: int,
    recipenlg_path_str: str,
) -> Any:
    """Streamlit-cached wrapper around ``ops.load_provenance``.

    Provenance is the expensive call in the editor (scans the 2.2 GB
    RecipeNLG ``full_dataset.csv`` for raw lines on every miss — ~3 s
    per render against the dev-box disk). Streamlit re-runs the whole
    script on every interaction, so without caching every click pays
    the full scan.

    Cache key: ``(variant_id, override_count, recipenlg_path_str)``.
    The ``_db`` underscore prefix tells Streamlit not to try to hash
    the SQLite connection. ``override_count`` makes any substitute /
    filter / reassign / component_assign write invalidate the cached
    result — every write changes the count by ±1, and the recompute
    that follows is what changes ``get_ingredient_stats`` (which
    ``load_variant_provenance`` reads to enumerate canonicals). The
    expensive corpus scan itself only depends on the variant's
    ``target_urls`` set (which doesn't change without a filter
    override), but the cheap summarization step does, so re-running
    on every write keeps the displayed data correct.
    """
    return ops.load_provenance(_db, variant_id, Path(recipenlg_path_str))


def _render_provenance_panel(
    st: Any,
    db: CatalogDB,
    detail: ops.VariantDetail,
    recipenlg_path: Path,
) -> None:
    """Per-canonical raw-form breakdown + per-source canonical reassignment.

    Surfaces are merged into one expander per canonical: clicking expands
    the breakdown, and the expander body holds the reassign form so the
    editor can pick a (recipe_id, raw_text) pair from the same view.
    """
    if not detail.stats:
        return
    st.subheader("Provenance & per-source reassignment")
    st.caption(
        "**Per-source** retag: corrects ONE source recipe's raw-line → "
        "canonical mapping. Use this when a single source's line was "
        "miscategorized (e.g. \"5 oz. all-purpose flour\" got mapped to "
        "\"plain flour\" when it should be \"flour\"). "
        "**To merge two canonicals across the WHOLE variant** "
        "(e.g. fold every \"plain flour\" entry into \"flour\"), use the "
        "**Substitute** panel above — that's a one-shot fold across all "
        "members; per-source retags here only touch the recipe you pick."
    )
    # The provenance load scans a 2.2 GB CSV (RecipeNLG full_dataset.csv)
    # for raw lines on every miss — typically a few seconds per variant.
    # Most editor sessions don't need it, so render only when the
    # maintainer explicitly asks. Cached load keeps subsequent toggles
    # cheap once the data is in memory.
    show = st.checkbox(
        "Show provenance & reassign UI (slow first load — scans the "
        "2.2 GB RecipeNLG corpus)",
        value=False,
        key=f"prov_toggle::{detail.variant.variant_id}",
    )
    if not show:
        return
    if not recipenlg_path.exists():
        st.caption(
            f"RecipeNLG corpus not found at `{recipenlg_path}` — "
            "provenance breakdowns will render empty until the dataset is "
            "available. The reassign UI below still works (it doesn't "
            "depend on the corpus join)."
        )
    # Streamlit-cached: keyed by (variant_id, override count, corpus path).
    # ``len(detail.overrides)`` is the freshness signal: any substitute /
    # filter / reassign / component_assign write changes the count,
    # invalidating the cache so the next render reflects post-write stats.
    prov = _cached_load_provenance(
        db,
        detail.variant.variant_id,
        len(detail.overrides),
        str(recipenlg_path),
    )
    if prov is None:
        st.error("Could not load provenance for this variant.")
        return
    st.caption(
        f"recipenlg members: {prov.n_recipenlg_members}  ·  "
        f"hit in corpus: {prov.n_recipenlg_hit}/{prov.n_recipenlg_members}  ·  "
        f"unmatched lines: {prov.unmatched_count}  ·  "
        f"other corpora (skipped): {prov.n_other_corpora}"
    )
    by_canonical = {c.canonical: c for c in prov.canonicals}
    member_recipe_ids = [m.recipe_id for m in detail.members]
    # 1jbk: with multi-component splits a single canonical produces
    # multiple ``detail.stats`` rows. Provenance is per-canonical
    # (the raw forms in the source corpus don't know about our
    # editorial splits), so iterate over unique canonicals — sum the
    # split rows' means to surface the canonical's total mass — and
    # keep the Streamlit keys unambiguous.
    unique_means: dict[str, float] = {}
    for stat in detail.stats:
        unique_means[stat.canonical_name] = (
            unique_means.get(stat.canonical_name, 0.0)
            + stat.mean_proportion
        )
    for canon, total_mean in unique_means.items():
        canon_prov = by_canonical.get(canon)
        n_obs = canon_prov.total_observations if canon_prov else 0
        with st.expander(
            f"{canon}  ·  mean {round(total_mean, 4)}  ·  "
            f"{n_obs} raw observations"
        ):
            _render_canonical_breakdown(st, canon_prov)
            _render_reassign_form(
                st, db, detail, canon, canon_prov, member_recipe_ids
            )


def _render_canonical_breakdown(
    st: Any, canon_prov: ops.VariantProvenance | Any
) -> None:
    """Raw-form table for one canonical (or empty state)."""
    if canon_prov is None or not canon_prov.forms:
        st.caption(
            "No raw forms found in source corpus for this canonical "
            "(the variant's members may be from non-recipenlg corpora, "
            "or the corpus CSV isn't on disk)."
        )
        return
    rows = [
        {
            "raw_form": f.form_key,
            "count": f.count,
            "mean_grams": (
                None if f.mean_grams is None else round(f.mean_grams, 1)
            ),
            "n_with_mass": f.n_with_grams,
            "example": f.example_raw_line,
        }
        for f in canon_prov.forms
    ]
    st.dataframe(rows, use_container_width=True, hide_index=True)
    distinct_recipes = len({rid for f in canon_prov.forms for rid in f.recipe_ids})
    st.caption(
        f"Total: {canon_prov.total_observations} observations across "
        f"{distinct_recipes} distinct recipes."
    )


def _build_reassign_options(
    db: CatalogDB,
    variant_id: str,
    canonical: str,
    canon_prov: Any,
) -> list[tuple[str, str, str]]:
    """One option per (contributing recipe, raw line) pair to retag.

    Each option is a tuple of ``(recipe_id, raw_text_substring, label)``
    where ``label`` is what the maintainer sees in the picker. recipe_id
    and raw_text stay internal — the maintainer picks a source by its
    title + corpus + literal raw line (when known), and the override
    write uses the tuple values directly. Two source paths feed the
    list, with corpus-line entries preferred when both are present:

    - **RecipeNLG corpus pairs** from ``canon_prov.forms``: real raw
      lines with literal text, one option per (recipe_id, raw_line).
      These give the cleanest retag UX because the substring is the
      literal source line.
    - **parsed_ingredients fallback** via
      ``list_canonical_contributors``: surfaces any contributing recipe
      that didn't appear in the corpus join (typically non-RecipeNLG
      sources). Substring defaults to the canonical name itself —
      the longest-whole-word matcher in
      ``add_canonical_reassign_override`` resolves it correctly.

    Returns ``[]`` when nothing contributes — possible when an active
    substitute has folded the canonical away.
    """
    contributors = ops.list_canonical_contributors(db, variant_id, canonical)
    contributors_by_rid = {c.recipe_id: c for c in contributors}

    options: list[tuple[str, str, str]] = []
    seen_rids_with_corpus_line: set[str] = set()
    if canon_prov is not None:
        for form in canon_prov.forms:
            for rid in form.recipe_ids:
                seen_rids_with_corpus_line.add(rid)
                contrib = contributors_by_rid.get(rid)
                title = (contrib.title if contrib else None) or "untitled"
                corpus = contrib.corpus if contrib else "?"
                options.append(
                    (
                        rid,
                        form.example_raw_line,
                        f"{title} ({corpus}) — {form.example_raw_line!r}",
                    )
                )

    for c in contributors:
        if c.recipe_id in seen_rids_with_corpus_line:
            continue
        title = c.title or "untitled"
        options.append(
            (
                c.recipe_id,
                canonical,
                (
                    f"{title} ({c.corpus}) — substring match "
                    f"{canonical!r}"
                ),
            )
        )
    return options


def _render_reassign_form(
    st: Any,
    db: CatalogDB,
    detail: ops.VariantDetail,
    canonical: str,
    canon_prov: Any,
    member_recipe_ids: list[str],
) -> None:
    """Per-source reassignment form scoped to one canonical.

    The maintainer picks a contributing source from a labeled selectbox
    (title + corpus + raw line / canonical-substring fallback) and types
    the new canonical name. recipe_id and raw_text stay internal — they
    were exposed as editable text inputs in the early prototype, but the
    only field the maintainer ever needs to set is ``new_canonical``,
    so the database identifiers don't belong in the form.
    """
    st.markdown("**Reassign one source's raw line → new canonical**")
    vid = detail.variant.variant_id
    options = _build_reassign_options(db, vid, canonical, canon_prov)
    if not options:
        st.caption(
            f"No contributing sources for **{canonical}** — the canonical "
            "may have been folded by an active substitute override. Clear "
            "any substitutes first if you want to reassign at the source "
            "level."
        )
        return

    idx = st.selectbox(
        "Contributing source recipe to retag",
        list(range(len(options))),
        format_func=lambda i: options[i][2],
        key=f"reassign_pick::{vid}::{canonical}",
    )
    selected_rid, selected_raw_text, _ = options[idx]
    cols = st.columns([4, 1])
    new_canonical = cols[0].text_input(
        "new_canonical",
        placeholder="e.g. flour",
        key=f"reassign_new::{vid}::{canonical}",
    )
    if cols[1].button(
        "Reassign",
        key=f"reassign_go::{vid}::{canonical}",
    ):
        if not new_canonical.strip():
            st.error("Enter a new canonical name.")
            return
        if selected_rid not in member_recipe_ids:
            st.error(
                f"Internal: selected recipe_id {selected_rid!r} is not "
                "a member of this variant. Please report this."
            )
            return
        result = ops.apply_canonical_reassign(
            db, vid, selected_rid, selected_raw_text, new_canonical
        )
        if result.ok:
            st.success(result.message)
            st.rerun()
        else:
            st.error(result.message)


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
    # 1jbk: split canonicals produce multiple stat rows; substitute is a
    # canonical-level operation, so dedupe to keep the selectbox clean.
    canonicals = list(dict.fromkeys(s.canonical_name for s in detail.stats))
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


def _highlight_canonical(directions: str, canonical: str) -> str:
    """Wrap case-insensitive occurrences of ``canonical`` in markdown bold.

    Used by the split-expander's source-context panel
    (RationalRecipes-cw2u) so the maintainer can spot every mention of
    the ingredient being split in the source's directions text. Match
    is plain-substring case-insensitive — adequate for short canonical
    names typical in recipe extraction (``flour``, ``rolled oats``,
    ``brown sugar``). Whole-word boundaries aren't enforced; it's a
    visual aid, not a correctness signal.
    """
    if not canonical:
        return directions
    pattern = re.compile(re.escape(canonical), re.IGNORECASE)
    return pattern.sub(lambda m: f"**{m.group(0)}**", directions)


def _render_split_source_context(
    st: Any,
    db: CatalogDB,
    variant_id: str,
    canonical: str,
) -> None:
    """Show source-recipe directions inside the split-across expander.

    For each variant member that contributes ``canonical`` (per
    ``list_canonical_contributors``), render the title + clickable URL
    + corpus tag. RecipeNLG members typically have cached
    ``directions_text``; render it with the canonical name bolded so
    the maintainer can spot the relevant lines (the apple-crumble
    cookbooks.com source's "Combine 1/2 cup of sugar and 1 ounce of
    flour … Mix butter, remaining flour and cinnamon" is exactly the
    kind of explicit split this surfaces). Non-RecipeNLG sources have
    no cached directions; show a "(directions not cached — open the
    URL to read)" caption with the link so the maintainer has a
    one-click path.
    """
    contributors = ops.list_canonical_contributors(db, variant_id, canonical)
    if not contributors:
        return
    st.markdown(
        "**Source recipes** (read these to estimate the split — the "
        f"canonical `{canonical}` is bolded in each recipe's directions "
        "where cached):"
    )
    for c in contributors:
        title = c.title or "(no title)"
        url = c.url or ""
        if url:
            st.markdown(f"- [{title}]({url}) · _{c.corpus}_")
        else:
            st.markdown(f"- {title} · _{c.corpus}_")
        if c.directions_text:
            st.markdown(
                "  > " + _highlight_canonical(
                    c.directions_text.replace("\n", " "), canonical
                )
            )
        else:
            st.caption(
                "    (directions not cached — open the URL to read)"
            )


def _render_components_panel(
    st: Any, db: CatalogDB, detail: ops.VariantDetail
) -> None:
    """Per-drop component grouping panel (kfp3).

    For multi-component dishes (apple crumble, layer cakes, pie shells)
    the maintainer labels each canonical with the component it belongs
    to ("crumble", "filling", etc.). The label is stored as a
    ``component_assign`` override; the recompute carries it onto
    ``variant_ingredient_stats.component`` without changing the per-
    recipe mass-fraction math. Re-labels go through clear+add (the
    helper rejects a duplicate label) — this UI surfaces the existing
    label per canonical so the editor sees what's set before adding.
    """
    st.subheader("Components (per-drop grouping)")
    st.caption(
        "Label each canonical with the component it belongs to "
        "(e.g. 'crumble', 'filling'). Renderer groups stats by "
        "component when any label is set; otherwise the drop renders "
        "as a single flat ingredient list. Re-label by clearing the "
        "existing override below first."
    )
    # 1jbk: split canonicals produce multiple stat rows; the labeling
    # form operates at the canonical level, so dedupe.
    canonicals = list(dict.fromkeys(s.canonical_name for s in detail.stats))
    if not canonicals:
        st.info("No canonical ingredients to label yet.")
        return

    # Pull the live label per canonical from the recomputed stats so the
    # form reflects post-clear state without a manual rerun. With splits,
    # multiple stat rows share a canonical — any non-null component is a
    # signal that the canonical "has a label."
    label_by_canonical: dict[str, str | None] = {}
    for s in detail.stats:
        if s.component:
            label_by_canonical[s.canonical_name] = s.component
        else:
            label_by_canonical.setdefault(s.canonical_name, None)

    # Existing component names sourced from the live stat rows (which
    # correctly enumerate every component a canonical participates in,
    # including both sides of a split). Surfaced as a caption beside
    # the input so the maintainer can copy-paste rather than retype,
    # but NOT pre-filled into the input — pre-filling under Streamlit
    # caused a "wrong default sticks" hazard where the textbox would
    # commit the suggestion if the maintainer didn't blur after typing.
    existing_components = sorted(
        {s.component for s in detail.stats if s.component}
    )

    # The component label is now per (canonical, component); a split
    # canonical produces multiple rows (kfp3+1jbk). For surfacing the
    # current label we group by canonical and show "crumble@0.9,
    # filling@0.1" when the canonical is split.
    label_by_canonical_summary: dict[str, str] = {}
    splits_seen: dict[str, list[tuple[str, float]]] = {}
    for s in detail.stats:
        if s.component:
            splits_seen.setdefault(s.canonical_name, []).append(
                (s.component, s.mean_proportion)
            )
    for c, pieces in splits_seen.items():
        total = sum(w for _, w in pieces) or 1.0
        label_by_canonical_summary[c] = ", ".join(
            f"{comp}@{w / total:.2f}" for comp, w in pieces
        )

    vid = detail.variant.variant_id
    cols = st.columns([3, 3, 2])
    selected_canonical = cols[0].selectbox(
        "Canonical",
        canonicals,
        format_func=lambda c: (
            f"{c}  ·  {label_by_canonical_summary[c]}"
            if label_by_canonical_summary.get(c)
            else c
        ),
        key=f"comp_canonical::{vid}",
    )
    placeholder = (
        ", ".join(existing_components)
        if existing_components
        else "crumble, filling, ..."
    )
    component_name = cols[1].text_input(
        "Component",
        placeholder=placeholder,
        key=f"comp_name::{vid}::{selected_canonical}",
    )
    current_label = label_by_canonical_summary.get(selected_canonical)
    if current_label:
        cols[2].caption(f"current: {current_label}")
        cols[2].caption("clear it below to relabel")
    if cols[2].button("Label", key=f"comp_apply::{vid}"):
        result = ops.apply_component_assign(
            db, vid, selected_canonical, component_name
        )
        if result.ok:
            st.success(result.message)
            st.rerun()
        else:
            st.error(result.message)

    # 1jbk: split-across-components form. Most canonicals get a single
    # label; the split affordance lives in an expander so the simple
    # form stays uncluttered. Two rows are sufficient for the common
    # apple-crumble flour case (crumble + filling-roux); a third can
    # be added inline by re-clearing and re-splitting once we hit a
    # dish that needs it.
    with st.expander("Split this canonical across components"):
        st.caption(
            "For canonicals whose mass spans multiple components — e.g. "
            "apple crumble's flour: most in the crumble, some as a roux "
            "thickener in the filling. Weights must sum to 1.0."
        )
        # cw2u: surface source-recipe directions so the maintainer can
        # read the split off the actual recipes instead of guessing.
        # Directions are cached only for RecipeNLG members (per F5 /
        # 15g4); WDC and other corpora show a URL fallback.
        _render_split_source_context(st, db, vid, selected_canonical)
        split_cols = st.columns([2, 1, 2, 1, 1])
        split_a_placeholder = (
            existing_components[0] if existing_components else "crumble"
        )
        split_a_name = split_cols[0].text_input(
            "Component A",
            placeholder=split_a_placeholder,
            key=f"split_a_name::{vid}::{selected_canonical}",
        )
        split_a_weight = split_cols[1].number_input(
            "Weight A",
            min_value=0.0,
            max_value=1.0,
            value=0.9,
            step=0.05,
            key=f"split_a_weight::{vid}::{selected_canonical}",
        )
        split_b_placeholder = (
            existing_components[1]
            if len(existing_components) >= 2
            else "filling"
        )
        split_b_name = split_cols[2].text_input(
            "Component B",
            placeholder=split_b_placeholder,
            key=f"split_b_name::{vid}::{selected_canonical}",
        )
        split_b_weight = split_cols[3].number_input(
            "Weight B",
            min_value=0.0,
            max_value=1.0,
            value=0.1,
            step=0.05,
            key=f"split_b_weight::{vid}::{selected_canonical}",
        )
        if split_cols[4].button(
            "Split", key=f"split_apply::{vid}::{selected_canonical}"
        ):
            result = ops.apply_component_split(
                db,
                vid,
                selected_canonical,
                [
                    (split_a_name, float(split_a_weight)),
                    (split_b_name, float(split_b_weight)),
                ],
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
    st: Any, db: CatalogDB, variant_id: str, recipenlg_path: Path
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
    _render_provenance_panel(st, db, detail, recipenlg_path)
    _render_substitute_panel(st, db, detail)
    _render_components_panel(st, db, detail)
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
                "canonical X into Y (substitute), label canonicals with a "
                "component (per-drop grouping), and clear active "
                "overrides. All writes go through `CatalogDB` so the CLI "
                "review tool sees them immediately."
            )
            return
        _render_detail(st, db, selected, args.recipenlg)
    finally:
        db.close()


if __name__ == "__main__":
    main()
