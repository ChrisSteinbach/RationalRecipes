"""Backfill the RationalRecipes-2p6 ingredient fold over an existing recipes.db.

The Pass 2 pipeline applies ``apply_fold_to_variant`` at variant build
time, but the production ``recipes.db`` was written before that change
landed. This CLI applies the fold to ``variant_ingredient_stats`` rows
in place — analogous to ``vwt.33``'s category backfill — so the catalog
JSON reflects the fix without a full re-scrape.

Operates per-variant: for each variant whose ingredient set contains
≥2 forms of any fold family, picks the keeper (largest summed
``mean_proportion``), sums the family forms' ``mean_proportion`` and
``min_sample_size`` into the keeper, takes the keeper's ``stddev``,
``density_g_per_ml`` and ``whole_unit_*`` fields, recomputes ``ratio``
against the new base mean, drops the other forms, and updates
``variants.canonical_ingredient_set`` to match.

Defaults to dry-run; pass ``--apply`` to mutate. ``--db`` overrides
the source path.
"""

from __future__ import annotations

import argparse
import math
import sys
from collections.abc import Sequence
from pathlib import Path

from rational_recipes.catalog_db import CatalogDB, IngredientStatsRow
from rational_recipes.scrape.ingredient_fold import (
    FOLD_MAP,
    families_present,
    pick_keeper,
)


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        type=Path,
        default=Path("output/catalog/recipes.db"),
        help="Path to recipes.db (default: %(default)s).",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Mutate the DB. Without this, the CLI prints a dry-run summary.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Stop after processing this many variants (for testing).",
    )
    return parser.parse_args(argv)


def _fold_one_variant(
    db: CatalogDB,
    variant_id: str,
    canonical_set: tuple[str, ...],
    *,
    apply: bool,
) -> tuple[bool, list[tuple[str, str]]]:
    """Apply the fold to one variant's stats. Returns ``(changed, dropped_pairs)``.

    ``dropped_pairs`` is a list of ``(family, dropped_form)`` for diagnostics.
    """
    families = families_present(canonical_set)
    if not families:
        return False, []

    stats = list(db.get_ingredient_stats(variant_id))
    by_name = {s.canonical_name: s for s in stats}
    dropped_pairs: list[tuple[str, str]] = []
    new_canonical = set(canonical_set)
    folds_to_apply: list[
        tuple[str, list[str], IngredientStatsRow, list[IngredientStatsRow]]
    ] = []

    for family, forms in families.items():
        # Limit to forms actually present in stats (filters can drop one).
        present_forms = [f for f in forms if f in by_name]
        if len(present_forms) < 2:
            continue
        totals = {f: by_name[f].mean_proportion for f in present_forms}
        keeper_name = pick_keeper(present_forms, totals)
        droppable = [f for f in present_forms if f != keeper_name]
        if not droppable:
            continue
        keeper_row = by_name[keeper_name]
        droppable_rows = [by_name[f] for f in droppable]
        folds_to_apply.append((family, droppable, keeper_row, droppable_rows))
        for f in droppable:
            dropped_pairs.append((family, f))
            new_canonical.discard(f)
        new_canonical.add(keeper_name)

    if not folds_to_apply:
        return False, []

    # Compute new mean for the keeper (sum of family means) and new
    # base-mean across all retained ingredients to recompute ratios.
    summed_means: dict[str, float] = {}
    summed_min_samples: dict[str, int] = {}
    for _family, _droppable, keeper_row, droppable_rows in folds_to_apply:
        new_mean = keeper_row.mean_proportion + sum(
            r.mean_proportion for r in droppable_rows
        )
        summed_means[keeper_row.canonical_name] = new_mean
        summed_min_samples[keeper_row.canonical_name] = max(
            keeper_row.min_sample_size,
            *(r.min_sample_size for r in droppable_rows),
        )

    # Detect base ingredient: ratio == 1.0 (within float tolerance) for
    # exactly one retained row in the original stats. The pipeline writes
    # the base as ``canonicals[0] if not provided``; for safety we look
    # at the actual ratio==1.0 row.
    base_name: str | None = None
    base_mean: float | None = None
    for s in stats:
        if (
            s.ratio is not None
            and math.isclose(s.ratio, 1.0, abs_tol=1e-9)
            and s.canonical_name in new_canonical
        ):
            base_name = s.canonical_name
            base_mean = summed_means.get(base_name, s.mean_proportion)
            break

    if not apply:
        return True, dropped_pairs

    conn = db.connection
    with conn:
        # Drop the non-keeper rows.
        for _family, droppable, _, _ in folds_to_apply:
            for f in droppable:
                conn.execute(
                    "DELETE FROM variant_ingredient_stats "
                    "WHERE variant_id = ? AND canonical_name = ?",
                    (variant_id, f),
                )
        # Update keepers with summed means + recomputed ratios.
        for keeper_name, new_mean in summed_means.items():
            keeper_row = by_name[keeper_name]
            new_min_sample = summed_min_samples[keeper_name]
            new_ratio: float | None
            if base_mean and base_mean > 0:
                new_ratio = new_mean / base_mean
            else:
                new_ratio = keeper_row.ratio
            conn.execute(
                """
                UPDATE variant_ingredient_stats
                   SET mean_proportion = ?,
                       ratio = ?,
                       min_sample_size = ?
                 WHERE variant_id = ? AND canonical_name = ?
                """,
                (
                    new_mean,
                    new_ratio,
                    new_min_sample,
                    variant_id,
                    keeper_name,
                ),
            )
        # Recompute ratios for *all* retained rows so the new base
        # propagates correctly. Skip rows whose ratio was already NULL
        # (pipeline didn't write one — leave it NULL).
        if base_mean and base_mean > 0 and base_name is not None:
            retained_rows = conn.execute(
                "SELECT canonical_name, mean_proportion, ratio "
                "FROM variant_ingredient_stats WHERE variant_id = ?",
                (variant_id,),
            ).fetchall()
            for canonical_name, mean_prop, old_ratio in retained_rows:
                if old_ratio is None:
                    continue
                if canonical_name == base_name:
                    new_r = 1.0
                else:
                    new_r = mean_prop / base_mean
                conn.execute(
                    "UPDATE variant_ingredient_stats SET ratio = ? "
                    "WHERE variant_id = ? AND canonical_name = ?",
                    (new_r, variant_id, canonical_name),
                )
        # Update canonical_ingredient_set on the variants row.
        conn.execute(
            "UPDATE variants SET canonical_ingredient_set = ? "
            "WHERE variant_id = ?",
            (",".join(sorted(new_canonical)), variant_id),
        )
    return True, dropped_pairs


def run(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    if not args.db.exists():
        print(f"recipes.db not found: {args.db}", file=sys.stderr)
        return 1

    db = CatalogDB.open(args.db)
    try:
        rows = db.connection.execute(
            "SELECT variant_id, canonical_ingredient_set FROM variants"
        ).fetchall()
        n_changed = 0
        n_visited = 0
        family_counts: dict[str, int] = dict.fromkeys(FOLD_MAP, 0)
        for variant_id, canonical_csv in rows:
            if args.limit is not None and n_visited >= args.limit:
                break
            n_visited += 1
            canonical_set = tuple(
                s for s in (canonical_csv or "").split(",") if s
            )
            changed, dropped = _fold_one_variant(
                db,
                variant_id,
                canonical_set,
                apply=args.apply,
            )
            if changed:
                n_changed += 1
                for family, _ in dropped:
                    family_counts[family] = family_counts.get(family, 0) + 1
        verb = "updated" if args.apply else "would update"
        print(
            f"{verb} {n_changed}/{n_visited} variants "
            f"(apply={args.apply})"
        )
        for family in sorted(family_counts):
            print(f"  {family:24s} fold ops: {family_counts[family]}")
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
