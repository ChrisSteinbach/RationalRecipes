"""Generic/specific ingredient fold map (RationalRecipes-2p6).

Folds duplicate ingredient pairs (e.g. ``salt`` + ``kosher salt``,
``oil`` + ``vegetable oil``) inside a single variant down to one
canonical form so the catalog reads as one coherent recipe instead of
preserving every source recipe's exact naming.

Intentionally narrow. The fold runs at Pass 2 variant aggregation only;
source-recipe canonicalization keeps per-synonym specificity (e.g.
``cheddar`` vs ``cheese``, ``red onion`` vs ``onion``) per the dfm
commit (e5ed810). New families must be added here only when:

  - one form's whitespace tokens are a strict subset of the other's, AND
  - the forms are routinely interchangeable in everyday cooking
    (i.e. a recipe author would substitute one for the other without
    a meaningful taste / texture / chemistry consequence).

Run ``scripts/discover_fold_candidates.py`` against a populated
``recipes.db`` to surface new candidates with their cross-variant
co-occurrence counts; the high-frequency rows are the ones worth
considering.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rational_recipes.scrape.pipeline_merged import MergedVariantResult

# Pre-approved fold families for RationalRecipes-2p6.
#
# - oil + salt: pre-approved in the bead's design field.
# - butter: required to satisfy the bead's acceptance criterion (Basic
#   Buttermilk Pancakes folds butter + unsalted butter to land at ≤11
#   distinct ingredients). Surfaced in bead notes for user review.
#
# Forms deliberately *excluded* (substring relationship is real but the
# ingredients are meaningfully different):
#   - ``garlic salt`` is a flavored salt, not interchangeable with salt.
#   - ``peanut butter`` is not butter.
#   - ``brown sugar`` is not white sugar (different molasses content).
#   - ``baking soda`` is not soda; ``cream cheese`` is not cheese; etc.
FOLD_MAP: Mapping[str, frozenset[str]] = {
    "oil": frozenset(
        {
            "oil",
            "vegetable oil",
            "canola oil",
            "cooking oil",
        }
    ),
    "salt": frozenset(
        {
            "salt",
            "kosher salt",
            "table salt",
            "sea salt",
            "fine salt",
        }
    ),
    "butter": frozenset(
        {
            "butter",
            "unsalted butter",
            "salted butter",
            "sweet butter",
        }
    ),
}


def _build_form_index(
    fold_map: Mapping[str, frozenset[str]],
) -> dict[str, str]:
    """{form → family_name} index, validated for ambiguity."""
    out: dict[str, str] = {}
    for family, forms in fold_map.items():
        for form in forms:
            if form in out and out[form] != family:
                raise ValueError(
                    f"form {form!r} appears in both {out[form]!r} and "
                    f"{family!r}; fold map must be unambiguous"
                )
            out[form] = family
    return out


def families_present(
    canonical_names: Iterable[str],
    fold_map: Mapping[str, frozenset[str]] = FOLD_MAP,
) -> dict[str, list[str]]:
    """Return ``{family: [forms]}`` for families with ≥2 forms in input.

    Used by both the live pipeline and the backfill CLI to short-circuit
    when a variant has nothing to fold.
    """
    form_to_family = _build_form_index(fold_map)
    grouped: dict[str, list[str]] = {}
    for name in canonical_names:
        family = form_to_family.get(name)
        if family is None:
            continue
        grouped.setdefault(family, []).append(name)
    return {f: sorted(forms) for f, forms in grouped.items() if len(forms) >= 2}


def pick_keeper(
    forms: Iterable[str],
    totals: Mapping[str, float],
) -> str:
    """Return the form with the largest summed total.

    Ties are broken alphabetically for determinism (so re-runs over the
    same data produce identical variant_ids). The bead's "bigger summed
    mass wins" rule maps to "largest summed proportion across rows" in
    practice — proportion is mass / per-row total, so the relative
    ranking matches mass when source recipes are roughly the same size.
    """
    sorted_forms = sorted(forms)
    return max(sorted_forms, key=lambda f: totals.get(f, 0.0))


def apply_fold_to_variant(
    variant: MergedVariantResult,
    fold_map: Mapping[str, frozenset[str]] = FOLD_MAP,
) -> bool:
    """Fold sibling forms inside ``variant`` down to a single keeper.

    Mutates ``variant.normalized_rows[*].cells``,
    ``variant.normalized_rows[*].proportions``,
    ``variant.canonical_ingredients`` and ``variant.header_ingredients``
    in place. Returns ``True`` iff the variant changed.

    Per RationalRecipes-2p6's design field:

      - Keeper: the form with the largest sum of proportions across the
        variant's source recipes. Reflects what the data is saying: if
        the recipes mostly used ``unsalted butter``, the merged row
        reads as ``unsalted butter``; otherwise as ``butter``.
      - Folded mass: sum of the family forms' values in each row. The
        keeper absorbs the dropped forms' contributions; the dropped
        forms are deleted.
      - For ``cells`` (display-only post-vwt.8), the per-row dominant
        form's cell stands in for the keeper's display value.
    """
    families = families_present(variant.canonical_ingredients, fold_map)
    if not families:
        return False

    new_canonical = set(variant.canonical_ingredients)
    new_header = list(variant.header_ingredients)
    changed = False

    for family, forms in families.items():
        del family  # only used for diagnostic clarity above
        # Variant-level totals across all rows pick the keeper.
        totals: dict[str, float] = {form: 0.0 for form in forms}
        for row in variant.normalized_rows:
            for form in forms:
                totals[form] += row.proportions.get(form, 0.0)
        keeper = pick_keeper(forms, totals)
        droppable = [f for f in forms if f != keeper]
        if not droppable:
            continue

        for row in variant.normalized_rows:
            # Sum proportions of all family forms present in this row,
            # collapse onto the keeper, then drop the others.
            present_forms = [f for f in forms if f in row.proportions]
            present_sum = sum(row.proportions[f] for f in present_forms)
            if present_sum > 0:
                row_keeper_form = max(
                    sorted(present_forms),
                    key=lambda f: row.proportions[f],
                )
                row.proportions[keeper] = present_sum
                # Display cell: the per-row dominant form's cell (or
                # any present form's cell) stands in for the keeper.
                if row_keeper_form in row.cells:
                    row.cells[keeper] = row.cells[row_keeper_form]
                else:
                    fallback = next(
                        (row.cells[f] for f in present_forms if f in row.cells),
                        None,
                    )
                    if fallback is not None:
                        row.cells[keeper] = fallback
            for form in droppable:
                row.proportions.pop(form, None)
                row.cells.pop(form, None)

        new_canonical.add(keeper)
        for form in droppable:
            new_canonical.discard(form)
        # Preserve header order: replace the first folded form's slot
        # with the keeper (if not already present), drop the rest.
        rebuilt_header: list[str] = []
        keeper_emitted = keeper in new_header
        for name in new_header:
            if name in droppable:
                if not keeper_emitted:
                    rebuilt_header.append(keeper)
                    keeper_emitted = True
                continue
            rebuilt_header.append(name)
        if not keeper_emitted and any(
            keeper in row.cells for row in variant.normalized_rows
        ):
            rebuilt_header.append(keeper)
        new_header = rebuilt_header
        changed = True

    if changed:
        variant.canonical_ingredients = frozenset(new_canonical)
        variant.header_ingredients = new_header
    return changed
