"""Generic/specific ingredient fold map (RationalRecipes-2p6).

Folds duplicate ingredient pairs (e.g. ``salt`` + ``kosher salt``,
``oil`` + ``vegetable oil``) inside a single variant down to one
canonical form so the catalog reads as one coherent recipe instead of
preserving every source recipe's exact naming.

Intentionally narrow. The fold runs at Pass 2 variant aggregation only;
source-recipe canonicalization keeps per-synonym specificity (e.g.
``cheddar`` vs ``cheese``, ``red onion`` vs ``onion``) per the dfm
commit (e5ed810). New families must be added here only when ONE of:

  - one form's whitespace tokens are a strict subset of the other's, AND
    the forms are routinely interchangeable in everyday cooking
    (i.e. a recipe author would substitute one for the other without
    a meaningful taste / texture / chemistry consequence), OR
  - one form is an unambiguous brand name for the other in cooking
    corpora (e.g. ``crisco`` for ``shortening``). Brand-name folds are
    admitted because the brand resolves to a single product family in
    practice; rare specialty SKUs (``butter-flavored crisco``) parse to
    distinct canonical forms upstream and are not affected, OR
  - one form is an asymmetric substitute for a generic, routinely used
    interchangeably in casual home-cooking corpora (e.g. ``margarine``
    for ``butter``). Substitute folds are admitted because the
    substitute resolves to the generic in casual recipe shapes (hash
    brown casserole, peanut butter fudge); applications that depend on
    chemistry (laminated dough, shortbread) are rare in this catalog.
    The substitute lives in the generic's family (margarine in butter,
    not vice versa).

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
#   ``margarine`` (RationalRecipes-oom): asymmetric substitute fold.
#   Margarine is interchangeable with butter in the casual recipe
#   shapes that dominate this catalog (hash brown casserole, peanut
#   butter fudge — Margarine Hash Brown Casserole n=114, Margarine
#   Peanut Butter Fudge n=128 in PWA admin feedback). Laminated dough
#   and shortbread, where butter chemistry matters, are rare enough to
#   accept as a tolerable false positive. Admitted under the substitute
#   clause above; lives in the ``butter`` family (margarine resolves to
#   butter, not vice versa).
# - shortening + crisco (RationalRecipes-0hq): brand-name fold. Crisco
#   is a vegetable shortening brand and parses standalone in baking
#   corpora; the two forms co-occur in single variants (Shortening
#   Pound Cake, Crisco Peanut Butter Cookies, Soda Crisco Angel
#   Biscuits). Admitted under the brand-name clause above.
# - baking soda + soda (RationalRecipes-0hq): standalone ``soda`` in
#   baking corpora is almost always abbreviated ``baking soda`` —
#   mass-fraction evidence (~0.4% of mixture) confirms this. The rare
#   true ``soda water`` use is rare enough to accept as a tolerable
#   false positive. Strict-subset clause applies (``soda`` ⊂
#   ``baking soda``).
# - nuts (RationalRecipes-oom): home recipes use ``nuts`` interchangeably
#   with specific nut types in the same dish (Pecan Pumpkin Bread n=445
#   has nuts in 160 source recipes and pecans in 68; Walnut Banana Bread
#   n=335 has nuts in 77 and walnuts in 45). Folding the named varieties
#   into the generic merges per-variant aggregated stats without
#   changing variant_ids (canonical-set hashing happens upstream of this
#   fold). Peanuts are excluded — they're legumes that behave differently
#   in baking chemistry. Admitted under the substitute clause.
# - white sugar (RationalRecipes-oom): when ``sugar`` (unmodified)
#   co-occurs with ``white sugar`` (and possibly ``brown sugar``) in a
#   variant, unmodified ``sugar`` is read as ``white sugar`` — the
#   default interpretation for a casual recipe author. The user opted
#   for the simple global rule rather than the asymmetric proposal in
#   the bead (case f5711861f14a White Sugar Butter Peanut Butter Cookies,
#   case 44089d87261d Soda Peanut Butter Cookies, both itemising all
#   three forms). The ``white sugar`` family explicitly contains only
#   {white sugar, sugar}; ``brown sugar`` STAYS SEPARATE (see exclusion
#   list below — different molasses content, different chemistry).
#   Strict-subset clause applies (``sugar`` ⊂ ``white sugar``).
#
# Forms deliberately *excluded* (substring relationship is real but the
# ingredients are meaningfully different):
#   - ``garlic salt`` is a flavored salt, not interchangeable with salt.
#   - ``peanut butter`` is not butter.
#   - ``brown sugar`` is not white sugar (different molasses content).
#     The RationalRecipes-oom ``white sugar`` family explicitly excludes
#     brown sugar; a variant with all three forms collapses to two
#     (white sugar with merged coverage, brown sugar separate).
#   - ``peanuts`` are legumes, not nuts; excluded from the ``nuts``
#     family above.
#   - ``cream cheese`` is not cheese; etc.
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
            "margarine",
        }
    ),
    "shortening": frozenset(
        {
            "shortening",
            "crisco",
        }
    ),
    "baking soda": frozenset(
        {
            "baking soda",
            "soda",
        }
    ),
    "nuts": frozenset(
        {
            "nuts",
            "walnuts",
            "pecans",
            "almonds",
            "hazelnuts",
            "cashews",
            "pine nuts",
            "macadamia nuts",
            "brazil nuts",
            "pistachios",
        }
    ),
    "white sugar": frozenset(
        {
            "white sugar",
            "sugar",
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
