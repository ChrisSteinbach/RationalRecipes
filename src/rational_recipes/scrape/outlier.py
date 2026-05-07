"""Per-recipe outlier scoring for the merged pipeline (Phase 3, bead 0g3).

Phase 3 of docs/design/recipe-scraping.md § "Failure mode B" is:

> Compute per-recipe distance from the group median (after dedup and
> variant-fit filtering) and surface outliers for human decision.

This module owns the "compute" half. Surfacing is Phase 2's review shell
(bead ``eco``) and is intentionally separate — the score is independently
valuable (sortable in any consumer, exportable to the SQLite writer) and
does not require the review UI to exist.

The metric is Euclidean distance from the per-ingredient median of the
variant's proportion matrix (g-per-100g). Rationale:

- Proportions are already in a common g/100g scale, so raw Euclidean
  distance is interpretable — a recipe with "sum of squared g/100g
  deltas" = 25 means its proportions are on average 5 g/100g off the
  median vector, summed across ingredients.
- Median (not mean) is robust to the outlier itself pulling the center.
- Missing ingredients in a row are treated as 0.0, which is correct: a
  pannkakor recipe that omits sugar contributes sugar=0 to its vector,
  and that genuinely moves it away from the median that does include
  sugar.
- Single-row and empty variants produce all-zero scores — no meaningful
  median exists, and a "distance from self" is zero.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence

import numpy as np


def compute_outlier_scores(
    proportion_vectors: Sequence[Mapping[str, float]],
    canonical_ingredients: Iterable[str],
) -> list[float]:
    """Euclidean distance from per-ingredient median for each row.

    ``proportion_vectors`` is one dict per recipe mapping canonical
    ingredient name → g-per-100g value. ``canonical_ingredients`` is
    the axis set for the variant; ingredients missing from a given row
    are treated as 0.0.

    Returns a list of floats aligned with ``proportion_vectors`` order.
    All zeros for empty or single-row input.
    """
    axes = sorted(set(canonical_ingredients))
    if len(proportion_vectors) <= 1 or not axes:
        return [0.0] * len(proportion_vectors)

    matrix = np.array(
        [[float(v.get(ing, 0.0)) for ing in axes] for v in proportion_vectors],
        dtype=float,
    )
    median = np.median(matrix, axis=0)
    diffs = matrix - median
    distances = np.sqrt(np.sum(diffs * diffs, axis=1))
    return [float(x) for x in distances]
