"""Cross-corpus merge + within-variant dedup for the scraping pipeline.

Two distinct dedup steps live here, because they solve different problems:

1. ``merge_corpora()`` runs at corpus-merge time. It joins RecipeNLG
   rows against WDC rows via URL first, then by ingredient-set Jaccard
   near-dup at a threshold of 0.5 (midpoint of the 0.4-0.6 range
   measured by bead 3cu on real saffranspannkaka / fläskpannkaka pairs;
   see docs/design/recipe-scraping.md § Deduplication). On a match, WDC
   is preferred because it carries richer structured fields
   (cookingMethod, durations, yield, keywords). Unmatched rows from
   either corpus pass through.

2. ``proportion_bucket_dedup()`` runs later, *within a single variant*,
   after unit normalization. It catches "same recipe reposted to a
   second host with a fresh URL" — a different failure mode than
   cross-corpus duplication of the *same* source. The heuristic hashes
   (ingredient, rough-proportion-bucket) tuples and collapses
   collisions.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from urllib.parse import urlparse

from rational_recipes.scrape.grouping import (
    jaccard_similarity,
    normalize_title,
)
from rational_recipes.scrape.recipenlg import Recipe
from rational_recipes.scrape.wdc import WDCRecipe

DEFAULT_NEAR_DUP_THRESHOLD = 0.5
"""Jaccard similarity threshold for cross-corpus near-dup detection.

Midpoint of the 0.4-0.6 range measured by bead 3cu on real
saffranspannkaka / fläskpannkaka pairs. Source-level default; not yet
a CLI flag. Tune once the merged stream produces false-positive /
false-negative evidence.
"""


@dataclass(frozen=True, slots=True)
class MergedRecipe:
    """A corpus-tagged recipe in the merged stream.

    The fields are the common subset of Recipe (RecipeNLG) and WDCRecipe
    the downstream pipeline needs: raw ingredient lines for LLM parsing,
    canonicalized names for grouping, URL for provenance, cooking
    methods for L3 (empty on the RecipeNLG side until a second signal
    exists). Corpus-specific fields stay on the source object, which is
    preserved via ``source``.
    """

    title: str
    ingredients: tuple[str, ...]
    ingredient_names: frozenset[str]
    url: str
    cooking_methods: frozenset[str]
    corpus: str
    source: Recipe | WDCRecipe


def _from_recipenlg(r: Recipe) -> MergedRecipe:
    return MergedRecipe(
        title=r.title,
        ingredients=tuple(r.ingredients),
        ingredient_names=r.ingredient_names,
        url=r.link,
        cooking_methods=frozenset(),
        corpus="recipenlg",
        source=r,
    )


def _from_wdc(w: WDCRecipe) -> MergedRecipe:
    return MergedRecipe(
        title=w.title,
        ingredients=tuple(w.ingredients),
        ingredient_names=w.ingredient_names,
        url=w.page_url,
        cooking_methods=w.cooking_methods,
        corpus="wdc",
        source=w,
    )


def _normalize_url(url: str) -> str:
    """Normalize URL for cross-corpus equality.

    Lowercases host, strips query/fragment, strips trailing slash.
    Mirrors ``comparison._normalize_url`` so the two match paths agree.
    """
    if not url:
        return ""
    parsed = urlparse(url.lower())
    path = parsed.path.rstrip("/")
    return f"{parsed.scheme}://{parsed.netloc}{path}"


@dataclass(frozen=True, slots=True)
class MergeStats:
    """Counts describing what happened during a cross-corpus merge."""

    recipenlg_in: int
    wdc_in: int
    url_duplicates: int
    near_dup_duplicates: int
    merged_out: int


def merge_corpora(
    recipenlg_recipes: Sequence[Recipe],
    wdc_recipes: Sequence[WDCRecipe],
    *,
    near_dup_threshold: float = DEFAULT_NEAR_DUP_THRESHOLD,
) -> tuple[list[MergedRecipe], MergeStats]:
    """Merge two corpora into a single deduped stream.

    Step 1 — exact URL join after normalization. When a RecipeNLG row's
    link matches a WDC row's page_url, the WDC copy wins (richer
    structured fields) and the RecipeNLG copy is dropped.

    Step 2 — ingredient-set Jaccard near-dup within the same normalized
    title group, among rows not already URL-matched. Pairs at or above
    ``near_dup_threshold`` are declared duplicates; WDC wins on match.

    Step 3 — emit everything that survived, WDC-preferred, in a stable
    order (WDC first, then RecipeNLG). Callers may re-order.
    """
    # Step 1: URL join. Build WDC-by-normalized-URL index once.
    wdc_by_url: dict[str, list[WDCRecipe]] = {}
    for w in wdc_recipes:
        key = _normalize_url(w.page_url)
        if key:
            wdc_by_url.setdefault(key, []).append(w)

    url_matched_rnlg: set[int] = set()
    url_matched_wdc: set[int] = set()
    url_duplicates = 0
    for r in recipenlg_recipes:
        key = _normalize_url(r.link)
        if key and key in wdc_by_url:
            for w in wdc_by_url[key]:
                url_matched_rnlg.add(id(r))
                url_matched_wdc.add(id(w))
            url_duplicates += 1

    # Step 2: near-dup within normalized title groups, only among
    # rows not already URL-matched.
    unmatched_rnlg = [r for r in recipenlg_recipes if id(r) not in url_matched_rnlg]
    unmatched_wdc = [w for w in wdc_recipes if id(w) not in url_matched_wdc]

    rnlg_by_title: dict[str, list[Recipe]] = {}
    for r in unmatched_rnlg:
        key = normalize_title(r.title)
        if key:
            rnlg_by_title.setdefault(key, []).append(r)

    wdc_by_title: dict[str, list[WDCRecipe]] = {}
    for w in unmatched_wdc:
        key = normalize_title(w.title)
        if key:
            wdc_by_title.setdefault(key, []).append(w)

    near_dup_matched_rnlg: set[int] = set()
    near_dup_duplicates = 0
    for title_key, rnlg_rows in rnlg_by_title.items():
        wdc_rows = wdc_by_title.get(title_key, [])
        if not wdc_rows:
            continue
        for r in rnlg_rows:
            if id(r) in near_dup_matched_rnlg:
                continue
            for w in wdc_rows:
                sim = jaccard_similarity(r.ingredient_names, w.ingredient_names)
                if sim >= near_dup_threshold:
                    near_dup_matched_rnlg.add(id(r))
                    near_dup_duplicates += 1
                    break

    dropped_rnlg_ids = url_matched_rnlg | near_dup_matched_rnlg

    # Step 3: emit. WDC is kept whole (it wins on every cross-corpus
    # match); RecipeNLG rows dropped only if matched to a WDC row.
    merged: list[MergedRecipe] = [_from_wdc(w) for w in wdc_recipes]
    merged.extend(
        _from_recipenlg(r) for r in recipenlg_recipes if id(r) not in dropped_rnlg_ids
    )

    stats = MergeStats(
        recipenlg_in=len(recipenlg_recipes),
        wdc_in=len(wdc_recipes),
        url_duplicates=url_duplicates,
        near_dup_duplicates=near_dup_duplicates,
        merged_out=len(merged),
    )
    return merged, stats


# --- Within-variant proportion-bucket hash dedup ---


def _proportion_bucket(
    proportions: dict[str, float],
    *,
    bucket_size: float,
) -> tuple[tuple[str, int], ...]:
    """Quantize ingredient proportions into a stable bucket tuple.

    ``proportions`` maps canonical ingredient name -> proportion in
    grams per 100g of total. Bucketing is floor-division by
    ``bucket_size`` so small measurement differences don't prevent a
    collision.
    """
    return tuple(sorted((ing, int(p // bucket_size)) for ing, p in proportions.items()))


DEFAULT_BUCKET_SIZE = 2.0
"""Default proportion-bucket width in grams-per-100g.

Two recipes whose ingredient proportions round to the same 2-g bucket
are treated as the same recipe for within-variant dedup. Coarse enough
to absorb measurement noise (a ±1g difference in a 50g-of-100g
ingredient), fine enough to separate distinct recipes. Tune with real
data — see ``RationalRecipes-toj``'s tuning note.
"""


def proportion_bucket_dedup[T](
    rows: Sequence[T],
    proportions_of: Callable[[T], dict[str, float]],
    *,
    bucket_size: float = DEFAULT_BUCKET_SIZE,
) -> list[T]:
    """Drop duplicates whose proportion-bucket fingerprints collide.

    The caller supplies a ``proportions_of`` function that extracts a
    normalized-proportion dict (ingredient -> grams per 100g total) from
    each row. Rows are grouped by their bucket-tuple hash; one
    representative per bucket survives (first one wins — order-stable).

    Meant to run *within a variant*, after normalization. Applying it
    across variants would collapse legitimately different recipes that
    happen to share a fingerprint across variant boundaries.
    """
    seen: dict[tuple[tuple[str, int], ...], T] = {}
    result: list[T] = []
    for row in rows:
        key = _proportion_bucket(proportions_of(row), bucket_size=bucket_size)
        if key in seen:
            continue
        seen[key] = row
        result.append(row)
    return result
