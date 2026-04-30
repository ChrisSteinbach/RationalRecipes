"""Title-based dish discovery on a recipe corpus.

Streams titles, normalizes them via the Level 1 title normalizer, and
counts occurrences. Used to surface common dish names in a corpus so
the user does not have to guess a query up front.

Variant-aware discovery (``enrich_with_variants``) layers Level 2
(ingredient-set) clustering on top: for each surviving title group from
pass 1, it runs ``group_by_ingredients`` to report how many distinct
variants exist and how they partition the group.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass

from rational_recipes.scrape.grouping import (
    GroupableRecipe,
    group_by_ingredients,
    normalize_title,
)


@dataclass(frozen=True, slots=True)
class DiscoveryResult:
    """One row in the discovery output: count and the normalized title."""

    count: int
    normalized_title: str


@dataclass(frozen=True, slots=True)
class VariantSummary:
    """One L2 cluster inside a discovered title group."""

    size: int
    canonical_ingredients: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class VariantDiscoveryResult:
    """A discovery result enriched with a Level 2 variant breakdown.

    ``other_count`` is the number of recipes in the title group that
    did not land in any L2 cluster meeting ``min_variant_size`` (either
    small residual clusters or recipes with empty ingredient sets).
    """

    count: int
    normalized_title: str
    variants: tuple[VariantSummary, ...]
    other_count: int


def count_titles(titles: Iterable[str]) -> Counter[str]:
    """Count normalized titles, skipping entries that normalize to empty."""
    counter: Counter[str] = Counter()
    for title in titles:
        key = normalize_title(title)
        if key:
            counter[key] += 1
    return counter


def discover(
    titles: Iterable[str],
    min_count: int = 20,
    top_k: int = 100,
) -> list[DiscoveryResult]:
    """Count titles, filter by min_count, return the top_k by count desc.

    Ties broken alphabetically on the normalized title for stable output.
    """
    counter = count_titles(titles)
    results = [
        DiscoveryResult(count=c, normalized_title=t)
        for t, c in counter.items()
        if c >= min_count
    ]
    results.sort(key=lambda r: (-r.count, r.normalized_title))
    return results[:top_k]


def enrich_with_variants[R: GroupableRecipe](
    results: Sequence[DiscoveryResult],
    recipes: Iterable[R],
    *,
    similarity_threshold: float = 0.6,
    min_variant_size: int = 3,
) -> list[VariantDiscoveryResult]:
    """Pass 2 of discovery: attach an L2 variant breakdown to each result.

    Streams ``recipes`` once, keeping only those whose normalized title
    appears in ``results``. This avoids holding the full corpus in memory
    — typical surviving sets contain tens of thousands of recipes at
    most, versus the 2.2M-row RecipeNLG dataset.

    For each surviving title, ``group_by_ingredients`` partitions the
    bucket by Jaccard similarity on the ingredient set; the leftover
    count (recipes in small clusters or with empty ingredient names) is
    reported as ``other_count``.
    """
    surviving = {r.normalized_title for r in results}
    buckets: dict[str, list[R]] = defaultdict(list)
    for recipe in recipes:
        key = normalize_title(recipe.title)
        if key in surviving:
            buckets[key].append(recipe)

    enriched: list[VariantDiscoveryResult] = []
    for result in results:
        bucket = buckets.get(result.normalized_title, [])
        groups = group_by_ingredients(
            bucket,
            similarity_threshold=similarity_threshold,
            min_group_size=min_variant_size,
        )
        variants = tuple(
            VariantSummary(
                size=g.size,
                canonical_ingredients=tuple(sorted(g.canonical_ingredients)),
            )
            for g in groups
        )
        other = len(bucket) - sum(v.size for v in variants)
        enriched.append(
            VariantDiscoveryResult(
                count=result.count,
                normalized_title=result.normalized_title,
                variants=variants,
                other_count=other,
            )
        )
    return enriched
