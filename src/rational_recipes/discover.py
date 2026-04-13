"""Title-based dish discovery on a recipe corpus.

Streams titles, normalizes them via the Level 1 title normalizer, and
counts occurrences. Used to surface common dish names in a corpus so
the user does not have to guess a query up front.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass

from rational_recipes.scrape.grouping import normalize_title


@dataclass(frozen=True, slots=True)
class DiscoveryResult:
    """One row in the discovery output: count and the normalized title."""

    count: int
    normalized_title: str


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
