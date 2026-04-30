"""Level 1 (title-based), Level 2 (ingredient-set), and Level 3
(cookingMethod) grouping.

Level 1 normalizes recipe titles and groups by exact match on the
normalized form. Level 2 clusters within each L1 group by Jaccard
similarity of ingredient sets (from the NER column). Level 3 splits
within each L2 group by distinct cookingMethod tag sets — the finest-
grained dish-identity signal WDC carries directly (ugnsmannkaka vs
stekpannkaka: same batter, different technique).

RecipeNLG rows carry no cookingMethod, so L3 is a no-op on pure-
RecipeNLG input. On mixed RecipeNLG+WDC streams, L3 partitions the
WDC portion while the RecipeNLG portion falls into the "unknown-method"
bucket.
"""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol, runtime_checkable


@runtime_checkable
class GroupableRecipe(Protocol):
    @property
    def title(self) -> str: ...
    @property
    def ingredient_names(self) -> frozenset[str]: ...


@runtime_checkable
class MethodedRecipe(Protocol):
    @property
    def cooking_methods(self) -> frozenset[str]: ...


# --- Level 1: Title normalization and grouping ---


def normalize_title(title: str) -> str:
    """Normalize a recipe title for grouping.

    Steps:
    - lowercase
    - strip leading/trailing whitespace
    - strip trailing "recipe" / "recipes"
    - strip possessives ('s, 's)
    - remove parenthesized text (e.g. "(Swedish Crisp Pancakes)")
    - collapse multiple spaces
    - strip leading/trailing whitespace again
    """
    t = title.lower().strip()
    # Remove parenthesized/bracketed text
    t = re.sub(r"\([^)]*\)", "", t)
    t = re.sub(r"\[[^\]]*\]", "", t)
    # Strip possessives
    t = re.sub(r"['\u2019]s\b", "", t)
    # Strip trailing "recipe(s)"
    t = re.sub(r"\brecipes?\s*$", "", t)
    # Collapse whitespace
    t = re.sub(r"\s+", " ", t).strip()
    return t


def group_by_title[R: GroupableRecipe](
    recipes: Sequence[R],
    min_group_size: int = 5,
) -> dict[str, list[R]]:
    """Level 1: group recipes by normalized title.

    Returns only groups that meet the minimum size threshold.
    """
    groups: dict[str, list[R]] = defaultdict(list)
    for recipe in recipes:
        key = normalize_title(recipe.title)
        if key:
            groups[key].append(recipe)

    return {k: v for k, v in groups.items() if len(v) >= min_group_size}


# --- Level 2: Ingredient-set grouping ---


def jaccard_similarity(a: frozenset[str], b: frozenset[str]) -> float:
    """Jaccard similarity between two sets."""
    if not a and not b:
        return 1.0
    intersection = len(a & b)
    union = len(a | b)
    return intersection / union if union > 0 else 0.0


@dataclass
class IngredientGroup[R: GroupableRecipe]:
    """A cluster of recipes sharing a similar ingredient set."""

    canonical_ingredients: frozenset[str]
    recipes: list[R]

    @property
    def size(self) -> int:
        return len(self.recipes)


def group_by_ingredients[R: GroupableRecipe](
    recipes: Sequence[R],
    similarity_threshold: float = 0.6,
    min_group_size: int = 3,
) -> list[IngredientGroup[R]]:
    """Level 2: cluster recipes by ingredient-set Jaccard similarity.

    Uses a greedy single-pass clustering: iterate recipes, assign each to
    the first existing cluster whose centroid has Jaccard >= threshold, or
    start a new cluster.

    Returns groups sorted by size (largest first), filtered by min_group_size.
    """
    clusters: list[IngredientGroup[R]] = []

    for recipe in recipes:
        names = recipe.ingredient_names
        if not names:
            continue

        placed = False
        for cluster in clusters:
            sim = jaccard_similarity(names, cluster.canonical_ingredients)
            if sim >= similarity_threshold:
                cluster.recipes.append(recipe)
                placed = True
                break

        if not placed:
            clusters.append(
                IngredientGroup(
                    canonical_ingredients=names,
                    recipes=[recipe],
                )
            )

    # Filter and sort
    result = [c for c in clusters if c.size >= min_group_size]
    result.sort(key=lambda c: c.size, reverse=True)
    return result


# --- Level 3: cookingMethod partition ---


DEFAULT_L3_MIN_VARIANT_SIZE = 3
"""Default min_variant_size for L3 sub-group filtering.

Matches L1/L2 defaults. Too small → unstable stats. Too large → over-drops
variants from smaller dish families. Tune empirically (bead 7eo acceptance)
by running the full pipeline on 3+ dish families and reporting sensitivity.
"""


@dataclass(frozen=True)
class MethodVariant[R: MethodedRecipe]:
    """One L3 sub-group: recipes sharing a cookingMethod tag set."""

    cooking_methods: frozenset[str]
    recipes: list[R]

    @property
    def size(self) -> int:
        return len(self.recipes)


def group_by_cooking_method[R: MethodedRecipe](
    recipes: Sequence[R],
    *,
    min_variant_size: int = DEFAULT_L3_MIN_VARIANT_SIZE,
) -> list[MethodVariant[R]]:
    """Level 3: partition recipes by distinct cookingMethod tag set.

    Algorithm (docs/design/recipe-scraping.md § Level 3):

    1. Bucket rows by their ``cooking_methods`` frozenset. Rows with
       empty methods form an "unknown-method" bucket.
    2. If the unknown bucket is a singleton, merge it into the largest
       non-empty bucket — a single un-tagged row should not splinter
       off as its own variant.
    3. Drop any resulting bucket below ``min_variant_size``.

    Returns buckets sorted by size (largest first).

    Pure-RecipeNLG input (no cookingMethod anywhere) degenerates to a
    single "unknown-method" bucket, which after the singleton-merge
    step and min-size filter either survives as-is (if large enough) or
    is dropped.
    """
    buckets: dict[frozenset[str], list[R]] = defaultdict(list)
    for recipe in recipes:
        buckets[recipe.cooking_methods].append(recipe)

    empty_key: frozenset[str] = frozenset()
    if empty_key in buckets and len(buckets[empty_key]) == 1:
        non_empty = {k: v for k, v in buckets.items() if k != empty_key}
        if non_empty:
            largest_key = max(non_empty, key=lambda k: len(non_empty[k]))
            stragglers = buckets.pop(empty_key)
            buckets[largest_key].extend(stragglers)

    result = [
        MethodVariant(cooking_methods=k, recipes=v)
        for k, v in buckets.items()
        if len(v) >= min_variant_size
    ]
    result.sort(key=lambda v: v.size, reverse=True)
    return result
