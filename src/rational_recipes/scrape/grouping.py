"""Level 1 (title-based) and Level 2 (ingredient-set) grouping.

Level 1 normalizes recipe titles and groups by exact match on the
normalized form. Level 2 clusters within each L1 group by Jaccard
similarity of ingredient sets (from the NER column).
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
