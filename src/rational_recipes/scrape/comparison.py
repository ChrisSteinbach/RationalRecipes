"""Cross-corpus comparison harness for RecipeNLG / WDC reconciliation.

Three analysis functions that show what each corpus contributes, where
they overlap, and where variant definitions may be too loose.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

from rational_recipes.scrape.grouping import (
    IngredientGroup,
    jaccard_similarity,
    normalize_title,
)
from rational_recipes.scrape.recipenlg import Recipe
from rational_recipes.scrape.wdc import WDCRecipe

# --- (a) Field complementarity map ---


def field_complementarity(
    recipenlg_recipes: Sequence[Recipe],
    wdc_recipes: Sequence[WDCRecipe],
) -> dict[str, dict[str, float]]:
    """Per-field, per-corpus fraction of recipes that have the field populated.

    Returns a dict mapping field names to {"recipenlg": fraction, "wdc": fraction}.
    """
    rnlg_total = len(recipenlg_recipes)
    wdc_total = len(wdc_recipes)

    def _rnlg_frac(predicate: Callable[[Any], bool]) -> float:
        if not rnlg_total:
            return 0.0
        return sum(1 for r in recipenlg_recipes if predicate(r)) / rnlg_total

    def _wdc_frac(predicate: Callable[[Any], bool]) -> float:
        if not wdc_total:
            return 0.0
        return sum(1 for r in wdc_recipes if predicate(r)) / wdc_total

    _dur_keys: dict[str, str] = {
        "total_time": "totaltime",
        "cook_time": "cooktime",
        "prep_time": "preptime",
    }

    result: dict[str, dict[str, float]] = {
        "ingredient_names": {
            "recipenlg": _rnlg_frac(lambda r: bool(r.ingredient_names)),
            "wdc": _wdc_frac(lambda r: bool(r.ingredient_names)),
        },
        "cooking_method": {
            "recipenlg": 0.0,
            "wdc": _wdc_frac(lambda r: bool(r.cooking_methods)),
        },
    }

    def _has_duration(key: str) -> Callable[[Any], bool]:
        return lambda r: any(d[0] == key for d in r.durations)

    for field_name, dur_key in _dur_keys.items():
        result[field_name] = {
            "recipenlg": 0.0,
            "wdc": _wdc_frac(_has_duration(dur_key)),
        }

    result["recipe_yield"] = {
        "recipenlg": 0.0,
        "wdc": _wdc_frac(lambda r: bool(r.recipe_yield)),
    }
    result["recipe_category"] = {
        "recipenlg": 0.0,
        "wdc": _wdc_frac(lambda r: bool(r.recipe_category)),
    }
    result["keywords"] = {
        "recipenlg": 0.0,
        "wdc": _wdc_frac(lambda r: bool(r.keywords)),
    }
    result["ner_names"] = {
        "recipenlg": _rnlg_frac(lambda r: bool(r.ner)),
        "wdc": 0.0,
    }
    result["source_url"] = {
        "recipenlg": _rnlg_frac(lambda r: bool(r.link)),
        "wdc": _wdc_frac(lambda r: bool(r.page_url)),
    }

    return result


# --- (b) URL / near-dup overlap ---


@dataclass(frozen=True)
class OverlapResult:
    """Result of cross-corpus overlap analysis."""

    url_matches: list[tuple[Recipe, WDCRecipe]]
    near_dup_matches: list[tuple[Recipe, WDCRecipe, float]]
    recipenlg_total: int
    wdc_total: int


def _normalize_url(url: str) -> str:
    """Normalize URL for comparison.

    Lowercases host, strips trailing slash, strips query/fragment.
    """
    if not url:
        return ""
    parsed = urlparse(url.lower())
    path = parsed.path.rstrip("/")
    return f"{parsed.scheme}://{parsed.netloc}{path}"


def url_overlap(
    recipenlg_recipes: Sequence[Recipe],
    wdc_recipes: Sequence[WDCRecipe],
    similarity_threshold: float = 0.8,
) -> OverlapResult:
    """Join RecipeNLG.link against WDC.page_url, plus Jaccard near-dup detection.

    Step 1: Exact URL join (after normalization).
    Step 2: For non-URL-matched recipes in the same normalized title group,
            compute ingredient-set Jaccard. Pairs above threshold are near-dups.
    """
    # Step 1: URL join
    url_matches: list[tuple[Recipe, WDCRecipe]] = []
    matched_rnlg_ids: set[int] = set()
    matched_wdc_ids: set[int] = set()

    wdc_by_url: dict[str, list[WDCRecipe]] = {}
    for wr in wdc_recipes:
        norm = _normalize_url(wr.page_url)
        if norm:
            wdc_by_url.setdefault(norm, []).append(wr)

    for rr in recipenlg_recipes:
        norm = _normalize_url(rr.link)
        if norm and norm in wdc_by_url:
            for wr in wdc_by_url[norm]:
                url_matches.append((rr, wr))
                matched_rnlg_ids.add(id(rr))
                matched_wdc_ids.add(id(wr))

    # Step 2: Near-dup via title + Jaccard
    unmatched_rnlg = [r for r in recipenlg_recipes if id(r) not in matched_rnlg_ids]
    unmatched_wdc = [r for r in wdc_recipes if id(r) not in matched_wdc_ids]

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

    near_dup_matches: list[tuple[Recipe, WDCRecipe, float]] = []
    for title_key in rnlg_by_title:
        if title_key not in wdc_by_title:
            continue
        for rr in rnlg_by_title[title_key]:
            for wr in wdc_by_title[title_key]:
                sim = jaccard_similarity(rr.ingredient_names, wr.ingredient_names)
                if sim >= similarity_threshold:
                    near_dup_matches.append((rr, wr, sim))

    return OverlapResult(
        url_matches=url_matches,
        near_dup_matches=near_dup_matches,
        recipenlg_total=len(recipenlg_recipes),
        wdc_total=len(wdc_recipes),
    )


# --- (c) Within-variant ratio comparison ---


@dataclass(frozen=True)
class VariantComparison:
    """Comparison of ingredient sets between two corpus groups for the same variant."""

    shared_ingredients: frozenset[str]
    recipenlg_only: frozenset[str]
    wdc_only: frozenset[str]
    recipenlg_count: int
    wdc_count: int
    per_ingredient_coverage: dict[str, dict[str, float]]


def within_variant_comparison(
    recipenlg_group: IngredientGroup[Recipe],
    wdc_group: IngredientGroup[WDCRecipe],
) -> VariantComparison:
    """Compare ingredient coverage between corpora for the same variant.

    For each ingredient that appears in either group, compute what fraction
    of recipes in each corpus contain it.
    """
    rnlg_recipes = recipenlg_group.recipes
    wdc_recipes = wdc_group.recipes
    rnlg_count = len(rnlg_recipes)
    wdc_count = len(wdc_recipes)

    all_rnlg_ingredients: set[str] = set()
    for r in rnlg_recipes:
        all_rnlg_ingredients |= r.ingredient_names

    all_wdc_ingredients: set[str] = set()
    for w in wdc_recipes:
        all_wdc_ingredients |= w.ingredient_names

    all_ingredients = all_rnlg_ingredients | all_wdc_ingredients
    shared = frozenset(all_rnlg_ingredients & all_wdc_ingredients)
    rnlg_only = frozenset(all_rnlg_ingredients - all_wdc_ingredients)
    wdc_only = frozenset(all_wdc_ingredients - all_rnlg_ingredients)

    per_ingredient_coverage: dict[str, dict[str, float]] = {}
    for ing in sorted(all_ingredients):
        rnlg_frac = (
            sum(1 for r in rnlg_recipes if ing in r.ingredient_names) / rnlg_count
            if rnlg_count
            else 0.0
        )
        wdc_frac = (
            sum(1 for r in wdc_recipes if ing in r.ingredient_names) / wdc_count
            if wdc_count
            else 0.0
        )
        per_ingredient_coverage[ing] = {
            "recipenlg": rnlg_frac,
            "wdc": wdc_frac,
        }

    return VariantComparison(
        shared_ingredients=shared,
        recipenlg_only=rnlg_only,
        wdc_only=wdc_only,
        recipenlg_count=rnlg_count,
        wdc_count=wdc_count,
        per_ingredient_coverage=per_ingredient_coverage,
    )
