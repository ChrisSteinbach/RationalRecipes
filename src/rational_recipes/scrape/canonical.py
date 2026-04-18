"""Cross-language canonicalization of ingredient names via IngredientFactory.

Extraction paths (RecipeNLG NER, WDC LLM) produce raw ingredient names in
whatever language the source recipe used. This module maps each raw name
to its English canonical form by looking it up through the ingredient
synonym table. Names that don't resolve are kept in their
lowercased-stripped original form so partial DB coverage doesn't silently
drop ingredients from the set.
"""

from __future__ import annotations

from collections.abc import Iterable

from rational_recipes.ingredient import Factory as IngredientFactory


def canonicalize_name(name: str) -> str:
    """Map a raw ingredient name to a canonical English form.

    Looks up the lowercased-stripped name in the ingredient synonym table.
    Returns the food's canonical name on hit (set during DB build from the
    first English alias defined for that food); returns the
    lowercased-stripped original on miss (empty input yields empty string).
    """
    normalized = name.lower().strip()
    if not normalized:
        return ""
    try:
        return IngredientFactory.get_by_name(normalized).canonical_name()
    except KeyError:
        return normalized


def canonicalize_names(names: Iterable[str]) -> frozenset[str]:
    """Canonicalize a batch of raw ingredient names."""
    return frozenset(c for c in (canonicalize_name(n) for n in names) if c)
