"""Load recipes from the RecipeNLG dataset CSV.

Expected CSV columns: (index), title, ingredients, directions, link, source, NER.
The ingredients and NER columns contain stringified Python lists.
"""

from __future__ import annotations

import ast
import csv
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path

from rational_recipes.scrape.canonical import canonicalize_names
from rational_recipes.scrape.loaders import filter_ingredient_lines


@dataclass(frozen=True, slots=True)
class Recipe:
    """One recipe from RecipeNLG."""

    row_index: int
    title: str
    ingredients: tuple[str, ...]
    ner: tuple[str, ...]
    source: str
    link: str
    # ``directions`` is a tuple of step strings as decoded from the
    # CSV's stringified Python list. Default ``()`` so existing call
    # sites (tests, fixtures) don't have to pass the field through —
    # the recipenlg loader is the only path that materially populates
    # it. F5 / 15g4: surfaces the source instructions for caching in
    # ``recipes.directions_text``.
    directions: tuple[str, ...] = ()
    _ingredient_names: frozenset[str] = field(
        default=frozenset(), init=False, repr=False
    )

    def __post_init__(self) -> None:
        """Eagerly compute ingredient_names once at construction.

        Previously a @property that recomputed canonicalize_names on every
        access — profiling showed this was the dominant bottleneck in
        merge_corpora (675s for 10 groups) because the near-dup loop
        accesses ingredient_names O(n*m) times per L1 group.
        """
        object.__setattr__(self, "_ingredient_names", canonicalize_names(self.ner))

    @property
    def ingredient_names(self) -> frozenset[str]:
        """Canonicalized ingredient names from the NER column."""
        return self._ingredient_names


def _parse_string_list(raw: str) -> tuple[str, ...]:
    """Parse a stringified Python list, e.g. '["a", "b"]'."""
    try:
        parsed = ast.literal_eval(raw)
        if isinstance(parsed, list):
            return tuple(str(item) for item in parsed)
    except (ValueError, SyntaxError):
        pass
    return ()


@dataclass
class RecipeNLGLoader:
    """Lazily loads recipes from the RecipeNLG full_dataset.csv."""

    path: Path
    _count: int | None = field(default=None, init=False, repr=False)

    def iter_recipes(self) -> Iterator[Recipe]:
        """Yield all recipes from the CSV."""
        with open(self.path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    row_index = int(row.get("", row.get("Unnamed: 0", "-1")))
                except (ValueError, TypeError):
                    row_index = -1
                yield Recipe(
                    row_index=row_index,
                    title=row.get("title", "").strip(),
                    ingredients=filter_ingredient_lines(
                        _parse_string_list(row.get("ingredients", "[]"))
                    ),
                    ner=_parse_string_list(row.get("NER", "[]")),
                    source=row.get("source", ""),
                    link=row.get("link", ""),
                    directions=_parse_string_list(row.get("directions", "[]")),
                )

    def search_title(self, query: str) -> Iterator[Recipe]:
        """Yield recipes whose title contains the query (case-insensitive)."""
        q = query.lower()
        for recipe in self.iter_recipes():
            if q in recipe.title.lower():
                yield recipe
