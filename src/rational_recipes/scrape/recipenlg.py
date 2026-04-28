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

    @property
    def ingredient_names(self) -> frozenset[str]:
        """Canonicalized ingredient names from the NER column.

        Each NER name is routed through IngredientFactory so cross-corpus
        comparison sees a shared English vocabulary.
        """
        return canonicalize_names(self.ner)


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
                )

    def search_title(self, query: str) -> Iterator[Recipe]:
        """Yield recipes whose title contains the query (case-insensitive)."""
        q = query.lower()
        for recipe in self.iter_recipes():
            if q in recipe.title.lower():
                yield recipe
