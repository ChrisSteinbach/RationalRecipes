"""Load recipes from the Web Data Commons (WDC) recipe corpus.

The WDC corpus ships as a zip of gzipped JSON-Lines files, one per host
site, with filenames like ``Recipe_{host}_October2023.json.gz``.
"""

from __future__ import annotations

import gzip
import json
import re
import zipfile
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# --- ISO 8601 duration parsing ---

_ISO8601_RE = re.compile(
    r"^P"
    r"(?:(\d+)Y)?"
    r"(?:(\d+)M)?"
    r"(?:(\d+)D)?"
    r"(?:T"
    r"(?:(\d+)H)?"
    r"(?:(\d+)M)?"
    r"(?:(\d+(?:\.\d+)?)S)?"
    r")?$"
)


def parse_iso8601_duration(s: str) -> float | None:
    """Parse an ISO 8601 duration string and return total minutes.

    Handles both verbose (``P0Y0M0DT0H35M0.000S``) and short
    (``PT20M``, ``PT1H30M``) forms.  Returns ``None`` if *s* cannot be
    parsed.
    """
    if not s:
        return None
    m = _ISO8601_RE.match(s.strip())
    if not m:
        return None
    hours = int(m.group(4) or 0)
    minutes = int(m.group(5) or 0)
    seconds = float(m.group(6) or 0)
    return hours * 60.0 + minutes + seconds / 60.0


# --- WDCRecipe dataclass ---


@dataclass(frozen=True, slots=True)
class WDCRecipe:
    """One recipe row from the WDC corpus."""

    row_id: int
    host: str
    title: str
    ingredients: tuple[str, ...]
    page_url: str
    cooking_methods: frozenset[str]
    durations: tuple[tuple[str, float], ...]
    recipe_category: str
    keywords: tuple[str, ...]
    recipe_yield: str
    ingredient_names: frozenset[str] = frozenset()


# --- WDCLoader ---

_ENTRY_RE = re.compile(r"^Recipe_(.+)_October2023\.json\.gz$")

_DURATION_FIELDS = ("totaltime", "cooktime", "preptime")


def _comma_split(raw: str) -> tuple[str, ...]:
    """Split a comma-separated string, stripping whitespace and empties."""
    if not raw:
        return ()
    return tuple(part.strip() for part in raw.split(",") if part.strip())


def _parse_row(row: dict[str, Any], host: str) -> WDCRecipe:
    ingredients_raw = row.get("recipeingredient", [])
    if not isinstance(ingredients_raw, list):
        ingredients_raw = []

    cooking_raw = row.get("cookingmethod", "")
    if not isinstance(cooking_raw, str):
        cooking_raw = ""

    durations: list[tuple[str, float]] = []
    for field in _DURATION_FIELDS:
        val = row.get(field, "")
        if isinstance(val, str) and val:
            parsed = parse_iso8601_duration(val)
            if parsed is not None:
                durations.append((field, parsed))

    keywords_raw = row.get("keywords", "")
    if not isinstance(keywords_raw, str):
        keywords_raw = ""

    return WDCRecipe(
        row_id=row.get("row_id", 0),
        host=host,
        title=row.get("name", ""),
        ingredients=tuple(str(i) for i in ingredients_raw),
        page_url=row.get("page_url", ""),
        cooking_methods=frozenset(
            part.strip() for part in cooking_raw.split(",") if part.strip()
        ),
        durations=tuple(durations),
        recipe_category=row.get("recipecategory", ""),
        keywords=_comma_split(keywords_raw),
        recipe_yield=row.get("recipeyield", ""),
    )


@dataclass
class WDCLoader:
    """Stream recipes from a WDC recipe corpus zip file."""

    zip_path: Path

    def list_hosts(self) -> list[str]:
        """Return hostnames extracted from zip entry names."""
        hosts: list[str] = []
        with zipfile.ZipFile(self.zip_path) as zf:
            for name in zf.namelist():
                m = _ENTRY_RE.match(name)
                if m:
                    hosts.append(m.group(1))
        return hosts

    def iter_host(self, host: str) -> Iterator[WDCRecipe]:
        """Yield all recipes from a single host's JSON-Lines entry."""
        entry_name = f"Recipe_{host}_October2023.json.gz"
        with zipfile.ZipFile(self.zip_path) as zf:
            with zf.open(entry_name) as raw:
                with gzip.open(raw, "rt", encoding="utf-8") as gz:
                    for line in gz:
                        row = json.loads(line)
                        yield _parse_row(row, host)

    def iter_all(self) -> Iterator[WDCRecipe]:
        """Yield all recipes across every host in the zip."""
        for host in self.list_hosts():
            yield from self.iter_host(host)

    def search_title(
        self,
        query: str,
        hosts: Sequence[str] | None = None,
    ) -> Iterator[WDCRecipe]:
        """Yield recipes whose title contains *query* (case-insensitive)."""
        q = query.lower()
        if hosts is not None:
            source: Iterator[WDCRecipe] = (r for h in hosts for r in self.iter_host(h))
        else:
            source = self.iter_all()
        for recipe in source:
            if q in recipe.title.lower():
                yield recipe
