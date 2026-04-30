"""Load recipes from the Web Data Commons (WDC) recipe corpus.

The WDC corpus ships as a zip of gzipped JSON-Lines files, one per host
site, with filenames like ``Recipe_{host}_October2023.json.gz``.
"""

from __future__ import annotations

import dataclasses
import gzip
import json
import re
import zipfile
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from rational_recipes.scrape.canonical import canonicalize_names
from rational_recipes.scrape.loaders import filter_ingredient_lines
from rational_recipes.scrape.parse import (
    OLLAMA_BASE_URL,
    ParsedIngredient,  # noqa: F401 — used by callers
    parse_ingredient_line,
)

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


def _collapse_by_page_url(recipes: Sequence[WDCRecipe]) -> list[WDCRecipe]:
    """Collapse multiple Recipe entities sharing a non-empty page_url.

    WDC pages can host several JSON-LD Recipe entities under the same
    page_url (variations, "also try" alternates, templating duplicates).
    Keep only the entity with the longest ingredient list per page_url
    so downstream `recipe_id = sha1(url|title)[:12]` can't collide.
    Empty page_urls are passed through unchanged — they cannot be
    attributed to a single page and there is no collision to resolve.
    """
    winners: dict[str, int] = {}
    for i, recipe in enumerate(recipes):
        if not recipe.page_url:
            continue
        existing = winners.get(recipe.page_url)
        if existing is None or len(recipe.ingredients) > len(
            recipes[existing].ingredients
        ):
            winners[recipe.page_url] = i
    keep = set(winners.values())
    return [r for i, r in enumerate(recipes) if not r.page_url or i in keep]


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

    title_raw = row.get("name", "")
    page_url_raw = row.get("page_url", "")
    return WDCRecipe(
        row_id=row.get("row_id", 0),
        host=host,
        title=title_raw if isinstance(title_raw, str) else "",
        ingredients=filter_ingredient_lines(
            tuple(str(i) for i in ingredients_raw)
        ),
        page_url=page_url_raw if isinstance(page_url_raw, str) else "",
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
        """Yield all recipes from a single host's JSON-Lines entry.

        Multiple Recipe entities sharing the same page_url are collapsed
        to the one with the longest ingredient list — see
        `_collapse_by_page_url`.
        """
        entry_name = f"Recipe_{host}_October2023.json.gz"
        rows: list[WDCRecipe] = []
        with zipfile.ZipFile(self.zip_path) as zf:
            with zf.open(entry_name) as raw:
                with gzip.open(raw, "rt", encoding="utf-8") as gz:
                    for line in gz:
                        rows.append(_parse_row(json.loads(line), host))
        yield from _collapse_by_page_url(rows)

    def iter_all(self, hosts: Sequence[str] | None = None) -> Iterator[WDCRecipe]:
        """Yield all recipes across the zip, optionally filtered to *hosts*."""
        selected = list(hosts) if hosts is not None else self.list_hosts()
        for host in selected:
            yield from self.iter_host(host)

    def search_title(
        self,
        query: str,
        hosts: Sequence[str] | None = None,
    ) -> Iterator[WDCRecipe]:
        """Yield recipes whose title contains *query* (case-insensitive)."""
        q = query.lower()
        for recipe in self.iter_all(hosts=hosts):
            if q in recipe.title.lower():
                yield recipe


# --- Language-neutral extraction ---

NEUTRAL_PROMPT = """\
You are an ingredient parser. Given a recipe ingredient line in ANY language,
extract structured fields.

Return ONLY a JSON object with these fields:
- "ingredient": the base ingredient name in lowercase ENGLISH. Translate from
  the source language. No preparation notes, no quantities, no units.
- "quantity": number (float). For fractions, convert to decimal. If no
  quantity, use 1.
- "unit": the unit of measurement in the original language, lowercase.
  For COUNTABLE items where NO unit is stated (e.g. "3 ägg", "2 apples",
  "3 Forellenfilet"), use "MEDIUM" as a size sentinel. If a size adjective
  is the only modifier (Swedish "stor"/"stort"/"stora", "liten"/"små";
  German "große"/"kleine"; etc.), emit "LARGE" or "SMALL" instead of the
  adjective. If the line HAS an explicit counter or packaging unit
  (Japanese 個/丁/かけ, Russian шт., "burk"/"paket"/"Dose"), keep that
  counter as the unit. If no unit AND the item is NOT countable (bare
  ingredient like "smör", "to taste", "适量"), use "" (empty string).
- "preparation": any preparation notes, translated to English. Empty string
  if none.

Examples in different languages:

Input: "3 dl vetemjöl"
Output: {"ingredient": "flour", "quantity": 3.0, "unit": "dl", "preparation": ""}

Input: "3 ägg"
Output: {"ingredient": "egg", "quantity": 3.0, "unit": "MEDIUM", "preparation": ""}

Input: "1 stort ägg"
Output: {"ingredient": "egg", "quantity": 1.0, "unit": "LARGE", "preparation": ""}

Input: "2 große Eier"
Output: {"ingredient": "egg", "quantity": 2.0, "unit": "LARGE", "preparation": ""}

Input: "3 kleine Zucchini"
Output: {"ingredient": "zucchini", "quantity": 3.0, "unit": "SMALL", "preparation": ""}

Input: "卵 3個"
Output: {"ingredient": "egg", "quantity": 3.0, "unit": "個", "preparation": ""}

Input: "молоко - 500 мл"
Output: {"ingredient": "milk", "quantity": 500.0, "unit": "мл", "preparation": ""}

Input: "250 g frysta, halvtinade blåbär"
Output: {"ingredient": "blueberries", "quantity": 250.0, "unit": "g",\
 "preparation": "frozen, half-thawed"}

Input: "smör"
Output: {"ingredient": "butter", "quantity": 1.0, "unit": "", "preparation": ""}
"""


def extract_ingredient_names(
    recipe: WDCRecipe,
    *,
    model: str = "gemma4:e2b",
    base_url: str = OLLAMA_BASE_URL,
) -> WDCRecipe:
    """Extract ingredient names from raw ingredient lines via LLM.

    The LLM returns names in the source language; each is routed through
    IngredientFactory to canonicalize to English so cross-corpus comparison
    sees a shared vocabulary. Returns a new WDCRecipe with
    ingredient_names populated.
    """
    raw_names: list[str] = []
    for line in recipe.ingredients:
        parsed = parse_ingredient_line(
            line,
            model=model,
            base_url=base_url,
            system_prompt=NEUTRAL_PROMPT,
        )
        if parsed and parsed.ingredient:
            raw_names.append(parsed.ingredient)
    return dataclasses.replace(recipe, ingredient_names=canonicalize_names(raw_names))


def extract_batch(
    recipes: Sequence[WDCRecipe],
    *,
    model: str = "gemma4:e2b",
    base_url: str = OLLAMA_BASE_URL,
    cache: dict[str, frozenset[str]] | None = None,
) -> list[WDCRecipe]:
    """Extract ingredient names for a batch of recipes.

    Uses an optional page_url-keyed cache to avoid re-extraction.
    """
    if cache is None:
        cache = {}
    result: list[WDCRecipe] = []
    for recipe in recipes:
        if recipe.page_url and recipe.page_url in cache:
            result.append(
                dataclasses.replace(recipe, ingredient_names=cache[recipe.page_url])
            )
        else:
            extracted = extract_ingredient_names(
                recipe,
                model=model,
                base_url=base_url,
            )
            if recipe.page_url:
                cache[recipe.page_url] = extracted.ingredient_names
            result.append(extracted)
    return result
