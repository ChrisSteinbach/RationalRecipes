"""Ingredient conversions from milliliters to grams.

Conversion data loaded from a SQLite database built from:
  - USDA FoodData Central SR Legacy (~8K foods, portion weights)
  - FAO/INFOODS Density Database v2.0 (~600 density values)
  - Supplementary data for ingredients not in either source

See scripts/build_db.py for the database build pipeline.
"""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

_DB_PATH = Path(__file__).parent / "data" / "ingredients.db"

# Portion unit names that represent whole-unit sizes (not volume measures).
# Used to filter portion data into wholeunits2grams mappings.
_VOLUME_UNITS = frozenset(
    {
        "cup",
        "tbsp",
        "tablespoon",
        "tsp",
        "teaspoon",
        "fl oz",
        "fluid ounce",
        "quart",
        "pint",
        "liter",
        "oz",  # weight, not a "whole unit"
    }
)


def _is_volume_or_weight_unit(unit_name: str) -> bool:
    """Return True if this portion unit is a volume/weight measure, not a
    whole-unit like 'large' or 'stick'."""
    lower = unit_name.strip().lower()
    for vol in _VOLUME_UNITS:
        if lower == vol or lower.startswith(vol + " ") or lower.startswith(vol + ","):
            return True
    return False


class Factory:
    """Factory and registry for ingredient instances.

    Looks up ingredients from a SQLite database, with an in-memory cache.
    """

    _INGREDIENTS: dict[str, Ingredient] = {}
    _conn: sqlite3.Connection | None = None
    _lock: threading.Lock = threading.Lock()

    @classmethod
    def _get_conn(cls) -> sqlite3.Connection:
        if cls._conn is None:
            cls._conn = sqlite3.connect(
                str(_DB_PATH),
                check_same_thread=False,
            )
        return cls._conn

    @classmethod
    def register(cls, ingredient: Ingredient) -> None:
        """Register ingredient name and synonyms (for backward compat)"""
        for name in ingredient.synonyms():
            cls._INGREDIENTS[name.lower().strip()] = ingredient

    @classmethod
    def get_by_name(cls, name: str) -> Ingredient:
        """Lookup an Ingredient instance by name.

        First checks the in-memory cache, then queries the SQLite database.
        Thread-safe: the shared connection and cache are protected by a lock.
        """
        key = name.lower().strip()

        with cls._lock:
            # Check cache first
            if key in cls._INGREDIENTS:
                return cls._INGREDIENTS[key]

            # Query the database
            ingredient = cls._load_from_db(key)
            if ingredient is None:
                suggestions = cls._suggest(key)
                if suggestions:
                    hint = "\n".join(f"  - {s}" for s in suggestions)
                    raise KeyError(f"{name!r}. Did you mean:\n{hint}")
                raise KeyError(key)

            cls._INGREDIENTS[key] = ingredient
            return ingredient

    @classmethod
    def _suggest(cls, name: str, limit: int = 5) -> list[str]:
        """Search for foods whose name or synonym contains all query words."""
        conn = cls._get_conn()
        words = name.lower().split()
        if not words:
            return []

        # Search both food names and synonyms
        conditions = " AND ".join("name LIKE ?" for _ in words)
        params = [f"%{w}%" for w in words]

        rows = conn.execute(
            "SELECT DISTINCT name FROM ("
            f"  SELECT f.name, length(f.name) AS len FROM food f "
            f"  WHERE {conditions} "
            "  UNION "
            f"  SELECT s.name, length(s.name) AS len FROM synonym s "
            f"  WHERE {conditions} "
            ") ORDER BY len LIMIT ?",
            params + params + [limit],
        ).fetchall()

        return [r[0] for r in rows]

    @classmethod
    def _load_from_db(cls, name: str) -> Ingredient | None:
        """Load an ingredient from the SQLite database by synonym lookup."""
        conn = cls._get_conn()

        # Find the food via synonym
        row = conn.execute(
            "SELECT f.id, f.name, f.canonical_name "
            "FROM synonym s JOIN food f ON f.id = s.food_id "
            "WHERE s.name = ? COLLATE NOCASE",
            (name,),
        ).fetchone()

        if row is None:
            return None

        food_id: int = row[0]
        food_name: str = row[1]
        canonical_name: str | None = row[2]

        # Get density (prefer fdc_derived, then supplementary, then fao)
        density_rows = conn.execute(
            "SELECT g_per_ml, source FROM density WHERE food_id = ? "
            "ORDER BY CASE source "
            "  WHEN 'fdc_derived' THEN 1 "
            "  WHEN 'supplementary' THEN 2 "
            "  ELSE 3 "
            "END",
            (food_id,),
        ).fetchall()

        if density_rows:
            density = density_rows[0][0]
            density_src = density_rows[0][1]
            density_alts = [(val, src) for val, src in density_rows]
        else:
            density = 1.0
            density_src = "default"
            density_alts = []

        # Get all synonyms for this food
        synonym_rows = conn.execute(
            "SELECT name FROM synonym WHERE food_id = ?",
            (food_id,),
        ).fetchall()
        all_names = [r[0] for r in synonym_rows]

        # Build names list: lookup name first, then other short aliases,
        # excluding the verbose FDC description.
        short_names = [n for n in all_names if n != food_name and n.lower() != name]
        names = [name] + short_names

        # Get portion data (for whole-unit conversions)
        portion_rows = conn.execute(
            "SELECT unit_name, gram_weight FROM portion WHERE food_id = ?",
            (food_id,),
        ).fetchall()

        wholeunits: dict[str, float] = {}
        default_wholeunit: str | None = None

        for unit_name, gram_weight in portion_rows:
            if not _is_volume_or_weight_unit(unit_name):
                wholeunits[unit_name] = gram_weight

        # Determine default whole unit
        lower_units = {k.lower(): k for k in wholeunits}
        if "medium" in lower_units:
            default_wholeunit = lower_units["medium"]

        ingredient = Ingredient(
            names=names,
            conversion=density,
            density_source=density_src,
            density_alternatives=density_alts,
            wholeunits2weight=wholeunits if wholeunits else None,
            default_wholeunit_weight=default_wholeunit,
            canonical_name=canonical_name,
        )

        # Cache all synonyms
        for syn in names:
            cls._INGREDIENTS[syn.lower().strip()] = ingredient

        return ingredient


class Ingredient:
    """Ingredient class converts between volume, weight and whole unit
    measurements"""

    def __init__(
        self,
        names: list[str],
        conversion: float,
        density_source: str = "default",
        density_alternatives: list[tuple[float, str]] | None = None,
        wholeunits2weight: dict[str, float] | None = None,
        default_wholeunit_weight: str | None = None,
        canonical_name: str | None = None,
    ) -> None:
        self._conversion = conversion
        self._density_source = density_source
        self._density_alternatives = density_alternatives or []
        self._name = names[0]
        self._names = names
        self._canonical_name = canonical_name
        self._wholeunits2grams: dict[str, float] = {}
        if wholeunits2weight is not None:
            for unit, weight in wholeunits2weight.items():
                self._wholeunits2grams[unit.lower()] = weight
        self._default_wholeunit_weight: str | None
        if default_wholeunit_weight is not None:
            self._default_wholeunit_weight = default_wholeunit_weight.lower()
        else:
            self._default_wholeunit_weight = None

    def name(self) -> str:
        """Returns ingredient name"""
        return self._name

    def canonical_name(self) -> str:
        """Preferred short English form for cross-language comparison.

        Falls back to ``name()`` when no canonical was set for this food.
        """
        return self._canonical_name or self._name

    def synonyms(self) -> list[str]:
        """Returns a list of ingredient synonyms"""
        return self._names

    @property
    def density(self) -> float:
        """Returns the density value (g/ml) used for volume conversions."""
        return self._conversion

    @property
    def density_source(self) -> str:
        """Returns the source of the density value (e.g. 'fdc_derived',
        'supplementary', 'fao', or 'default')."""
        return self._density_source

    def density_alternatives(self) -> list[tuple[float, str]]:
        """Returns all available density values as (g_per_ml, source) tuples,
        ordered by source preference."""
        return list(self._density_alternatives)

    def milliliters2grams(self, milliliters: float) -> float:
        """Convert milliliter measure to grams"""
        return milliliters * self._conversion

    def grams2milliliters(self, grams: float) -> float:
        """Convert measure in grams to milliliters"""
        return grams / self._conversion

    def wholeunits2grams(self, wholeunit: str) -> float | None:
        """Convert whole unit measurement to grams"""
        if self._wholeunits2grams is None:
            return None
        try:
            return self._wholeunits2grams[wholeunit.lower()]
        except KeyError:
            return None

    def grams2wholeunits(self, grams: float) -> float | None:
        """Convert measure in grams to the default wholeunit
        (if such exists)"""
        weight = self.default_wholeunit_weight()
        if weight is not None:
            return grams / weight
        return None

    def default_wholeunit_weight(self) -> float | None:
        """Returns a standard weight for an ingredient, or None if there is no
        such weight"""
        if self._default_wholeunit_weight:
            return self._wholeunits2grams[self._default_wholeunit_weight]
        else:
            return None

    def __repr__(self) -> str:
        return self.name()

    def __str__(self) -> str:
        return self.name()
