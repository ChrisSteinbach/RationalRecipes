#!/usr/bin/env python3
"""Build the ingredients SQLite database from USDA FDC and FAO/INFOODS data.

Data sources:
  - USDA FoodData Central SR Legacy: food names, portion weights
  - FAO/INFOODS Density Database v2.0: density (g/mL) values

Run scripts/download_data.sh first to fetch the raw data files.

Usage:
    python3 scripts/build_db.py
"""

from __future__ import annotations

import csv
import re
import sqlite3
import sys
from pathlib import Path

try:
    import openpyxl
except ImportError:
    print(
        "Error: openpyxl is required. Install with: pip install openpyxl",
        file=sys.stderr,
    )
    sys.exit(1)

ROOT = Path(__file__).parent.parent
DATA_DIR = ROOT / "data"
FDC_DIR = DATA_DIR / "fdc"
FAO_DIR = DATA_DIR / "fao"
DB_PATH = ROOT / "src" / "rational_recipes" / "data" / "ingredients.db"

US_CUP_ML = 236.588
US_TBSP_ML = 14.787
US_TSP_ML = 4.929


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS food (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL,
    canonical_name  TEXT,            -- short English form; NULL = fall back to name
    source          TEXT NOT NULL,   -- 'fdc', 'fao', 'supplementary'
    fdc_id          INTEGER,         -- USDA FDC ID (NULL for non-FDC foods)
    UNIQUE(name, source)
);

CREATE TABLE IF NOT EXISTS synonym (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    food_id     INTEGER NOT NULL REFERENCES food(id),
    name        TEXT NOT NULL UNIQUE COLLATE NOCASE
);

CREATE TABLE IF NOT EXISTS density (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    food_id     INTEGER NOT NULL REFERENCES food(id),
    g_per_ml    REAL NOT NULL,
    source      TEXT NOT NULL,  -- 'fdc_derived', 'fao', 'supplementary'
    notes       TEXT
);

CREATE TABLE IF NOT EXISTS portion (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    food_id     INTEGER NOT NULL REFERENCES food(id),
    unit_name   TEXT NOT NULL,
    gram_weight REAL NOT NULL,
    source      TEXT NOT NULL,  -- 'fdc', 'supplementary'
    notes       TEXT
);

CREATE INDEX IF NOT EXISTS idx_synonym_name ON synonym(name COLLATE NOCASE);
CREATE INDEX IF NOT EXISTS idx_density_food ON density(food_id);
CREATE INDEX IF NOT EXISTS idx_portion_food ON portion(food_id);
CREATE INDEX IF NOT EXISTS idx_food_fdc_id ON food(fdc_id);
"""


# ---------------------------------------------------------------------------
# USDA FDC loading
# ---------------------------------------------------------------------------


def load_fdc_foods(conn: sqlite3.Connection) -> dict[str, int]:
    """Load all SR Legacy foods into the food table.

    Returns mapping of fdc_id (str) -> food.id (int).
    """
    fdc_id_map: dict[str, int] = {}
    food_csv = FDC_DIR / "food.csv"

    with open(food_csv) as f:
        reader = csv.DictReader(f)
        for row in reader:
            fdc_id = row["fdc_id"]
            name = row["description"]
            cur = conn.execute(
                "INSERT INTO food (name, source, fdc_id) VALUES (?, 'fdc', ?)",
                (name, int(fdc_id)),
            )
            food_db_id = cur.lastrowid
            assert food_db_id is not None
            fdc_id_map[fdc_id] = food_db_id

            # Register the food name as a synonym
            try:
                conn.execute(
                    "INSERT INTO synonym (food_id, name) VALUES (?, ?)",
                    (food_db_id, name),
                )
            except sqlite3.IntegrityError:
                pass  # duplicate synonym

    return fdc_id_map


def load_fdc_portions(conn: sqlite3.Connection, fdc_id_map: dict[str, int]) -> None:
    """Load portion data from food_portion.csv."""
    portion_csv = FDC_DIR / "food_portion.csv"

    with open(portion_csv) as f:
        reader = csv.DictReader(f)
        for row in reader:
            fdc_id = row["fdc_id"]
            food_db_id = fdc_id_map.get(fdc_id)
            if food_db_id is None:
                continue

            modifier = row["modifier"].strip()
            if not modifier:
                continue

            try:
                amount = float(row["amount"])
                gram_weight = float(row["gram_weight"])
            except (ValueError, TypeError):
                continue

            if amount <= 0 or gram_weight <= 0:
                continue

            # Normalize to per-unit weight
            per_unit = gram_weight / amount

            conn.execute(
                "INSERT INTO portion (food_id, unit_name, gram_weight, source) "
                "VALUES (?, ?, ?, 'fdc')",
                (food_db_id, modifier, round(per_unit, 4)),
            )


def derive_fdc_densities(conn: sqlite3.Connection) -> int:
    """Derive density (g/mL) from cup/tbsp/tsp portion data.

    Returns count of densities derived.
    """
    count = 0

    # For each food that has portion data, try to derive density
    rows = conn.execute(
        "SELECT DISTINCT food_id FROM portion WHERE source = 'fdc'"
    ).fetchall()

    for (food_id,) in rows:
        density = _derive_density_for_food(conn, food_id)
        if density is not None:
            conn.execute(
                "INSERT INTO density (food_id, g_per_ml, source, notes) "
                "VALUES (?, ?, 'fdc_derived', ?)",
                (food_id, round(density, 6), "derived from portion data"),
            )
            count += 1

    return count


def _derive_density_for_food(conn: sqlite3.Connection, food_id: int) -> float | None:
    """Derive density for a single food from its portion data."""
    raw_portions = conn.execute(
        "SELECT unit_name, gram_weight FROM portion "
        "WHERE food_id = ? AND source = 'fdc'",
        (food_id,),
    ).fetchall()
    portions: list[tuple[str, float]] = [
        (str(row[0]), float(row[1])) for row in raw_portions
    ]

    # Try cup portions first
    cup_portions = [
        (name, weight) for name, weight in portions if "cup" in name.lower()
    ]
    if cup_portions:
        # Prefer plain "cup", then "fluid" cups, then any cup
        # Exclude "whipped" cups as they measure aerated volume
        plain = [(n, w) for n, w in cup_portions if n.strip().lower() == "cup"]
        fluid = [(n, w) for n, w in cup_portions if "fluid" in n.lower()]
        non_whipped = [(n, w) for n, w in cup_portions if "whip" not in n.lower()]
        best = plain or fluid or non_whipped or cup_portions
        _name, weight = best[0]
        return weight / US_CUP_ML

    # Fall back to tablespoon
    tbsp_portions = [
        (name, weight)
        for name, weight in portions
        if name.strip().lower() in ("tbsp", "tablespoon")
    ]
    if tbsp_portions:
        return tbsp_portions[0][1] / US_TBSP_ML

    # Fall back to teaspoon
    tsp_portions = [
        (name, weight)
        for name, weight in portions
        if re.match(r"^tsp\b", name.strip().lower())
    ]
    if tsp_portions:
        return tsp_portions[0][1] / US_TSP_ML

    # Fall back to fluid ounce (29.5735 mL)
    floz_portions = [
        (name, weight)
        for name, weight in portions
        if name.strip().lower() in ("fl oz", "fluid ounce")
    ]
    if floz_portions:
        return floz_portions[0][1] / 29.5735

    return None


# ---------------------------------------------------------------------------
# FAO/INFOODS loading
# ---------------------------------------------------------------------------


def load_fao_densities(conn: sqlite3.Connection) -> int:
    """Load density values from FAO/INFOODS database.

    Creates food entries for items not already in the DB, and adds density
    records for all items with usable density or specific gravity values.

    Returns count of densities loaded.
    """
    xlsx_path = FAO_DIR / "density_db_v2.xlsx"
    wb = openpyxl.load_workbook(xlsx_path, read_only=True)
    ws = wb["Density DB"]

    count = 0
    for row in ws.iter_rows(min_row=2, values_only=True):
        name = row[0]
        density_raw = row[1]
        sg_raw = row[2]
        source_tag = row[3]

        if not name or not isinstance(name, str):
            continue

        name = name.strip()
        if not name:
            continue

        # Determine density value
        density: float | None = None
        if isinstance(density_raw, (int, float)):
            density = float(density_raw)
        elif isinstance(sg_raw, (int, float)):
            # Specific gravity ≈ density in g/mL (at typical food temps)
            density = float(sg_raw)

        if density is None or density <= 0:
            continue

        fao_source = f"fao/{source_tag}" if source_tag else "fao"

        # Check if this food already exists (from FDC)
        existing = conn.execute(
            "SELECT id FROM food WHERE name = ? COLLATE NOCASE",
            (name,),
        ).fetchone()

        if existing:
            food_id = existing[0]
        else:
            cur = conn.execute(
                "INSERT INTO food (name, canonical_name, source) VALUES (?, ?, 'fao')",
                (name, name.lower()),
            )
            food_id = cur.lastrowid
            assert food_id is not None
            # Register as synonym
            try:
                conn.execute(
                    "INSERT INTO synonym (food_id, name) VALUES (?, ?)",
                    (food_id, name),
                )
            except sqlite3.IntegrityError:
                pass

        conn.execute(
            "INSERT INTO density (food_id, g_per_ml, source, notes) "
            "VALUES (?, ?, ?, ?)",
            (food_id, round(density, 6), fao_source, None),
        )
        count += 1

    wb.close()
    return count


# ---------------------------------------------------------------------------
# Supplementary data
# ---------------------------------------------------------------------------

# Ingredients not in FDC/FAO, or needing specific synonym/portion overrides
SUPPLEMENTARY: list[dict[str, str | float | list[str]]] = [
    {
        "name": "Rum",
        "synonyms": ["rum"],
        "density": 0.94,
        "source_note": "typical 40% ABV spirit",
    },
    {
        "name": "Juice (generic)",
        "synonyms": ["juice"],
        "density": 1.05,
        "source_note": "approximate for mixed fruit juices",
    },
    {
        "name": "Apple (raw)",
        "synonyms": ["apple", "apples"],
        "density": 0.56,
        "source_note": "chopped/diced apple bulk density",
    },
    {
        "name": "Raisins",
        "synonyms": ["raisin", "raisins"],
        "density": 0.65,
        "source_note": "packed raisins bulk density",
    },
    {
        "name": "Candied peel",
        "synonyms": ["peel"],
        "density": 0.75,
        "source_note": "chopped candied peel estimate",
    },
    {
        "name": "Grated cheese (generic)",
        "synonyms": ["grated cheese"],
        "density": 0.38,
        "source_note": "loosely packed grated cheese",
    },
    {
        "name": "Potato starch",
        "synonyms": [
            "potato starch",
            # Swedish: "potatismjöl" literally translates to "potato flour" but
            # in Swedish baking it denotes the starch (thickening agent, used
            # like cornstarch), not the flour sense in the FDC "Potato flour"
            # entry. Route to starch, not flour.
            "potatismjöl",
            "potatismjol",
        ],
        "density": 0.72,
        "source_note": "bulk density of potato starch powder",
    },
    {
        "name": "Cardamom seeds",
        "synonyms": ["cardamom seed", "cardamom seeds"],
        "density": 0.65,
        "source_note": "whole cardamom seed bulk density",
    },
    {
        "name": "Vanilla sugar",
        "synonyms": ["vanilla sugar", "vaniljsocker"],
        "density": 0.85,
        "source_note": (
            "Swedish vaniljsocker: ~95% granulated sugar + ~5% vanillin; "
            "bulk density close to granulated sugar"
        ),
    },
    {
        "name": "Lingonberries",
        "synonyms": ["lingonberry", "lingonberries", "lingon"],
        "density": 0.60,
        "source_note": "raw lingonberries, similar to other small red berries",
    },
    {
        "name": "Almond flour",
        "synonyms": [
            "almond flour",
            "almond meal",
            "ground almond meal",
            "mandelmjöl",
            "mandelmjol",
        ],
        "density": 0.45,
        "source_note": "ground blanched almonds bulk density",
    },
    {
        "name": "Swedish syrup",
        "synonyms": ["sirap", "ljus sirap", "mörk sirap", "mork sirap"],
        "density": 1.40,
        "source_note": (
            "Swedish sirap: inverted-sugar syrup between US corn syrup and "
            "molasses in character; density typical for heavy food syrups"
        ),
    },
]

# Synonym aliases: map common recipe names to FDC food descriptions
# These let Factory.get_by_name("flour") resolve to the right FDC food.
FDC_SYNONYM_ALIASES: list[tuple[str, str]] = [
    # (synonym, fdc_food_description)
    ("milk", "Milk, whole, 3.25% milkfat, with added vitamin D"),
    ("water", "Beverages, water, tap, drinking"),
    ("egg", "Egg, whole, raw, fresh"),
    ("eggs", "Egg, whole, raw, fresh"),
    ("egg yolk", "Egg, yolk, raw, fresh"),
    ("yolk", "Egg, yolk, raw, fresh"),
    ("egg white", "Egg, white, raw, fresh"),
    # The first synonym listed per food sets its canonical name — put the
    # preferred English short form first, longer/variant names after.
    ("flour", "Wheat flour, white, all-purpose, enriched, bleached"),
    ("all purpose flour", "Wheat flour, white, all-purpose, enriched, bleached"),
    ("plain flour", "Wheat flour, white, all-purpose, enriched, bleached"),
    ("salt", "Salt, table"),
    ("butter", "Butter, without salt"),
    ("sugar", "Sugars, granulated"),
    ("granulated sugar", "Sugars, granulated"),
    ("brown sugar", "Sugars, brown"),
    ("icing sugar", "Sugars, powdered"),
    ("powder sugar", "Sugars, powdered"),
    ("confectioner's sugar", "Sugars, powdered"),
    ("honey", "Honey"),
    ("cream", "Cream, fluid, heavy whipping"),
    ("sour cream", "Cream, sour, cultured"),
    ("cocoa", "Cocoa, dry powder, unsweetened"),
    ("cornstarch", "Cornstarch"),
    ("baking soda", "Leavening agents, baking soda"),
    ("bicarbonate", "Leavening agents, baking soda"),
    ("bicarbonate of soda", "Leavening agents, baking soda"),
    (
        "baking powder",
        "Leavening agents, baking powder, double-acting, sodium aluminum sulfate",
    ),
    ("vanilla extract", "Vanilla extract"),
    ("buttermilk", "Milk, buttermilk, fluid, whole"),
    ("butter milk", "Milk, buttermilk, fluid, whole"),
    ("blueberries", "Blueberries, raw"),
    ("potato", "Potatoes, flesh and skin, raw"),
    ("potatoes", "Potatoes, flesh and skin, raw"),
    ("shredded potato", "Potatoes, flesh and skin, raw"),
    ("grated potato", "Potatoes, flesh and skin, raw"),
    ("onion", "Onions, raw"),
    ("onions", "Onions, raw"),
    ("rice", "Rice, white, long-grain, regular, raw, enriched"),
    ("fresh yeast", "Leavening agents, yeast, baker's, compressed"),
    ("molasses", "Molasses"),
    ("black treacle", "Molasses"),
    ("cinnamon", "Spices, cinnamon, ground"),
    ("ground cinnamon", "Spices, cinnamon, ground"),
    ("nutmeg", "Spices, nutmeg, ground"),
    ("ground nutmeg", "Spices, nutmeg, ground"),
    ("cardamom", "Spices, cardamom"),
    ("ground cardamom", "Spices, cardamom"),
    ("ground ginger", "Spices, ginger, ground"),
    ("ground cloves", "Spices, cloves, ground"),
    ("cloves", "Spices, cloves, ground"),
    ("all spice", "Spices, allspice, ground"),
    ("ricotta", "Cheese, ricotta, whole milk"),
    ("ricotta cheese", "Cheese, ricotta, whole milk"),
    ("grated parmesan", "Cheese, parmesan, grated"),
    ("corn syrup", "Syrups, corn, light"),
    ("chocolate 70 percent", "Chocolate, dark, 70-85% cacao solids"),
    ("dark chocolate", "Chocolate, dark, 70-85% cacao solids"),
    ("vegetable shortening", "Shortening, vegetable, household, composite"),
    ("crisco", "Shortening, vegetable, household, composite"),
    ("malt extract", "Syrups, malt"),
    ("malt syrup", "Syrups, malt"),
    # English names for FDC foods not registered under a short name yet,
    # including longer variants observed in the RecipeNLG NER column.
    ("saffron", "Spices, saffron"),
    ("saffron threads", "Spices, saffron"),
    ("bacon", "Pork, cured, bacon, unprepared"),
    ("salt pork", "Pork, cured, bacon, unprepared"),
    # Margarine is a distinct fat (plant oils, not dairy) — kept separate
    # from butter rather than aliased to it. Parallel to 'butter' ->
    # 'Butter, without salt': point at the 'stick' variant for a
    # representative density.
    (
        "margarine",
        (
            "Margarine, 80% fat, stick, includes regular and hydrogenated"
            " corn and soybean oils"
        ),
    ),
    ("white flour", "Wheat flour, white, all-purpose, enriched, bleached"),
    ("plain wheat flour", "Wheat flour, white, all-purpose, enriched, bleached"),
    ("whipping cream", "Cream, fluid, heavy whipping"),
    ("heavy cream", "Cream, fluid, heavy whipping"),
    ("powdered sugar", "Sugars, powdered"),
    ("confectioners", "Sugars, powdered"),
    ("confectioners sugar", "Sugars, powdered"),
    # Short-grain rice is a common pannkakor / rice-pudding ingredient in
    # the RecipeNLG NER — map to the FDC rice entry.
    ("short-grain rice", "Rice, white, long-grain, regular, raw, enriched"),
    ("short grain rice", "Rice, white, long-grain, regular, raw, enriched"),
    # --- Swedish aliases (pannkakor dish family) ---
    # Pure language/orthographic variants of FDC foods. Each pair covers
    # both the accented form (as produced by the LLM on ica.se / tasteline.com
    # text) and the ASCII-folded form (in case OCR/normalization strips
    # diacritics upstream).
    ("vetemjöl", "Wheat flour, white, all-purpose, enriched, bleached"),
    ("vetemjol", "Wheat flour, white, all-purpose, enriched, bleached"),
    ("mjöl", "Wheat flour, white, all-purpose, enriched, bleached"),
    ("mjol", "Wheat flour, white, all-purpose, enriched, bleached"),
    ("mjölk", "Milk, whole, 3.25% milkfat, with added vitamin D"),
    ("mjolk", "Milk, whole, 3.25% milkfat, with added vitamin D"),
    ("ägg", "Egg, whole, raw, fresh"),
    ("agg", "Egg, whole, raw, fresh"),
    ("socker", "Sugars, granulated"),
    ("strösocker", "Sugars, granulated"),
    ("strosocker", "Sugars, granulated"),
    ("farin", "Sugars, brown"),
    ("farinsocker", "Sugars, brown"),
    ("smör", "Butter, without salt"),
    ("smor", "Butter, without salt"),
    ("grädde", "Cream, fluid, heavy whipping"),
    ("gradde", "Cream, fluid, heavy whipping"),
    ("vispgrädde", "Cream, fluid, heavy whipping"),
    ("vispgradde", "Cream, fluid, heavy whipping"),
    ("kardemumma", "Spices, cardamom"),
    ("kanel", "Spices, cinnamon, ground"),
    (
        "bakpulver",
        "Leavening agents, baking powder, double-acting, sodium aluminum sulfate",
    ),
    ("jäst", "Leavening agents, yeast, baker's, compressed"),
    ("jast", "Leavening agents, yeast, baker's, compressed"),
    ("havregryn", "Oats"),
    ("blåbär", "Blueberries, raw"),
    ("blabar", "Blueberries, raw"),
    ("vatten", "Beverages, water, tap, drinking"),
    ("saffran", "Spices, saffron"),
    ("ris", "Rice, white, long-grain, regular, raw, enriched"),
    # grötris = Swedish short-grain rice used for rice porridge and
    # saffranspannkaka; approximated by the FDC rice entry.
    ("grötris", "Rice, white, long-grain, regular, raw, enriched"),
    ("mandel", "Almonds"),
    ("mandlar", "Almonds"),
    ("sötmandel", "Almonds"),
    ("honung", "Honey"),
    # risgrynsgröt = cooked rice porridge used as a base in saffranspannkaka;
    # map to rice so it merges with RecipeNLG recipes that list rice as the
    # corresponding raw ingredient.
    ("risgrynsgröt", "Rice, white, long-grain, regular, raw, enriched"),
    ("fläsk", "Pork, cured, bacon, unprepared"),
    ("flask", "Pork, cured, bacon, unprepared"),
    ("sidfläsk", "Pork, cured, bacon, unprepared"),
    ("sidflask", "Pork, cured, bacon, unprepared"),
]

# Supplementary portion data (whole-unit weights not in FDC)
SUPPLEMENTARY_PORTIONS: list[tuple[str, str, float, str]] = [
    # (fdc_food_description, unit_name, gram_weight, notes)
    # EU egg sizes (midpoints of EU size bands)
    ("Egg, whole, raw, fresh", "EU XL", 78, "EU ≥73g, midpoint ~78g"),
    ("Egg, whole, raw, fresh", "EU LARGE", 68, "EU 63-73g, midpoint ~68g"),
    ("Egg, whole, raw, fresh", "EU MEDIUM", 58, "EU 53-63g, midpoint ~58g"),
    ("Egg, whole, raw, fresh", "EU SMALL", 48, "EU <53g, midpoint ~48g"),
    # XL alias for US extra large
    ("Egg, whole, raw, fresh", "XL", 56, "alias for USDA extra large"),
    # Butter units
    ("Butter, without salt", "CUBE", 56.699, "half stick"),
    ("Butter, without salt", "KNOB", 30, "approximate"),
    # Potato baking sizes
    ("Potatoes, flesh and skin, raw", "large baking", 340, "US baking potato"),
    ("Potatoes, flesh and skin, raw", "medium baking", 283, "US baking potato"),
    ("Potatoes, flesh and skin, raw", "small baking", 226, "US baking potato"),
]


def load_supplementary_foods(conn: sqlite3.Connection) -> None:
    """Insert the SUPPLEMENTARY foods, their synonyms, and their densities.

    Canonical name for supplementary entries is the first synonym
    (by convention, put the English form first).
    """
    for entry in SUPPLEMENTARY:
        name = str(entry["name"])
        synonyms = list(entry["synonyms"])  # type: ignore[arg-type]
        density = float(entry["density"])  # type: ignore[arg-type]
        source_note = str(entry.get("source_note", ""))
        canonical = synonyms[0].lower() if synonyms else name.lower()

        cur = conn.execute(
            "INSERT INTO food (name, canonical_name, source) "
            "VALUES (?, ?, 'supplementary')",
            (name, canonical),
        )
        food_id = cur.lastrowid
        assert food_id is not None

        for syn in synonyms:
            try:
                conn.execute(
                    "INSERT INTO synonym (food_id, name) VALUES (?, ?)",
                    (food_id, syn),
                )
            except sqlite3.IntegrityError:
                pass

        conn.execute(
            "INSERT INTO density (food_id, g_per_ml, source, notes) "
            "VALUES (?, ?, 'supplementary', ?)",
            (food_id, density, source_note),
        )


def load_synonym_aliases(conn: sqlite3.Connection) -> None:
    """Process FDC_SYNONYM_ALIASES: register short names as synonyms.

    Idempotent — safe to call multiple times. Run once before FAO loading
    (so FDC-target aliases like 'butter' claim their canonical synonym
    before FAO's auto-synonym 'Butter' does), and once after FAO loading
    (to resolve aliases whose target only exists in FAO, e.g.
    'havregryn' -> 'Oats'). The first alias defined for a food also sets
    the food's canonical short form — so order matters here.
    """
    for synonym, fdc_name in FDC_SYNONYM_ALIASES:
        row = conn.execute(
            "SELECT id, canonical_name FROM food WHERE name = ? COLLATE NOCASE "
            "ORDER BY CASE source WHEN 'fdc' THEN 1 WHEN 'fao' THEN 2 ELSE 3 END "
            "LIMIT 1",
            (fdc_name,),
        ).fetchone()
        if row is None:
            continue  # target not loaded yet; will retry in second pass

        food_id, canonical_name = row
        try:
            conn.execute(
                "INSERT INTO synonym (food_id, name) VALUES (?, ?)",
                (food_id, synonym),
            )
        except sqlite3.IntegrityError:
            pass  # synonym already exists
        if canonical_name is None:
            conn.execute(
                "UPDATE food SET canonical_name = ? WHERE id = ?",
                (synonym.lower(), food_id),
            )


def load_supplementary_portions(conn: sqlite3.Connection) -> None:
    """Load the SUPPLEMENTARY_PORTIONS table of whole-unit weights."""
    for fdc_name, unit_name, gram_weight, notes in SUPPLEMENTARY_PORTIONS:
        row = conn.execute(
            "SELECT id FROM food WHERE name = ? AND source = 'fdc'",
            (fdc_name,),
        ).fetchone()
        if row is None:
            print(
                f"  Warning: FDC food not found for portion: '{fdc_name}'",
                file=sys.stderr,
            )
            continue

        conn.execute(
            "INSERT INTO portion (food_id, unit_name, gram_weight, source, notes) "
            "VALUES (?, ?, ?, 'supplementary', ?)",
            (row[0], unit_name, gram_weight, notes),
        )


def report_missing_aliases(conn: sqlite3.Connection) -> None:
    """After all loading phases, warn about aliases that never resolved."""
    for synonym, fdc_name in FDC_SYNONYM_ALIASES:
        row = conn.execute(
            "SELECT 1 FROM food WHERE name = ? COLLATE NOCASE LIMIT 1",
            (fdc_name,),
        ).fetchone()
        if row is None:
            print(
                f"  Warning: food not found for synonym '{synonym}': '{fdc_name}'",
                file=sys.stderr,
            )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    # Check data files exist
    for path, label in [
        (FDC_DIR / "food.csv", "USDA FDC food.csv"),
        (FDC_DIR / "food_portion.csv", "USDA FDC food_portion.csv"),
        (FAO_DIR / "density_db_v2.xlsx", "FAO/INFOODS density database"),
    ]:
        if not path.exists():
            print(f"Error: {label} not found at {path}", file=sys.stderr)
            print("Run scripts/download_data.sh first.", file=sys.stderr)
            sys.exit(1)

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Remove old DB if it exists
    if DB_PATH.exists():
        DB_PATH.unlink()

    conn = sqlite3.connect(str(DB_PATH))
    conn.executescript(SCHEMA)

    print("Loading USDA FDC foods...")
    fdc_id_map = load_fdc_foods(conn)
    print(f"  {len(fdc_id_map)} foods loaded")

    print("Loading FDC portion data...")
    load_fdc_portions(conn, fdc_id_map)
    portion_count = conn.execute("SELECT COUNT(*) FROM portion").fetchone()[0]
    print(f"  {portion_count} portions loaded")

    print("Deriving densities from FDC portion data...")
    fdc_density_count = derive_fdc_densities(conn)
    print(f"  {fdc_density_count} densities derived")

    # Load supplementary foods first, then process aliases once so FDC-target
    # aliases (e.g. 'butter' -> 'Butter, without salt') claim their canonical
    # synonym before FAO's auto-synonym insertion shadows them.
    print("Loading supplementary foods...")
    load_supplementary_foods(conn)

    print("Registering synonym aliases (FDC/supplementary pass)...")
    load_synonym_aliases(conn)

    # FAO loading may create new foods (e.g. 'Oats') that aren't in FDC.
    print("Loading FAO/INFOODS density data...")
    fao_count = load_fao_densities(conn)
    print(f"  {fao_count} densities loaded")

    # Second pass picks up aliases pointing at FAO-only foods
    # (e.g. 'havregryn' -> 'Oats').
    print("Registering synonym aliases (FAO pass)...")
    load_synonym_aliases(conn)

    print("Loading supplementary portions...")
    load_supplementary_portions(conn)

    report_missing_aliases(conn)

    conn.commit()

    # Print summary
    food_count = conn.execute("SELECT COUNT(*) FROM food").fetchone()[0]
    synonym_count = conn.execute("SELECT COUNT(*) FROM synonym").fetchone()[0]
    density_count = conn.execute("SELECT COUNT(*) FROM density").fetchone()[0]
    portion_count = conn.execute("SELECT COUNT(*) FROM portion").fetchone()[0]
    db_size = DB_PATH.stat().st_size

    print(f"\nDatabase built: {DB_PATH}")
    print(f"  Foods:    {food_count}")
    print(f"  Synonyms: {synonym_count}")
    print(f"  Densities: {density_count}")
    print(f"  Portions:  {portion_count}")
    print(f"  Size:      {db_size / 1024:.0f} KB")

    conn.close()


if __name__ == "__main__":
    main()
