"""SQLite backing store for the recipe catalog (bead vwt.6).

Replaces the per-variant CSV + manifest.json pair for downstream
consumers. One ``recipes.db`` file holds every extracted variant plus
its provenance (raw ingredient lines, parsed ingredient rows, outlier
scores) and is served client-side via sql.js — same pattern already
used for ``ingredients.db``.

The authoritative schema lives here as ``_SCHEMA`` — a list of CREATE
statements run at ``open()`` time. A write to a brand-new file or a
re-open of an existing one both produce the same schema; the schema is
idempotent (``IF NOT EXISTS``).

Two notable deviations from the design doc (``docs/design/full-catalog.md``
§ Track 0), both documented inline:

- ``variants.base_ingredient`` and ``variants.confidence_level`` are
  added because the PWA's ``CuratedRecipe`` shape carries them and
  round-tripping the 4 hand-curated recipes requires it.
- ``variant_ingredient_stats.min_sample_size`` replaces the design
  doc's ``n_nonzero``. The PWA consumes min_sample_size (statistical
  required-sample concept) and curated JSON stores values that exceed
  actual row counts. Pipeline-produced variants fill this from
  ``calculate_minimum_sample_sizes``.

Mean/stddev/CI units are **fraction 0..1** (matching CuratedRecipe
JSON semantics), not percent. ``upsert_variant`` divides the pipeline's
percent-form ``MergedNormalizedRow.proportions`` by 100 before writing.
"""

from __future__ import annotations

import math
import sqlite3
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from rational_recipes.scrape.pipeline_merged import (
    MergedNormalizedRow,
    MergedVariantResult,
)

_SCHEMA: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS recipes (
      recipe_id      TEXT PRIMARY KEY,
      url            TEXT,
      title          TEXT,
      corpus         TEXT NOT NULL
                     CHECK(corpus IN ('recipenlg', 'wdc', 'curated')),
      language       TEXT,
      source_type    TEXT DEFAULT 'url'
                     CHECK(source_type IN ('url', 'book', 'text')),
      cooking_method TEXT,
      cook_time_min  INTEGER,
      total_time_min INTEGER,
      extracted_at   TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS raw_ingredients (
      recipe_id  TEXT NOT NULL REFERENCES recipes(recipe_id),
      line_index INTEGER NOT NULL,
      raw_line   TEXT NOT NULL,
      PRIMARY KEY (recipe_id, line_index)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS parsed_ingredients (
      recipe_id       TEXT NOT NULL REFERENCES recipes(recipe_id),
      canonical_name  TEXT NOT NULL,
      quantity        REAL,
      quantity_min    REAL,
      quantity_max    REAL,
      unit            TEXT,
      grams           REAL,
      preparation     TEXT,
      PRIMARY KEY (recipe_id, canonical_name)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS variants (
      variant_id                 TEXT PRIMARY KEY,
      normalized_title           TEXT NOT NULL,
      display_title              TEXT,
      category                   TEXT,
      description                TEXT,
      base_ingredient            TEXT,
      cooking_methods            TEXT,
      canonical_ingredient_set   TEXT NOT NULL,
      n_recipes                  INTEGER NOT NULL,
      confidence_level           REAL,
      review_status              TEXT,
      review_note                TEXT,
      reviewed_at                TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_variants_nrecipes ON variants(n_recipes)",
    "CREATE INDEX IF NOT EXISTS idx_variants_category ON variants(category)",
    "CREATE INDEX IF NOT EXISTS idx_variants_title    ON variants(normalized_title)",
    """
    CREATE TABLE IF NOT EXISTS variant_members (
      variant_id    TEXT NOT NULL REFERENCES variants(variant_id),
      recipe_id     TEXT NOT NULL REFERENCES recipes(recipe_id),
      outlier_score REAL,
      PRIMARY KEY (variant_id, recipe_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS variant_ingredient_stats (
      variant_id       TEXT NOT NULL REFERENCES variants(variant_id),
      canonical_name   TEXT NOT NULL,
      ordinal          INTEGER NOT NULL,
      mean_proportion  REAL NOT NULL,
      stddev           REAL,
      ci_lower         REAL,
      ci_upper         REAL,
      ratio            REAL,
      min_sample_size  INTEGER NOT NULL,
      density_g_per_ml REAL,
      whole_unit_name  TEXT,
      whole_unit_grams REAL,
      PRIMARY KEY (variant_id, canonical_name)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS variant_sources (
      variant_id  TEXT NOT NULL REFERENCES variants(variant_id),
      ordinal     INTEGER NOT NULL,
      source_type TEXT NOT NULL
                  CHECK(source_type IN ('url', 'book', 'text')),
      title       TEXT,
      ref         TEXT NOT NULL,
      PRIMARY KEY (variant_id, ordinal)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS query_runs (
      l1_group_key      TEXT PRIMARY KEY,
      run_at            TEXT NOT NULL,
      corpus_revisions  TEXT,
      variants_produced INTEGER NOT NULL,
      dry               INTEGER NOT NULL
                        CHECK(dry IN (0, 1))
    )
    """,
)


@dataclass(frozen=True, slots=True)
class VariantRow:
    """A row from the ``variants`` table."""

    variant_id: str
    normalized_title: str
    display_title: str | None
    category: str | None
    description: str | None
    base_ingredient: str | None
    cooking_methods: tuple[str, ...]
    canonical_ingredient_set: tuple[str, ...]
    n_recipes: int
    confidence_level: float | None
    review_status: str | None
    review_note: str | None
    reviewed_at: str | None


@dataclass(frozen=True, slots=True)
class IngredientStatsRow:
    """A row from ``variant_ingredient_stats``."""

    canonical_name: str
    ordinal: int
    mean_proportion: float
    stddev: float | None
    ci_lower: float | None
    ci_upper: float | None
    ratio: float | None
    min_sample_size: int
    density_g_per_ml: float | None
    whole_unit_name: str | None
    whole_unit_grams: float | None


@dataclass(frozen=True, slots=True)
class VariantMemberRow:
    """A row from ``variant_members`` joined with ``recipes``."""

    recipe_id: str
    url: str | None
    title: str | None
    corpus: str
    source_type: str
    outlier_score: float | None


@dataclass(frozen=True, slots=True)
class VariantSourceRow:
    """A row from ``variant_sources``."""

    source_type: str
    title: str | None
    ref: str


@dataclass(frozen=True, slots=True)
class ListFilters:
    """Filters accepted by ``CatalogDB.list_variants``."""

    min_sample_size: int | None = None
    category: str | None = None
    title_search: str | None = None
    order_by: str = "n_recipes_desc"
    include_dropped: bool = False


_ORDER_BY_SQL = {
    "n_recipes_desc": "n_recipes DESC, normalized_title ASC",
    "n_recipes_asc": "n_recipes ASC, normalized_title ASC",
    "title_asc": "normalized_title ASC",
    "title_desc": "normalized_title DESC",
}


class CatalogDB:
    """Thin wrapper around ``sqlite3.Connection`` exposing writer + reader.

    One instance owns one connection. Not thread-safe; open a fresh
    instance per thread or serialize access.
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    @property
    def connection(self) -> sqlite3.Connection:
        return self._conn

    @classmethod
    def open(cls, path: str | Path) -> CatalogDB:
        """Open or create a catalog DB at ``path`` and apply the schema."""
        conn = sqlite3.connect(str(path))
        conn.execute("PRAGMA foreign_keys = ON")
        db = cls(conn)
        db._apply_schema()
        return db

    @classmethod
    def in_memory(cls) -> CatalogDB:
        """Open an in-memory catalog DB with the schema applied."""
        conn = sqlite3.connect(":memory:")
        conn.execute("PRAGMA foreign_keys = ON")
        db = cls(conn)
        db._apply_schema()
        return db

    def close(self) -> None:
        self._conn.close()

    def _apply_schema(self) -> None:
        with self._conn:
            for stmt in _SCHEMA:
                self._conn.execute(stmt)

    # --- Writer ---

    def upsert_variant(
        self,
        variant: MergedVariantResult,
        l1_key: str,
        *,
        category: str | None = None,
        description: str | None = None,
        base_ingredient: str | None = None,
        confidence_level: float | None = None,
        display_title: str | None = None,
        language: str | None = None,
    ) -> None:
        """Write a variant and all its referenced rows in one transaction.

        Idempotent on ``variant.variant_id``: existing rows for the same
        variant + its members are replaced wholesale, not merged. The
        ``variant_id`` is deterministic (sha1 of normalized title +
        sorted ingredients + sorted methods), so re-running a stable
        pipeline produces byte-identical DB content.

        ``l1_key`` records the L1 group the variant came from; it's used
        by ``record_l1_run`` / ``is_l1_fresh`` for incremental resumes.
        """
        conn = self._conn
        variant_id = variant.variant_id
        stats = _compute_ingredient_stats(variant, base_ingredient)
        outlier_scores = variant.outlier_scores()

        with conn:
            conn.execute(
                "DELETE FROM variant_ingredient_stats WHERE variant_id = ?",
                (variant_id,),
            )
            conn.execute(
                "DELETE FROM variant_members WHERE variant_id = ?", (variant_id,)
            )
            conn.execute(
                "DELETE FROM variant_sources WHERE variant_id = ?", (variant_id,)
            )
            conn.execute("DELETE FROM variants WHERE variant_id = ?", (variant_id,))

            conn.execute(
                """
                INSERT INTO variants (
                  variant_id, normalized_title, display_title, category,
                  description, base_ingredient, cooking_methods,
                  canonical_ingredient_set, n_recipes, confidence_level
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    variant_id,
                    l1_key,
                    display_title or variant.variant_title,
                    category,
                    description,
                    base_ingredient,
                    ",".join(sorted(variant.cooking_methods)),
                    ",".join(sorted(variant.canonical_ingredients)),
                    len(variant.normalized_rows),
                    confidence_level,
                ),
            )

            for row, score in zip(variant.normalized_rows, outlier_scores, strict=True):
                recipe_id = _recipe_id_for_row(row)
                _upsert_recipe_row(conn, recipe_id, row, language=language)
                conn.execute(
                    """
                    INSERT INTO variant_members (
                      variant_id, recipe_id, outlier_score
                    ) VALUES (?, ?, ?)
                    """,
                    (variant_id, recipe_id, score),
                )

            for stat in stats:
                conn.execute(
                    """
                    INSERT INTO variant_ingredient_stats (
                      variant_id, canonical_name, ordinal, mean_proportion,
                      stddev, ci_lower, ci_upper, ratio, min_sample_size,
                      density_g_per_ml, whole_unit_name, whole_unit_grams
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        variant_id,
                        stat.canonical_name,
                        stat.ordinal,
                        stat.mean_proportion,
                        stat.stddev,
                        stat.ci_lower,
                        stat.ci_upper,
                        stat.ratio,
                        stat.min_sample_size,
                        stat.density_g_per_ml,
                        stat.whole_unit_name,
                        stat.whole_unit_grams,
                    ),
                )

    def upsert_recipe(
        self,
        *,
        recipe_id: str,
        url: str | None,
        title: str | None,
        corpus: str,
        language: str | None = None,
        source_type: str = "url",
        cooking_method: str | None = None,
        cook_time_min: int | None = None,
        total_time_min: int | None = None,
        extracted_at: str | None = None,
        raw_lines: Sequence[str] = (),
        parsed: Sequence[ParsedIngredientRow] = (),
    ) -> None:
        """Upsert one recipe row + its raw/parsed ingredient children."""
        conn = self._conn
        with conn:
            conn.execute(
                "DELETE FROM raw_ingredients WHERE recipe_id = ?", (recipe_id,)
            )
            conn.execute(
                "DELETE FROM parsed_ingredients WHERE recipe_id = ?", (recipe_id,)
            )
            conn.execute(
                """
                INSERT INTO recipes (
                  recipe_id, url, title, corpus, language, source_type,
                  cooking_method, cook_time_min, total_time_min, extracted_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(recipe_id) DO UPDATE SET
                  url=excluded.url,
                  title=excluded.title,
                  corpus=excluded.corpus,
                  language=excluded.language,
                  source_type=excluded.source_type,
                  cooking_method=excluded.cooking_method,
                  cook_time_min=excluded.cook_time_min,
                  total_time_min=excluded.total_time_min,
                  extracted_at=excluded.extracted_at
                """,
                (
                    recipe_id,
                    url,
                    title,
                    corpus,
                    language,
                    source_type,
                    cooking_method,
                    cook_time_min,
                    total_time_min,
                    extracted_at,
                ),
            )
            for i, line in enumerate(raw_lines):
                conn.execute(
                    "INSERT INTO raw_ingredients (recipe_id, line_index, raw_line)"
                    " VALUES (?, ?, ?)",
                    (recipe_id, i, line),
                )
            for p in parsed:
                conn.execute(
                    """
                    INSERT INTO parsed_ingredients (
                      recipe_id, canonical_name, quantity, quantity_min,
                      quantity_max, unit, grams, preparation
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        recipe_id,
                        p.canonical_name,
                        p.quantity,
                        p.quantity_min,
                        p.quantity_max,
                        p.unit,
                        p.grams,
                        p.preparation,
                    ),
                )

    def add_variant_member(
        self, variant_id: str, recipe_id: str, outlier_score: float | None = None
    ) -> None:
        with self._conn:
            self._conn.execute(
                """
                INSERT OR REPLACE INTO variant_members (
                  variant_id, recipe_id, outlier_score
                ) VALUES (?, ?, ?)
                """,
                (variant_id, recipe_id, outlier_score),
            )

    def add_variant_source(
        self,
        variant_id: str,
        ordinal: int,
        source_type: str,
        ref: str,
        title: str | None = None,
    ) -> None:
        with self._conn:
            self._conn.execute(
                """
                INSERT OR REPLACE INTO variant_sources (
                  variant_id, ordinal, source_type, title, ref
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (variant_id, ordinal, source_type, title, ref),
            )

    def record_l1_run(
        self,
        l1_key: str,
        *,
        corpus_revisions: str | None,
        variants_produced: int,
        dry: bool,
        run_at: str,
    ) -> None:
        """Mark an L1 group as processed for resumability."""
        with self._conn:
            self._conn.execute(
                """
                INSERT OR REPLACE INTO query_runs (
                  l1_group_key, run_at, corpus_revisions,
                  variants_produced, dry
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (l1_key, run_at, corpus_revisions, variants_produced, 1 if dry else 0),
            )

    def is_l1_fresh(self, l1_key: str, corpus_revisions: str | None) -> bool:
        """True when ``l1_key`` has a run row matching ``corpus_revisions``.

        A NULL fingerprint in the DB matches a NULL argument; any other
        mismatch returns False so the caller re-processes the group.
        """
        row = self._conn.execute(
            "SELECT corpus_revisions FROM query_runs WHERE l1_group_key = ?",
            (l1_key,),
        ).fetchone()
        if row is None:
            return False
        return bool(row[0] == corpus_revisions)

    # --- Reader ---

    def list_variants(self, filters: ListFilters | None = None) -> list[VariantRow]:
        """Compiled SELECT over the ``variants`` table.

        Default order: ``n_recipes DESC, normalized_title ASC``. Drops
        variants with ``review_status = 'drop'`` unless
        ``include_dropped=True``.
        """
        f = filters or ListFilters()
        where: list[str] = []
        params: list[Any] = []
        if f.min_sample_size is not None:
            where.append("n_recipes >= ?")
            params.append(f.min_sample_size)
        if f.category is not None:
            where.append("category = ?")
            params.append(f.category)
        if f.title_search:
            where.append("LOWER(normalized_title) LIKE ?")
            params.append(f"%{f.title_search.lower()}%")
        if not f.include_dropped:
            where.append("(review_status IS NULL OR review_status != 'drop')")

        sql = "SELECT * FROM variants"
        if where:
            sql += " WHERE " + " AND ".join(where)
        order = _ORDER_BY_SQL.get(f.order_by, _ORDER_BY_SQL["n_recipes_desc"])
        sql += " ORDER BY " + order

        return [_variant_from_row(r) for r in self._conn.execute(sql, params)]

    def get_variant(self, variant_id: str) -> VariantRow | None:
        row = self._conn.execute(
            "SELECT * FROM variants WHERE variant_id = ?", (variant_id,)
        ).fetchone()
        return _variant_from_row(row) if row is not None else None

    def get_variant_members(self, variant_id: str) -> list[VariantMemberRow]:
        rows = self._conn.execute(
            """
            SELECT r.recipe_id, r.url, r.title, r.corpus, r.source_type,
                   m.outlier_score
            FROM variant_members m
            JOIN recipes r ON r.recipe_id = m.recipe_id
            WHERE m.variant_id = ?
            ORDER BY m.outlier_score IS NULL, m.outlier_score ASC,
                     r.recipe_id ASC
            """,
            (variant_id,),
        ).fetchall()
        return [
            VariantMemberRow(
                recipe_id=r[0],
                url=r[1],
                title=r[2],
                corpus=r[3],
                source_type=r[4] or "url",
                outlier_score=r[5],
            )
            for r in rows
        ]

    def get_ingredient_stats(self, variant_id: str) -> list[IngredientStatsRow]:
        rows = self._conn.execute(
            """
            SELECT canonical_name, ordinal, mean_proportion, stddev,
                   ci_lower, ci_upper, ratio, min_sample_size,
                   density_g_per_ml, whole_unit_name, whole_unit_grams
            FROM variant_ingredient_stats
            WHERE variant_id = ?
            ORDER BY ordinal ASC
            """,
            (variant_id,),
        ).fetchall()
        return [
            IngredientStatsRow(
                canonical_name=r[0],
                ordinal=r[1],
                mean_proportion=r[2],
                stddev=r[3],
                ci_lower=r[4],
                ci_upper=r[5],
                ratio=r[6],
                min_sample_size=r[7],
                density_g_per_ml=r[8],
                whole_unit_name=r[9],
                whole_unit_grams=r[10],
            )
            for r in rows
        ]

    def get_variant_sources(self, variant_id: str) -> list[VariantSourceRow]:
        rows = self._conn.execute(
            """
            SELECT source_type, title, ref FROM variant_sources
            WHERE variant_id = ?
            ORDER BY ordinal ASC
            """,
            (variant_id,),
        ).fetchall()
        return [VariantSourceRow(source_type=r[0], title=r[1], ref=r[2]) for r in rows]


@dataclass(frozen=True, slots=True)
class ParsedIngredientRow:
    """One row for the ``parsed_ingredients`` table."""

    canonical_name: str
    quantity: float | None = None
    quantity_min: float | None = None
    quantity_max: float | None = None
    unit: str | None = None
    grams: float | None = None
    preparation: str | None = None


@dataclass(frozen=True, slots=True)
class _ComputedStat:
    canonical_name: str
    ordinal: int
    mean_proportion: float
    stddev: float | None
    ci_lower: float | None
    ci_upper: float | None
    ratio: float | None
    min_sample_size: int
    density_g_per_ml: float | None = None
    whole_unit_name: str | None = None
    whole_unit_grams: float | None = None


def _compute_ingredient_stats(
    variant: MergedVariantResult,
    base_ingredient: str | None,
) -> list[_ComputedStat]:
    """Compute per-ingredient stats from ``variant.normalized_rows``.

    ``MergedNormalizedRow.proportions`` is in percent (g per 100g total);
    this function converts to fraction 0..1 for DB storage.
    """
    rows = variant.normalized_rows
    n = len(rows)
    if n == 0:
        return []

    canonicals = sorted(variant.canonical_ingredients)
    header_order = {name: i for i, name in enumerate(variant.header_ingredients)}

    stats: list[_ComputedStat] = []
    means: dict[str, float] = {}
    for name in canonicals:
        values = [row.proportions.get(name, 0.0) / 100.0 for row in rows]
        mean = sum(values) / n
        means[name] = mean
        stddev: float | None
        ci_lower: float | None
        ci_upper: float | None
        if n >= 2:
            variance = sum((v - mean) ** 2 for v in values) / (n - 1)
            stddev = math.sqrt(variance)
            half_width = 1.96 * stddev / math.sqrt(n)
            ci_lower = max(0.0, mean - half_width)
            ci_upper = mean + half_width
        else:
            stddev = None
            ci_lower = None
            ci_upper = None
        fallback_ordinal = len(header_order) + canonicals.index(name)
        stats.append(
            _ComputedStat(
                canonical_name=name,
                ordinal=header_order.get(name, fallback_ordinal),
                mean_proportion=mean,
                stddev=stddev,
                ci_lower=ci_lower,
                ci_upper=ci_upper,
                ratio=None,
                min_sample_size=sum(
                    1 for row in rows if row.proportions.get(name, 0.0) > 0.0
                ),
            )
        )

    base = base_ingredient or (canonicals[0] if canonicals else None)
    base_mean = means.get(base or "", 0.0) if base else 0.0
    if base_mean > 0.0:
        stats = [
            _ComputedStat(
                canonical_name=s.canonical_name,
                ordinal=s.ordinal,
                mean_proportion=s.mean_proportion,
                stddev=s.stddev,
                ci_lower=s.ci_lower,
                ci_upper=s.ci_upper,
                ratio=s.mean_proportion / base_mean,
                min_sample_size=s.min_sample_size,
            )
            for s in stats
        ]
    stats.sort(key=lambda s: (s.ordinal, s.canonical_name))
    return [
        _ComputedStat(
            canonical_name=s.canonical_name,
            ordinal=i,
            mean_proportion=s.mean_proportion,
            stddev=s.stddev,
            ci_lower=s.ci_lower,
            ci_upper=s.ci_upper,
            ratio=s.ratio,
            min_sample_size=s.min_sample_size,
        )
        for i, s in enumerate(stats)
    ]


def _recipe_id_for_row(row: MergedNormalizedRow) -> str:
    """Derive a 12-hex recipe id from url + title."""
    import hashlib

    payload = f"{row.url}|{row.title}".encode()
    return hashlib.sha1(payload, usedforsecurity=False).hexdigest()[:12]


def _upsert_recipe_row(
    conn: sqlite3.Connection,
    recipe_id: str,
    row: MergedNormalizedRow,
    *,
    language: str | None,
) -> None:
    conn.execute(
        """
        INSERT INTO recipes (
          recipe_id, url, title, corpus, language, source_type
        ) VALUES (?, ?, ?, ?, ?, 'url')
        ON CONFLICT(recipe_id) DO UPDATE SET
          url=excluded.url,
          title=excluded.title,
          corpus=excluded.corpus,
          language=excluded.language
        """,
        (recipe_id, row.url, row.title, row.corpus, language),
    )
    conn.execute("DELETE FROM parsed_ingredients WHERE recipe_id = ?", (recipe_id,))
    for name, cell in row.cells.items():
        quantity, unit = _split_cell(cell)
        conn.execute(
            """
            INSERT INTO parsed_ingredients (
              recipe_id, canonical_name, quantity, unit
            ) VALUES (?, ?, ?, ?)
            """,
            (recipe_id, name, quantity, unit),
        )


def _split_cell(cell: str) -> tuple[float | None, str | None]:
    """Parse a ``rr-stats`` cell like ``"100 g"`` into (quantity, unit).

    Returns ``(None, None)`` for unparseable input, ``(0.0, None)`` for
    the zero sentinel ``"0"``. Used only to populate parsed_ingredients
    for lossy provenance — callers needing fidelity should prefer
    ``upsert_recipe(... parsed=...)`` with rich rows.
    """
    cell = cell.strip()
    if not cell or cell == "0":
        return 0.0 if cell == "0" else None, None
    parts = cell.split(None, 1)
    try:
        qty = float(parts[0])
    except ValueError:
        return None, None
    unit = parts[1].strip() if len(parts) > 1 else None
    return qty, unit


def _variant_from_row(row: Iterable[Any]) -> VariantRow:
    r = list(row)
    return VariantRow(
        variant_id=r[0],
        normalized_title=r[1],
        display_title=r[2],
        category=r[3],
        description=r[4],
        base_ingredient=r[5],
        cooking_methods=tuple(s for s in (r[6] or "").split(",") if s),
        canonical_ingredient_set=tuple(s for s in (r[7] or "").split(",") if s),
        n_recipes=r[8],
        confidence_level=r[9],
        review_status=r[10],
        review_note=r[11],
        reviewed_at=r[12],
    )
