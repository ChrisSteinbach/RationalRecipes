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

import json
import math
import sqlite3
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from rational_recipes.scrape.parse import ParsedIngredient
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
    # vwt.16 Pass-1 sink. One row per (corpus, recipe_id, line_index) so
    # re-runs are idempotent at the recipe-line grain. parsed_json is
    # NULL when the LLM (or regex fallback) failed; callers treat that
    # as "tried, gave up". The (raw_line, model, seed) index supports
    # vwt.16 option-2 line-text dedup: same line text in another recipe
    # reuses the cached parse instead of paying the LLM cost again.
    """
    CREATE TABLE IF NOT EXISTS parsed_ingredient_lines (
      corpus      TEXT NOT NULL,
      recipe_id   TEXT NOT NULL,
      line_index  INTEGER NOT NULL,
      raw_line    TEXT NOT NULL,
      parsed_json TEXT,
      model       TEXT NOT NULL,
      seed        INTEGER NOT NULL,
      PRIMARY KEY (corpus, recipe_id, line_index)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_parsed_lines_text "
    "ON parsed_ingredient_lines(raw_line, model, seed)",
    # sj18 sidecar: per-variant editorial overrides. Two override kinds —
    # ``substitute`` folds canonical X into Y for the variant; ``filter``
    # excludes one source recipe from the average. Stored as JSON payload
    # so the schema doesn't grow per new override type. Insertion + delete
    # both trigger a stats recompute that overwrites
    # ``variant_ingredient_stats`` from ``variant_members`` +
    # ``parsed_ingredients`` with the active override set applied. The
    # underlying ``variant_members`` rows are never deleted, so any
    # filter/substitute is reversible by clearing the override.
    """
    CREATE TABLE IF NOT EXISTS variant_overrides (
      override_id    INTEGER PRIMARY KEY AUTOINCREMENT,
      variant_id     TEXT NOT NULL REFERENCES variants(variant_id),
      override_type  TEXT NOT NULL
                     CHECK(override_type IN ('substitute', 'filter')),
      payload        TEXT NOT NULL,
      created_at     TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_overrides_variant "
    "ON variant_overrides(variant_id)",
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


OverrideType = Literal["substitute", "filter"]
_VALID_OVERRIDE_TYPES: frozenset[str] = frozenset({"substitute", "filter"})


@dataclass(frozen=True, slots=True)
class VariantOverrideRow:
    """One editorial override on a variant (sj18 sidecar)."""

    override_id: int
    variant_id: str
    override_type: OverrideType
    payload: dict[str, Any]
    created_at: str


ReviewStatus = Literal["accept", "drop", "annotate"]
_VALID_REVIEW_STATUSES: frozenset[str] = frozenset(
    {"accept", "drop", "annotate"}
)


@dataclass(frozen=True, slots=True)
class ListFilters:
    """Filters accepted by ``CatalogDB.list_variants``."""

    min_sample_size: int | None = None
    category: str | None = None
    title_search: str | None = None
    order_by: str = "n_recipes_desc"
    include_dropped: bool = False
    # Narrow the result to variants still needing review (status IS NULL).
    # Independent of ``include_dropped``: ``pending_only`` implies NULL, so
    # dropped rows are excluded regardless.
    pending_only: bool = False


_ORDER_BY_SQL = {
    "n_recipes_desc": "n_recipes DESC, normalized_title ASC",
    "n_recipes_asc": "n_recipes ASC, normalized_title ASC",
    "title_asc": "normalized_title ASC",
    "title_desc": "normalized_title DESC",
}


class CatalogDB:
    """Thin wrapper around ``sqlite3.Connection`` exposing writer + reader.

    One instance owns one connection. Not inherently thread-safe — callers
    sharing a single instance across threads must serialize access (e.g.
    via ``threading.Lock``). ``open()`` sets ``check_same_thread=False``
    so a locked connection can be used from worker threads.
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    @property
    def connection(self) -> sqlite3.Connection:
        return self._conn

    @classmethod
    def open(cls, path: str | Path) -> CatalogDB:
        """Open or create a catalog DB at ``path`` and apply the schema."""
        conn = sqlite3.connect(str(path), check_same_thread=False)
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        db = cls(conn)
        db._apply_schema()
        return db

    @classmethod
    def in_memory(cls) -> CatalogDB:
        """Open an in-memory catalog DB with the schema applied."""
        conn = sqlite3.connect(":memory:", check_same_thread=False)
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

        # WDC pages can host multiple JSON-LD Recipe entities under one
        # page_url with identical title, which collide on
        # _recipe_id_for_row's sha1(url|title) hash. Dedupe here so the
        # variant_members PK doesn't fire; first occurrence wins.
        seen_ids: set[str] = set()
        unique_rows: list[tuple[MergedNormalizedRow, str, float]] = []
        for row, score in zip(variant.normalized_rows, outlier_scores, strict=True):
            rid = _recipe_id_for_row(row)
            if rid in seen_ids:
                continue
            seen_ids.add(rid)
            unique_rows.append((row, rid, score))

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
                    len(unique_rows),
                    confidence_level,
                ),
            )

            for row, recipe_id, score in unique_rows:
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

    def delete_stale_variants_for_l1(
        self,
        l1_key: str,
        keep_variant_ids: Iterable[str],
    ) -> int:
        """Drop variants for ``l1_key`` whose ids aren't in ``keep_variant_ids``.

        Used by the Pass 2 re-cluster path so a policy change (e.g.
        RationalRecipes-dos top-N cap) actually shrinks the on-disk
        variant set rather than accumulating stale rows. Cascade-deletes
        from variant_ingredient_stats / variant_members / variant_sources
        so nothing dangles. Returns the number of variant rows removed.
        """
        keep = set(keep_variant_ids)
        with self._conn:
            cursor = self._conn.execute(
                "SELECT variant_id FROM variants WHERE normalized_title = ?",
                (l1_key,),
            )
            existing = [row[0] for row in cursor.fetchall()]
            stale = [vid for vid in existing if vid not in keep]
            for vid in stale:
                self._conn.execute(
                    "DELETE FROM variant_ingredient_stats WHERE variant_id = ?",
                    (vid,),
                )
                self._conn.execute(
                    "DELETE FROM variant_members WHERE variant_id = ?",
                    (vid,),
                )
                self._conn.execute(
                    "DELETE FROM variant_sources WHERE variant_id = ?",
                    (vid,),
                )
                self._conn.execute(
                    "DELETE FROM variants WHERE variant_id = ?", (vid,)
                )
        return len(stale)

    def update_review_status(
        self,
        variant_id: str,
        status: ReviewStatus | None,
        note: str | None = None,
    ) -> None:
        """Persist a reviewer decision for a single variant (bead vwt.9).

        Status ``None`` clears the decision (reverts to pending). Each
        call runs a single UPDATE in its own transaction so an abandoned
        review session never leaves the DB half-written. Timestamps
        ``reviewed_at`` in ISO-8601 UTC; writes ``None`` when clearing.
        """
        if status is not None and status not in _VALID_REVIEW_STATUSES:
            raise ValueError(
                f"invalid review status {status!r}; expected one of "
                f"{sorted(_VALID_REVIEW_STATUSES)} or None"
            )
        reviewed_at = (
            datetime.now(UTC).isoformat() if status is not None else None
        )
        with self._conn:
            self._conn.execute(
                """
                UPDATE variants
                   SET review_status = ?, review_note = ?, reviewed_at = ?
                 WHERE variant_id = ?
                """,
                (status, note, reviewed_at, variant_id),
            )

    # --- Editorial overrides (sj18) ---

    def add_substitute_override(
        self,
        variant_id: str,
        from_name: str,
        to_name: str,
    ) -> int:
        """Record ``from_name → to_name`` substitution; recompute stats.

        Returns the new ``override_id``. Folds the X mass-fraction into Y
        on the post-override view by aggregating grams under the resolved
        canonical name before averaging. Original ``variant_members`` and
        ``parsed_ingredients`` rows are untouched — clearing the override
        restores the pre-substitution stats.
        """
        if not from_name or not to_name:
            raise ValueError("substitute requires non-empty from_name and to_name")
        if from_name == to_name:
            raise ValueError("substitute from_name and to_name must differ")
        if self.get_variant(variant_id) is None:
            raise ValueError(f"variant_id {variant_id!r} not found")
        payload = json.dumps({"from": from_name, "to": to_name}, ensure_ascii=False)
        override_id = self._insert_override(variant_id, "substitute", payload)
        self._recompute_stats_for_variant(variant_id)
        return override_id

    def add_filter_override(
        self,
        variant_id: str,
        recipe_id: str,
        reason: str = "",
    ) -> int:
        """Exclude ``recipe_id`` from the variant's average; recompute stats.

        The ``variant_members`` row stays in place (so the operation is
        reversible by clearing the override). Returns the new
        ``override_id``. Raises ``ValueError`` if the recipe is not a
        member of the variant.
        """
        if self.get_variant(variant_id) is None:
            raise ValueError(f"variant_id {variant_id!r} not found")
        is_member = self._conn.execute(
            "SELECT 1 FROM variant_members WHERE variant_id = ? AND recipe_id = ?",
            (variant_id, recipe_id),
        ).fetchone()
        if is_member is None:
            raise ValueError(
                f"recipe_id {recipe_id!r} is not a member of variant {variant_id!r}"
            )
        payload = json.dumps(
            {"recipe_id": recipe_id, "reason": reason}, ensure_ascii=False
        )
        override_id = self._insert_override(variant_id, "filter", payload)
        self._recompute_stats_for_variant(variant_id)
        return override_id

    def list_overrides(self, variant_id: str) -> list[VariantOverrideRow]:
        """Active overrides for ``variant_id`` ordered by creation time."""
        rows = self._conn.execute(
            """
            SELECT override_id, variant_id, override_type, payload, created_at
            FROM variant_overrides
            WHERE variant_id = ?
            ORDER BY override_id ASC
            """,
            (variant_id,),
        ).fetchall()
        return [
            VariantOverrideRow(
                override_id=r[0],
                variant_id=r[1],
                override_type=r[2],
                payload=json.loads(r[3]),
                created_at=r[4],
            )
            for r in rows
        ]

    def clear_override(self, override_id: int) -> bool:
        """Remove one override by id; recompute the variant's stats.

        Returns ``True`` if a row was removed (and stats recomputed),
        ``False`` if the id was not found. Recomputation runs even when
        the cleared override was the only one — it restores the baseline
        stats by replaying with an empty override set.
        """
        row = self._conn.execute(
            "SELECT variant_id FROM variant_overrides WHERE override_id = ?",
            (override_id,),
        ).fetchone()
        if row is None:
            return False
        variant_id = row[0]
        with self._conn:
            self._conn.execute(
                "DELETE FROM variant_overrides WHERE override_id = ?",
                (override_id,),
            )
        self._recompute_stats_for_variant(variant_id)
        return True

    def clear_all_overrides(self, variant_id: str) -> int:
        """Wipe every override for ``variant_id``; recompute stats once."""
        count_row = self._conn.execute(
            "SELECT COUNT(*) FROM variant_overrides WHERE variant_id = ?",
            (variant_id,),
        ).fetchone()
        removed = int(count_row[0]) if count_row else 0
        if removed == 0:
            return 0
        with self._conn:
            self._conn.execute(
                "DELETE FROM variant_overrides WHERE variant_id = ?",
                (variant_id,),
            )
        self._recompute_stats_for_variant(variant_id)
        return removed

    def _insert_override(
        self,
        variant_id: str,
        override_type: OverrideType,
        payload: str,
    ) -> int:
        if override_type not in _VALID_OVERRIDE_TYPES:
            raise ValueError(
                f"invalid override_type {override_type!r}; expected one of "
                f"{sorted(_VALID_OVERRIDE_TYPES)}"
            )
        with self._conn:
            cursor = self._conn.execute(
                """
                INSERT INTO variant_overrides (
                  variant_id, override_type, payload, created_at
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    variant_id,
                    override_type,
                    payload,
                    datetime.now(UTC).isoformat(),
                ),
            )
        return int(cursor.lastrowid or 0)

    def _recompute_stats_for_variant(self, variant_id: str) -> None:
        """Recompute ``variant_ingredient_stats`` applying active overrides.

        Reads per-recipe gram quantities from ``parsed_ingredients`` for
        each (non-excluded) variant member, applies any substitution
        mapping (resolved transitively), aggregates grams into resolved
        canonical names, normalizes per recipe to fractions, then averages
        across the surviving members. Curated metadata columns
        (``density_g_per_ml``, ``whole_unit_name``, ``whole_unit_grams``)
        are carried forward from the existing stat row when the resolved
        name is unchanged — recompute should not silently drop curated
        per-ingredient annotations.
        """
        overrides = self.list_overrides(variant_id)
        substitutions: dict[str, str] = {}
        excluded_recipes: set[str] = set()
        for ov in overrides:
            if ov.override_type == "substitute":
                substitutions[ov.payload["from"]] = ov.payload["to"]
            elif ov.override_type == "filter":
                excluded_recipes.add(ov.payload["recipe_id"])

        def resolve(name: str) -> str:
            seen: set[str] = set()
            current = name
            while current in substitutions and current not in seen:
                seen.add(current)
                current = substitutions[current]
            return current

        member_rows = self._conn.execute(
            "SELECT recipe_id FROM variant_members WHERE variant_id = ?"
            " ORDER BY recipe_id ASC",
            (variant_id,),
        ).fetchall()
        active_member_ids = [
            r[0] for r in member_rows if r[0] not in excluded_recipes
        ]

        per_recipe_proportions: list[dict[str, float]] = []
        for rid in active_member_ids:
            ing_rows = self._conn.execute(
                "SELECT canonical_name, quantity FROM parsed_ingredients"
                " WHERE recipe_id = ?",
                (rid,),
            ).fetchall()
            grams: dict[str, float] = {}
            for name, qty in ing_rows:
                if qty is None or qty <= 0.0:
                    continue
                resolved = resolve(name)
                grams[resolved] = grams.get(resolved, 0.0) + float(qty)
            total = sum(grams.values())
            if total <= 0.0:
                per_recipe_proportions.append({})
                continue
            per_recipe_proportions.append(
                {n: g / total for n, g in grams.items()}
            )

        all_names: set[str] = set()
        for p in per_recipe_proportions:
            all_names.update(p.keys())

        existing = {s.canonical_name: s for s in self.get_ingredient_stats(variant_id)}
        n = len(per_recipe_proportions)

        new_rows: list[tuple[Any, ...]] = []
        sorted_names = sorted(all_names)
        next_ord = max(
            (s.ordinal for s in existing.values()),
            default=-1,
        ) + 1
        for name in sorted_names:
            values = [p.get(name, 0.0) for p in per_recipe_proportions]
            mean = sum(values) / n if n > 0 else 0.0
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
            min_sample = sum(1 for v in values if v > 0.0)
            prior = existing.get(name)
            if prior is not None:
                ordinal = prior.ordinal
                density = prior.density_g_per_ml
                whole_unit_name = prior.whole_unit_name
                whole_unit_grams = prior.whole_unit_grams
            else:
                ordinal = next_ord
                next_ord += 1
                density = None
                whole_unit_name = None
                whole_unit_grams = None
            new_rows.append(
                (
                    variant_id,
                    name,
                    ordinal,
                    mean,
                    stddev,
                    ci_lower,
                    ci_upper,
                    None,  # ratio: recomputed below once base mean is known
                    min_sample,
                    density,
                    whole_unit_name,
                    whole_unit_grams,
                )
            )

        # Rebuild ratio column relative to the base ingredient (per
        # _compute_ingredient_stats: base = canonical[0] when not set).
        variant = self.get_variant(variant_id)
        base_ingredient = (
            variant.base_ingredient if variant is not None else None
        )
        if base_ingredient is None and sorted_names:
            base_ingredient = sorted_names[0]
        base_mean = 0.0
        if base_ingredient is not None:
            for row in new_rows:
                if row[1] == base_ingredient:
                    base_mean = row[3]
                    break
        if base_mean > 0.0:
            new_rows = [
                (
                    *row[:7],
                    row[3] / base_mean,
                    *row[8:],
                )
                for row in new_rows
            ]

        # Sort by (ordinal, name) and reassign dense ordinals so consumers
        # that ORDER BY ordinal see a contiguous sequence.
        new_rows.sort(key=lambda r: (r[2], r[1]))
        new_rows = [
            (row[0], row[1], i, *row[3:]) for i, row in enumerate(new_rows)
        ]

        with self._conn:
            self._conn.execute(
                "DELETE FROM variant_ingredient_stats WHERE variant_id = ?",
                (variant_id,),
            )
            self._conn.executemany(
                """
                INSERT INTO variant_ingredient_stats (
                  variant_id, canonical_name, ordinal, mean_proportion,
                  stddev, ci_lower, ci_upper, ratio, min_sample_size,
                  density_g_per_ml, whole_unit_name, whole_unit_grams
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                new_rows,
            )
            # n_recipes and canonical_ingredient_set are derived counts —
            # update them so downstream readers (render_drop.py) see the
            # post-override view without needing override-aware queries.
            self._conn.execute(
                """
                UPDATE variants
                   SET n_recipes = ?, canonical_ingredient_set = ?
                 WHERE variant_id = ?
                """,
                (n, ",".join(sorted_names), variant_id),
            )

    def update_display_title(self, variant_id: str, display_title: str) -> None:
        """Overwrite a variant's ``display_title`` (Pass 3, vwt.24).

        Single-row UPDATE in its own transaction so a partial Pass 3
        leaves earlier titles intact rather than rolling back wholesale.
        """
        with self._conn:
            self._conn.execute(
                "UPDATE variants SET display_title = ? WHERE variant_id = ?",
                (display_title, variant_id),
            )

    def update_category(self, variant_id: str, category: str | None) -> None:
        """Overwrite a variant's ``category`` (vwt.33 backfill).

        Single-row UPDATE in its own transaction. Pass ``None`` to clear
        the column (the PWA falls back to "uncategorized" for NULL).
        """
        with self._conn:
            self._conn.execute(
                "UPDATE variants SET category = ? WHERE variant_id = ?",
                (category, variant_id),
            )

    def iter_variant_ids_titles(self) -> Iterable[tuple[str, str]]:
        """Stream ``(variant_id, normalized_title)`` for every variant.

        Used by the categorize-backfill CLI to walk the table without
        loading the whole row set into memory.
        """
        for row in self._conn.execute(
            "SELECT variant_id, normalized_title FROM variants"
        ):
            yield (row[0], row[1])

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

    # --- Parsed-line cache (vwt.16 Pass 1) ---

    def upsert_parsed_lines(
        self,
        rows: Iterable[ParsedLineRow],
    ) -> None:
        """Persist a batch of parsed ingredient lines in one transaction.

        ``parsed_json=None`` records a parse attempt that returned no
        usable structure; callers should not retry these on resume.
        """
        with self._conn:
            self._conn.executemany(
                """
                INSERT OR REPLACE INTO parsed_ingredient_lines (
                  corpus, recipe_id, line_index, raw_line,
                  parsed_json, model, seed
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        r.corpus,
                        r.recipe_id,
                        r.line_index,
                        r.raw_line,
                        r.parsed_json,
                        r.model,
                        r.seed,
                    )
                    for r in rows
                ],
            )

    def get_parsed_lines_for_recipe(
        self,
        corpus: str,
        recipe_id: str,
    ) -> list[ParsedLineRow]:
        """All parsed-line rows for one recipe, ordered by line_index."""
        cursor = self._conn.execute(
            """
            SELECT corpus, recipe_id, line_index, raw_line,
                   parsed_json, model, seed
            FROM parsed_ingredient_lines
            WHERE corpus = ? AND recipe_id = ?
            ORDER BY line_index
            """,
            (corpus, recipe_id),
        )
        return [
            ParsedLineRow(
                corpus=r[0],
                recipe_id=r[1],
                line_index=r[2],
                raw_line=r[3],
                parsed_json=r[4],
                model=r[5],
                seed=r[6],
            )
            for r in cursor
        ]

    def has_parsed_lines_for_recipe(
        self,
        corpus: str,
        recipe_id: str,
        *,
        model: str | None = None,
        seed: int | None = None,
    ) -> bool:
        """True iff any parsed_ingredient_lines row exists for this recipe.

        ``model``/``seed`` narrow the check to that specific (model, seed)
        — a swap of either invalidates resumability for that recipe and
        forces re-parse.
        """
        sql = (
            "SELECT 1 FROM parsed_ingredient_lines "
            "WHERE corpus = ? AND recipe_id = ?"
        )
        params: list[Any] = [corpus, recipe_id]
        if model is not None:
            sql += " AND model = ?"
            params.append(model)
        if seed is not None:
            sql += " AND seed = ?"
            params.append(seed)
        sql += " LIMIT 1"
        return self._conn.execute(sql, params).fetchone() is not None

    def lookup_cached_parse(
        self,
        raw_line: str,
        model: str,
        seed: int,
    ) -> tuple[bool, str | None]:
        """Return cached parse for ``(raw_line, model, seed)``.

        Tuple return distinguishes the three cases without sentinels:
          * ``(False, None)`` — no row matches; caller must call the LLM.
          * ``(True, None)`` — cached failure (NULL parsed_json); caller
            should record a failure without retrying.
          * ``(True, "<json>")`` — cached successful parse payload.

        Determinism (parse.py pins temperature=0, seed=42) lets us reuse
        any prior parse with the same ``(raw_line, model, seed)`` even
        if it came from a different recipe — line-text dedup baked into
        Pass 1 (vwt.16 option 2).
        """
        row = self._conn.execute(
            """
            SELECT parsed_json
            FROM parsed_ingredient_lines
            WHERE raw_line = ? AND model = ? AND seed = ?
            LIMIT 1
            """,
            (raw_line, model, seed),
        ).fetchone()
        if row is None:
            return (False, None)
        return (True, row[0])

    def invalidate_non_english_parses(self) -> int:
        """Delete parsed_ingredient_lines rows whose parsed_json is non-ASCII."""
        cursor = self._conn.execute(
            """
            SELECT corpus, recipe_id, line_index, parsed_json
            FROM parsed_ingredient_lines
            WHERE parsed_json IS NOT NULL
            """
        )
        keys: list[tuple[str, str, int]] = [
            (row[0], row[1], row[2])
            for row in cursor
            if row[3] is not None and any(ord(c) > 127 for c in row[3])
        ]
        if not keys:
            return 0
        with self._conn:
            self._conn.executemany(
                """
                DELETE FROM parsed_ingredient_lines
                WHERE corpus = ? AND recipe_id = ? AND line_index = ?
                """,
                keys,
            )
        return len(keys)

    def count_parsed_lines(
        self,
        *,
        corpus: str | None = None,
        model: str | None = None,
        seed: int | None = None,
    ) -> int:
        """Row count in ``parsed_ingredient_lines`` with optional filters."""
        sql = "SELECT COUNT(*) FROM parsed_ingredient_lines"
        clauses: list[str] = []
        params: list[Any] = []
        if corpus is not None:
            clauses.append("corpus = ?")
            params.append(corpus)
        if model is not None:
            clauses.append("model = ?")
            params.append(model)
        if seed is not None:
            clauses.append("seed = ?")
            params.append(seed)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        row = self._conn.execute(sql, params).fetchone()
        return int(row[0]) if row else 0

    def iter_parsed_lines(
        self,
        *,
        corpus: str | None = None,
        model: str | None = None,
        seed: int | None = None,
    ) -> Iterable[ParsedLineRow]:
        """Stream parsed-line rows; for warming an in-memory dedup cache."""
        sql = (
            "SELECT corpus, recipe_id, line_index, raw_line, "
            "parsed_json, model, seed FROM parsed_ingredient_lines"
        )
        clauses: list[str] = []
        params: list[Any] = []
        if corpus is not None:
            clauses.append("corpus = ?")
            params.append(corpus)
        if model is not None:
            clauses.append("model = ?")
            params.append(model)
        if seed is not None:
            clauses.append("seed = ?")
            params.append(seed)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        for r in self._conn.execute(sql, params):
            yield ParsedLineRow(
                corpus=r[0],
                recipe_id=r[1],
                line_index=r[2],
                raw_line=r[3],
                parsed_json=r[4],
                model=r[5],
                seed=r[6],
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
        if f.pending_only:
            where.append("review_status IS NULL")
        elif not f.include_dropped:
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

    def bulk_ingredient_names(self) -> dict[str, tuple[str, ...]]:
        """Map ``variant_id`` → tuple of canonical_name ordered by ordinal.

        Sourced from ``variant_ingredient_stats``. After
        RationalRecipes-70o this matches ``canonical_ingredient_set``,
        because the frequency filter is applied at variant-formation
        time so both representations agree by construction.
        """
        rows = self._conn.execute(
            """
            SELECT variant_id, canonical_name
            FROM variant_ingredient_stats
            ORDER BY variant_id, ordinal ASC
            """
        ).fetchall()
        out: dict[str, list[str]] = {}
        for variant_id, canonical_name in rows:
            out.setdefault(variant_id, []).append(canonical_name)
        return {vid: tuple(names) for vid, names in out.items()}

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

    def get_variant_source_ingredients(
        self, variant_id: str
    ) -> list[tuple[str, list[ParsedIngredientRow]]]:
        """Per-source parsed-ingredient lists for a variant (bead zh6).

        Returns a list of ``(recipe_id, ingredients)`` tuples — one entry
        per row in ``variant_members`` for ``variant_id``, in the same
        order as ``get_variant_members`` (best-outlier-score first, then
        recipe_id ascending). Each ``ingredients`` list is ordered by
        ``canonical_name``. Source recipes with zero ``parsed_ingredients``
        rows still appear with an empty list rather than being silently
        dropped — the position in the list is the user-facing "#N" index
        and must stay stable.
        """
        rows = self._conn.execute(
            """
            SELECT m.recipe_id, p.canonical_name, p.quantity, p.unit
            FROM variant_members m
            LEFT JOIN parsed_ingredients p ON p.recipe_id = m.recipe_id
            WHERE m.variant_id = ?
            ORDER BY m.outlier_score IS NULL, m.outlier_score ASC,
                     m.recipe_id ASC, p.canonical_name ASC
            """,
            (variant_id,),
        ).fetchall()
        out: list[tuple[str, list[ParsedIngredientRow]]] = []
        last_recipe_id: str | None = None
        current: list[ParsedIngredientRow] = []
        for recipe_id, canonical_name, quantity, unit in rows:
            if recipe_id != last_recipe_id:
                if last_recipe_id is not None:
                    out.append((last_recipe_id, current))
                current = []
                last_recipe_id = recipe_id
            if canonical_name is not None:
                current.append(
                    ParsedIngredientRow(
                        canonical_name=canonical_name,
                        quantity=quantity,
                        unit=unit,
                    )
                )
        if last_recipe_id is not None:
            out.append((last_recipe_id, current))
        return out

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


def emit_variants_to_db(
    variants: Sequence[MergedVariantResult],
    db: CatalogDB,
    *,
    delete_stale_for_l1: bool = False,
) -> int:
    """Write merged-pipeline variants to the catalog DB.

    Mirrors ``pipeline_merged.emit_variants`` (CSV+manifest) but targets
    SQLite. Variants with empty ``normalized_rows`` are skipped — they
    can't be averaged. Returns the number of variants written.

    Each variant goes through ``CatalogDB.upsert_variant``, which means
    ``variants`` + ``variant_members`` + ``variant_ingredient_stats``
    are populated atomically per variant. The L1 key recorded on each
    variant row is ``normalize_title(variant.variant_title)``.

    ``delete_stale_for_l1`` removes variants under each L1 key touched
    by this run that are not in the input set. Useful for re-runs of
    the same title query that should converge on the new variant set
    rather than accumulate stale rows. Defaults to ``False`` so callers
    don't accidentally drop unrelated work that happens to share an L1
    key (the existing 337-variant catalog DB is the load-bearing
    motivation for the conservative default).
    """
    from rational_recipes.scrape.grouping import normalize_title

    written = 0
    by_l1_keep: dict[str, set[str]] = {}
    for variant in variants:
        if not variant.normalized_rows:
            continue
        l1_key = normalize_title(variant.variant_title)
        db.upsert_variant(variant, l1_key=l1_key)
        by_l1_keep.setdefault(l1_key, set()).add(variant.variant_id)
        written += 1
    if delete_stale_for_l1:
        for l1_key, keep in by_l1_keep.items():
            db.delete_stale_variants_for_l1(l1_key, keep)
    return written


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
class ParsedLineRow:
    """One row for the ``parsed_ingredient_lines`` table (vwt.16 Pass 1)."""

    corpus: str
    recipe_id: str
    line_index: int
    raw_line: str
    parsed_json: str | None
    model: str
    seed: int

    def to_parsed(self) -> ParsedIngredient | None:
        """Decode ``parsed_json`` back into a ``ParsedIngredient``.

        Returns ``None`` for a cached failure (NULL parsed_json) or a
        malformed payload — callers see "no parse" either way.
        """
        return parsed_from_json(self.parsed_json, self.raw_line)


def parsed_to_json(parsed: ParsedIngredient | None) -> str | None:
    """Serialize a ParsedIngredient for the parsed_ingredient_lines table.

    ``raw`` is excluded because it's the lookup key (``raw_line`` column).
    Returns ``None`` for a None parse so callers can pass through the
    cached-failure sentinel.
    """
    if parsed is None:
        return None
    return json.dumps(
        {
            "quantity": parsed.quantity,
            "unit": parsed.unit,
            "ingredient": parsed.ingredient,
            "preparation": parsed.preparation,
        },
        ensure_ascii=False,
    )


def parsed_from_json(payload: str | None, raw: str) -> ParsedIngredient | None:
    """Inverse of ``parsed_to_json``. Tolerates malformed payloads."""
    if payload is None:
        return None
    try:
        d = json.loads(payload)
    except json.JSONDecodeError:
        return None
    if not isinstance(d, dict):
        return None
    try:
        return ParsedIngredient(
            quantity=float(d["quantity"]),
            unit=str(d["unit"]),
            ingredient=str(d["ingredient"]),
            preparation=str(d.get("preparation", "")),
            raw=raw,
        )
    except (KeyError, ValueError, TypeError):
        return None


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
        min_sample = sum(
            1 for row in rows if row.proportions.get(name, 0.0) > 0.0
        )

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
                min_sample_size=min_sample,
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
