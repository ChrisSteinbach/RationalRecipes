"""Tests for the two-pass parse architecture (vwt.16).

Pass 1 hits the LLM (parse_fn). Pass 2 reads cached parses out of
parsed_ingredient_lines. Tests verify (a) Pass 1 idempotency and
line-text dedup, (b) Pass 2 produces identical variants without the
LLM, (c) cache invalidation on model/seed change, and (d) the
two-call workflow (warm cache once, sweep thresholds many times).
"""

from __future__ import annotations

import csv
import gzip
import json
import zipfile
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest

from rational_recipes.catalog_db import CatalogDB
from rational_recipes.scrape.catalog_pipeline import (
    DEFAULT_PARSE_MODEL,
    DEFAULT_PARSE_SEED,
    recipenlg_recipe_id,
    run_catalog_pipeline,
    wdc_recipe_id,
)
from rational_recipes.scrape.parse import ParsedIngredient
from rational_recipes.scrape.recipenlg import Recipe, RecipeNLGLoader
from rational_recipes.scrape.wdc import WDCLoader, WDCRecipe

# --- Fixture corpora (smaller, repeatable) ---

_RECIPENLG_FIELDS = ["", "title", "ingredients", "directions", "link", "source", "NER"]


def _write_recipenlg_csv(path: Path, rows: list[dict[str, object]]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=_RECIPENLG_FIELDS)
        w.writeheader()
        for i, row in enumerate(rows):
            w.writerow(
                {
                    "": str(i),
                    "title": str(row["title"]),
                    "ingredients": repr(list(row.get("ingredients", []))),
                    "directions": "[]",
                    "link": str(row.get("link", "")),
                    "source": "test",
                    "NER": repr(list(row.get("ner", []))),
                }
            )


def _write_wdc_zip(
    path: Path,
    host_to_rows: dict[str, list[dict[str, object]]],
) -> None:
    with zipfile.ZipFile(path, "w") as zf:
        for host, rows in host_to_rows.items():
            entry = f"Recipe_{host}_October2023.json.gz"
            payload = "\n".join(json.dumps(r) for r in rows).encode()
            zf.writestr(entry, gzip.compress(payload))


@pytest.fixture()
def synthetic_corpora(tmp_path: Path) -> tuple[Path, Path]:
    """One L1 group ('pannkakor') with mixed corpora and a repeated line.

    The repeated raw line ("200 g flour") appears in every recipenlg
    row plus the WDC row → exercises the line-text dedup path.
    """
    csv_path = tmp_path / "rnlg.csv"
    rows: list[dict[str, object]] = []
    for i in range(3):
        rows.append(
            {
                "title": "Pannkakor",
                "link": f"https://a.example/p/{i}",
                "ingredients": ["200 g flour", "400 ml milk"],
                "ner": ["flour", "milk"],
            }
        )
    _write_recipenlg_csv(csv_path, rows)

    zip_path = tmp_path / "wdc.zip"
    _write_wdc_zip(
        zip_path,
        {
            "example.com": [
                {
                    "row_id": i,
                    "name": "Pannkakor",
                    "page_url": f"https://example.com/p/{i}",
                    "recipeingredient": ["200 g flour", "300 ml milk"],
                    "cookingmethod": "stekt",
                }
                for i in range(2)
            ],
        },
    )
    return csv_path, zip_path


def _parsed(qty: float, unit: str, ing: str) -> ParsedIngredient:
    return ParsedIngredient(
        quantity=qty,
        unit=unit,
        ingredient=ing,
        preparation="",
        raw=f"{qty} {unit} {ing}",
    )


def _make_parse_fn(call_log: list[list[str]]) -> object:
    """A parse_fn that records each call's lines and returns canned parses."""

    def parse(lines: list[str]) -> list[ParsedIngredient | None]:
        call_log.append(list(lines))
        out: list[ParsedIngredient | None] = []
        for line in lines:
            parts = line.split(maxsplit=2)
            if len(parts) < 3:
                out.append(None)
                continue
            try:
                qty = float(parts[0])
            except ValueError:
                out.append(None)
                continue
            out.append(_parsed(qty, parts[1], parts[2]))
        return out

    return parse


def _open_db(tmp_path: Path, name: str = "recipes.db") -> CatalogDB:
    return CatalogDB.open(tmp_path / name)


def _run_pipeline(
    db: CatalogDB,
    csv_path: Path,
    zip_path: Path,
    parse_fn: Any,
    **overrides: Any,
) -> Any:
    """Drive run_catalog_pipeline with the standard fixture defaults.

    Tests express only what differs (e.g. ``do_pass2=False`` for a
    Pass-1-only run, or ``model="model-b"`` for a model-swap test).
    Defaults match the fixture corpora — three recipenlg rows + two
    WDC rows under one L1 group, l2 threshold 0.3, both passes on.
    """
    kwargs: dict[str, Any] = dict(
        db=db,
        rnlg_loader=RecipeNLGLoader(path=csv_path),
        wdc_loader=WDCLoader(zip_path=zip_path),
        parse_fn=parse_fn,
        corpus_revisions="rev-1",
        l1_min=3,
        l2_threshold=0.3,
        l2_min=2,
        l3_min=2,
        do_pass1=True,
        do_pass2=True,
    )
    kwargs.update(overrides)
    return run_catalog_pipeline(**kwargs)


# --- Pass 1 behavior ---


class TestRecipeIdHelpers:
    def test_recipenlg_uses_row_index(self) -> None:
        r = Recipe(
            row_index=42,
            title="x",
            ingredients=(),
            ner=(),
            source="s",
            link="l",
        )
        assert recipenlg_recipe_id(r) == "rnlg:42"

    def test_wdc_uses_page_url_when_present(self) -> None:
        w = WDCRecipe(
            row_id=1,
            host="h",
            title="t",
            ingredients=(),
            page_url="https://x.test/p/1",
            cooking_methods=frozenset(),
            durations=(),
            recipe_category="",
            keywords=(),
            recipe_yield="",
        )
        assert wdc_recipe_id(w) == "wdc:https://x.test/p/1"

    def test_wdc_falls_back_to_host_row_id(self) -> None:
        w = WDCRecipe(
            row_id=7,
            host="h",
            title="t",
            ingredients=(),
            page_url="",
            cooking_methods=frozenset(),
            durations=(),
            recipe_category="",
            keywords=(),
            recipe_yield="",
        )
        assert wdc_recipe_id(w) == "wdc:h:7"


class TestPass1WritesCache:
    def test_populates_parsed_ingredient_lines(
        self, synthetic_corpora: tuple[Path, Path], tmp_path: Path
    ) -> None:
        csv_path, zip_path = synthetic_corpora
        db = _open_db(tmp_path)
        calls: list[list[str]] = []

        _run_pipeline(
            db, csv_path, zip_path, _make_parse_fn(calls), do_pass2=False
        )
        # 3 rnlg rows × 2 lines + 2 wdc rows × 2 lines = 10 line rows.
        assert db.count_parsed_lines() == 10
        # Both corpora are represented.
        assert db.count_parsed_lines(corpus="recipenlg") == 6
        assert db.count_parsed_lines(corpus="wdc") == 4

    def test_line_text_dedup_collapses_repeated_lines(
        self, synthetic_corpora: tuple[Path, Path], tmp_path: Path
    ) -> None:
        """Same raw_line text in many recipes → one LLM batch covers them."""
        csv_path, zip_path = synthetic_corpora
        db = _open_db(tmp_path)
        calls: list[list[str]] = []

        _run_pipeline(
            db, csv_path, zip_path, _make_parse_fn(calls), do_pass2=False
        )
        # Every raw_line text the LLM saw must appear in at most one
        # call; later occurrences hit the in-process line_text_cache or
        # the persisted DB cache.
        all_lines = [line for batch in calls for line in batch]
        assert len(all_lines) == len(set(all_lines)), (
            f"line repeated across LLM calls: {all_lines}"
        )

    def test_pass1_is_idempotent(
        self, synthetic_corpora: tuple[Path, Path], tmp_path: Path
    ) -> None:
        csv_path, zip_path = synthetic_corpora
        db = _open_db(tmp_path)

        first_calls: list[list[str]] = []
        _run_pipeline(
            db, csv_path, zip_path, _make_parse_fn(first_calls), do_pass2=False
        )
        rows_before = db.count_parsed_lines()

        # Re-run Pass 1 on the same DB → no LLM calls, no new rows.
        second_calls: list[list[str]] = []
        _run_pipeline(
            db, csv_path, zip_path, _make_parse_fn(second_calls), do_pass2=False
        )
        assert second_calls == [], "Pass 1 re-call should hit cache 100%"
        assert db.count_parsed_lines() == rows_before

    def test_model_swap_invalidates_resume(
        self, synthetic_corpora: tuple[Path, Path], tmp_path: Path
    ) -> None:
        csv_path, zip_path = synthetic_corpora
        db = _open_db(tmp_path)

        _run_pipeline(
            db,
            csv_path,
            zip_path,
            _make_parse_fn([]),
            model="model-a",
            do_pass2=False,
        )
        # New model → must re-parse every recipe, write new rows.
        second_calls: list[list[str]] = []
        _run_pipeline(
            db,
            csv_path,
            zip_path,
            _make_parse_fn(second_calls),
            model="model-b",
            do_pass2=False,
        )
        assert second_calls != [], "model swap should re-LLM the lines"
        # Schema PK is (corpus, recipe_id, line_index): the new model
        # overwrites the old rows, so only model-b survives. The
        # invariant tested here is "no stale rows left tagged with the
        # old model after a re-parse" — important so that downstream
        # Pass 2 lookups always see the freshest parse for a recipe.
        assert db.count_parsed_lines(model="model-a") == 0
        assert db.count_parsed_lines(model="model-b") > 0

    def test_cached_failure_does_not_retry(
        self, synthetic_corpora: tuple[Path, Path], tmp_path: Path
    ) -> None:
        """Pre-seeded NULL parsed_json → Pass 1 honours it and skips LLM."""
        csv_path, zip_path = synthetic_corpora
        db = _open_db(tmp_path)
        # Pre-seed a failure for "200 g flour" — every recipenlg row uses
        # this exact line, so the second pass should not try to re-parse
        # it via LLM.
        from rational_recipes.catalog_db import ParsedLineRow

        db.upsert_parsed_lines(
            [
                ParsedLineRow(
                    corpus="recipenlg",
                    recipe_id="rnlg:0",
                    line_index=0,
                    raw_line="200 g flour",
                    parsed_json=None,
                    model=DEFAULT_PARSE_MODEL,
                    seed=DEFAULT_PARSE_SEED,
                )
            ]
        )

        calls: list[list[str]] = []
        _run_pipeline(
            db, csv_path, zip_path, _make_parse_fn(calls), do_pass2=False
        )
        # The LLM never sees "200 g flour" — it was a cached failure.
        all_lines = {line for batch in calls for line in batch}
        assert "200 g flour" not in all_lines


# --- Pass 2 behavior (no LLM) ---


class TestPass2NoLLM:
    def _warm(
        self,
        db: CatalogDB,
        csv_path: Path,
        zip_path: Path,
    ) -> None:
        _run_pipeline(db, csv_path, zip_path, _make_parse_fn([]), do_pass2=False)

    def test_pass2_only_writes_variants_without_llm_calls(
        self, synthetic_corpora: tuple[Path, Path], tmp_path: Path
    ) -> None:
        csv_path, zip_path = synthetic_corpora
        db = _open_db(tmp_path)
        self._warm(db, csv_path, zip_path)

        calls: list[list[str]] = []

        def trip_wire(lines: list[str]) -> list[ParsedIngredient | None]:
            calls.append(list(lines))
            raise AssertionError(
                "Pass 2 must not call parse_fn (LLM); cache was warmed"
            )

        stats = _run_pipeline(
            db, csv_path, zip_path, trip_wire, do_pass1=False
        )
        assert calls == []
        assert stats.variants_produced >= 1
        titles = {v.normalized_title for v in db.list_variants()}
        assert "pannkakor" in titles

    def test_full_run_equals_split_runs(
        self, synthetic_corpora: tuple[Path, Path], tmp_path: Path
    ) -> None:
        """Pass1+Pass2 in one call ↔ Pass1 then Pass2 in two calls.

        Variant ids must be byte-identical: same input, same parses,
        same clustering thresholds, same outputs.
        """
        csv_path, zip_path = synthetic_corpora
        fixed_now = lambda: "2026-04-25T00:00:00+00:00"  # noqa: E731

        def variants_after(do_split: bool) -> list[tuple[str, str, int]]:
            db_path = tmp_path / ("split.db" if do_split else "single.db")
            db = CatalogDB.open(db_path)
            try:
                if do_split:
                    _run_pipeline(
                        db,
                        csv_path,
                        zip_path,
                        _make_parse_fn([]),
                        do_pass2=False,
                        now_fn=fixed_now,
                    )
                    _run_pipeline(
                        db,
                        csv_path,
                        zip_path,
                        _make_parse_fn([]),
                        do_pass1=False,
                        now_fn=fixed_now,
                    )
                else:
                    _run_pipeline(
                        db,
                        csv_path,
                        zip_path,
                        _make_parse_fn([]),
                        now_fn=fixed_now,
                    )
                return [
                    (v.variant_id, v.normalized_title, v.n_recipes)
                    for v in db.list_variants()
                ]
            finally:
                db.close()

        assert variants_after(False) == variants_after(True)

    def test_pass2_skips_variants_for_uncached_recipes(
        self, synthetic_corpora: tuple[Path, Path], tmp_path: Path
    ) -> None:
        """Empty cache + Pass 2 → no variants (every recipe drops as no parse)."""
        csv_path, zip_path = synthetic_corpora
        db = _open_db(tmp_path)

        calls: list[list[str]] = []
        stats = _run_pipeline(
            db, csv_path, zip_path, _make_parse_fn(calls), do_pass1=False
        )
        assert calls == []
        assert stats.variants_produced == 0


# --- Threshold-sweep workflow ---


class TestThresholdSweep:
    """Pass1 once, then re-run Pass2 with different thresholds — no LLM."""

    def test_second_sweep_does_not_call_parse_fn(
        self, synthetic_corpora: tuple[Path, Path], tmp_path: Path
    ) -> None:
        csv_path, zip_path = synthetic_corpora
        db = _open_db(tmp_path)

        # Warm the cache.
        warm_calls: list[list[str]] = []
        _run_pipeline(
            db, csv_path, zip_path, _make_parse_fn(warm_calls), do_pass2=False
        )
        assert warm_calls != []

        # Now sweep three different thresholds — none should LLM.
        for threshold in (0.2, 0.4, 0.6):
            sweep_calls: list[list[str]] = []
            # Each sweep run gets a fresh corpus_revisions so query_runs
            # doesn't short-circuit Pass 2 on the L1 group.
            _run_pipeline(
                db,
                csv_path,
                zip_path,
                _make_parse_fn(sweep_calls),
                corpus_revisions=f"rev-{threshold}",
                l2_threshold=threshold,
                do_pass1=False,
            )
            assert sweep_calls == [], (
                f"Pass 2 sweep at threshold={threshold} should not LLM"
            )


# --- CLI flags ---


class TestCliFlags:
    def _import_cli(self):
        from rational_recipes.cli import scrape_catalog as cli

        return cli

    def test_pass1_only_warms_cache_no_variants(
        self,
        synthetic_corpora: tuple[Path, Path],
        tmp_path: Path,
    ) -> None:
        cli = self._import_cli()
        csv_path, zip_path = synthetic_corpora
        out_db = tmp_path / "out.db"

        rc = cli.run(
            [
                "--recipenlg",
                str(csv_path),
                "--wdc-zip",
                str(zip_path),
                "--output-db",
                str(out_db),
                "--l1-min",
                "3",
                "--l2-threshold",
                "0.3",
                "--l2-min",
                "2",
                "--l3-min",
                "2",
                "--language-filter",
                "all",
                "--skip-preflight",
                "--pass1-only",
            ],
            parse_fn=_make_parse_fn([]),
            extract_fn=lambda recipes: list(recipes),
        )
        assert rc == 0
        db = CatalogDB.open(out_db)
        try:
            assert db.count_parsed_lines() > 0
            assert db.list_variants() == []
        finally:
            db.close()

    def test_pass2_only_after_pass1_writes_variants(
        self,
        synthetic_corpora: tuple[Path, Path],
        tmp_path: Path,
    ) -> None:
        cli = self._import_cli()
        csv_path, zip_path = synthetic_corpora
        out_db = tmp_path / "out.db"

        # Warm cache via --pass1-only.
        cli.run(
            [
                "--recipenlg",
                str(csv_path),
                "--wdc-zip",
                str(zip_path),
                "--output-db",
                str(out_db),
                "--l1-min",
                "3",
                "--l2-threshold",
                "0.3",
                "--l2-min",
                "2",
                "--l3-min",
                "2",
                "--language-filter",
                "all",
                "--skip-preflight",
                "--pass1-only",
            ],
            parse_fn=_make_parse_fn([]),
            extract_fn=lambda recipes: list(recipes),
        )

        # Now Pass 2 with a parse_fn that would crash if called.
        def trip_wire(lines: list[str]) -> list[ParsedIngredient | None]:
            raise AssertionError(
                "Pass 2 should not call parse_fn — cache is warm"
            )

        rc = cli.run(
            [
                "--recipenlg",
                str(csv_path),
                "--wdc-zip",
                str(zip_path),
                "--output-db",
                str(out_db),
                "--l1-min",
                "3",
                "--l2-threshold",
                "0.3",
                "--l2-min",
                "2",
                "--l3-min",
                "2",
                "--language-filter",
                "all",
                "--skip-preflight",
                "--pass2-only",
            ],
            parse_fn=trip_wire,
            extract_fn=lambda recipes: list(recipes),
        )
        assert rc == 0
        db = CatalogDB.open(out_db)
        try:
            titles = {v.normalized_title for v in db.list_variants()}
            assert "pannkakor" in titles
        finally:
            db.close()


# --- Schema migration ---


class TestSchemaMigration:
    """Existing recipes.db files must keep working after the upgrade."""

    def test_old_db_without_parsed_lines_table_keeps_working(
        self, tmp_path: Path
    ) -> None:
        """Old DB without parsed_ingredient_lines: schema upgrades, data intact."""
        # Build a minimal "old-shape" DB by hand: just the tables that
        # existed before vwt.16.
        import sqlite3

        path = tmp_path / "old.db"
        conn = sqlite3.connect(str(path))
        conn.execute(
            """
            CREATE TABLE variants (
              variant_id TEXT PRIMARY KEY,
              normalized_title TEXT NOT NULL,
              display_title TEXT,
              category TEXT,
              description TEXT,
              base_ingredient TEXT,
              cooking_methods TEXT,
              canonical_ingredient_set TEXT NOT NULL,
              n_recipes INTEGER NOT NULL,
              confidence_level REAL,
              review_status TEXT,
              review_note TEXT,
              reviewed_at TEXT
            )
            """
        )
        conn.execute(
            "INSERT INTO variants VALUES "
            "('v1','t',NULL,NULL,NULL,NULL,'','flour,milk',3,NULL,NULL,NULL,NULL)"
        )
        conn.commit()
        conn.close()

        # Open via CatalogDB → new schema applied without dropping data.
        db = CatalogDB.open(path)
        try:
            names = {
                r[0]
                for r in db.connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            assert "parsed_ingredient_lines" in names
            # Original variant survives.
            v_ids = [
                r[0]
                for r in db.connection.execute("SELECT variant_id FROM variants")
            ]
            assert v_ids == ["v1"]
        finally:
            db.close()

    def test_index_present(self, tmp_path: Path) -> None:
        db = CatalogDB.open(tmp_path / "db.sqlite")
        idx = {
            r[0]
            for r in db.connection.execute(
                "SELECT name FROM sqlite_master WHERE type='index'"
            )
        }
        assert "idx_parsed_lines_text" in idx


# --- Backward-compat: extract_fn still accepted but ignored ---


def _ignore_extract(recipes: Sequence[WDCRecipe]) -> list[WDCRecipe]:
    return list(recipes)


class TestBackwardCompat:
    def test_extract_fn_passed_but_unused_in_pass2(
        self, synthetic_corpora: tuple[Path, Path], tmp_path: Path
    ) -> None:
        csv_path, zip_path = synthetic_corpora
        db = _open_db(tmp_path)

        called = {"n": 0}

        def counting(recipes: Sequence[WDCRecipe]) -> list[WDCRecipe]:
            called["n"] += 1
            return list(recipes)

        _run_pipeline(
            db, csv_path, zip_path, _make_parse_fn([]), extract_fn=counting
        )
        # extract_fn was passed in but never invoked — Pass 2 derives
        # ingredient_names from the parsed_ingredient_lines cache.
        assert called["n"] == 0
