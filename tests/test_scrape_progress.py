"""Tests for scripts/scrape_progress.py — DB-driven progress reporter."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from rational_recipes.catalog_db import CatalogDB, ParsedLineRow
from rational_recipes.cli.scrape_progress import main, render_report


def _seed_db(path: Path) -> CatalogDB:
    db = CatalogDB.open(path)
    db.upsert_parsed_lines(
        [
            ParsedLineRow(
                corpus="recipenlg",
                recipe_id="rnlg:1",
                line_index=0,
                raw_line="200 g flour",
                parsed_json='{"ingredient": "flour", "quantity": 200, "unit": "g"}',
                model="gemma4:e2b",
                seed=42,
            ),
            ParsedLineRow(
                corpus="recipenlg",
                recipe_id="rnlg:1",
                line_index=1,
                raw_line="bogus line",
                parsed_json=None,  # parse failure
                model="gemma4:e2b",
                seed=42,
            ),
            ParsedLineRow(
                corpus="wdc",
                recipe_id="wdc:https://example.com/p/1",
                line_index=0,
                raw_line="500 ml milk",
                parsed_json='{"ingredient": "milk", "quantity": 500, "unit": "ml"}',
                model="gemma4:e2b",
                seed=42,
            ),
        ]
    )
    db.record_l1_run(
        "pannkakor",
        corpus_revisions="rev-1",
        variants_produced=2,
        dry=False,
        run_at="2026-04-29T01:23:45+00:00",
    )
    db.record_l1_run(
        "banana bread",
        corpus_revisions="rev-1",
        variants_produced=0,
        dry=True,
        run_at="2026-04-29T01:24:00+00:00",
    )
    return db


class TestRenderReport:
    def test_includes_pass1_pass2_and_variants_sections(
        self, tmp_path: Path
    ) -> None:
        db_path = tmp_path / "recipes.db"
        db = _seed_db(db_path)
        db.close()

        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        try:
            report = render_report(conn, str(db_path))
        finally:
            conn.close()

        assert "Pass 1 — parsed_ingredient_lines" in report
        assert "rows: 3" in report
        assert "parse failures: 1" in report
        assert "recipes covered: 2" in report
        assert "recipenlg=2" in report
        assert "wdc=1" in report
        assert "gemma4:e2b=3" in report

        assert "Pass 2 — query_runs" in report
        assert "total: 2" in report
        assert "dry: 1" in report
        assert "with-variants: 1" in report
        assert "2026-04-29T01:24:00" in report

        assert "Variants table" in report

    def test_handles_empty_db(self, tmp_path: Path) -> None:
        db_path = tmp_path / "empty.db"
        CatalogDB.open(db_path).close()
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        try:
            report = render_report(conn, str(db_path))
        finally:
            conn.close()
        assert "rows: 0" in report
        assert "total: 0" in report


class TestMain:
    def test_missing_db_returns_1(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        rc = main(["--db", str(tmp_path / "nope.db")])
        assert rc == 1
        captured = capsys.readouterr()
        assert "DB not found" in captured.err

    def test_existing_db_prints_report(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        db_path = tmp_path / "recipes.db"
        _seed_db(db_path).close()
        rc = main(["--db", str(db_path)])
        assert rc == 0
        captured = capsys.readouterr()
        assert "Pass 1 — parsed_ingredient_lines" in captured.out
        assert "Pass 2 — query_runs" in captured.out
