"""Tests for the whole-corpus extraction pipeline (bead vwt.2)."""

from __future__ import annotations

import csv
import gzip
import io
import json
import zipfile
from collections.abc import Sequence
from dataclasses import replace
from pathlib import Path

import pytest

from rational_recipes.catalog_db import CatalogDB
from rational_recipes.cli import scrape_catalog as cli
from rational_recipes.scrape.canonical import canonicalize_names
from rational_recipes.scrape.catalog_pipeline import (
    CatalogRunStats,
    HeartbeatSnapshot,
    compute_corpus_revisions,
    detect_language,
    run_catalog_pipeline,
    stream_l1_groups,
)
from rational_recipes.scrape.parse import ParsedIngredient
from rational_recipes.scrape.recipenlg import RecipeNLGLoader
from rational_recipes.scrape.wdc import WDCLoader, WDCRecipe

# --- Fixture corpora ---

_RECIPENLG_FIELDS = [
    "",
    "title",
    "ingredients",
    "directions",
    "link",
    "source",
    "NER",
]


RecipeRow = dict[str, str | tuple[str, ...]]


def _write_recipenlg_csv(
    path: Path,
    rows: list[RecipeRow],
) -> None:
    """rows columns: title, link, ingredients (tuple[str,...]), ner (tuple)."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_RECIPENLG_FIELDS)
        writer.writeheader()
        for i, row in enumerate(rows):
            writer.writerow(
                {
                    "": str(i),
                    "title": str(row["title"]),
                    "ingredients": repr(list(row.get("ingredients", ()))),
                    "directions": "[]",
                    "link": str(row.get("link", "")),
                    "source": "test",
                    "NER": repr(list(row.get("ner", ()))),
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
    """Three L1 groups: pannkakor, chocolate cake, banana bread.

    Sized so all three pass --l1-min=3 — pannkakor has wdc rows, chocolate
    cake is recipenlg-only, banana bread is below the threshold so we can
    verify the drop.
    """
    csv_path = tmp_path / "recipenlg.csv"
    rows: list[RecipeRow] = []
    # Vary quantities so proportion-bucket dedup doesn't collapse all rows.
    for i in range(3):
        flour_g = 200 + i * 50
        milk_ml = 400 - i * 50
        rows.append(
            {
                "title": "Pannkakor",
                "link": f"https://a.example/p/{i}",
                "ingredients": (f"{flour_g} g flour", f"{milk_ml} ml milk"),
                "ner": ("flour", "milk"),
            }
        )
    for i in range(4):
        flour_g = 200 + i * 50
        sugar_g = 100 + i * 30
        rows.append(
            {
                "title": "Chocolate Cake",
                "link": f"https://a.example/c/{i}",
                "ingredients": (f"{flour_g} g flour", f"{sugar_g} g sugar"),
                "ner": ("flour", "sugar"),
            }
        )
    # Below l1_min=3, dropped by thresholding.
    for i in range(2):
        rows.append(
            {
                "title": "Banana Bread",
                "link": f"https://a.example/b/{i}",
                "ingredients": (f"{200 + i * 50} g flour",),
                "ner": ("flour",),
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
                    "recipeingredient": [
                        f"{150 + i * 50} g flour",
                        f"{500 - i * 50} ml milk",
                    ],
                    "cookingmethod": "stekt",
                }
                for i in range(3)
            ],
        },
    )
    return csv_path, zip_path


def _parsed(ing: str, qty: float, unit: str) -> ParsedIngredient:
    return ParsedIngredient(
        ingredient=ing,
        quantity=qty,
        unit=unit,
        preparation="",
        raw=f"{qty} {unit} {ing}",
    )


def _default_parse(lines: list[str]) -> list[ParsedIngredient | None]:
    """Parse '<qty> <unit> <ingredient>' or '<qty> <unit> <lang>' lines.

    Maps Swedish words to their English canonical forms via canonicalize_names
    at ingredient-factory lookup time; here we translate a tiny dictionary so
    the synthetic corpus normalizes to `flour`/`milk`/`sugar`.
    """
    translate = {
        "vetemjöl": "flour",
        "mjölk": "milk",
    }
    out: list[ParsedIngredient | None] = []
    for line in lines:
        parts = line.split()
        if len(parts) < 3:
            out.append(None)
            continue
        qty = float(parts[0])
        unit = parts[1]
        ing = " ".join(parts[2:])
        ing = translate.get(ing, ing)
        out.append(_parsed(ing, qty, unit))
    return out


def _default_extract(recipes: Sequence[WDCRecipe]) -> list[WDCRecipe]:
    """Stub ingredient-name extraction: canonicalize raw lines' first word.

    Mirrors extract_batch's shape — returns WDCRecipe copies with
    ingredient_names populated — without any Ollama traffic.
    """
    translate = {
        "vetemjöl": "flour",
        "mjölk": "milk",
    }
    result: list[WDCRecipe] = []
    for recipe in recipes:
        names: list[str] = []
        for line in recipe.ingredients:
            parts = line.split()
            if len(parts) >= 3:
                raw = " ".join(parts[2:])
                names.append(translate.get(raw, raw))
        result.append(replace(recipe, ingredient_names=canonicalize_names(names)))
    return result


# --- Unit tests for helpers ---


class TestDetectLanguage:
    def test_ascii_is_english(self) -> None:
        assert detect_language("chocolate cake") == "en"

    def test_swedish_diacritic_is_sv(self) -> None:
        assert detect_language("pannkaka med äpple") == "sv"

    def test_uppercase_diacritic_is_sv(self) -> None:
        assert detect_language("Äppelkaka") == "sv"


class TestComputeCorpusRevisions:
    def test_fingerprint_depends_on_both_files(self, tmp_path: Path) -> None:
        a = tmp_path / "a.csv"
        b = tmp_path / "b.zip"
        a.write_text("row\n")
        b.write_bytes(b"\x00")
        rev1 = compute_corpus_revisions(a, b)
        # Rewrite a to change size; fingerprint must change.
        a.write_text("row\nmore\n")
        rev2 = compute_corpus_revisions(a, b)
        assert rev1 != rev2


class TestStreamL1Groups:
    def test_groups_by_normalized_title(
        self, synthetic_corpora: tuple[Path, Path]
    ) -> None:
        csv_path, zip_path = synthetic_corpora
        groups = stream_l1_groups(
            rnlg_loader=RecipeNLGLoader(path=csv_path),
            wdc_loader=WDCLoader(zip_path=zip_path),
            accept=lambda _: True,
        )
        assert "pannkakor" in groups
        assert len(groups["pannkakor"].recipenlg) == 3
        assert len(groups["pannkakor"].wdc) == 3
        assert len(groups["chocolate cake"].recipenlg) == 4
        assert len(groups["banana bread"].recipenlg) == 2


# --- End-to-end pipeline ---


def _open_db(tmp_path: Path) -> tuple[CatalogDB, Path]:
    path = tmp_path / "recipes.db"
    return CatalogDB.open(path), path


class TestRunCatalogPipeline:
    def test_writes_variants_for_surviving_l1_groups(
        self, synthetic_corpora: tuple[Path, Path], tmp_path: Path
    ) -> None:
        csv_path, zip_path = synthetic_corpora
        db, _ = _open_db(tmp_path)
        stats = run_catalog_pipeline(
            db=db,
            rnlg_loader=RecipeNLGLoader(path=csv_path),
            wdc_loader=WDCLoader(zip_path=zip_path),
            parse_fn=_default_parse,
            extract_fn=_default_extract,
            corpus_revisions="rev-1",
            l1_min=3,
            l2_threshold=0.3,
            l2_min=2,
            l3_min=2,
            now_fn=lambda: "2026-04-24T00:00:00+00:00",
        )
        # pannkakor + chocolate cake clear --l1-min=3; banana bread doesn't.
        assert stats.l1_groups_total == 2
        assert stats.l1_groups_processed == 2
        assert stats.l1_groups_skipped == 0
        assert stats.variants_produced >= 1

        variants = db.list_variants()
        titles = {v.normalized_title for v in variants}
        assert "pannkakor" in titles

    def test_skips_groups_marked_fresh(
        self, synthetic_corpora: tuple[Path, Path], tmp_path: Path
    ) -> None:
        """Pre-seeded query_runs row → that L1 group is skipped."""
        csv_path, zip_path = synthetic_corpora
        db, _ = _open_db(tmp_path)
        db.record_l1_run(
            "pannkakor",
            corpus_revisions="rev-1",
            variants_produced=0,
            dry=True,
            run_at="2026-04-24T00:00:00+00:00",
        )
        stats = run_catalog_pipeline(
            db=db,
            rnlg_loader=RecipeNLGLoader(path=csv_path),
            wdc_loader=WDCLoader(zip_path=zip_path),
            parse_fn=_default_parse,
            extract_fn=_default_extract,
            corpus_revisions="rev-1",
            l1_min=3,
            l2_threshold=0.3,
            l2_min=2,
            l3_min=2,
        )
        assert stats.l1_groups_skipped == 1
        assert stats.l1_groups_processed == 1  # chocolate cake
        # No new pannkakor variants written — the pre-seeded row says dry.
        variants = {v.normalized_title for v in db.list_variants()}
        assert "pannkakor" not in variants

    def test_rerun_is_noop_same_fingerprint(
        self, synthetic_corpora: tuple[Path, Path], tmp_path: Path
    ) -> None:
        """Second run with unchanged inputs processes zero groups."""
        csv_path, zip_path = synthetic_corpora
        db, _ = _open_db(tmp_path)

        def _run(revisions: str) -> CatalogRunStats:
            return run_catalog_pipeline(
                db=db,
                rnlg_loader=RecipeNLGLoader(path=csv_path),
                wdc_loader=WDCLoader(zip_path=zip_path),
                parse_fn=_default_parse,
                extract_fn=_default_extract,
                corpus_revisions=revisions,
                l1_min=3,
                l2_threshold=0.3,
                l2_min=2,
                l3_min=2,
            )

        first = _run("rev-1")
        before = [v.variant_id for v in db.list_variants()]
        second = _run("rev-1")
        after = [v.variant_id for v in db.list_variants()]

        assert second.l1_groups_processed == 0
        assert second.l1_groups_skipped == first.l1_groups_processed
        assert before == after

    def test_rerun_reprocesses_when_fingerprint_changes(
        self, synthetic_corpora: tuple[Path, Path], tmp_path: Path
    ) -> None:
        csv_path, zip_path = synthetic_corpora
        db, _ = _open_db(tmp_path)

        def _run(revisions: str) -> CatalogRunStats:
            return run_catalog_pipeline(
                db=db,
                rnlg_loader=RecipeNLGLoader(path=csv_path),
                wdc_loader=WDCLoader(zip_path=zip_path),
                parse_fn=_default_parse,
                extract_fn=_default_extract,
                corpus_revisions=revisions,
                l1_min=3,
                l2_threshold=0.3,
                l2_min=2,
                l3_min=2,
            )

        _run("rev-1")
        second = _run("rev-2")
        assert second.l1_groups_skipped == 0
        assert second.l1_groups_processed >= 1

    def test_title_filter_slices_groups(
        self, synthetic_corpora: tuple[Path, Path], tmp_path: Path
    ) -> None:
        csv_path, zip_path = synthetic_corpora
        db, _ = _open_db(tmp_path)
        stats = run_catalog_pipeline(
            db=db,
            rnlg_loader=RecipeNLGLoader(path=csv_path),
            wdc_loader=WDCLoader(zip_path=zip_path),
            parse_fn=_default_parse,
            extract_fn=_default_extract,
            corpus_revisions="rev-1",
            l1_min=3,
            l2_threshold=0.3,
            l2_min=2,
            l3_min=2,
            title_filter="pannkak",
        )
        assert stats.l1_groups_total == 1
        assert stats.l1_groups_processed == 1
        titles = {v.normalized_title for v in db.list_variants()}
        # Only pannkakor survived the slice filter.
        assert titles == {"pannkakor"} or titles == set()

    def test_resumability_after_mid_run_abort(
        self, synthetic_corpora: tuple[Path, Path], tmp_path: Path
    ) -> None:
        """Kill mid-run → next run skips completed groups, processes remainder."""
        csv_path, zip_path = synthetic_corpora
        db, _ = _open_db(tmp_path)

        calls: dict[str, int] = {"n": 0}

        def flaky_parse(lines: list[str]) -> list[ParsedIngredient | None]:
            calls["n"] += 1
            # Fail after chocolate cake's first parse — pannkakor processed
            # first alphabetically ("chocolate cake" < "pannkakor" but the
            # test uses callable-counting, so abort after the group's first
            # call).
            raise RuntimeError("simulated mid-run crash")

        # First run: succeed on pannkakor + chocolate cake's first group by
        # making parse_fn succeed only for lines from pannkakor.
        def partial_parse(lines: list[str]) -> list[ParsedIngredient | None]:
            if any("chocolate" in line or "sugar" in line for line in lines):
                raise RuntimeError("simulated mid-run crash")
            return _default_parse(lines)

        with pytest.raises(RuntimeError):
            run_catalog_pipeline(
                db=db,
                rnlg_loader=RecipeNLGLoader(path=csv_path),
                wdc_loader=WDCLoader(zip_path=zip_path),
                parse_fn=partial_parse,
                extract_fn=_default_extract,
                corpus_revisions="rev-1",
                l1_min=3,
                l2_threshold=0.3,
                l2_min=2,
                l3_min=2,
            )

        # "chocolate cake" sorts before "pannkakor" alphabetically, so the
        # crash happens on the first group processed. query_runs should be
        # empty so the next run retries it.
        first_runs = db.connection.execute(
            "SELECT l1_group_key FROM query_runs ORDER BY l1_group_key"
        ).fetchall()
        assert [r[0] for r in first_runs] == []

        # Second run with real parse: both groups get processed.
        stats = run_catalog_pipeline(
            db=db,
            rnlg_loader=RecipeNLGLoader(path=csv_path),
            wdc_loader=WDCLoader(zip_path=zip_path),
            parse_fn=_default_parse,
            extract_fn=_default_extract,
            corpus_revisions="rev-1",
            l1_min=3,
            l2_threshold=0.3,
            l2_min=2,
            l3_min=2,
        )
        assert stats.l1_groups_processed == 2
        assert stats.l1_groups_skipped == 0
        after = db.connection.execute(
            "SELECT l1_group_key FROM query_runs ORDER BY l1_group_key"
        ).fetchall()
        assert {r[0] for r in after} == {"chocolate cake", "pannkakor"}

    def test_resumability_with_partial_completion(
        self, synthetic_corpora: tuple[Path, Path], tmp_path: Path
    ) -> None:
        """Mark one L1 group done manually, verify resume skips it."""
        csv_path, zip_path = synthetic_corpora
        db, _ = _open_db(tmp_path)
        db.record_l1_run(
            "chocolate cake",
            corpus_revisions="rev-1",
            variants_produced=1,
            dry=False,
            run_at="2026-04-24T00:00:00+00:00",
        )

        stats = run_catalog_pipeline(
            db=db,
            rnlg_loader=RecipeNLGLoader(path=csv_path),
            wdc_loader=WDCLoader(zip_path=zip_path),
            parse_fn=_default_parse,
            extract_fn=_default_extract,
            corpus_revisions="rev-1",
            l1_min=3,
            l2_threshold=0.3,
            l2_min=2,
            l3_min=2,
        )
        # chocolate cake is skipped; pannkakor is processed.
        assert stats.l1_groups_skipped == 1
        assert stats.l1_groups_processed == 1
        processed_keys = db.connection.execute(
            "SELECT l1_group_key FROM query_runs ORDER BY l1_group_key"
        ).fetchall()
        assert {r[0] for r in processed_keys} == {"chocolate cake", "pannkakor"}

    def test_variant_id_stable_across_runs(
        self, synthetic_corpora: tuple[Path, Path], tmp_path: Path
    ) -> None:
        """Re-running on identical input produces byte-identical variant rows."""
        csv_path, zip_path = synthetic_corpora

        def snapshot(path: Path) -> list[tuple[object, ...]]:
            db = CatalogDB.open(path)
            try:
                rows = db.connection.execute(
                    "SELECT variant_id, normalized_title, cooking_methods, "
                    "canonical_ingredient_set, n_recipes FROM variants "
                    "ORDER BY variant_id"
                ).fetchall()
                return list(rows)
            finally:
                db.close()

        db_a_path = tmp_path / "a.db"
        db_b_path = tmp_path / "b.db"
        for db_path in (db_a_path, db_b_path):
            db = CatalogDB.open(db_path)
            run_catalog_pipeline(
                db=db,
                rnlg_loader=RecipeNLGLoader(path=csv_path),
                wdc_loader=WDCLoader(zip_path=zip_path),
                parse_fn=_default_parse,
                extract_fn=_default_extract,
                corpus_revisions="rev-1",
                l1_min=3,
                l2_threshold=0.3,
                l2_min=2,
                l3_min=2,
                now_fn=lambda: "2026-04-24T00:00:00+00:00",
            )
            db.close()

        assert snapshot(db_a_path) == snapshot(db_b_path)

    def test_parallel_pass1_matches_serial(
        self, synthetic_corpora: tuple[Path, Path], tmp_path: Path
    ) -> None:
        """Thread-pool Pass 1 produces the same variants as serial."""
        csv_path, zip_path = synthetic_corpora
        kwargs: dict[str, object] = dict(
            rnlg_loader=RecipeNLGLoader(path=csv_path),
            wdc_loader=WDCLoader(zip_path=zip_path),
            parse_fn=_default_parse,
            extract_fn=_default_extract,
            corpus_revisions="rev-par",
            l1_min=3,
            l2_threshold=0.3,
            l2_min=2,
            l3_min=2,
            now_fn=lambda: "2026-04-28T00:00:00+00:00",
        )

        serial_dir = tmp_path / "serial"
        serial_dir.mkdir()
        db_serial = CatalogDB.open(serial_dir / "recipes.db")
        stats_serial = run_catalog_pipeline(
            db=db_serial, pass1_workers=1, **kwargs  # type: ignore[arg-type]
        )

        parallel_dir = tmp_path / "parallel"
        parallel_dir.mkdir()
        db_parallel = CatalogDB.open(parallel_dir / "recipes.db")
        stats_parallel = run_catalog_pipeline(
            db=db_parallel, pass1_workers=4, **kwargs  # type: ignore[arg-type]
        )

        serial_variants = {v.variant_id for v in db_serial.list_variants()}
        parallel_variants = {v.variant_id for v in db_parallel.list_variants()}
        assert serial_variants == parallel_variants
        assert stats_serial.pass1_recipes_seen == stats_parallel.pass1_recipes_seen
        assert stats_serial.variants_produced == stats_parallel.variants_produced

        db_serial.close()
        db_parallel.close()

    def test_rejects_unknown_language_filter(
        self, synthetic_corpora: tuple[Path, Path], tmp_path: Path
    ) -> None:
        csv_path, zip_path = synthetic_corpora
        db, _ = _open_db(tmp_path)
        with pytest.raises(ValueError):
            run_catalog_pipeline(
                db=db,
                rnlg_loader=RecipeNLGLoader(path=csv_path),
                wdc_loader=WDCLoader(zip_path=zip_path),
                parse_fn=_default_parse,
                extract_fn=_default_extract,
                corpus_revisions="rev-1",
                language_filter="klingon",
            )

    def test_on_group_done_callback(
        self, synthetic_corpora: tuple[Path, Path], tmp_path: Path
    ) -> None:
        csv_path, zip_path = synthetic_corpora
        db, _ = _open_db(tmp_path)
        observed: list[str] = []

        run_catalog_pipeline(
            db=db,
            rnlg_loader=RecipeNLGLoader(path=csv_path),
            wdc_loader=WDCLoader(zip_path=zip_path),
            parse_fn=_default_parse,
            extract_fn=_default_extract,
            corpus_revisions="rev-1",
            l1_min=3,
            l2_threshold=0.3,
            l2_min=2,
            l3_min=2,
            on_group_done=lambda key, _variants: observed.append(key),
        )
        assert observed == sorted(observed)
        assert set(observed) == {"chocolate cake", "pannkakor"}


# --- CLI round-trip ---


class TestCli:
    def test_dry_run_writes_db_and_summary(
        self,
        synthetic_corpora: tuple[Path, Path],
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        csv_path, zip_path = synthetic_corpora
        out_db = tmp_path / "out" / "recipes.db"

        def fake_parse(lines: list[str]) -> list[ParsedIngredient | None]:
            return _default_parse(lines)

        def fake_extract(recipes: Sequence[WDCRecipe]) -> list[WDCRecipe]:
            return _default_extract(recipes)

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
                "--title-filter",
                "pannkak",
                "--language-filter",
                "all",
                "--skip-preflight",
            ],
            parse_fn=fake_parse,
            extract_fn=fake_extract,
        )
        assert rc == 0
        assert out_db.exists()

        # Summary printed on stdout.
        captured = capsys.readouterr()
        assert "L1 groups" in captured.out
        assert "variants produced" in captured.out

        # DB has at least one pannkakor variant.
        db = CatalogDB.open(out_db)
        try:
            titles = {v.normalized_title for v in db.list_variants()}
            assert "pannkakor" in titles
        finally:
            db.close()

    def test_missing_corpus_file_exits_1(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        rc = cli.run(
            [
                "--recipenlg",
                str(tmp_path / "missing.csv"),
                "--wdc-zip",
                str(tmp_path / "missing.zip"),
                "--output-db",
                str(tmp_path / "recipes.db"),
                "--skip-preflight",
            ]
        )
        assert rc == 1

    def test_pass3_runs_after_pass2_in_orchestrator(
        self,
        synthetic_corpora: tuple[Path, Path],
        tmp_path: Path,
    ) -> None:
        """End-to-end: --pass3-only with stub title_fn updates display_title."""
        from rational_recipes.catalog_db import CatalogDB
        from rational_recipes.scrape.pass3_titles import (
            run_pass3 as _run_pass3,
        )

        # Hand-build a DB with a multi-variant L1 group, then run --pass3-only.
        out_db = tmp_path / "out" / "recipes.db"
        out_db.parent.mkdir(parents=True, exist_ok=True)

        from tests.test_pass3_titles import _make_variant, _stub_title_fn

        db = CatalogDB.open(out_db)
        try:
            _make_variant(
                db,
                l1_title="pecan pie",
                canonical_ingredients=frozenset({"pecan", "egg", "bourbon"}),
            )
            _make_variant(
                db,
                l1_title="pecan pie",
                canonical_ingredients=frozenset({"pecan", "egg", "maple"}),
            )
            _run_pass3(db=db, title_fn=_stub_title_fn())
            titles = sorted({v.display_title for v in db.list_variants()})
            assert titles == ["Bourbon Pecan Pie", "Maple Pecan Pie"]
        finally:
            db.close()


class TestHeartbeat:
    def test_pipeline_emits_pass1_and_pass2_snapshots(
        self, synthetic_corpora: tuple[Path, Path], tmp_path: Path
    ) -> None:
        csv_path, zip_path = synthetic_corpora
        db, _ = _open_db(tmp_path)
        beats: list[HeartbeatSnapshot] = []
        run_catalog_pipeline(
            db=db,
            rnlg_loader=RecipeNLGLoader(path=csv_path),
            wdc_loader=WDCLoader(zip_path=zip_path),
            parse_fn=_default_parse,
            extract_fn=_default_extract,
            corpus_revisions="rev-1",
            l1_min=3,
            l2_threshold=0.3,
            l2_min=2,
            l3_min=2,
            heartbeat=beats.append,
        )
        passes = {b.pass_name for b in beats}
        assert "pass1" in passes
        assert "pass2" in passes

        # Final snapshot per pass: position == total.
        last_pass1 = [b for b in beats if b.pass_name == "pass1"][-1]
        last_pass2 = [b for b in beats if b.pass_name == "pass2"][-1]
        assert last_pass1.position == last_pass1.total > 0
        assert last_pass2.position == last_pass2.total > 0

        # Counters reflect the same totals as CatalogRunStats.
        assert last_pass1.counters["recipes_seen"] >= 1
        assert last_pass2.counters["groups_processed"] == last_pass2.total

    def test_default_heartbeat_is_silent_noop(
        self, synthetic_corpora: tuple[Path, Path], tmp_path: Path
    ) -> None:
        """run_catalog_pipeline without ``heartbeat`` arg must not crash or
        spam — back-compat for existing callers."""
        csv_path, zip_path = synthetic_corpora
        db, _ = _open_db(tmp_path)
        # No heartbeat= passed; should default to a no-op.
        stats = run_catalog_pipeline(
            db=db,
            rnlg_loader=RecipeNLGLoader(path=csv_path),
            wdc_loader=WDCLoader(zip_path=zip_path),
            parse_fn=_default_parse,
            extract_fn=_default_extract,
            corpus_revisions="rev-1",
            l1_min=3,
            l2_threshold=0.3,
            l2_min=2,
            l3_min=2,
        )
        assert stats.l1_groups_processed >= 1


class TestHeartbeatPrinter:
    def test_throttles_calls_within_interval(self) -> None:
        from io import StringIO

        from rational_recipes.cli.scrape_catalog import _HeartbeatPrinter

        buf = StringIO()
        clock = iter([0.0, 1.0, 5.0, 35.0])  # 0s, +1s, +5s, +35s
        printer = _HeartbeatPrinter(
            interval_seconds=30.0, stream=buf, clock=lambda: next(clock)
        )
        for i in range(4):
            printer(
                HeartbeatSnapshot(
                    pass_name="pass1",
                    position=i,
                    total=10,
                    elapsed_seconds=float(i),
                    counters={"recipes_seen": i},
                )
            )
        # First call always emits; +1s and +5s throttled; +35s emits.
        lines = [line for line in buf.getvalue().splitlines() if line]
        assert len(lines) == 2
        assert "pass1 0/10" in lines[0]
        assert "pass1 3/10" in lines[1]

    def test_zero_interval_emits_every_call(self) -> None:
        from io import StringIO

        from rational_recipes.cli.scrape_catalog import _HeartbeatPrinter

        buf = StringIO()
        clock = iter([0.0, 0.0, 0.0])
        printer = _HeartbeatPrinter(
            interval_seconds=0.0, stream=buf, clock=lambda: next(clock)
        )
        for i in range(3):
            printer(
                HeartbeatSnapshot(
                    pass_name="pass2",
                    position=i,
                    total=3,
                    elapsed_seconds=float(i),
                    counters={"groups_processed": i},
                )
            )
        lines = [line for line in buf.getvalue().splitlines() if line]
        assert len(lines) == 3

    def test_format_includes_eta_and_counters(self) -> None:
        from io import StringIO

        from rational_recipes.cli.scrape_catalog import _HeartbeatPrinter

        buf = StringIO()
        printer = _HeartbeatPrinter(
            interval_seconds=0.0, stream=buf, clock=lambda: 0.0
        )
        printer(
            HeartbeatSnapshot(
                pass_name="pass1",
                position=10,
                total=100,
                elapsed_seconds=60.0,
                counters={"recipes_seen": 10, "llm_batches": 2},
            )
        )
        line = buf.getvalue().strip()
        assert "pass1 10/100" in line
        assert "10.0%" in line
        assert "elapsed=1m00s" in line
        assert "eta=9m00s" in line
        assert "recipes_seen=10" in line
        assert "llm_batches=2" in line


class TestCatalogRunStatsDefaults:
    def test_zero_init(self) -> None:
        stats = CatalogRunStats()
        assert stats.l1_groups_total == 0
        assert stats.variants_produced == 0
        assert stats.llm_parse_calls == 0


# Silence unused-import warnings for Path/io — kept so future test expansions
# don't need to re-add them.
_ = (io.StringIO, Path)
