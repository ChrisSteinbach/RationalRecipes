"""Tests for the Pass 3 profiler CLI (vwt.29)."""

from __future__ import annotations

import json
from pathlib import Path

from rational_recipes.catalog_db import CatalogDB
from rational_recipes.cli.profile_pass3 import main
from rational_recipes.scrape.merge import MergedRecipe
from rational_recipes.scrape.pipeline_merged import (
    MergedNormalizedRow,
    MergedVariantResult,
)


def _make_variant(
    db: CatalogDB,
    *,
    l1_title: str,
    canonical_ingredients: frozenset[str],
) -> str:
    row = MergedNormalizedRow(
        url=f"https://x.example/{'_'.join(sorted(canonical_ingredients))}",
        title=l1_title,
        corpus="recipenlg",
        cells={n: "100 g" for n in canonical_ingredients},
        proportions={
            n: 100.0 / len(canonical_ingredients) for n in canonical_ingredients
        },
    )
    variant = MergedVariantResult(
        variant_title=l1_title,
        canonical_ingredients=canonical_ingredients,
        cooking_methods=frozenset(),
        normalized_rows=[row],
        header_ingredients=sorted(canonical_ingredients),
    )
    db.upsert_variant(variant, l1_key=l1_title)
    return variant.variant_id


def test_profile_pass3_runs_against_db_and_writes_jsonl(
    tmp_path: Path, monkeypatch, capsys  # type: ignore[no-untyped-def]
) -> None:
    """End-to-end: seed a DB, monkey-patch urlopen to a deterministic
    Ollama stub, run the profiler, assert JSONL + summary land on disk."""
    db_path = tmp_path / "recipes.db"
    db = CatalogDB.open(db_path)
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
    db.close()

    body = (
        b'{"response": "{\\"title\\": \\"Stub Pecan Pie\\"}", '
        b'"total_duration": 200000000, '
        b'"prompt_eval_count": 50, '
        b'"prompt_eval_duration": 100000000, '
        b'"eval_count": 7, '
        b'"eval_duration": 50000000}'
    )

    class _FakeResponse:
        status = 200

        def __enter__(self) -> _FakeResponse:
            return self

        def __exit__(self, *_: object) -> None:
            return None

        def read(self) -> bytes:
            return body

    def fake_urlopen(req, timeout):  # type: ignore[no-untyped-def]
        return _FakeResponse()

    monkeypatch.setattr(
        "rational_recipes.scrape.pass3_titles.urllib.request.urlopen",
        fake_urlopen,
    )

    out = tmp_path / "profile.jsonl"
    rc = main(
        [
            "--db",
            str(db_path),
            "--ollama-url",
            "http://stub.invalid:1",
            "--pass3-workers",
            "1",
            "--output",
            str(out),
            "--skip-preflight",
        ]
    )
    assert rc == 0

    # JSONL: one line per LLM call (one per-variant call each).
    lines = out.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2
    rec = json.loads(lines[0])
    assert rec["family"] == "pecan pie"
    assert rec["sibling_count"] == 1  # one sibling per call
    assert rec["success"] is True
    assert rec["ollama_total_seconds"] == 0.2
    assert rec["ollama_prompt_eval_count"] == 50

    # Summary file is written next to JSONL.
    summary_path = out.with_suffix(out.suffix + ".summary.json")
    assert summary_path.exists()
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["count"] == 2
    assert summary["successes"] == 2
    assert summary["wallclock_seconds"] >= 0
    assert summary["workers"] == 1

    # CLI prints the timing summary lines.
    captured = capsys.readouterr()
    assert "pass 3 timing" in captured.out
    assert "pass 3 ollama" in captured.out
    assert "pass 3 by siblings" in captured.out


def test_profile_pass3_missing_db(tmp_path: Path, capsys) -> None:  # type: ignore[no-untyped-def]
    """Missing DB → exit 1 with a useful message."""
    rc = main(
        [
            "--db",
            str(tmp_path / "nope.db"),
            "--skip-preflight",
            "--ollama-url",
            "http://stub.invalid:1",
        ]
    )
    assert rc == 1
    err = capsys.readouterr().err
    assert "recipes.db not found" in err


# Suppress unused-import lint guard for the indirectly-relied-on type.
_ = MergedRecipe
