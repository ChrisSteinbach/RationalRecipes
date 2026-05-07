"""Tests for ``scripts/synthesize_instructions.py`` (RationalRecipes-ia1x)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# scripts/ is on pythonpath via pyproject.toml's pytest config.
import synthesize_instructions  # noqa: E402

from rational_recipes.catalog_db import CatalogDB
from rational_recipes.scrape.pipeline_merged import (
    MergedNormalizedRow,
    MergedVariantResult,
)


def _seed_variant(db: CatalogDB) -> str:
    """Build a tiny variant in ``db`` and return its variant_id."""
    rows = [
        MergedNormalizedRow(
            url=f"https://example.com/r/{i}",
            title="pannkakor",
            corpus="recipenlg",
            cells={"flour": "100 g", "milk": "200 ml"},
            proportions={"flour": 33.0 + i, "milk": 67.0 - i},
        )
        for i in range(3)
    ]
    variant = MergedVariantResult(
        variant_title="pannkakor",
        canonical_ingredients=frozenset({"flour", "milk"}),
        cooking_methods=frozenset(),
        normalized_rows=rows,
        header_ingredients=["flour", "milk"],
    )
    db.upsert_variant(variant, l1_key="pannkakor", base_ingredient="flour")
    return variant.variant_id


def _write_recipenlg_csv(path: Path, rows: list[dict[str, str]]) -> None:
    import csv

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=("title", "ingredients", "directions", "link", "source", "NER"),
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


class TestBuildSynthesisPrompt:
    def test_prompt_carries_variant_and_ingredient_signal(
        self, tmp_path: Path
    ) -> None:
        db_path = tmp_path / "recipes.db"
        db = CatalogDB.open(db_path)
        try:
            vid = _seed_variant(db)
            variant = db.get_variant(vid)
            assert variant is not None
            stats = db.get_ingredient_stats(vid)
            members = db.get_variant_members(vid)
            sources = synthesize_instructions.collect_source_instructions(
                members,
                recipenlg_path=None,
                max_sources=10,
            )
            prompt = synthesize_instructions.build_synthesis_prompt(
                variant, stats, sources
            )
        finally:
            db.close()

        # Variant title + cluster size show up.
        assert "pannkakor" in prompt
        assert "3" in prompt  # n_recipes / cluster size
        # Ingredient profile is in the prompt.
        assert "flour" in prompt
        assert "milk" in prompt
        # Per-source block is present even with no recovered steps,
        # so the LLM sees how many sources fed the average.
        assert "Source 1" in prompt
        assert "Source 3" in prompt

    def test_prompt_includes_recovered_recipenlg_steps(
        self, tmp_path: Path
    ) -> None:
        db_path = tmp_path / "recipes.db"
        db = CatalogDB.open(db_path)
        try:
            vid = _seed_variant(db)
            members = db.get_variant_members(vid)
            recipenlg = tmp_path / "rnlg.csv"
            _write_recipenlg_csv(
                recipenlg,
                [
                    {
                        "title": "pannkakor",
                        "ingredients": "['100 g flour', '200 ml milk']",
                        "directions": (
                            "['Whisk flour and milk.', 'Cook on hot griddle.']"
                        ),
                        "link": m.url or "",
                        "source": "Recipes1M",
                        "NER": "['flour', 'milk']",
                    }
                    for m in members
                ],
            )
            sources = synthesize_instructions.collect_source_instructions(
                members,
                recipenlg_path=recipenlg,
                max_sources=10,
            )
        finally:
            db.close()

        # All three sources got their directions back.
        assert all(len(s.steps) == 2 for s in sources)
        assert "Whisk flour and milk." in sources[0].steps

    def test_max_sources_caps_the_prompt(self, tmp_path: Path) -> None:
        db_path = tmp_path / "recipes.db"
        db = CatalogDB.open(db_path)
        try:
            vid = _seed_variant(db)
            members = db.get_variant_members(vid)
            sources = synthesize_instructions.collect_source_instructions(
                members,
                recipenlg_path=None,
                max_sources=2,
            )
        finally:
            db.close()
        assert len(sources) == 2


class TestSynthesizeOrchestration:
    def test_dry_run_returns_prompt_no_save(self, tmp_path: Path) -> None:
        db_path = tmp_path / "recipes.db"
        db = CatalogDB.open(db_path)
        try:
            vid = _seed_variant(db)
        finally:
            db.close()

        out = synthesize_instructions.synthesize(
            vid,
            db_path=db_path,
            recipenlg_path=None,
            max_sources=10,
            dry_run=True,
            save=True,  # save flag is ignored under dry-run
        )
        assert "pannkakor" in out
        assert "Task: produce a single canonical instruction set" in out

        # Dry-run never persists, even with --save.
        db = CatalogDB.open(db_path)
        try:
            v = db.get_variant(vid)
            assert v is not None
            assert v.canonical_instructions is None
            assert v.canonical_instructions_reviewed_at is None
        finally:
            db.close()

    def test_non_dry_run_raises_when_ollama_unreachable(
        self, tmp_path: Path
    ) -> None:
        db_path = tmp_path / "recipes.db"
        db = CatalogDB.open(db_path)
        try:
            vid = _seed_variant(db)
        finally:
            db.close()

        # Localhost on a port nothing listens on — the urlopen call
        # fails fast and the wrapper surfaces it as SynthesisError.
        with pytest.raises(synthesize_instructions.SynthesisError):
            synthesize_instructions.synthesize(
                vid,
                db_path=db_path,
                recipenlg_path=None,
                max_sources=10,
                dry_run=False,
                save=False,
                model="gemma4:e2b",
                base_url="http://127.0.0.1:1",
            )

    def test_non_dry_run_without_model_exits(self, tmp_path: Path) -> None:
        db_path = tmp_path / "recipes.db"
        db = CatalogDB.open(db_path)
        try:
            vid = _seed_variant(db)
        finally:
            db.close()

        with pytest.raises(SystemExit, match="model"):
            synthesize_instructions.synthesize(
                vid,
                db_path=db_path,
                recipenlg_path=None,
                max_sources=10,
                dry_run=False,
                save=False,
                model=None,
            )

    def test_unknown_variant_id_exits(self, tmp_path: Path) -> None:
        db_path = tmp_path / "recipes.db"
        db = CatalogDB.open(db_path)
        try:
            _seed_variant(db)
        finally:
            db.close()

        with pytest.raises(SystemExit, match="not found"):
            synthesize_instructions.synthesize(
                "nope",
                db_path=db_path,
                recipenlg_path=None,
                max_sources=10,
                dry_run=True,
                save=False,
            )


class TestDeterminismConstants:
    """Pin deterministic settings so the eventual Ollama call carries them."""

    def test_temperature_is_zero(self) -> None:
        assert synthesize_instructions.SYNTHESIS_TEMPERATURE == 0.0

    def test_seed_is_42(self) -> None:
        assert synthesize_instructions.SYNTHESIS_SEED == 42


class TestNumCtxForwarding:
    """``num_ctx`` must reach the Ollama options block when set.

    The synth-deep endpoint (NP=1) is provisioned to allow up to 32 k
    context for the recommended candidates. Passing ``num_ctx=32768``
    is how the eval driver opts into that ceiling. Capture the request
    body and assert the option lands.
    """

    def test_llm_synthesize_includes_num_ctx_when_set(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured: dict[str, object] = {}

        def fake_urlopen(req, timeout: float = 0):  # type: ignore[no-untyped-def]
            import json as _json

            captured["body"] = _json.loads(req.data.decode())

            class _Resp:
                def __enter__(self_inner):  # type: ignore[no-untyped-def]
                    return self_inner

                def __exit__(self_inner, *_a):  # type: ignore[no-untyped-def]
                    return False

                def read(self_inner) -> bytes:  # type: ignore[no-untyped-def]
                    return _json.dumps({"response": "ok"}).encode()

            return _Resp()

        import urllib.request as _ur

        monkeypatch.setattr(_ur, "urlopen", fake_urlopen)
        out = synthesize_instructions._llm_synthesize(
            prompt="x",
            model="mistral-small:24b",
            base_url="http://x:1",
            timeout=1.0,
            num_ctx=32768,
        )
        assert out == "ok"
        body = captured["body"]
        assert isinstance(body, dict)
        opts = body.get("options")
        assert isinstance(opts, dict)
        assert opts.get("num_ctx") == 32768

    def test_llm_synthesize_omits_num_ctx_when_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured: dict[str, object] = {}

        def fake_urlopen(req, timeout: float = 0):  # type: ignore[no-untyped-def]
            import json as _json

            captured["body"] = _json.loads(req.data.decode())

            class _Resp:
                def __enter__(self_inner):  # type: ignore[no-untyped-def]
                    return self_inner

                def __exit__(self_inner, *_a):  # type: ignore[no-untyped-def]
                    return False

                def read(self_inner) -> bytes:  # type: ignore[no-untyped-def]
                    return _json.dumps({"response": "ok"}).encode()

            return _Resp()

        import urllib.request as _ur

        monkeypatch.setattr(_ur, "urlopen", fake_urlopen)
        synthesize_instructions._llm_synthesize(
            prompt="x", model="m", base_url="http://x:1", timeout=1.0
        )
        body = captured["body"]
        assert isinstance(body, dict)
        opts = body.get("options")
        assert isinstance(opts, dict)
        assert "num_ctx" not in opts


class TestCli:
    def test_dry_run_main_writes_prompt_to_stdout(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        db_path = tmp_path / "recipes.db"
        db = CatalogDB.open(db_path)
        try:
            vid = _seed_variant(db)
        finally:
            db.close()

        rc = synthesize_instructions.main(
            [vid, "--db", str(db_path), "--dry-run"]
        )
        assert rc == 0
        out = capsys.readouterr().out
        assert "pannkakor" in out
        assert out.endswith("\n")

    def test_missing_db_exits_with_error(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        rc = synthesize_instructions.main(
            ["x", "--db", str(tmp_path / "missing.db"), "--dry-run"]
        )
        assert rc == 1
        assert "not found" in capsys.readouterr().err


# Ensure scripts/ is importable when this test runs in isolation. The
# pyproject pytest config adds it to pythonpath, but be defensive in case
# someone runs the file directly.
def _ensure_scripts_on_path() -> None:
    scripts = Path(__file__).resolve().parent.parent / "scripts"
    if str(scripts) not in sys.path:
        sys.path.insert(0, str(scripts))


_ensure_scripts_on_path()
