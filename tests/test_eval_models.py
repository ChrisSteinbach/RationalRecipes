"""Tests for ``scripts/eval_models.py`` (RationalRecipes-2n09)."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

# scripts/ is on pythonpath via pyproject.toml's pytest config.
import eval_models  # noqa: E402
import pytest

from rational_recipes.scrape.parse import ParsedIngredient


@pytest.fixture
def fake_parsed() -> ParsedIngredient:
    return ParsedIngredient(
        quantity=1.0,
        unit="cup",
        ingredient="flour",
        preparation="",
        raw="1 cup flour",
    )


class TestRunParseEval:
    def test_runs_each_model_x_line_and_records_attempts(
        self, fake_parsed: ParsedIngredient
    ) -> None:
        sample = (
            ("brand", "1 c. Crisco"),
            ("common", "1 cup flour"),
        )

        def fake_parse(line: str, **_: object) -> ParsedIngredient:
            return ParsedIngredient(
                quantity=1.0,
                unit="cup",
                ingredient="x",
                preparation="",
                raw=line,
            )

        with (
            patch.object(
                eval_models,
                "list_available_models",
                return_value={"a", "b"},
            ),
            patch.object(eval_models, "parse_ingredient_line", side_effect=fake_parse),
        ):
            attempts, skipped = eval_models.run_parse_eval(
                ["a", "b"],
                sample,
                base_url="http://localhost:0",
                timeout=1.0,
            )
        assert skipped == []
        # 2 models x 2 lines = 4 attempts
        assert len(attempts) == 4
        models = sorted({a.model for a in attempts})
        assert models == ["a", "b"]
        # Each attempt carries the category from its sample tuple
        for a in attempts:
            assert a.category in ("brand", "common")
            assert a.parsed is not None

    def test_skips_models_not_loaded_on_server(
        self, fake_parsed: ParsedIngredient
    ) -> None:
        sample = (("common", "1 cup flour"),)
        with (
            patch.object(
                eval_models,
                "list_available_models",
                return_value={"present"},
            ),
            patch.object(
                eval_models,
                "parse_ingredient_line",
                return_value=fake_parsed,
            ),
        ):
            attempts, skipped = eval_models.run_parse_eval(
                ["present", "missing"],
                sample,
                base_url="http://localhost:0",
                timeout=1.0,
            )
        assert skipped == ["missing"]
        assert {a.model for a in attempts} == {"present"}

    def test_records_failure_when_parser_returns_none(self) -> None:
        sample = (("common", "garbage line"),)
        with (
            patch.object(
                eval_models,
                "list_available_models",
                return_value={"m"},
            ),
            patch.object(
                eval_models, "parse_ingredient_line", return_value=None
            ),
        ):
            attempts, _ = eval_models.run_parse_eval(
                ["m"],
                sample,
                base_url="http://localhost:0",
                timeout=1.0,
            )
        assert len(attempts) == 1
        assert attempts[0].parsed is None
        assert attempts[0].error is None

    def test_records_error_when_parser_raises(self) -> None:
        sample = (("common", "garbage line"),)

        def boom(*_: object, **__: object) -> ParsedIngredient:
            raise RuntimeError("boom")

        with (
            patch.object(
                eval_models,
                "list_available_models",
                return_value={"m"},
            ),
            patch.object(eval_models, "parse_ingredient_line", side_effect=boom),
        ):
            attempts, _ = eval_models.run_parse_eval(
                ["m"],
                sample,
                base_url="http://localhost:0",
                timeout=1.0,
            )
        assert len(attempts) == 1
        assert attempts[0].parsed is None
        assert attempts[0].error is not None
        assert "boom" in attempts[0].error


class TestSummarize:
    def test_per_model_summary_counts_failures_per_category(self) -> None:
        ok = ParsedIngredient(
            quantity=1.0, unit="cup", ingredient="x", preparation="", raw="r"
        )
        attempts = [
            eval_models.ParseAttempt("brand", "x1", "m1", ok, 1.0),
            eval_models.ParseAttempt("brand", "x2", "m1", None, 2.0),
            eval_models.ParseAttempt("unit", "x3", "m1", None, 1.5),
            eval_models.ParseAttempt("unit", "x3", "m2", ok, 0.5),
        ]
        summary = eval_models._summarize_per_model(attempts)
        assert summary["m1"]["n"] == 3
        assert summary["m1"]["failed"] == 2
        assert summary["m1"]["failed_by_category"] == {"brand": 1, "unit": 1}
        assert summary["m2"]["failed"] == 0


class TestRenderMarkdown:
    def test_renders_summary_and_per_line_blocks(self) -> None:
        ok = ParsedIngredient(
            quantity=1.0, unit="cup", ingredient="flour", preparation="", raw="r"
        )
        attempts = [
            eval_models.ParseAttempt(
                "brand", "1 c. Crisco", "model-a", ok, 1.2
            ),
            eval_models.ParseAttempt(
                "brand", "1 c. Crisco", "model-b", None, 0.7
            ),
        ]
        md = eval_models.render_markdown(
            attempts, skipped=["model-c"], base_url="http://x:0"
        )
        # Header + key sections
        assert "RationalRecipes-2n09" in md
        assert "model-c" in md  # skipped flagged
        assert "1 c. Crisco" in md
        assert "_(parse failed)_" in md
        # Per-model summary table includes both models
        assert "`model-a`" in md
        assert "`model-b`" in md


class TestCheckReachable:
    def test_returns_false_on_connection_refused(self) -> None:
        # Port 1 reliably refuses on linux loopback.
        assert eval_models.check_ollama_reachable(
            "http://127.0.0.1:1", timeout=0.5
        ) is False


class TestUnloadModel:
    def test_unload_returns_false_on_connection_refused(self) -> None:
        assert (
            eval_models.unload_model(
                "any-model", "http://127.0.0.1:1", timeout=0.5
            )
            is False
        )


class TestEvictionBetweenModels:
    """run_parse_eval must evict the previous candidate before loading the next.

    On the project's single-GPU Ollama box the 24-35GB candidates can't
    co-reside, so a missing eviction trips ``HTTP 500 — resource
    limitations`` on the second model's first call.
    """

    def test_unload_called_between_models_not_before_first(self) -> None:
        sample = (("common", "1 cup flour"),)
        ok = ParsedIngredient(
            quantity=1.0, unit="cup", ingredient="flour", preparation="", raw="r"
        )
        unload_calls: list[str] = []

        def fake_unload(model: str, *_: object, **__: object) -> bool:
            unload_calls.append(model)
            return True

        with (
            patch.object(
                eval_models, "list_available_models", return_value={"a", "b", "c"}
            ),
            patch.object(eval_models, "parse_ingredient_line", return_value=ok),
            patch.object(eval_models, "unload_model", side_effect=fake_unload),
        ):
            eval_models.run_parse_eval(
                ["a", "b", "c"],
                sample,
                base_url="http://localhost:0",
                timeout=1.0,
            )
        # Unload happens before model b (evicting a) and before c (evicting b).
        # Never before a (nothing to unload yet).
        assert unload_calls == ["a", "b"]


class TestMainCli:
    def test_aborts_when_ollama_unreachable(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        out = tmp_path / "out.md"
        rc = eval_models.main(
            [
                "--models",
                "x",
                "--output",
                str(out),
                "--ollama-url",
                "http://127.0.0.1:1",
                "--timeout",
                "0.5",
            ]
        )
        assert rc == 2
        assert "unreachable" in capsys.readouterr().err.lower()


# Ensure scripts/ is importable when this test runs in isolation. The
# pyproject pytest config adds it to pythonpath, but be defensive in case
# someone runs the file directly.
def _ensure_scripts_on_path() -> None:
    scripts = Path(__file__).resolve().parent.parent / "scripts"
    if str(scripts) not in sys.path:
        sys.path.insert(0, str(scripts))


_ensure_scripts_on_path()
