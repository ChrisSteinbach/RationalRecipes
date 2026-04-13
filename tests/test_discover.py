"""Tests for the rr-discover title-based dish discovery."""

from __future__ import annotations

import json
import subprocess
import sys
import textwrap
from pathlib import Path

from rational_recipes.discover import DiscoveryResult, count_titles, discover


class TestCountTitles:
    def test_counts_normalized_duplicates(self) -> None:
        counter = count_titles(
            [
                "Swedish Pancakes",
                "swedish pancakes",
                "Swedish Pancakes Recipe",
                "Chocolate Cake",
            ]
        )
        assert counter["swedish pancakes"] == 3
        assert counter["chocolate cake"] == 1

    def test_skips_empty_normalizations(self) -> None:
        counter = count_titles(["", "   ", "(only parens)"])
        assert sum(counter.values()) == 0

    def test_handles_possessive_and_trailing_recipe(self) -> None:
        counter = count_titles(
            [
                "Grandma's Apple Pie",
                "grandma apple pie recipe",
            ]
        )
        assert counter["grandma apple pie"] == 2


class TestDiscover:
    def test_filters_by_min_count(self) -> None:
        titles = ["pancakes"] * 5 + ["crepes"] * 2
        results = discover(titles, min_count=3, top_k=10)
        assert results == [DiscoveryResult(count=5, normalized_title="pancakes")]

    def test_top_k_cuts_output(self) -> None:
        titles = ["a"] * 3 + ["b"] * 3 + ["c"] * 3
        results = discover(titles, min_count=1, top_k=2)
        assert len(results) == 2

    def test_sorted_by_count_descending(self) -> None:
        titles = ["small"] * 2 + ["big"] * 5 + ["medium"] * 3
        results = discover(titles, min_count=1, top_k=10)
        assert [r.normalized_title for r in results] == ["big", "medium", "small"]

    def test_ties_broken_alphabetically(self) -> None:
        titles = ["bravo"] * 2 + ["alpha"] * 2 + ["charlie"] * 2
        results = discover(titles, min_count=1, top_k=10)
        assert [r.normalized_title for r in results] == ["alpha", "bravo", "charlie"]

    def test_empty_input(self) -> None:
        assert discover([], min_count=1, top_k=10) == []

    def test_stream_input_is_consumed_once(self) -> None:
        titles = iter(["pancakes", "pancakes", "crepes"])
        results = discover(titles, min_count=1, top_k=10)
        assert len(results) == 2


class TestDiscoverCLI:
    """End-to-end tests for the rr-discover entry point."""

    def _make_fixture(self, tmp_path: Path) -> Path:
        csv_content = textwrap.dedent("""\
            ,title,ingredients,directions,link,source,NER
            0,Swedish Pancakes,"[]","[]",,Test,"[]"
            1,swedish pancakes recipe,"[]","[]",,Test,"[]"
            2,Swedish Pancakes,"[]","[]",,Test,"[]"
            3,Chocolate Cake,"[]","[]",,Test,"[]"
            4,chocolate cake,"[]","[]",,Test,"[]"
            5,Lonely Dish,"[]","[]",,Test,"[]"
        """)
        fixture = tmp_path / "fixture.csv"
        fixture.write_text(csv_content, encoding="utf-8")
        return fixture

    def _run(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, "-m", "rational_recipes.discover_cli", *args],
            capture_output=True,
            text=True,
            check=False,
        )

    def test_text_output(self, tmp_path: Path) -> None:
        fixture = self._make_fixture(tmp_path)
        result = self._run(
            "--corpus",
            str(fixture),
            "--min",
            "2",
            "--top",
            "10",
            "--format",
            "text",
        )
        assert result.returncode == 0, result.stderr
        lines = result.stdout.strip().splitlines()
        assert len(lines) == 2
        assert "swedish pancakes" in lines[0]
        assert lines[0].strip().startswith("3")
        assert "chocolate cake" in lines[1]
        assert lines[1].strip().startswith("2")

    def test_json_output(self, tmp_path: Path) -> None:
        fixture = self._make_fixture(tmp_path)
        result = self._run(
            "--corpus",
            str(fixture),
            "--min",
            "2",
            "--format",
            "json",
        )
        assert result.returncode == 0, result.stderr
        data = json.loads(result.stdout)
        assert data == [
            {"count": 3, "normalized_title": "swedish pancakes"},
            {"count": 2, "normalized_title": "chocolate cake"},
        ]

    def test_csv_output(self, tmp_path: Path) -> None:
        fixture = self._make_fixture(tmp_path)
        result = self._run(
            "--corpus",
            str(fixture),
            "--min",
            "2",
            "--format",
            "csv",
        )
        assert result.returncode == 0, result.stderr
        lines = result.stdout.strip().splitlines()
        assert lines[0] == "count,normalized_title"
        assert lines[1] == "3,swedish pancakes"
        assert lines[2] == "2,chocolate cake"

    def test_missing_corpus_exits_nonzero(self, tmp_path: Path) -> None:
        result = self._run("--corpus", str(tmp_path / "missing.csv"))
        assert result.returncode == 1
        assert "not found" in result.stderr.lower()
