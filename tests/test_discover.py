"""Tests for the rr-discover title-based dish discovery."""

from __future__ import annotations

import json
import subprocess
import sys
import textwrap
from pathlib import Path

from rational_recipes.discover import (
    DiscoveryResult,
    count_titles,
    discover,
    enrich_with_variants,
)
from rational_recipes.scrape.recipenlg import Recipe


def _recipe(title: str, ner: tuple[str, ...] = (), row_index: int = 0) -> Recipe:
    return Recipe(
        row_index=row_index,
        title=title,
        ingredients=(),
        ner=ner,
        source="test",
        link="",
    )


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


class TestEnrichWithVariants:
    def test_splits_title_group_into_two_variants(self) -> None:
        classic = ("flour", "milk", "egg", "butter", "salt")
        leavened = ("flour", "buttermilk", "baking powder", "egg", "sugar")
        recipes = [_recipe("Pancakes", ner=classic, row_index=i) for i in range(5)] + [
            _recipe("pancakes", ner=leavened, row_index=i) for i in range(5, 11)
        ]

        results = [DiscoveryResult(count=11, normalized_title="pancakes")]
        enriched = enrich_with_variants(results, recipes, min_variant_size=3)

        assert len(enriched) == 1
        r = enriched[0]
        assert r.count == 11
        assert r.normalized_title == "pancakes"
        assert len(r.variants) == 2
        assert sorted(v.size for v in r.variants) == [5, 6]
        assert r.other_count == 0
        # Ingredients are sorted alphabetically for stable output
        for v in r.variants:
            assert list(v.canonical_ingredients) == sorted(v.canonical_ingredients)

    def test_ignores_recipes_not_in_surviving_set(self) -> None:
        recipes = [
            _recipe("Pancakes", ner=("flour", "milk", "egg"), row_index=i)
            for i in range(4)
        ] + [
            _recipe("Chocolate Cake", ner=("flour", "cocoa", "sugar"), row_index=i)
            for i in range(4, 8)
        ]

        results = [DiscoveryResult(count=4, normalized_title="pancakes")]
        enriched = enrich_with_variants(results, recipes, min_variant_size=3)

        assert len(enriched) == 1
        entry = enriched[0]
        assert entry.normalized_title == "pancakes"
        assert sum(v.size for v in entry.variants) + entry.other_count == 4

    def test_other_count_captures_small_clusters(self) -> None:
        classic = ("flour", "milk", "egg", "butter", "salt")
        recipes = [_recipe("Pancakes", ner=classic, row_index=i) for i in range(3)] + [
            _recipe(
                "pancakes",
                ner=("chickpea", "water", "turmeric", "cumin", "onion"),
                row_index=99,
            )
        ]

        results = [DiscoveryResult(count=4, normalized_title="pancakes")]
        enriched = enrich_with_variants(results, recipes, min_variant_size=3)

        assert len(enriched[0].variants) == 1
        assert enriched[0].variants[0].size == 3
        assert enriched[0].other_count == 1

    def test_empty_bucket_when_no_recipes_match(self) -> None:
        results = [DiscoveryResult(count=5, normalized_title="pancakes")]
        enriched = enrich_with_variants(results, [], min_variant_size=3)

        assert len(enriched) == 1
        assert enriched[0].variants == ()
        assert enriched[0].other_count == 0

    def test_returns_empty_for_empty_results(self) -> None:
        recipes = [_recipe("Pancakes", ner=("flour", "milk", "egg"))]
        enriched = enrich_with_variants([], recipes)
        assert enriched == []

    def test_preserves_result_ordering(self) -> None:
        recipes = [
            _recipe("Pancakes", ner=("flour", "milk", "egg"), row_index=i)
            for i in range(3)
        ] + [
            _recipe("Chocolate Cake", ner=("flour", "cocoa", "sugar"), row_index=i)
            for i in range(3, 6)
        ]
        results = [
            DiscoveryResult(count=3, normalized_title="chocolate cake"),
            DiscoveryResult(count=3, normalized_title="pancakes"),
        ]
        enriched = enrich_with_variants(results, recipes, min_variant_size=3)
        assert [r.normalized_title for r in enriched] == [
            "chocolate cake",
            "pancakes",
        ]


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

    def _make_variant_fixture(self, tmp_path: Path) -> Path:
        # 4 classic pancakes + 4 leavened pancakes = two distinct variants
        # in the same normalized title group. min_variant_size=3 keeps both.
        classic_ner = '[""flour"", ""milk"", ""egg"", ""butter"", ""salt""]'
        leavened_ner = (
            '[""flour"", ""buttermilk"", ""baking powder"", ""egg"", ""sugar""]'
        )
        csv_content = textwrap.dedent(f"""\
            ,title,ingredients,directions,link,source,NER
            0,Pancakes,"[]","[]",,Test,"{classic_ner}"
            1,Pancakes,"[]","[]",,Test,"{classic_ner}"
            2,pancakes,"[]","[]",,Test,"{classic_ner}"
            3,Pancakes,"[]","[]",,Test,"{classic_ner}"
            4,Pancakes,"[]","[]",,Test,"{leavened_ner}"
            5,pancakes,"[]","[]",,Test,"{leavened_ner}"
            6,Pancakes,"[]","[]",,Test,"{leavened_ner}"
            7,Pancakes,"[]","[]",,Test,"{leavened_ner}"
        """)
        fixture = tmp_path / "variants.csv"
        fixture.write_text(csv_content, encoding="utf-8")
        return fixture

    def test_variants_json_output(self, tmp_path: Path) -> None:
        fixture = self._make_variant_fixture(tmp_path)
        result = self._run(
            "--corpus",
            str(fixture),
            "--min",
            "2",
            "--variants",
            "--format",
            "json",
        )
        assert result.returncode == 0, result.stderr
        data = json.loads(result.stdout)
        assert len(data) == 1
        entry = data[0]
        assert entry["normalized_title"] == "pancakes"
        assert entry["count"] == 8
        assert len(entry["variants"]) == 2
        assert sorted(v["size"] for v in entry["variants"]) == [4, 4]
        assert entry["other_count"] == 0
        # Each variant reports a non-empty, sorted ingredient list
        for v in entry["variants"]:
            assert v["canonical_ingredients"]
            assert v["canonical_ingredients"] == sorted(v["canonical_ingredients"])

    def test_variants_text_output_shows_breakdown(self, tmp_path: Path) -> None:
        fixture = self._make_variant_fixture(tmp_path)
        result = self._run(
            "--corpus",
            str(fixture),
            "--min",
            "2",
            "--variants",
            "--format",
            "text",
        )
        assert result.returncode == 0, result.stderr
        out = result.stdout
        assert "pancakes" in out
        assert "2 variants" in out
        # Canonical ingredients from one of the variants should appear
        assert "flour" in out and "milk" in out

    def test_variants_csv_output(self, tmp_path: Path) -> None:
        fixture = self._make_variant_fixture(tmp_path)
        result = self._run(
            "--corpus",
            str(fixture),
            "--min",
            "2",
            "--variants",
            "--format",
            "csv",
        )
        assert result.returncode == 0, result.stderr
        lines = result.stdout.strip().splitlines()
        assert lines[0] == (
            "count,normalized_title,variant_rank,variant_size,canonical_ingredients"
        )
        variant_rows = [line for line in lines[1:] if ",pancakes," in line]
        assert len(variant_rows) == 2  # no "other" row because other_count == 0
