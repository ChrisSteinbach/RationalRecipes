"""Tests for batched ingredient-line parsing (vwt.13).

Tests that exercise the LLM round-trip pin ``use_regex_prefilter=False``
so the vwt.17 regex shortcut doesn't intercept the canned prompts before
they hit the patched Ollama.
"""

from unittest.mock import patch

from rational_recipes.scrape.parse import (
    ParsedIngredient,
    parse_ingredient_lines,
)


def _llm_parse(*args, **kwargs):  # type: ignore[no-untyped-def]
    """Wrap parse_ingredient_lines with use_regex_prefilter=False."""
    kwargs.setdefault("use_regex_prefilter", False)
    return parse_ingredient_lines(*args, **kwargs)


class TestBatchedParseHappyPath:
    def test_empty_input_returns_empty(self) -> None:
        assert parse_ingredient_lines([]) == []

    def test_single_line_uses_per_line_path(self) -> None:
        """Length-1 input bypasses batching — no array-tracking risk."""
        with patch("rational_recipes.scrape.parse._ollama_generate") as mock_gen:
            mock_gen.return_value = (
                '{"quantity": 1.0, "unit": "cup",'
                ' "ingredient": "flour", "preparation": ""}'
            )
            results = _llm_parse(["1 cup flour"])
            assert mock_gen.call_count == 1
            assert results == [
                ParsedIngredient(
                    quantity=1.0,
                    unit="cup",
                    ingredient="flour",
                    preparation="",
                    raw="1 cup flour",
                )
            ]

    def test_multi_line_uses_one_batched_call(self) -> None:
        """Three lines → one LLM call returning a results array."""
        with patch("rational_recipes.scrape.parse._ollama_generate") as mock_gen:
            mock_gen.return_value = (
                '{"results": ['
                '{"quantity": 1.0, "unit": "cup",'
                ' "ingredient": "flour", "preparation": ""},'
                '{"quantity": 2.0, "unit": "MEDIUM",'
                ' "ingredient": "egg", "preparation": ""},'
                '{"quantity": 0.5, "unit": "tsp",'
                ' "ingredient": "salt", "preparation": ""}'
                "]}"
            )
            results = _llm_parse(
                ["1 cup flour", "2 eggs", "1/2 tsp salt"]
            )
            assert mock_gen.call_count == 1
            assert len(results) == 3
            assert results[0] is not None and results[0].ingredient == "flour"
            assert results[1] is not None and results[1].ingredient == "egg"
            assert results[2] is not None and results[2].ingredient == "salt"

    def test_raw_line_preserved_per_position(self) -> None:
        """Each ParsedIngredient's `raw` is the input line at that index."""
        with patch("rational_recipes.scrape.parse._ollama_generate") as mock_gen:
            mock_gen.return_value = (
                '{"results": ['
                '{"quantity": 1.0, "unit": "cup",'
                ' "ingredient": "flour", "preparation": ""},'
                '{"quantity": 2.0, "unit": "MEDIUM",'
                ' "ingredient": "egg", "preparation": ""}'
                "]}"
            )
            results = _llm_parse(["LINE_A", "LINE_B"])
            assert results[0] is not None and results[0].raw == "LINE_A"
            assert results[1] is not None and results[1].raw == "LINE_B"


class TestBatchedParseFallback:
    def test_length_mismatch_falls_back_to_per_line(self) -> None:
        """Batched response shorter than input → per-line retry, length preserved."""
        responses = iter(
            [
                # Batched: only 1 result for 2 inputs → invalid
                '{"results": [{"quantity": 1.0, "unit": "cup",'
                ' "ingredient": "flour", "preparation": ""}]}',
                # Per-line fallback for line 1
                '{"quantity": 1.0, "unit": "cup",'
                ' "ingredient": "flour", "preparation": ""}',
                # Per-line fallback for line 2
                '{"quantity": 2.0, "unit": "MEDIUM",'
                ' "ingredient": "egg", "preparation": ""}',
            ]
        )
        with patch(
            "rational_recipes.scrape.parse._ollama_generate",
            side_effect=lambda *a, **kw: next(responses),
        ) as mock_gen:
            results = _llm_parse(["1 cup flour", "2 eggs"])
            # 1 batched call + 2 per-line calls = 3
            assert mock_gen.call_count == 3
            assert len(results) == 2
            assert results[0] is not None and results[0].ingredient == "flour"
            assert results[1] is not None and results[1].ingredient == "egg"

    def test_missing_results_key_falls_back(self) -> None:
        """Batched response without 'results' key → per-line retry."""
        responses = iter(
            [
                # Batched: model returned a single object, no wrapper
                '{"quantity": 1.0, "unit": "cup",'
                ' "ingredient": "flour", "preparation": ""}',
                # Per-line success for both
                '{"quantity": 1.0, "unit": "cup",'
                ' "ingredient": "flour", "preparation": ""}',
                '{"quantity": 2.0, "unit": "MEDIUM",'
                ' "ingredient": "egg", "preparation": ""}',
            ]
        )
        with patch(
            "rational_recipes.scrape.parse._ollama_generate",
            side_effect=lambda *a, **kw: next(responses),
        ):
            results = _llm_parse(["1 cup flour", "2 eggs"])
            assert len(results) == 2
            assert all(r is not None for r in results)

    def test_malformed_json_falls_back(self) -> None:
        responses = iter(
            [
                "not json at all",
                '{"quantity": 1.0, "unit": "cup",'
                ' "ingredient": "flour", "preparation": ""}',
                None,  # second per-line call returns None
            ]
        )
        with patch(
            "rational_recipes.scrape.parse._ollama_generate",
            side_effect=lambda *a, **kw: next(responses),
        ):
            results = _llm_parse(["1 cup flour", "2 eggs"])
            assert len(results) == 2
            assert results[0] is not None
            assert results[1] is None  # parse failure preserved

    def test_ollama_returns_none_falls_back(self) -> None:
        """Ollama unreachable → batched returns None → per-line tried."""
        responses = iter(
            [
                None,  # batched call: connection failure
                '{"quantity": 1.0, "unit": "cup",'
                ' "ingredient": "flour", "preparation": ""}',
                '{"quantity": 2.0, "unit": "MEDIUM",'
                ' "ingredient": "egg", "preparation": ""}',
            ]
        )
        with patch(
            "rational_recipes.scrape.parse._ollama_generate",
            side_effect=lambda *a, **kw: next(responses),
        ):
            results = _llm_parse(["1 cup flour", "2 eggs"])
            assert len(results) == 2
            assert all(r is not None for r in results)


class TestBatchedParseBisection:
    def test_failed_batch_bisects_instead_of_per_line(self) -> None:
        """4-line failure → 1 batched + 2 half-batches = 3 calls, not 1 + 4."""
        responses = iter(
            [
                # First call: 4 lines → return only 3 results → length mismatch
                '{"results": ['
                '{"quantity": 1.0, "unit": "cup",'
                ' "ingredient": "x", "preparation": ""},'
                '{"quantity": 1.0, "unit": "cup",'
                ' "ingredient": "x", "preparation": ""},'
                '{"quantity": 1.0, "unit": "cup",'
                ' "ingredient": "x", "preparation": ""}'
                "]}",
                # Left half (2 lines): succeeds
                '{"results": ['
                '{"quantity": 1.0, "unit": "cup",'
                ' "ingredient": "flour", "preparation": ""},'
                '{"quantity": 2.0, "unit": "MEDIUM",'
                ' "ingredient": "egg", "preparation": ""}'
                "]}",
                # Right half (2 lines): succeeds
                '{"results": ['
                '{"quantity": 0.5, "unit": "tsp",'
                ' "ingredient": "salt", "preparation": ""},'
                '{"quantity": 1.0, "unit": "cup",'
                ' "ingredient": "milk", "preparation": ""}'
                "]}",
            ]
        )
        with patch(
            "rational_recipes.scrape.parse._ollama_generate",
            side_effect=lambda *a, **kw: next(responses),
        ) as mock_gen:
            results = _llm_parse(
                ["1 cup flour", "2 eggs", "1/2 tsp salt", "1 cup milk"]
            )
            # Bisection: 1 failed batched + 2 half-batched = 3 calls.
            # Old per-line fallback would have been 1 + 4 = 5.
            assert mock_gen.call_count == 3
            assert len(results) == 4
            assert results[0] is not None and results[0].ingredient == "flour"
            assert results[1] is not None and results[1].ingredient == "egg"
            assert results[2] is not None and results[2].ingredient == "salt"
            assert results[3] is not None and results[3].ingredient == "milk"

    def test_singleton_in_bisection_falls_back_to_per_line(self) -> None:
        """When a half bisects down to a singleton, that leaf goes per-line."""
        responses = iter(
            [
                # Outer 2-line batch: length mismatch (1 result for 2 inputs)
                '{"results": [{"quantity": 1.0, "unit": "cup",'
                ' "ingredient": "x", "preparation": ""}]}',
                # First singleton: per-line succeeds
                '{"quantity": 1.0, "unit": "cup",'
                ' "ingredient": "flour", "preparation": ""}',
                # Second singleton: per-line returns None (genuinely unparseable)
                None,
            ]
        )
        with patch(
            "rational_recipes.scrape.parse._ollama_generate",
            side_effect=lambda *a, **kw: next(responses),
        ) as mock_gen:
            results = _llm_parse(["1 cup flour", "garbage line"])
            # 1 batched + 2 per-line (not batched-then-per-line) = 3 calls
            assert mock_gen.call_count == 3
            assert len(results) == 2
            assert results[0] is not None and results[0].ingredient == "flour"
            assert results[1] is None  # singleton parse failure preserved


class TestBatchedParseChunking:
    def test_batches_above_max_size_get_split(self) -> None:
        """31-line input → 2 batched calls (30 + 1)."""
        # Build a response shape for the chunk size we expect on each call.
        def fake_generate(*args: object, **kwargs: object) -> str:
            # Inspect prompt to learn how many lines this call contains.
            prompt = args[0] if args else kwargs.get("prompt", "")
            assert isinstance(prompt, str)
            # The prompt embeds a JSON list of input strings; count "line "
            # tokens we put there.
            n = prompt.count('"line ')
            results = ",".join(
                '{"quantity": 1.0, "unit": "cup",'
                ' "ingredient": "flour", "preparation": ""}'
                for _ in range(n)
            )
            return f'{{"results": [{results}]}}'

        with patch(
            "rational_recipes.scrape.parse._ollama_generate",
            side_effect=fake_generate,
        ) as mock_gen:
            lines = [f"line {i}" for i in range(31)]
            results = _llm_parse(lines)
            # 30 + 1 = 2 batched calls
            assert mock_gen.call_count == 2
            assert len(results) == 31

    def test_num_predict_scales_with_batch_size(self) -> None:
        """Bigger batches must request more output tokens to avoid truncation."""
        captured: list[int] = []

        def fake_generate(*args: object, **kwargs: object) -> str:
            captured.append(int(kwargs["num_predict"]))
            n = (args[0] if args else kwargs.get("prompt", "")).count('"line ')
            results = ",".join(
                '{"quantity": 1.0, "unit": "cup",'
                ' "ingredient": "x", "preparation": ""}'
                for _ in range(n)
            )
            return f'{{"results": [{results}]}}'

        with patch(
            "rational_recipes.scrape.parse._ollama_generate",
            side_effect=fake_generate,
        ):
            _llm_parse(
                [f"line {i}" for i in range(10)], num_predict=64
            )
            # User asked for 64; 10 lines × 80 + 50 = 850 floor.
            assert captured == [850]
