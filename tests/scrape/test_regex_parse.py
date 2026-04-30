"""Tests for the regex-first ingredient parser (vwt.17).

The parser must:
  * Correctly parse the canonical "QTY [UNIT] NAME [, PREP]" shape.
  * Bias toward returning None on anything ambiguous so the caller
    falls through to the LLM (no silent misparses).
  * Stay deterministic (no hidden state between calls).
"""

from __future__ import annotations

import pytest

from rational_recipes.scrape.regex_parse import (
    _parse_atomic_quantity,
    parse_quantity,
    regex_parse_line,
)

# --- Quantity parser ---


class TestParseQuantity:
    def test_integer(self) -> None:
        result = parse_quantity("2")
        assert result is not None
        assert result.quantity == 2.0

    def test_decimal(self) -> None:
        result = parse_quantity("1.5")
        assert result is not None
        assert result.quantity == 1.5

    def test_simple_fraction(self) -> None:
        result = parse_quantity("1/2")
        assert result is not None
        assert result.quantity == 0.5

    def test_mixed_number(self) -> None:
        result = parse_quantity("1 1/2")
        assert result is not None
        assert result.quantity == 1.5

    def test_unicode_half(self) -> None:
        result = parse_quantity("½")
        assert result is not None
        assert result.quantity == 0.5

    def test_unicode_third(self) -> None:
        result = parse_quantity("⅓")
        assert result is not None
        assert result.quantity == pytest.approx(1.0 / 3.0)

    def test_unicode_fraction_with_leading_int(self) -> None:
        result = parse_quantity("1½")
        assert result is not None
        assert result.quantity == 1.5

    def test_range_uses_midpoint(self) -> None:
        result = parse_quantity("2-3")
        assert result is not None
        assert result.quantity == 2.5

    def test_range_with_spaces(self) -> None:
        result = parse_quantity("1 - 2")
        assert result is not None
        assert result.quantity == 1.5

    def test_range_with_mixed_numbers(self) -> None:
        result = parse_quantity("1 1/2 - 2 1/2")
        assert result is not None
        # midpoint of 1.5 and 2.5
        assert result.quantity == 2.0

    def test_zero_denominator_rejected(self) -> None:
        assert _parse_atomic_quantity("1/0") is None

    def test_garbage_rejected(self) -> None:
        assert parse_quantity("flour") is None
        assert parse_quantity("") is None
        assert parse_quantity("a few") is None


# --- Full-line parse: happy paths ---


class TestRegexParseLineHappyPath:
    def test_cup_flour(self) -> None:
        result = regex_parse_line("1 cup flour")
        assert result is not None
        p = result.parsed
        assert p.quantity == 1.0
        assert p.unit == "cup"
        assert p.ingredient == "flour"
        assert p.preparation == ""
        assert p.raw == "1 cup flour"

    def test_tablespoons_sugar(self) -> None:
        result = regex_parse_line("2 tbsp sugar")
        assert result is not None
        assert result.parsed.quantity == 2.0
        assert result.parsed.unit == "tbsp"
        assert result.parsed.ingredient == "sugar"

    def test_decimal_quantity(self) -> None:
        result = regex_parse_line("1.5 cups milk")
        assert result is not None
        assert result.parsed.quantity == 1.5
        assert result.parsed.unit == "cups"
        assert result.parsed.ingredient == "milk"

    def test_fraction_quantity(self) -> None:
        result = regex_parse_line("1/2 tsp salt")
        assert result is not None
        assert result.parsed.quantity == 0.5
        assert result.parsed.unit == "tsp"
        assert result.parsed.ingredient == "salt"

    def test_mixed_number(self) -> None:
        result = regex_parse_line("1 1/2 cups flour")
        assert result is not None
        assert result.parsed.quantity == 1.5

    def test_preparation_after_comma(self) -> None:
        result = regex_parse_line("1 cup butter, melted")
        assert result is not None
        assert result.parsed.preparation == "melted"
        assert result.parsed.ingredient == "butter"

    def test_grams_weight_unit(self) -> None:
        result = regex_parse_line("200 g flour")
        assert result is not None
        assert result.parsed.quantity == 200.0
        assert result.parsed.unit == "g"
        assert result.parsed.ingredient == "flour"

    def test_milliliter_volume_unit(self) -> None:
        result = regex_parse_line("400 ml milk")
        assert result is not None
        assert result.parsed.unit == "ml"

    def test_two_word_unit_fl_oz(self) -> None:
        result = regex_parse_line("8 fl oz milk")
        assert result is not None
        assert result.parsed.unit == "fl oz"
        assert result.parsed.ingredient == "milk"


# --- Confidence rejection: ambiguous lines fall through ---


class TestRegexParseLineRejection:
    def test_rejects_unknown_ingredient(self) -> None:
        # Synthetic gibberish: must NOT be silently fuzzy-matched to
        # something that looks vaguely similar.
        assert regex_parse_line("1 cup zxzxzxzx") is None

    def test_rejects_no_quantity(self) -> None:
        assert regex_parse_line("a pinch of salt") is None

    def test_rejects_only_quantity(self) -> None:
        assert regex_parse_line("2 cups") is None

    def test_rejects_parenthetical(self) -> None:
        # "1 cup flour (about 4 oz)" — parenthetical comment should
        # send to LLM, not get truncated.
        assert regex_parse_line("1 cup flour (about 4 oz)") is None

    def test_rejects_or_alternates(self) -> None:
        assert regex_parse_line("1 cup butter or margarine") is None

    def test_rejects_and_compound(self) -> None:
        assert regex_parse_line("1 cup flour and water") is None

    def test_rejects_slash_in_name(self) -> None:
        # "salt/pepper" — alternates style.
        assert regex_parse_line("1 tsp salt/pepper") is None

    def test_rejects_empty(self) -> None:
        assert regex_parse_line("") is None
        assert regex_parse_line("   ") is None


# --- Cross-language: Swedish lines fall through ---


class TestRegexParseLineSwedish:
    def test_swedish_dl_mjol_falls_through(self) -> None:
        # "2 dl mjöl" — dl IS a registered unit, but "mjöl" is unlikely
        # to canonicalize via USDA synonym table at high confidence.
        # The parser should fall through to the LLM rather than fuzzy-
        # matching to an English look-alike.
        result = regex_parse_line("2 dl mjöl")
        # Acceptable outcome: None (let LLM handle it). If a future DB
        # update adds "mjöl" as a synonym this test should flip — that's
        # a real improvement, not a regression.
        if result is not None:
            # If accepted, the canonical name MUST be a real USDA entry,
            # not silently mapped to something arbitrary.
            assert result.parsed.ingredient
            assert result.similarity >= 0.85

    def test_swedish_lonnsirap_does_not_misparse(self) -> None:
        # "lönnsirap" is Swedish for maple syrup. It must NOT silently
        # fuzzy-match to "syrup" or any English-looking variant — the
        # similarity floor blocks this.
        result = regex_parse_line("1 dl lönnsirap")
        if result is not None:
            # Canonical can only be a high-confidence hit. In practice
            # this should return None today.
            assert result.similarity >= 0.85

    def test_swedish_rodlok_does_not_misparse(self) -> None:
        # "rödlök" is Swedish for red onion. Without a Swedish synonym
        # in the DB, this must fall through, NOT silently match to a
        # different onion variety.
        result = regex_parse_line("1 stor rödlök")
        # Most likely None due to the qty word "stor" disrupting parse.
        # If it does come through, we still demand the similarity floor.
        if result is not None:
            assert result.similarity >= 0.85


# --- Determinism ---


class TestDeterminism:
    def test_same_input_same_output(self) -> None:
        line = "1 1/2 cups flour"
        first = regex_parse_line(line)
        second = regex_parse_line(line)
        assert first == second


# --- Hybrid integration: regex pre-filter inside parse_ingredient_lines ---


class TestHybridParseIntegration:
    """The vwt.17 contract: regex covers easy lines, LLM covers the rest.

    The public ``parse_ingredient_lines`` contract is unchanged — same
    signature, same return length, same order. The only observable
    difference is that the LLM is not asked about lines the regex
    accepted with confidence.
    """

    def test_easy_lines_skip_llm(self) -> None:
        from unittest.mock import patch

        from rational_recipes.scrape.parse import parse_ingredient_lines

        with patch(
            "rational_recipes.scrape.parse._ollama_generate"
        ) as mock_gen:
            results = parse_ingredient_lines(["1 cup flour", "2 tbsp sugar"])
            # Both lines parse cleanly via regex → no LLM call.
            assert mock_gen.call_count == 0
            assert len(results) == 2
            assert results[0] is not None
            assert results[1] is not None
            assert results[0].ingredient == "flour"
            assert results[1].ingredient == "sugar"

    def test_mixed_easy_and_hard_lines(self) -> None:
        from unittest.mock import patch

        from rational_recipes.scrape.parse import parse_ingredient_lines

        # The 'or'-style line falls through; its single LLM call covers
        # only that one residue line. parse_ingredient_lines uses the
        # per-line LLM path for length-1 input, so mock returns a bare
        # JSON object (no "results" wrapper).
        residue_response = (
            '{"quantity": 1.0, "unit": "cup",'
            ' "ingredient": "butter", "preparation": ""}'
        )
        with patch(
            "rational_recipes.scrape.parse._ollama_generate"
        ) as mock_gen:
            mock_gen.return_value = residue_response
            results = parse_ingredient_lines(
                [
                    "1 cup flour",
                    "1 cup butter or margarine",  # falls through
                    "2 tbsp sugar",
                ]
            )
            # Exactly one LLM round-trip for the single residue line.
            # parse_ingredient_lines uses parse_ingredient_line for
            # length-1 LLM input.
            assert mock_gen.call_count == 1
            assert len(results) == 3
            assert results[0] is not None and results[0].ingredient == "flour"
            assert (
                results[1] is not None and results[1].ingredient == "butter"
            )
            assert results[2] is not None and results[2].ingredient == "sugar"

    def test_length_preserved_when_llm_fails(self) -> None:
        from unittest.mock import patch

        from rational_recipes.scrape.parse import parse_ingredient_lines

        with patch(
            "rational_recipes.scrape.parse._ollama_generate",
            return_value=None,  # LLM unreachable for the residue line
        ):
            results = parse_ingredient_lines(
                ["1 cup flour", "1 cup something or other"]
            )
            assert len(results) == 2
            assert results[0] is not None
            assert results[1] is None  # LLM gave up; position preserved

    def test_disabling_prefilter_routes_everything_to_llm(self) -> None:
        from unittest.mock import patch

        from rational_recipes.scrape.parse import parse_ingredient_lines

        # With the prefilter disabled, even an "easy" line goes via LLM.
        with patch(
            "rational_recipes.scrape.parse._ollama_generate"
        ) as mock_gen:
            mock_gen.return_value = (
                '{"quantity": 1.0, "unit": "cup",'
                ' "ingredient": "flour", "preparation": ""}'
            )
            parse_ingredient_lines(
                ["1 cup flour"], use_regex_prefilter=False
            )
            assert mock_gen.call_count == 1
