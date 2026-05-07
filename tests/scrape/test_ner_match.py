"""Tests for the RecipeNLG NER-column ingredient-name resolver (am5).

The matcher must:
  * Pick the longest NER value that appears as a substring of the line.
  * Return None on tie-at-longest or no-match so the caller falls
    through to the LLM.
  * Be case-insensitive and tolerant of surrounding whitespace.
"""

from __future__ import annotations

from rational_recipes.scrape.ner_match import resolve_ner_for_line


class TestResolveNerForLine:
    def test_single_substring_match(self) -> None:
        assert resolve_ner_for_line(
            "1 c. firmly packed brown sugar",
            ("brown sugar", "milk", "vanilla"),
        ) == "brown sugar"

    def test_longest_substring_wins(self) -> None:
        # Both "chicken" and "cream of chicken soup" appear in the line —
        # the longer one is the right name.
        assert resolve_ner_for_line(
            "1 (10 3/4 oz.) can cream of chicken soup",
            ("chicken", "cream of chicken soup"),
        ) == "cream of chicken soup"

    def test_returns_none_when_no_match(self) -> None:
        assert resolve_ner_for_line(
            "1 large container Cool Whip",
            ("pineapple", "condensed milk", "lemons"),
        ) is None

    def test_returns_none_for_empty_inputs(self) -> None:
        assert resolve_ner_for_line("", ("flour",)) is None
        assert resolve_ner_for_line("1 cup flour", ()) is None

    def test_skips_blank_ner_entries(self) -> None:
        assert resolve_ner_for_line(
            "1 cup flour",
            ("", "  ", "flour"),
        ) == "flour"

    def test_case_insensitive_match(self) -> None:
        assert resolve_ner_for_line(
            "1/2 c. shredded Cheddar cheese",
            ("Cheddar cheese",),
        ) == "Cheddar cheese"
        # Lowercased NER also resolves an uppercase line.
        assert resolve_ner_for_line(
            "1/2 c. shredded CHEDDAR CHEESE",
            ("cheddar cheese",),
        ) == "cheddar cheese"

    def test_tie_at_longest_returns_none(self) -> None:
        # Two equally-long candidates both substring-match — the matcher
        # can't disambiguate, so it returns None and lets the LLM handle it.
        assert resolve_ner_for_line(
            "mix the salt and dill thoroughly",
            ("salt", "dill"),
        ) is None

    def test_substring_across_word_boundaries(self) -> None:
        # NER often gives the singular ("egg") for a plural line ("2 eggs").
        # Cross-boundary substring is intentional for that case.
        assert resolve_ner_for_line(
            "2 eggs",
            ("egg",),
        ) == "egg"

    def test_strips_whitespace_from_ner_candidate(self) -> None:
        assert resolve_ner_for_line(
            "1 cup flour",
            ("  flour  ",),
        ) == "flour"

    def test_dedupes_repeated_ner_value(self) -> None:
        # RecipeNLG NER is per-line, so when the same ingredient appears
        # in two lines the NER list contains duplicates. The matcher must
        # collapse those instead of reading them as a tie.
        assert resolve_ner_for_line(
            "1 cup butter",
            ("flour", "butter", "sugar", "butter", "vanilla"),
        ) == "butter"

    def test_real_recipenlg_row(self) -> None:
        # From dataset row 0 ("No-Bake Nut Cookies"): every line resolves.
        ingredients = (
            "1 c. firmly packed brown sugar",
            "1/2 c. evaporated milk",
            "1/2 tsp. vanilla",
            "1/2 c. broken nuts (pecans)",
            "2 Tbsp. butter or margarine",
            "3 1/2 c. bite size shredded rice biscuits",
        )
        ner = ("brown sugar", "milk", "vanilla", "nuts", "butter", "rice biscuits")
        assert resolve_ner_for_line(ingredients[0], ner) == "brown sugar"
        assert resolve_ner_for_line(ingredients[1], ner) == "milk"
        assert resolve_ner_for_line(ingredients[2], ner) == "vanilla"
        assert resolve_ner_for_line(ingredients[3], ner) == "nuts"
        assert resolve_ner_for_line(ingredients[4], ner) == "butter"
        assert resolve_ner_for_line(ingredients[5], ner) == "rice biscuits"
