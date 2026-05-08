"""Tests for the loader-level prose-line filter."""

from __future__ import annotations

from rational_recipes.scrape.loaders import (
    filter_ingredient_lines,
    looks_like_prose,
)


class TestLooksLikeProse:
    def test_normal_ingredient_passes(self) -> None:
        assert not looks_like_prose("1 cup flour")

    def test_short_unit_qty_passes(self) -> None:
        assert not looks_like_prose("2 large eggs")

    def test_swedish_passes(self) -> None:
        assert not looks_like_prose("3 dl vetemjöl")

    def test_single_sentence_end_passes(self) -> None:
        # Many real lines have one period (e.g. abbreviations) — only
        # repeated sentence-end markers should trip the filter.
        assert not looks_like_prose("1 Tbsp. butter")

    def test_two_sentence_ends_pass(self) -> None:
        assert not looks_like_prose("Mix well. Then bake.")

    def test_just_under_length_limit_passes(self) -> None:
        line = "a" * 150
        assert not looks_like_prose(line)

    def test_over_length_limit_drops(self) -> None:
        line = "a" * 151
        assert looks_like_prose(line)

    def test_http_url_drops(self) -> None:
        assert looks_like_prose("see http://example.com for details")

    def test_https_url_drops(self) -> None:
        assert looks_like_prose("recipe at https://example.com/page")

    def test_three_sentence_ends_drop(self) -> None:
        # Three ". " boundaries — the trailing period without a following
        # space is not counted, matching the literal '. ' marker.
        assert looks_like_prose("First do this. Then that. Finally bake. Done")

    def test_mixed_sentence_punctuation_drops(self) -> None:
        assert looks_like_prose("What now? Mix. Bake!  ")

    def test_long_recipe_note_drops(self) -> None:
        line = (
            "Note: this recipe was adapted from a family heirloom and the "
            "quantities have been adjusted to suit modern taste preferences "
            "for less sugar and more whole grains."
        )
        assert looks_like_prose(line)


class TestFilterIngredientLines:
    def test_drops_prose_keeps_ingredients(self) -> None:
        lines = (
            "1 cup flour",
            "see http://example.com",
            "2 eggs",
            "Mix it. Bake it. Eat it. Done",
            "1 tsp salt",
        )
        assert filter_ingredient_lines(lines) == (
            "1 cup flour",
            "2 eggs",
            "1 tsp salt",
        )

    def test_empty_input(self) -> None:
        assert filter_ingredient_lines(()) == ()

    def test_all_pass(self) -> None:
        lines = ("1 cup flour", "2 eggs", "1 tsp salt")
        assert filter_ingredient_lines(lines) == lines

    def test_all_dropped(self) -> None:
        lines = (
            "https://example.com",
            "a" * 200,
            "One. Two. Three. Four",
        )
        assert filter_ingredient_lines(lines) == ()
