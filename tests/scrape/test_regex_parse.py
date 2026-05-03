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

    def test_strips_informational_parenthetical(self) -> None:
        # r6w: parenthetical asides describe but don't override the
        # leading quantity. "1 cup flour (about 4 oz)" → take the
        # 1-cup leading qty as authoritative; the paren content is
        # advisory. The vwt.17 parser rejected this; r6w accepts it
        # so the line stays off the LLM hot path.
        result = regex_parse_line("1 cup flour (about 4 oz)")
        assert result is not None
        assert result.parsed.quantity == 1.0
        assert result.parsed.unit == "cup"
        assert result.parsed.ingredient == "flour"

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


# --- r6w expansion: hit-rate boosters --------------------------------------
#
# These cover patterns that the conservative vwt.17 parser declined but
# that any honest reading of the cache (gemma4:e2b, 1.4M rows) shows
# the LLM successfully handles. Each is a deterministic transform that
# preserves correctness — the canonicalize step still has to vouch for
# the resulting name, so a wrong rewrite still ends in None and the
# LLM picks it up.


class TestPeriodSuffixedUnits:
    """RecipeNLG ubiquitously writes ``c.``, ``tsp.``, ``Tbsp.``, ``lb.``,
    ``oz.``. The pre-r6w parser rejected all of them. Period-stripping
    inside the unit check turns every one into a regex hit."""

    def test_cup_with_period(self) -> None:
        result = regex_parse_line("1 c. flour")
        assert result is not None
        assert result.parsed.quantity == 1.0
        assert result.parsed.unit == "c"
        assert result.parsed.ingredient == "flour"

    def test_tsp_with_period(self) -> None:
        result = regex_parse_line("1 tsp. salt")
        assert result is not None
        assert result.parsed.unit == "tsp"
        assert result.parsed.ingredient == "salt"

    def test_tablespoon_abbrev_with_period(self) -> None:
        result = regex_parse_line("2 Tbsp. sugar")
        assert result is not None
        assert result.parsed.unit == "tbsp"
        assert result.parsed.quantity == 2.0
        assert result.parsed.ingredient == "sugar"

    def test_pound_abbrev_with_period(self) -> None:
        result = regex_parse_line("1 lb. butter")
        assert result is not None
        assert result.parsed.unit == "lb"
        assert result.parsed.ingredient == "butter"

    def test_ounce_abbrev_with_period(self) -> None:
        result = regex_parse_line("4 oz. cream cheese")
        assert result is not None
        assert result.parsed.unit == "oz"


class TestBulletPrefix:
    """ica.se / WDC pages frequently emit bullet-list markup that flows
    into the raw line. The bullet itself is meaningless punctuation —
    strip and parse what's left."""

    def test_unicode_bullet(self) -> None:
        result = regex_parse_line("• 2 teaspoons baking soda")
        assert result is not None
        assert result.parsed.quantity == 2.0
        assert result.parsed.ingredient == "baking soda"

    def test_dash_bullet(self) -> None:
        result = regex_parse_line("- 1 cup flour")
        assert result is not None
        assert result.parsed.ingredient == "flour"

    def test_asterisk_bullet(self) -> None:
        result = regex_parse_line("* 1 cup flour")
        assert result is not None
        assert result.parsed.ingredient == "flour"

    def test_middle_dot_bullet(self) -> None:
        # · — common in some Swedish recipe sites
        result = regex_parse_line("· 1 cup flour")
        assert result is not None
        assert result.parsed.ingredient == "flour"


class TestParentheticalStripping:
    """Parenthetical asides describe the line; they aren't authoritative.
    We strip them and try to parse the remainder. If that doesn't yield
    a confident result, fall through to LLM as before."""

    def test_strip_inline_paren(self) -> None:
        # "60 grams (2 ounces) chopped walnuts" → after strip,
        # "60 grams chopped walnuts" — qty=60, unit=grams, prep=chopped,
        # ing=walnuts.
        result = regex_parse_line("60 grams (2 ounces) chopped walnuts")
        assert result is not None
        assert result.parsed.quantity == 60.0
        assert result.parsed.unit == "grams"
        # Pre-name "chopped" goes to preparation.
        assert "chopped" in result.parsed.preparation.lower()
        assert "walnut" in result.parsed.ingredient.lower()
        # Critical: NEVER returns the Swedish DB canonical "valnötter".
        assert "valn" not in result.parsed.ingredient.lower()

    def test_strip_trailing_paren(self) -> None:
        result = regex_parse_line("1/4 onion (put through press)")
        assert result is not None
        assert result.parsed.quantity == 0.25
        assert result.parsed.ingredient == "onion"

    def test_strip_optional_paren(self) -> None:
        result = regex_parse_line("1 lb sausage (like Jimmy Dean)")
        assert result is not None
        assert result.parsed.unit == "lb"
        assert "sausage" in result.parsed.ingredient.lower()

    def test_unbalanced_paren_left_alone(self) -> None:
        # Don't get clever with malformed parens — leave it to the LLM.
        # "1 cup flour (no closing" stays intact, which means the regex
        # rejects it because of the literal "(" still in the name.
        # We don't assert pass/fail strictly; just that the parser doesn't
        # crash and behaves predictably.
        result = regex_parse_line("1 cup flour (no closing")
        # Acceptable outcomes: None (fall through) or a parsed result
        # whose ingredient doesn't include the malformed paren content.
        if result is not None:
            assert "(" not in result.parsed.ingredient


class TestLeadingPrepKeywords:
    """The LLM strips ``chopped`` / ``sliced`` / ``thinly sliced`` from
    the ingredient name and puts them in preparation. The regex should
    do the same so its output is mergeable with LLM rows."""

    def test_chopped_pecans(self) -> None:
        result = regex_parse_line("1 cup chopped pecans")
        assert result is not None
        assert result.parsed.unit == "cup"
        assert "chopped" in result.parsed.preparation.lower()
        assert "pecan" in result.parsed.ingredient.lower()
        # English canonical, never Swedish.
        assert "pekan" not in result.parsed.ingredient.lower()

    def test_thinly_sliced_carrots(self) -> None:
        # Adverb + verb both peel off into preparation. We use carrots
        # because the ingredients DB resolves it cleanly; ``lettuce`` /
        # ``marjoram`` fall through here (DB gap, separate concern).
        result = regex_parse_line("4 cups thinly sliced carrots")
        assert result is not None
        assert result.parsed.quantity == 4.0
        assert result.parsed.unit == "cups"
        assert "thinly" in result.parsed.preparation.lower()
        assert "sliced" in result.parsed.preparation.lower()
        assert "carrot" in result.parsed.ingredient.lower()

    def test_grated_cheese(self) -> None:
        result = regex_parse_line("1/2 c. grated cheese")
        assert result is not None
        assert result.parsed.unit == "c"
        assert "grated" in result.parsed.preparation.lower()
        assert result.parsed.ingredient == "cheese"

    def test_finely_chopped_fresh_basil(self) -> None:
        # Adverb + verb + state-adjective all peel off into preparation.
        # Basil is in the synonym DB; the adjacent test for marjoram
        # would fail simply because the DB doesn't carry it (DB gap,
        # not a parser gap).
        result = regex_parse_line(
            "1 1/2 teaspoons finely chopped fresh basil"
        )
        assert result is not None
        assert result.parsed.quantity == 1.5
        assert result.parsed.unit == "teaspoons"
        prep = result.parsed.preparation.lower()
        assert "finely" in prep
        assert "chopped" in prep
        assert "fresh" in prep
        assert "basil" in result.parsed.ingredient.lower()

    def test_melted_butter(self) -> None:
        result = regex_parse_line("2 tbsp melted butter")
        assert result is not None
        assert "melted" in result.parsed.preparation.lower()
        assert result.parsed.ingredient == "butter"


class TestEnglishCanonicalContract:
    """Lock in the r6w correctness contract: regex output is always
    English, never the raw Swedish DB canonical."""

    def test_walnut_canonical_is_english(self) -> None:
        result = regex_parse_line("1 cup walnuts")
        assert result is not None
        assert "walnut" in result.parsed.ingredient.lower()
        assert "valn" not in result.parsed.ingredient.lower()

    def test_tomato_canonical_is_english(self) -> None:
        result = regex_parse_line("2 cups tomatoes")
        assert result is not None
        assert "tomato" in result.parsed.ingredient.lower()

    def test_oil_canonical_is_english(self) -> None:
        result = regex_parse_line("2 tablespoons oil")
        assert result is not None
        assert result.parsed.ingredient.lower() != "olja"
        assert "oil" in result.parsed.ingredient.lower()


class TestProductionConventions:
    """Match the LLM's output conventions for whole-unit canonicalization."""

    def test_bare_quantity_eggs_uses_medium(self) -> None:
        result = regex_parse_line("3 eggs")
        assert result is not None
        # LLM emits "MEDIUM" for the bare-egg shape; regex matches.
        assert result.parsed.unit == "MEDIUM"
        assert result.parsed.ingredient == "egg"

    def test_explicit_large_eggs(self) -> None:
        result = regex_parse_line("2 large eggs")
        assert result is not None
        # LLM emits "LARGE"; regex should match for cache-merge consistency.
        assert result.parsed.unit == "LARGE"
        assert result.parsed.ingredient == "egg"


class TestUnitPreferenceWeightOverContainer:
    """Recipe shorthand often expresses packaged goods two ways at once:
    the leading qty counts containers, the paren gives the authoritative
    weight/volume per container. Per the user's r6w note, the
    preference order is weight > volume > container. The regex promotes
    the paren weight/volume to qty/unit and uses the leading qty as a
    multiplier so totals come out right."""

    def test_one_pkg_with_weight_promotes_to_weight(self) -> None:
        # "1 (10 oz.) pkg. chopped frozen spinach" — leading qty=1 is
        # a container count; the actual weight is 10 oz.
        result = regex_parse_line(
            "1 (10 oz.) pkg. chopped frozen spinach, drained"
        )
        assert result is not None
        assert result.parsed.quantity == 10.0
        assert result.parsed.unit == "oz"
        assert "spinach" in result.parsed.ingredient.lower()

    def test_two_cans_with_weight_each_multiplies(self) -> None:
        # "2 (20 oz. each) cans crushed pineapple" — explicit "each"
        # means 2 × 20 = 40 oz total. The "each" qualifier strips off
        # before the paren content gets parsed.
        result = regex_parse_line(
            "2 (20 oz. each) cans crushed pineapple, drained"
        )
        assert result is not None
        assert result.parsed.quantity == 40.0
        assert result.parsed.unit == "oz"
        assert "pineapple" in result.parsed.ingredient.lower()

    def test_one_carton_with_weight_promotes(self) -> None:
        # "1 (8 oz.) carton sour cream" — without the unit-preference
        # rule, the regex would parse as qty=1, unit=carton. With the
        # rule, qty=8 oz (paren weight) wins.
        result = regex_parse_line("1 (8 oz.) carton sour cream")
        assert result is not None
        assert result.parsed.quantity == 8.0
        assert result.parsed.unit == "oz"
        assert "sour cream" in result.parsed.ingredient.lower()

    def test_two_cream_cheese_with_weight_multiplies(self) -> None:
        # "2 (8-oz.) cream cheese, softened" — bare qty + paren weight,
        # no follow-up packaging word. Total = 2 × 8 = 16 oz.
        result = regex_parse_line("2 (8-oz.) cream cheese, softened")
        assert result is not None
        assert result.parsed.quantity == 16.0
        assert result.parsed.unit == "oz"
        assert result.parsed.ingredient == "cream cheese"
        assert "softened" in result.parsed.preparation.lower()

    def test_volume_unit_keeps_leading_qty(self) -> None:
        # When the leading qty already has a volume unit (cup), the
        # paren is informational — DON'T override.
        result = regex_parse_line("60 grams (2 ounces) chopped walnuts")
        assert result is not None
        assert result.parsed.quantity == 60.0
        assert result.parsed.unit == "grams"

    def test_size_unit_keeps_leading_qty(self) -> None:
        # MEDIUM/LARGE/SMALL units carry their own gram conversion per
        # ingredient. They aren't containers, so the unit-preference
        # rule doesn't fire — keep the leading qty.
        result = regex_parse_line("1 medium onion (about 4 oz)")
        assert result is not None
        # The MEDIUM size resolves via wholeunits2grams downstream;
        # don't squash it into 4 oz.
        assert result.parsed.quantity == 1.0
        assert result.parsed.unit == "MEDIUM"
        assert result.parsed.ingredient == "onion"

    def test_optional_paren_with_no_qty_does_not_trigger(self) -> None:
        # Plain non-qty asides ("(optional)", "(if desired)") leave
        # everything untouched.
        result = regex_parse_line("3 eggs (optional)")
        assert result is not None
        assert result.parsed.quantity == 3.0
        assert result.parsed.unit == "MEDIUM"
        assert result.parsed.ingredient == "egg"

    def test_paren_with_unrecognized_unit_falls_through(self) -> None:
        # "2 (12 widget) cream cheese" — paren has a digit and a token,
        # but the token isn't a registered unit. The leading qty is bare
        # and the paren is qty-hugging — too ambiguous, kick to LLM.
        assert regex_parse_line("2 (12 widget) cream cheese") is None


class TestExpandedSwedishCanonicals:
    """Lock in the additional Swedish canonical→English translations
    discovered during the cache shadow comparison."""

    def test_mushrooms_returns_english(self) -> None:
        result = regex_parse_line("8 oz drained mushrooms")
        assert result is not None
        assert "champinjon" not in result.parsed.ingredient.lower()
        assert "mushroom" in result.parsed.ingredient.lower()

    def test_mint_returns_english(self) -> None:
        result = regex_parse_line("1 tbsp dried mint")
        assert result is not None
        assert result.parsed.ingredient.lower() != "mynta"
        assert "mint" in result.parsed.ingredient.lower()

    def test_parmesan_returns_english(self) -> None:
        result = regex_parse_line("3 tbsp grated Parmesan cheese")
        assert result is not None
        assert "parmesanost" not in result.parsed.ingredient.lower()
        assert "parmesan" in result.parsed.ingredient.lower()


class TestNaturalLanguageRange:
    """Both ``X-Y`` and ``X to Y`` ranges yield the midpoint, matching
    the LLM prompt's documented behavior."""

    def test_to_range_simple(self) -> None:
        result = regex_parse_line("8 to 10 large apples")
        assert result is not None
        # Midpoint of 8 and 10.
        assert result.parsed.quantity == 9.0
        assert result.parsed.unit == "LARGE"
        assert result.parsed.ingredient == "apple"

    def test_to_range_with_fraction(self) -> None:
        result = regex_parse_line("1/2 to 1 cup sugar")
        assert result is not None
        # Midpoint of 0.5 and 1.0.
        assert result.parsed.quantity == 0.75

    def test_to_inside_word_does_not_trigger(self) -> None:
        # "tomato" contains "to" but "to" inside a word can never match
        # \s+to\s+. The parser shouldn't treat this as a range.
        result = regex_parse_line("1 cup tomato sauce")
        assert result is not None
        assert result.parsed.quantity == 1.0
        # Must reach the canonical lookup successfully (English).
        assert "tomato" in result.parsed.ingredient.lower()


class TestApproximationPrefix:
    """Recipe writers often prepend ``approximately`` / ``about`` to a
    qty. The LLM strips these silently; the regex follows."""

    def test_approximately_prefix(self) -> None:
        result = regex_parse_line("approximately 1 lb. pork chops")
        # We accept either a confident parse (lb pork chops resolves)
        # or a None (DB doesn't know "pork chops"). The prefix MUST NOT
        # leak into the quantity parse.
        if result is not None:
            assert result.parsed.quantity == 1.0
            assert result.parsed.unit == "lb"

    def test_about_prefix(self) -> None:
        result = regex_parse_line("about 2 cups flour")
        assert result is not None
        assert result.parsed.quantity == 2.0
        assert result.parsed.unit == "cups"
        assert result.parsed.ingredient == "flour"

    def test_roughly_prefix(self) -> None:
        result = regex_parse_line("roughly 1 tbsp olive oil")
        assert result is not None
        assert result.parsed.quantity == 1.0
        assert result.parsed.unit == "tbsp"
        assert result.parsed.ingredient.lower().endswith("olive oil")
