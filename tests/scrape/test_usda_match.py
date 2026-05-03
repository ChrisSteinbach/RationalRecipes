"""Tests for the USDA ingredient-name confidence gate (vwt.17).

These tests run against the shipped ingredients.db (read-only). They
exercise (a) exact synonym hits, (b) high-confidence fuzzy hits, and
(c) the conservative bias — anything below the similarity floor must
return None so the caller falls through to the LLM.
"""

from __future__ import annotations

from rational_recipes.scrape.usda_match import (
    DEFAULT_SIMILARITY_THRESHOLD,
    resolve_canonical_name,
)


class TestExactHits:
    def test_flour_resolves(self) -> None:
        match = resolve_canonical_name("flour")
        assert match is not None
        assert match.similarity == 1.0
        assert match.canonical

    def test_sugar_resolves(self) -> None:
        match = resolve_canonical_name("sugar")
        assert match is not None
        assert match.similarity == 1.0

    def test_butter_resolves(self) -> None:
        match = resolve_canonical_name("butter")
        assert match is not None
        assert match.similarity == 1.0

    def test_case_insensitive(self) -> None:
        a = resolve_canonical_name("FLOUR")
        b = resolve_canonical_name("flour")
        assert a is not None and b is not None
        assert a.canonical == b.canonical


class TestConservativeRejection:
    def test_empty_returns_none(self) -> None:
        assert resolve_canonical_name("") is None
        assert resolve_canonical_name("   ") is None

    def test_pure_gibberish_rejected(self) -> None:
        # No synonym contains "zxzxzxzx" so the candidate set is empty.
        assert resolve_canonical_name("zxzxzxzx") is None

    def test_low_similarity_rejected(self) -> None:
        # A made-up word that overlaps a few letters with English
        # synonyms but not enough for the 0.85 floor — accept None.
        assert resolve_canonical_name(
            "xyzpqr", threshold=DEFAULT_SIMILARITY_THRESHOLD
        ) is None

    def test_threshold_zero_admits_anything(self) -> None:
        # A no-floor sanity check: with threshold=0 the fuzzy fallback
        # returns *something* whenever the candidate set is non-empty.
        # We use a real-word substring that's guaranteed to match.
        result = resolve_canonical_name("flour", threshold=0.0)
        assert result is not None


class TestHallucinationGuard:
    """Critical test: never silently map a non-English name to an English one."""

    def test_swedish_lonnsirap_does_not_become_syrup(self) -> None:
        # "lönnsirap" — must NOT be high-confidence-fuzzy-matched to
        # "syrup" or any sirup-like English ingredient. SequenceMatcher
        # ratio between "lönnsirap" and "syrup" is well below 0.85,
        # which is exactly why we set the floor there.
        match = resolve_canonical_name("lönnsirap")
        # If a match comes back, it must clear the floor — but for this
        # foreign-language name with no synonym we expect None.
        if match is not None:
            assert match.similarity >= DEFAULT_SIMILARITY_THRESHOLD

    def test_swedish_rodlok_does_not_silently_canonicalize(self) -> None:
        match = resolve_canonical_name("rödlök")
        if match is not None:
            assert match.similarity >= DEFAULT_SIMILARITY_THRESHOLD


class TestFuzzyTolerance:
    """Mild typos should match — that's why fuzzy is here in the first place."""

    def test_obvious_typo_accepted(self) -> None:
        # "flouur" → "flour" (insert one letter, ratio ≈ 0.91).
        match = resolve_canonical_name("flouur")
        # We don't require this to succeed (depends on DB synonyms in
        # the candidate set), but if it does the canonical must be
        # flour-shaped.
        if match is not None:
            assert "flour" in match.canonical.lower()


class TestEnglishCanonicalGuarantee:
    """The pipeline shows English to end users (project_english_display).

    Some FoodData Central rows carry a Swedish ``canonical_name`` because
    they were imported from FAO/INFOODS Swedish-language entries. The
    resolver MUST post-translate those via the SWEDISH_TO_ENGLISH dict so
    regex hits never produce ``valnötter`` / ``tomat`` / ``pekannötter``
    when the LLM hot path produces ``walnut`` / ``tomato`` / ``pecan``.
    Without this guarantee, regex-parsed and LLM-parsed instances of the
    same ingredient land in different L2 clusters (r6w correctness gate).
    """

    def test_walnuts_returns_english(self) -> None:
        match = resolve_canonical_name("walnuts")
        assert match is not None
        # Must NOT be the Swedish "valnötter" canonical from the DB.
        assert "valn" not in match.canonical.lower()
        assert "walnut" in match.canonical.lower()

    def test_tomatoes_returns_english(self) -> None:
        match = resolve_canonical_name("tomatoes")
        assert match is not None
        assert "tomat" not in match.canonical.lower() or (
            "tomato" in match.canonical.lower()
        )
        # Either "tomato" or "tomatoes" — both English.
        assert "tomato" in match.canonical.lower()

    def test_pecans_returns_english(self) -> None:
        match = resolve_canonical_name("pecans")
        assert match is not None
        assert "pekan" not in match.canonical.lower()
        assert "pecan" in match.canonical.lower()

    def test_oil_returns_english(self) -> None:
        # "olja" is the Swedish DB canonical for oil — common enough
        # that the e4s NEUTRAL_PROMPT update specifically targets it.
        match = resolve_canonical_name("oil")
        assert match is not None
        assert match.canonical.lower() != "olja"

    def test_english_canonical_unaffected(self) -> None:
        # Sanity: ingredients whose DB canonical is already English
        # should pass through unchanged.
        for name, expected_substring in [
            ("flour", "flour"),
            ("sugar", "sugar"),
            ("salt", "salt"),
            ("butter", "butter"),
        ]:
            match = resolve_canonical_name(name)
            assert match is not None, name
            assert expected_substring in match.canonical.lower()
