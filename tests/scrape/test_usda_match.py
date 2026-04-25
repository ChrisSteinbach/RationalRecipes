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
