"""Tests for cross-language ingredient name canonicalization."""

from __future__ import annotations

from rational_recipes.scrape.canonical import canonicalize_name, canonicalize_names


class TestCanonicalizeName:
    def test_english_passthrough(self) -> None:
        """English canonicals resolve to themselves."""
        assert canonicalize_name("flour") == "flour"
        assert canonicalize_name("milk") == "milk"
        assert canonicalize_name("egg") == "egg"

    def test_english_plural_to_singular(self) -> None:
        """Plural-vs-singular synonyms collapse to the canonical form."""
        assert canonicalize_name("eggs") == "egg"

    def test_swedish_to_english(self) -> None:
        """Swedish ingredient names route to English canonicals."""
        assert canonicalize_name("vetemjöl") == "flour"
        assert canonicalize_name("mjölk") == "milk"
        assert canonicalize_name("ägg") == "egg"
        assert canonicalize_name("socker") == "sugar"
        assert canonicalize_name("smör") == "butter"
        assert canonicalize_name("grädde") == "cream"

    def test_case_insensitive(self) -> None:
        """Lookup is case-insensitive and whitespace is stripped."""
        assert canonicalize_name("  FLOUR  ") == "flour"
        assert canonicalize_name("Vetemjöl") == "flour"

    def test_unknown_preserved_lowercased(self) -> None:
        """Unknown names come back lowercased and stripped, not dropped."""
        assert canonicalize_name("  UNKNOWN_INGREDIENT  ") == "unknown_ingredient"

    def test_empty_returns_empty(self) -> None:
        assert canonicalize_name("") == ""
        assert canonicalize_name("   ") == ""

    def test_saffron_alias(self) -> None:
        """Saffron is reachable via both English and Swedish names."""
        assert canonicalize_name("saffron") == "saffron"
        assert canonicalize_name("saffran") == "saffron"

    def test_swedish_pannkakor_staples(self) -> None:
        """The pannkakor core vocabulary collapses to English canonicals."""
        sv_to_en = {
            "vetemjöl": "flour",
            "mjölk": "milk",
            "ägg": "egg",
            "socker": "sugar",
            "smör": "butter",
            "salt": "salt",
            "bakpulver": "baking powder",
            "kanel": "cinnamon",
            "kardemumma": "cardamom",
            "fläsk": "bacon",
            "saffran": "saffron",
        }
        for swedish, english in sv_to_en.items():
            assert canonicalize_name(swedish) == english, (
                f"{swedish!r} did not canonicalize to {english!r}"
            )


class TestCanonicalizeNames:
    def test_batch_canonicalization(self) -> None:
        """A mixed-language batch becomes a single English frozenset."""
        names = ["vetemjöl", "mjölk", "ägg", "flour"]
        assert canonicalize_names(names) == frozenset({"flour", "milk", "egg"})

    def test_empty_input(self) -> None:
        assert canonicalize_names([]) == frozenset()

    def test_empty_strings_dropped(self) -> None:
        """Empty/whitespace strings don't produce entries."""
        assert canonicalize_names(["", "  ", "flour"]) == frozenset({"flour"})

    def test_unknown_names_kept_lowercased(self) -> None:
        result = canonicalize_names(["flour", "UNKNOWN_X"])
        assert result == frozenset({"flour", "unknown_x"})
