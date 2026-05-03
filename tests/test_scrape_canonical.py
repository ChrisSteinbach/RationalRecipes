"""Tests for cross-language ingredient name canonicalization."""

from __future__ import annotations

from rational_recipes.scrape.canonical import (
    SWEDISH_TO_ENGLISH,
    canonicalize_name,
    canonicalize_names,
)


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


class TestSwedishStaticDictionary:
    """Static-dictionary translations for Swedish nouns the synonym DB misses."""

    def test_swedish_baking_leak_terms_translate(self) -> None:
        """Common Swedish baking words that previously leaked are now English."""
        leaks = {
            "pekannötter": "pecans",
            "lönnsirap": "maple syrup",
            "färskost": "cream cheese",
            "vinäger": "vinegar",
            "vaniljsocker": "vanilla sugar",
            "rågmjöl": "rye flour",
            "krossade tomater": "crushed tomatoes",
            "tomatpuré": "tomato paste",
            # Per-synonym (dfm): preserve specificity for words that map to a
            # specific English form via SWEDISH_TO_ENGLISH, even when the
            # food's umbrella canonical is more generic.
            "vispgrädde": "whipping cream",
            "gräddfil": "sour cream",
            "kvarg": "quark",
            "filmjölk": "buttermilk",
            "äggula": "egg yolk",
            "äggvita": "egg white",
            "valnötter": "walnuts",
            "hasselnötter": "hazelnuts",
            "jordnötter": "peanuts",
            "lagerblad": "bay leaf",
            "rosmarin": "rosemary",
            "persilja": "parsley",
            "ingefära": "ginger",
            "muskot": "nutmeg",
            "olivolja": "olive oil",
            "rapsolja": "canola oil",  # explicit dict entry
            "russin": "raisin",  # DB canonical singular
            # Per-synonym (dfm): "rödlök" is Swedish for the SPECIFIC red
            # variety, not the generic onion umbrella. Preserve.
            "rödlök": "red onion",
            "vitlök": "garlic",
            "morot": "carrot",
            "äpple": "apple",
            "jordgubbar": "strawberries",
            "soja": "soy sauce",
            "sojasås": "soy sauce",
        }
        for swedish, english in leaks.items():
            assert canonicalize_name(swedish) == english, (
                f"{swedish!r} canonicalized to {canonicalize_name(swedish)!r}, "
                f"expected {english!r}"
            )

    def test_swedish_canonical_in_db_gets_post_translated(self) -> None:
        """Foods whose ingredients-DB canonical is itself Swedish are rewritten.

        ``olja``, ``tomat``, ``peppar`` are stored as canonical_name on
        their food rows, so the synonym lookup returns the Swedish form.
        Post-translation must rewrite these to English.
        """
        assert canonicalize_name("olja") == "oil"
        assert canonicalize_name("tomat") == "tomato"
        assert canonicalize_name("peppar") == "pepper"

    def test_translation_is_case_insensitive(self) -> None:
        assert canonicalize_name("Pekannötter") == "pecans"
        assert canonicalize_name("  LÖNNSIRAP  ") == "maple syrup"

    def test_english_inputs_unaffected(self) -> None:
        """English ingredients still resolve to their English canonical."""
        assert canonicalize_name("flour") == "flour"
        assert canonicalize_name("butter") == "butter"
        assert canonicalize_name("eggs") == "egg"

    def test_dictionary_values_have_no_swedish_diacritics(self) -> None:
        """Every dictionary value must be plain ASCII English."""
        diacritics = set("åäöÅÄÖ")
        for sv, en in SWEDISH_TO_ENGLISH.items():
            assert not (set(en) & diacritics), (
                f"Swedish residue in translation value: {sv!r} -> {en!r}"
            )
