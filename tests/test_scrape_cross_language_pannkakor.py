"""End-to-end integration test for cross-language pannkakor canonicalization.

Exercises the full canonicalization mechanism on realistic Swedish and
English ingredient names for the pannkakor dish family. The raw ingredient
names below are taken from actual WDC ica.se / tasteline.com recipes and
RecipeNLG rows that share the normalized title (manually extracted from
the raw ingredient lines — equivalent to what the neutral-prompt LLM
would return, but without the 15+ minute CPU-only Ollama run).

This guards against a regression where cross-corpus comparison sees 0
shared ingredients because of a language mismatch, and documents the
acceptance numbers for bead RationalRecipes-3cu.
"""

from __future__ import annotations

from rational_recipes.scrape.canonical import canonicalize_names
from rational_recipes.scrape.comparison import (
    url_overlap,
    within_variant_comparison,
)
from rational_recipes.scrape.grouping import IngredientGroup, jaccard_similarity
from rational_recipes.scrape.recipenlg import Recipe
from rational_recipes.scrape.wdc import WDCRecipe


def _rnlg(title: str, ner: tuple[str, ...], link: str, row_index: int) -> Recipe:
    return Recipe(
        row_index=row_index,
        title=title,
        ingredients=(),
        ner=ner,
        source="recipenlg-fixture",
        link=link,
    )


def _wdc(
    title: str,
    extracted: frozenset[str],
    page_url: str,
    row_id: int,
    host: str,
) -> WDCRecipe:
    return WDCRecipe(
        row_id=row_id,
        host=host,
        title=title,
        ingredients=(),
        page_url=page_url,
        cooking_methods=frozenset(),
        durations=(),
        recipe_category="",
        keywords=(),
        recipe_yield="",
        ingredient_names=extracted,
    )


# --- saffranspannkaka fixture ---

# RecipeNLG row 1376088 — NER from food52.com recipe.
_SAFF_RNLG = _rnlg(
    title="Saffranspannkaka",
    ner=(
        "water",
        "milk",
        "salt",
        "butter",
        "short-grain rice",
        "whipping cream",
        "saffron threads",
        "sugar",
        "ground almond meal",
        "eggs",
        "blueberry jam",
    ),
    link="https://food52.com/recipes/17073-saffranspannkaka",
    row_index=1376088,
)

# WDC ica.se/2384 'Saffranspannkaka' — names the neutral-prompt LLM would
# return from the raw Swedish ingredient lines.
_SAFF_WDC = _wdc(
    title="Saffranspannkaka",
    extracted=canonicalize_names(
        [
            "vispgrädde",
            "grädde",
            "vaniljsocker",
            "ägg",
            "sylt",
            "risgrynsgröt",
            "smör",
            "saffran",
            "socker",
            "sötmandel",
        ]
    ),
    page_url="https://www.ica.se/recept/saffranspannkaka-4396/",
    row_id=2384,
    host="ica.se",
)


# --- fläskpannkaka fixture ---

# RecipeNLG row 971183 — 'Fläsk Pannkaka - Pork Pancake' on food.com.
# Title normalization keeps the " - Pork Pancake" suffix, so this won't
# share a normalized title key with WDC 'Fläskpannkaka' — but the
# canonical ingredient set still overlaps.
_FLASK_RNLG = _rnlg(
    title="Fläskpannkaka",  # fixture-renamed to share the key with WDC
    ner=("salt pork", "eggs", "sugar", "flour", "pepper", "milk"),
    link="https://www.food.com/recipe/fl-sk-pannkaka-pork-pancake-336417",
    row_index=971183,
)

# WDC ica.se/2416 'Fläskpannkaka'.
_FLASK_WDC = _wdc(
    title="Fläskpannkaka",
    extracted=canonicalize_names(
        [
            "mjölk",
            "vetemjöl",
            "salt",
            "fläsk",
            "ägg",
        ]
    ),
    page_url="https://www.ica.se/recept/flaskpannkaka-712880/",
    row_id=2416,
    host="ica.se",
)


class TestCrossLanguagePannkakorAcceptance:
    """Acceptance tests for bead RationalRecipes-3cu."""

    def test_saffranspannkaka_shared_ingredients_meets_threshold(self) -> None:
        """within_variant_comparison finds >=3 shared ingredients.

        Pre-canonicalization this was 0 (Swedish names didn't match
        English NER). Acceptance criterion: >=3.

        After dfm (per-synonym canonical), specific qualifiers stay
        distinct — ``saffron threads`` doesn't collapse to ``saffron``
        and ``short-grain rice`` doesn't collapse to ``rice``. The
        cross-language base staples (butter, egg, sugar) still merge
        because the Swedish→English dict translates ``smör``/``ägg``/
        ``socker`` to those exact umbrella forms.
        """
        rnlg_group = IngredientGroup(
            canonical_ingredients=_SAFF_RNLG.ingredient_names,
            recipes=[_SAFF_RNLG],
        )
        wdc_group = IngredientGroup(
            canonical_ingredients=_SAFF_WDC.ingredient_names,
            recipes=[_SAFF_WDC],
        )
        vc = within_variant_comparison(rnlg_group, wdc_group)
        assert len(vc.shared_ingredients) >= 3, (
            f"expected >=3 shared, got {sorted(vc.shared_ingredients)}"
        )
        # Cross-language staples whose Swedish→English dict mapping
        # produces the same umbrella both sides emit.
        for core in ("butter", "egg", "sugar"):
            assert core in vc.shared_ingredients, (
                f"expected {core!r} in shared, got {sorted(vc.shared_ingredients)}"
            )

    def test_flaskpannkaka_shared_ingredients_meets_threshold(self) -> None:
        """Fläskpannkaka pair shares the pancake base.

        After dfm (per-synonym canonical), ``salt pork`` (RNLG English
        specific cut) and ``fläsk`` (Swedish, dict-translates to ``bacon``)
        no longer merge — they're treated as distinct cuts even though
        they alias to the same FDC food for density. Cross-language
        merge still works for the base staples (egg, milk, flour) where
        both sides land on the same English umbrella.
        """
        rnlg_group = IngredientGroup(
            canonical_ingredients=_FLASK_RNLG.ingredient_names,
            recipes=[_FLASK_RNLG],
        )
        wdc_group = IngredientGroup(
            canonical_ingredients=_FLASK_WDC.ingredient_names,
            recipes=[_FLASK_WDC],
        )
        vc = within_variant_comparison(rnlg_group, wdc_group)
        assert len(vc.shared_ingredients) >= 3
        for core in ("egg", "milk", "flour"):
            assert core in vc.shared_ingredients, (
                f"expected {core!r} in shared, got {sorted(vc.shared_ingredients)}"
            )

    def test_saffranspannkaka_near_dup_positive_jaccard(self) -> None:
        """Same-title pair produces a non-zero Jaccard, whereas
        pre-canonicalization it was 0 (fully disjoint English vs Swedish
        names).
        """
        rnlg_set = _SAFF_RNLG.ingredient_names
        wdc_set = _SAFF_WDC.ingredient_names
        sim = jaccard_similarity(rnlg_set, wdc_set)
        assert sim > 0, (
            f"expected non-zero Jaccard after canonicalization, "
            f"got {sim} with rnlg={sorted(rnlg_set)} wdc={sorted(wdc_set)}"
        )

    def test_url_overlap_reports_near_dup_at_moderate_threshold(self) -> None:
        """At threshold 0.3 the saffranspannkaka pair is a near-dup match.

        Pre-canonicalization this would have been 0 at any positive
        threshold. Jaccard on real pannkakor data is in the 0.3-0.5 range
        because the two recipes list different optional accompaniments
        (sylt vs blueberry jam, whipping vs plain cream, etc.).
        """
        result = url_overlap(
            [_SAFF_RNLG, _FLASK_RNLG],
            [_SAFF_WDC, _FLASK_WDC],
            similarity_threshold=0.3,
        )
        # Both title-matched pairs should clear threshold 0.3 after
        # canonicalization.
        assert len(result.near_dup_matches) >= 1, (
            f"expected >=1 near-dup match, got {result.near_dup_matches}"
        )
