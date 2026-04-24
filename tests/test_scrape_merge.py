"""Tests for cross-corpus merge + within-variant proportion-bucket dedup."""

from __future__ import annotations

from rational_recipes.scrape.merge import (
    DEFAULT_BUCKET_SIZE,
    DEFAULT_NEAR_DUP_THRESHOLD,
    MergedRecipe,
    merge_corpora,
    proportion_bucket_dedup,
)
from rational_recipes.scrape.recipenlg import Recipe
from rational_recipes.scrape.wdc import WDCRecipe


def _rnlg(
    title: str,
    link: str,
    ingredients: tuple[str, ...],
    ner: tuple[str, ...] | None = None,
) -> Recipe:
    return Recipe(
        row_index=0,
        title=title,
        ingredients=ingredients,
        source="",
        link=link,
        ner=ner if ner is not None else ingredients,
    )


def _wdc(
    title: str,
    page_url: str,
    ingredients: tuple[str, ...],
    names: frozenset[str] | None = None,
    cooking_methods: frozenset[str] = frozenset(),
) -> WDCRecipe:
    return WDCRecipe(
        row_id=0,
        host="example.com",
        title=title,
        ingredients=ingredients,
        page_url=page_url,
        cooking_methods=cooking_methods,
        durations=(),
        recipe_category="",
        keywords=(),
        recipe_yield="",
        ingredient_names=names if names is not None else frozenset(ingredients),
    )


class TestMergeCorpora:
    def test_empty_inputs_yield_empty_output(self) -> None:
        merged, stats = merge_corpora([], [])
        assert merged == []
        assert stats.recipenlg_in == 0
        assert stats.wdc_in == 0
        assert stats.merged_out == 0

    def test_disjoint_corpora_concatenate(self) -> None:
        r = _rnlg("Pancakes", "https://a.com/r/1", ("flour",))
        w = _wdc("Pannkakor", "https://b.se/r/2", ("mjöl",))

        merged, stats = merge_corpora([r], [w])

        assert stats.recipenlg_in == 1
        assert stats.wdc_in == 1
        assert stats.url_duplicates == 0
        assert stats.near_dup_duplicates == 0
        assert len(merged) == 2
        # WDC comes first, then RecipeNLG
        assert merged[0].corpus == "wdc"
        assert merged[1].corpus == "recipenlg"

    def test_url_match_prefers_wdc(self) -> None:
        r = _rnlg("Pancakes", "https://ica.se/r/42", ("flour", "milk"))
        w = _wdc(
            "Pannkakor",
            "https://ica.se/r/42",
            ("mjöl", "mjölk"),
            cooking_methods=frozenset({"stekt"}),
        )

        merged, stats = merge_corpora([r], [w])

        assert stats.url_duplicates == 1
        assert stats.near_dup_duplicates == 0
        assert len(merged) == 1
        assert merged[0].corpus == "wdc"
        assert merged[0].cooking_methods == frozenset({"stekt"})

    def test_url_match_ignores_trailing_slash_and_case(self) -> None:
        r = _rnlg("Pancakes", "https://ICA.se/r/42/", ("flour",))
        w = _wdc("Pannkakor", "https://ica.se/R/42", ("flour",))

        merged, stats = merge_corpora([r], [w])

        # Match is case-insensitive-host, path is lowercased (existing
        # behavior from comparison._normalize_url); differing path case
        # ("R/42" vs "r/42") keeps these SEPARATE post-lowercase.
        # Trailing slash alone does match — verify the path-case case.
        # This documents the current contract: the normalizer lowercases
        # the whole URL including path, so /R/42 and /r/42 collide.
        assert stats.url_duplicates == 1
        assert len(merged) == 1

    def test_near_dup_prefers_wdc(self) -> None:
        shared = ("flour", "milk", "egg", "salt", "sugar")
        r = _rnlg("Pancakes", "https://a.com/r/1", shared, ner=shared)
        w = _wdc("Pancakes", "https://b.com/r/2", shared, names=frozenset(shared))

        merged, stats = merge_corpora([r], [w])

        assert stats.near_dup_duplicates == 1
        assert len(merged) == 1
        assert merged[0].corpus == "wdc"

    def test_near_dup_below_threshold_kept_separate(self) -> None:
        r_ings = ("flour", "milk", "egg", "salt")
        w_ings = ("flour", "buttermilk", "baking soda", "vanilla")
        r = _rnlg("Pancakes", "https://a.com/r/1", r_ings, ner=r_ings)
        w = _wdc("Pancakes", "https://b.com/r/2", w_ings, names=frozenset(w_ings))

        merged, stats = merge_corpora([r], [w])

        # Jaccard = 1/7 = 0.14 — below 0.3 threshold; both kept.
        assert stats.near_dup_duplicates == 0
        assert len(merged) == 2

    def test_near_dup_threshold_respects_override(self) -> None:
        r_ings = ("flour", "milk", "egg", "salt")
        w_ings = ("flour", "milk", "vanilla", "sugar", "butter", "salt")
        r = _rnlg("Pancakes", "https://a.com/r/1", r_ings, ner=r_ings)
        w = _wdc("Pancakes", "https://b.com/r/2", w_ings, names=frozenset(w_ings))

        # Jaccard = 3/7 ≈ 0.43 — above default 0.3 (merges) and above
        # explicit 0.5 (also merges).
        _, stats_default = merge_corpora([r], [w])
        assert stats_default.near_dup_duplicates == 1

        _, stats_strict = merge_corpora([r], [w], near_dup_threshold=0.5)
        assert stats_strict.near_dup_duplicates == 0

    def test_near_dup_only_within_same_title_group(self) -> None:
        """Ingredient-set near-dup runs only inside normalized-title
        buckets — no cross-title matching. Otherwise unrelated dishes
        with overlapping basics would collapse."""
        shared = ("flour", "milk", "egg", "salt", "sugar")
        r = _rnlg("Pancakes", "https://a.com/r/1", shared, ner=shared)
        w = _wdc("Waffles", "https://b.com/r/2", shared, names=frozenset(shared))

        merged, stats = merge_corpora([r], [w])

        assert stats.near_dup_duplicates == 0
        assert len(merged) == 2

    def test_default_threshold_is_0_3(self) -> None:
        """Documented contract: the default lives at 0.3 — lowered from
        0.5 by RationalRecipes-toj validation. A threshold sweep on
        the pannkak/ica.se slice with deterministic LLM extraction
        showed the documented saffranspannkaka cross-corpus pair sits
        at Jaccard ~0.3-0.4 because the two recipes list different
        optional accompaniments. 0.3 catches it with no false
        positives in the 43-row stream."""
        assert DEFAULT_NEAR_DUP_THRESHOLD == 0.3

    def test_url_match_wins_over_near_dup_count(self) -> None:
        """When a row is already URL-matched it isn't re-counted as a
        near-dup, even if its ingredients would also Jaccard-match."""
        shared = ("flour", "milk", "egg", "salt", "sugar")
        r = _rnlg("Pancakes", "https://ica.se/r/1", shared, ner=shared)
        w = _wdc("Pancakes", "https://ica.se/r/1", shared, names=frozenset(shared))

        _, stats = merge_corpora([r], [w])

        assert stats.url_duplicates == 1
        assert stats.near_dup_duplicates == 0


class TestProportionBucketDedup:
    def test_empty_input(self) -> None:
        assert proportion_bucket_dedup([], lambda _: {}) == []

    def test_identical_rows_collapse(self) -> None:
        rows = [
            {"id": "a", "props": {"flour": 40.0, "milk": 50.0, "egg": 10.0}},
            {"id": "b", "props": {"flour": 40.0, "milk": 50.0, "egg": 10.0}},
        ]
        result = proportion_bucket_dedup(rows, lambda r: r["props"])  # type: ignore[arg-type,return-value]
        assert len(result) == 1
        assert result[0]["id"] == "a"  # first wins

    def test_slight_variation_within_bucket_collapses(self) -> None:
        """Within-bucket variation collapses (floor-div bucketing)."""
        rows = [
            {"id": "a", "props": {"flour": 40.0, "milk": 50.0, "egg": 10.0}},
            {"id": "b", "props": {"flour": 41.0, "milk": 51.0, "egg": 11.0}},
        ]
        result = proportion_bucket_dedup(rows, lambda r: r["props"])  # type: ignore[arg-type,return-value]
        assert len(result) == 1

    def test_different_proportions_kept(self) -> None:
        rows = [
            {"id": "a", "props": {"flour": 40.0, "milk": 50.0, "egg": 10.0}},
            {"id": "b", "props": {"flour": 30.0, "milk": 60.0, "egg": 10.0}},
        ]
        result = proportion_bucket_dedup(rows, lambda r: r["props"])  # type: ignore[arg-type,return-value]
        assert len(result) == 2

    def test_bucket_size_is_tunable(self) -> None:
        """A 10g-step difference collapses at bucket_size=20 but not at default."""
        rows = [
            {"id": "a", "props": {"flour": 40.0, "milk": 50.0}},
            {"id": "b", "props": {"flour": 50.0, "milk": 40.0}},
        ]
        default = proportion_bucket_dedup(rows, lambda r: r["props"])  # type: ignore[arg-type,return-value]
        assert len(default) == 2

        loose = proportion_bucket_dedup(rows, lambda r: r["props"], bucket_size=20.0)  # type: ignore[arg-type,return-value]
        assert len(loose) == 1

    def test_default_bucket_size(self) -> None:
        assert DEFAULT_BUCKET_SIZE == 2.0

    def test_works_on_merged_recipes(self) -> None:
        """Smoke test: the function takes any row type + an extractor."""
        mr_a = MergedRecipe(
            title="t",
            ingredients=(),
            ingredient_names=frozenset(),
            url="",
            cooking_methods=frozenset(),
            corpus="wdc",
            source=_wdc("t", "", ()),
        )
        mr_b = MergedRecipe(
            title="t",
            ingredients=(),
            ingredient_names=frozenset(),
            url="",
            cooking_methods=frozenset(),
            corpus="wdc",
            source=_wdc("t", "", ()),
        )
        props = {"flour": 40.0, "milk": 50.0}
        result = proportion_bucket_dedup([mr_a, mr_b], lambda _: props)
        assert len(result) == 1
