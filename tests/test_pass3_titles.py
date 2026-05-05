"""Tests for Pass 3 distinctive title generation (bead vwt.24)."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

from rational_recipes.catalog_db import CatalogDB
from rational_recipes.scrape.merge import MergedRecipe
from rational_recipes.scrape.pass3_titles import (
    AMBIGUOUS_FAMILY_SUFFIXES,
    FAMILY_DISPLAY_OVERRIDES,
    STOP_LIST_DESCRIPTORS,
    Pass3CallTiming,
    Pass3Stats,
    TitleFn,
    _deduplicate_titles,
    _extract_descriptor,
    _ollama_title_call,
    _substitute_stop_list_descriptor,
    _variants_to_slots,
    _VariantSlot,
    apply_ambiguous_suffix,
    apply_family_display_override,
    build_default_title_fn,
    build_title_prompt,
    format_pass3_summary,
    parse_title_response,
    run_pass3,
    summarize_pass3_timings,
    validate_title_ends_with_family,
)
from rational_recipes.scrape.pipeline_merged import (
    MergedNormalizedRow,
    MergedVariantResult,
)

# --- Fixtures ---


def _make_variant(
    db: CatalogDB,
    *,
    l1_title: str,
    canonical_ingredients: frozenset[str],
    cooking_methods: frozenset[str] = frozenset(),
    category: str | None = None,
) -> str:
    """Insert one variant via upsert_variant; return the variant_id."""
    row = MergedNormalizedRow(
        url=f"https://x.example/{'_'.join(sorted(canonical_ingredients))}",
        title=l1_title,
        corpus="recipenlg",
        cells={n: "100 g" for n in canonical_ingredients},
        proportions={
            n: 100.0 / len(canonical_ingredients) for n in canonical_ingredients
        },
    )
    variant = MergedVariantResult(
        variant_title=l1_title,
        canonical_ingredients=canonical_ingredients,
        cooking_methods=cooking_methods,
        normalized_rows=[row],
        header_ingredients=sorted(canonical_ingredients),
    )
    db.upsert_variant(variant, l1_key=l1_title, category=category)
    return variant.variant_id


def _stub_title_fn() -> TitleFn:
    """Pick the first ingredient that no sibling shares; fall back to family."""

    def fn(
        family: str,
        my_ingredients: frozenset[str],
        my_methods: frozenset[str],
        siblings: Sequence[frozenset[str]],
    ) -> str | None:
        sibling_union: set[str] = set()
        for sib in siblings:
            sibling_union.update(sib)
        distinctive = sorted(my_ingredients - sibling_union)
        if distinctive:
            return f"{distinctive[0].title()} {family.title()}"
        return family.title()

    return fn


# --- Pure helpers ---


class TestBuildTitlePrompt:
    def test_payload_has_family_and_siblings(self) -> None:
        prompt = build_title_prompt(
            "pecan pie",
            frozenset({"pecan", "bourbon"}),
            frozenset(),
            [frozenset({"pecan", "maple syrup"})],
        )
        assert "pecan pie" in prompt
        assert "bourbon" in prompt
        assert "maple syrup" in prompt
        # Sibling list is JSON, so the descriptor key must surface.
        assert "siblings" in prompt

    def test_deterministic_serialization(self) -> None:
        """Same content twice → byte-identical prompt (matters for caching)."""
        a = build_title_prompt(
            "x", frozenset({"a", "b"}), frozenset({"c"}), [frozenset({"d", "e"})]
        )
        b = build_title_prompt(
            "x", frozenset({"a", "b"}), frozenset({"c"}), [frozenset({"d", "e"})]
        )
        assert a == b


class TestParseTitleResponse:
    def test_extracts_clean_title(self) -> None:
        raw = '{"title": "Bourbon Pecan Pie"}'
        assert parse_title_response(raw) == "Bourbon Pecan Pie"

    def test_collapses_whitespace(self) -> None:
        raw = '{"title": "  Maple   Pecan  Pie  "}'
        assert parse_title_response(raw) == "Maple Pecan Pie"

    def test_handles_extra_text_around_json(self) -> None:
        raw = 'sure, here you go: {"title": "Cocoa Cake"} hope this helps!'
        assert parse_title_response(raw) == "Cocoa Cake"

    def test_returns_none_for_missing_title(self) -> None:
        assert parse_title_response('{"foo": "bar"}') is None

    def test_returns_none_for_empty(self) -> None:
        assert parse_title_response("") is None
        assert parse_title_response("   ") is None

    def test_returns_none_for_non_json(self) -> None:
        assert parse_title_response("not json at all") is None

    def test_returns_none_for_empty_title_string(self) -> None:
        assert parse_title_response('{"title": ""}') is None


# --- Deduplication (vwt.32) ---


class TestDeduplicateTitles:
    def test_no_duplicates_unchanged(self) -> None:
        slots = [
            _VariantSlot("v1", frozenset({"a", "b"}), frozenset()),
            _VariantSlot("v2", frozenset({"c", "d"}), frozenset()),
        ]
        titles: list[str | None] = ["A Pie", "C Pie"]
        result = _deduplicate_titles("pie", slots, titles)
        assert result == ["A Pie", "C Pie"]

    def test_ingredient_based_dedup(self) -> None:
        slots = [
            _VariantSlot("v1", frozenset({"sour cream", "onion"}), frozenset()),
            _VariantSlot("v2", frozenset({"sour cream", "pepper"}), frozenset()),
        ]
        titles: list[str | None] = ["Sour Cream Pie", "Sour Cream Pie"]
        result = _deduplicate_titles("pie", slots, titles)
        assert len(set(result)) == 2
        assert "Onion Sour Cream Pie" in result or "Sour Cream Onion Pie" in result
        assert "Pepper Sour Cream Pie" in result or "Sour Cream Pepper Pie" in result

    def test_ingredient_inserted_before_family(self) -> None:
        slots = [
            _VariantSlot("v1", frozenset({"butter", "onion"}), frozenset()),
            _VariantSlot("v2", frozenset({"butter", "garlic"}), frozenset()),
        ]
        titles: list[str | None] = ["Butter Corn Casserole", "Butter Corn Casserole"]
        result = _deduplicate_titles("corn casserole", slots, titles)
        assert result[0] == "Butter Onion Corn Casserole"
        assert result[1] == "Butter Garlic Corn Casserole"

    def test_method_used_when_no_unique_ingredient(self) -> None:
        slots = [
            _VariantSlot("v1", frozenset({"a"}), frozenset({"bake"})),
            _VariantSlot("v2", frozenset({"a"}), frozenset({"fry"})),
        ]
        titles: list[str | None] = ["Pie", "Pie"]
        result = _deduplicate_titles("pie", slots, titles)
        assert len(set(result)) == 2
        assert any("Bake" in t for t in result)
        assert any("Fry" in t for t in result)

    def test_numeric_suffix_fallback(self) -> None:
        slots = [
            _VariantSlot("v1", frozenset({"a"}), frozenset()),
            _VariantSlot("v2", frozenset({"a"}), frozenset()),
        ]
        titles: list[str | None] = ["A Pie", "A Pie"]
        result = _deduplicate_titles("pie", slots, titles)
        assert len(set(result)) == 2
        assert "A Pie" in result
        assert "A Pie (2)" in result

    def test_none_titles_resolved_to_family(self) -> None:
        slots = [
            _VariantSlot("v1", frozenset({"a"}), frozenset()),
            _VariantSlot("v2", frozenset({"b"}), frozenset()),
        ]
        titles: list[str | None] = [None, None]
        result = _deduplicate_titles("pie", slots, titles)
        assert len(set(result)) == 2
        # Ingredient-based dedup should differentiate them.
        assert any("A" in t.upper() for t in result)

    def test_existing_titles_respected(self) -> None:
        slots = [
            _VariantSlot("v1", frozenset({"butter"}), frozenset()),
        ]
        titles: list[str | None] = ["Butter Pie"]
        result = _deduplicate_titles(
            "pie", slots, titles, existing_titles=frozenset({"Butter Pie"}),
        )
        assert result[0] != "Butter Pie"
        assert "2" in result[0]  # numeric suffix

    def test_cascading_collisions(self) -> None:
        """Ingredient dedup creates a new collision → numeric resolves it."""
        slots = [
            _VariantSlot("v1", frozenset({"butter", "onion"}), frozenset()),
            _VariantSlot("v2", frozenset({"butter", "onion"}), frozenset()),
        ]
        titles: list[str | None] = ["Butter Pie", "Butter Pie"]
        result = _deduplicate_titles("pie", slots, titles)
        # Identical ingredients → no dedup from phase 2 → numeric suffix.
        assert len(set(result)) == 2

    def test_three_way_collision_mixed_resolution(self) -> None:
        slots = [
            _VariantSlot("v1", frozenset({"cream", "onion"}), frozenset()),
            _VariantSlot("v2", frozenset({"cream", "pepper"}), frozenset()),
            _VariantSlot("v3", frozenset({"cream"}), frozenset()),
        ]
        titles: list[str | None] = ["Cream Pie", "Cream Pie", "Cream Pie"]
        result = _deduplicate_titles("pie", slots, titles)
        assert len(set(result)) == 3


# --- Slot construction reads variant_ingredient_stats (vwt.us1) ---


def _delete_stats_row(db: CatalogDB, variant_id: str, canonical_name: str) -> None:
    """Drop one row from variant_ingredient_stats to mimic the freq filter."""
    with db._conn:  # noqa: SLF001 — test reaches into the connection on purpose
        db._conn.execute(  # noqa: SLF001
            "DELETE FROM variant_ingredient_stats "
            "WHERE variant_id = ? AND canonical_name = ?",
            (variant_id, canonical_name),
        )


class TestVariantsToSlots:
    def test_slot_uses_variant_ingredient_stats_not_canonical_set(self) -> None:
        """Slot ingredients come from variant_ingredient_stats, not the
        canonical_ingredient_set on the variant row. The frequency
        filter (vwt.26) drops noise ingredients from the stats table
        but leaves canonical_ingredient_set intact, so a slot that
        reads canonical_ingredient_set would surface filtered names."""
        db = CatalogDB.in_memory()
        variant_id = _make_variant(
            db,
            l1_title="refrigerator rolls",
            canonical_ingredients=frozenset({"a", "b", "c", "yeast"}),
        )
        # Simulate the freq-filter outcome: yeast is absent from
        # variant_ingredient_stats but present in canonical_ingredient_set.
        _delete_stats_row(db, variant_id, "yeast")

        variant_row = db.get_variant(variant_id)
        assert variant_row is not None
        assert "yeast" in variant_row.canonical_ingredient_set

        ingredient_names = db.bulk_ingredient_names()
        slots = _variants_to_slots([variant_row], ingredient_names)

        assert len(slots) == 1
        assert slots[0].ingredients == frozenset({"a", "b", "c"})
        assert "yeast" not in slots[0].ingredients

    def test_missing_stats_yields_empty_ingredient_set(self) -> None:
        """A variant with no rows in variant_ingredient_stats — e.g. one
        whose ingredients were entirely freq-filtered out — gets an
        empty frozenset rather than a KeyError."""
        db = CatalogDB.in_memory()
        variant_id = _make_variant(
            db,
            l1_title="empty stats",
            canonical_ingredients=frozenset({"a"}),
        )
        with db._conn:  # noqa: SLF001
            db._conn.execute(  # noqa: SLF001
                "DELETE FROM variant_ingredient_stats WHERE variant_id = ?",
                (variant_id,),
            )

        variant_row = db.get_variant(variant_id)
        assert variant_row is not None
        slots = _variants_to_slots([variant_row], db.bulk_ingredient_names())

        assert slots[0].ingredients == frozenset()


class TestBulkIngredientNames:
    def test_orders_by_ordinal(self) -> None:
        """bulk_ingredient_names returns names ordered by ordinal so
        downstream callers see them in display order."""
        db = CatalogDB.in_memory()
        variant_id = _make_variant(
            db,
            l1_title="mix",
            canonical_ingredients=frozenset({"flour", "sugar", "egg"}),
        )
        names = db.bulk_ingredient_names()
        assert variant_id in names
        # All three made it through (single-recipe variant, freq filter
        # is gated on n >= 5 so it doesn't fire here).
        assert set(names[variant_id]) == {"flour", "sugar", "egg"}


class TestRunPass3SiblingContext:
    def test_sibling_context_uses_filtered_ingredients(self) -> None:
        """Sibling sets fed to the LLM come from variant_ingredient_stats,
        not canonical_ingredient_set. Reading the wrong source would
        leak filtered names like 'yeast' into the prompt context and
        ultimately into chosen titles."""
        db = CatalogDB.in_memory()
        v1 = _make_variant(
            db,
            l1_title="refrigerator rolls",
            canonical_ingredients=frozenset({"flour", "milk", "yeast"}),
        )
        v2 = _make_variant(
            db,
            l1_title="refrigerator rolls",
            canonical_ingredients=frozenset({"flour", "butter", "yeast"}),
        )
        _delete_stats_row(db, v1, "yeast")
        _delete_stats_row(db, v2, "yeast")

        seen: list[tuple[frozenset[str], list[frozenset[str]]]] = []

        def fn(
            family: str,
            my: frozenset[str],
            methods: frozenset[str],
            siblings: Sequence[frozenset[str]],
        ) -> str | None:
            seen.append((my, [frozenset(s) for s in siblings]))
            return f"{family.title()}"

        run_pass3(db=db, title_fn=fn)

        # Both variant ingredient sets must omit yeast.
        for my, siblings in seen:
            assert "yeast" not in my
            for sib in siblings:
                assert "yeast" not in sib


# --- run_pass3 against a real DB ---


class TestRunPass3:
    def test_singleton_l1_group_skips_llm(self) -> None:
        db = CatalogDB.in_memory()
        _make_variant(
            db,
            l1_title="banana bread",
            canonical_ingredients=frozenset({"flour", "banana"}),
        )

        calls: list[tuple[str, frozenset[str]]] = []

        def fn(
            family: str,
            my: frozenset[str],
            methods: frozenset[str],
            siblings: Sequence[frozenset[str]],
        ) -> str | None:
            calls.append((family, my))
            return "should not happen"

        stats = run_pass3(db=db, title_fn=fn)
        assert calls == []
        assert stats.variants_singleton == 1
        assert stats.llm_calls == 0
        v = db.list_variants()[0]
        assert v.display_title == "Banana Bread"

    def test_multi_variant_group_gets_distinct_titles(self) -> None:
        db = CatalogDB.in_memory()
        _make_variant(
            db,
            l1_title="pecan pie",
            canonical_ingredients=frozenset({"pecan", "egg", "bourbon"}),
        )
        _make_variant(
            db,
            l1_title="pecan pie",
            canonical_ingredients=frozenset({"pecan", "egg", "maple"}),
        )
        _make_variant(
            db,
            l1_title="pecan pie",
            canonical_ingredients=frozenset({"pecan", "egg", "chocolate"}),
        )

        stats = run_pass3(db=db, title_fn=_stub_title_fn())

        variants = db.list_variants()
        titles = {v.display_title for v in variants}
        # All three siblings must end up with distinct display titles.
        assert len(titles) == 3
        assert "Bourbon Pecan Pie" in titles
        assert "Maple Pecan Pie" in titles
        assert "Chocolate Pecan Pie" in titles
        assert stats.variants_titled == 3
        assert stats.llm_calls == 3
        assert stats.llm_failures == 0

    def test_idempotent_skips_already_titled(self) -> None:
        db = CatalogDB.in_memory()
        _make_variant(
            db,
            l1_title="pecan pie",
            canonical_ingredients=frozenset({"pecan", "egg", "bourbon"}),
        )
        _make_variant(
            db,
            l1_title="pecan pie",
            canonical_ingredients=frozenset({"pecan", "egg", "maple"}),
        )

        first = run_pass3(db=db, title_fn=_stub_title_fn())
        before = {v.variant_id: v.display_title for v in db.list_variants()}

        # Use a stub that would mangle the titles if invoked.
        def bad_fn(*_: object) -> str | None:
            return "SHOULD NOT BE USED"

        second = run_pass3(db=db, title_fn=bad_fn)
        after = {v.variant_id: v.display_title for v in db.list_variants()}

        assert before == after
        assert first.variants_titled == 2
        assert second.variants_titled == 0
        assert second.variants_skipped == 2
        assert second.llm_calls == 0

    def test_force_retitles_everything(self) -> None:
        db = CatalogDB.in_memory()
        _make_variant(
            db,
            l1_title="pecan pie",
            canonical_ingredients=frozenset({"pecan", "egg", "bourbon"}),
        )
        _make_variant(
            db,
            l1_title="pecan pie",
            canonical_ingredients=frozenset({"pecan", "egg", "maple"}),
        )

        run_pass3(db=db, title_fn=_stub_title_fn())

        seen_calls: list[str] = []

        def fn(
            family: str,
            my: frozenset[str],
            methods: frozenset[str],
            siblings: Sequence[frozenset[str]],
        ) -> str | None:
            seen_calls.append(family)
            return f"X {family.title()}"

        stats = run_pass3(db=db, title_fn=fn, force=True)
        assert stats.variants_titled == 2
        assert len(seen_calls) == 2
        # Stub returns "X Pecan Pie" for both; dedup makes them unique.
        titles = {v.display_title for v in db.list_variants()}
        assert len(titles) == 2
        assert stats.variants_deduped > 0

    def test_falls_back_to_family_on_llm_failure(self) -> None:
        db = CatalogDB.in_memory()
        _make_variant(
            db,
            l1_title="pecan pie",
            canonical_ingredients=frozenset({"pecan", "egg", "bourbon"}),
        )
        _make_variant(
            db,
            l1_title="pecan pie",
            canonical_ingredients=frozenset({"pecan", "egg", "maple"}),
        )

        def fn(*_: object) -> str | None:
            return None

        stats = run_pass3(db=db, title_fn=fn)
        assert stats.llm_failures == 2
        # Both fall back to "Pecan Pie" then get deduped.
        titles = {v.display_title for v in db.list_variants()}
        assert len(titles) == 2
        assert stats.variants_deduped > 0

    def test_parallel_workers_match_serial(self) -> None:
        # Build the same DB twice; run Pass 3 once serially, once parallel.
        def build() -> CatalogDB:
            db = CatalogDB.in_memory()
            _make_variant(
                db,
                l1_title="pecan pie",
                canonical_ingredients=frozenset({"pecan", "egg", "bourbon"}),
            )
            _make_variant(
                db,
                l1_title="pecan pie",
                canonical_ingredients=frozenset({"pecan", "egg", "maple"}),
            )
            _make_variant(
                db,
                l1_title="pecan pie",
                canonical_ingredients=frozenset({"pecan", "egg", "chocolate"}),
            )
            _make_variant(
                db,
                l1_title="apple cake",
                canonical_ingredients=frozenset({"apple", "flour", "cinnamon"}),
            )
            _make_variant(
                db,
                l1_title="apple cake",
                canonical_ingredients=frozenset({"apple", "flour", "cardamom"}),
            )
            return db

        db_serial = build()
        run_pass3(db=db_serial, title_fn=_stub_title_fn(), max_workers=1)
        serial = {v.variant_id: v.display_title for v in db_serial.list_variants()}

        db_parallel = build()
        run_pass3(db=db_parallel, title_fn=_stub_title_fn(), max_workers=4)
        parallel = {v.variant_id: v.display_title for v in db_parallel.list_variants()}

        assert serial == parallel

    def test_max_siblings_caps_prompt_context(self) -> None:
        """max_siblings limits how many sibling sets the LLM sees."""
        db = CatalogDB.in_memory()
        # Create 5 variants in the same L1 group.
        for ing in ["bourbon", "maple", "chocolate", "honey", "rum"]:
            _make_variant(
                db,
                l1_title="pecan pie",
                canonical_ingredients=frozenset({"pecan", "egg", ing}),
            )

        seen_sibling_counts: list[int] = []

        def counting_fn(
            family: str,
            my_ingredients: frozenset[str],
            my_methods: frozenset[str],
            siblings: Sequence[frozenset[str]],
        ) -> str | None:
            seen_sibling_counts.append(len(siblings))
            distinctive = sorted(my_ingredients - {s for sib in siblings for s in sib})
            if distinctive:
                return f"{distinctive[0].title()} {family.title()}"
            return family.title()

        # With max_siblings=2, each call should see at most 2 siblings.
        run_pass3(db=db, title_fn=counting_fn, max_siblings=2)
        assert all(c <= 2 for c in seen_sibling_counts)
        assert len(seen_sibling_counts) == 5  # One call per variant.

    def test_duplicate_titles_deduplicated(self) -> None:
        """LLM returning identical titles for different variants gets deduped."""
        db = CatalogDB.in_memory()
        _make_variant(
            db,
            l1_title="corn casserole",
            canonical_ingredients=frozenset({"corn", "sour cream", "onion"}),
        )
        _make_variant(
            db,
            l1_title="corn casserole",
            canonical_ingredients=frozenset({"corn", "sour cream", "pepper"}),
        )
        _make_variant(
            db,
            l1_title="corn casserole",
            canonical_ingredients=frozenset({"corn", "sour cream", "cheese"}),
        )

        # Stub that always returns the same title for every variant.
        def dup_fn(
            family: str,
            my_ingredients: frozenset[str],
            my_methods: frozenset[str],
            siblings: Sequence[frozenset[str]],
        ) -> str | None:
            return f"Sour Cream {family.title()}"

        stats = run_pass3(db=db, title_fn=dup_fn)
        variants = db.list_variants()
        titles = [v.display_title for v in variants]
        # All three must be unique.
        assert len(set(titles)) == 3
        assert stats.variants_deduped > 0

    def test_dedup_stats_zero_when_no_duplicates(self) -> None:
        db = CatalogDB.in_memory()
        _make_variant(
            db,
            l1_title="pecan pie",
            canonical_ingredients=frozenset({"pecan", "egg", "bourbon"}),
        )
        _make_variant(
            db,
            l1_title="pecan pie",
            canonical_ingredients=frozenset({"pecan", "egg", "maple"}),
        )
        stats = run_pass3(db=db, title_fn=_stub_title_fn())
        assert stats.variants_deduped == 0

    def test_on_group_done_fires_once_per_group(self) -> None:
        """Hook must fire total_groups times with monotonic position == total."""
        db = CatalogDB.in_memory()
        # Three L1 families each with two variants → three groups need titling.
        groups: list[tuple[str, list[set[str]]]] = [
            ("pecan pie", [
                {"pecan", "egg", "bourbon"}, {"pecan", "egg", "maple"},
            ]),
            ("apple cake", [
                {"apple", "flour", "cinnamon"}, {"apple", "flour", "cardamom"},
            ]),
            ("banana bread", [
                {"banana", "flour", "walnut"}, {"banana", "flour", "chocolate"},
            ]),
        ]
        for family, ings in groups:
            for ing in ings:
                _make_variant(
                    db, l1_title=family, canonical_ingredients=frozenset(ing),
                )

        beats: list[tuple[int, int]] = []
        run_pass3(
            db=db,
            title_fn=_stub_title_fn(),
            on_group_done=lambda pos, tot: beats.append((pos, tot)),
        )

        # Three multi-variant L1 groups → three beats.
        assert len(beats) == 3
        # Total is constant; position counts up 1..N (in serial order).
        totals = {tot for _pos, tot in beats}
        assert totals == {3}
        positions = [pos for pos, _tot in beats]
        assert sorted(positions) == [1, 2, 3]

    def test_on_group_done_default_is_noop(self) -> None:
        """Omitting on_group_done must not raise (default None path)."""
        db = CatalogDB.in_memory()
        _make_variant(
            db,
            l1_title="pecan pie",
            canonical_ingredients=frozenset({"pecan", "egg", "bourbon"}),
        )
        _make_variant(
            db,
            l1_title="pecan pie",
            canonical_ingredients=frozenset({"pecan", "egg", "maple"}),
        )
        # No on_group_done supplied: must complete cleanly.
        stats = run_pass3(db=db, title_fn=_stub_title_fn())
        assert stats.variants_titled == 2


# --- Determinism contract for the live Ollama path ---


@dataclass
class _RecordingHTTPCall:
    """Capture the request payload sent to urllib so we can assert on it."""

    payload: dict[str, object] = field(default_factory=dict)


def test_ollama_call_pins_temperature_and_seed(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """Live LLM path must use temperature=0 + seed=42 (vwt.24 contract)."""
    import json as _json

    captured = _RecordingHTTPCall()

    class _FakeResponse:
        def __enter__(self) -> _FakeResponse:
            return self

        def __exit__(self, *_: object) -> None:
            return None

        def read(self) -> bytes:
            return b'{"response": "{\\"title\\": \\"Bourbon Pecan Pie\\"}"}'

    def fake_urlopen(req, timeout):  # type: ignore[no-untyped-def]
        captured.payload = _json.loads(req.data)
        return _FakeResponse()

    monkeypatch.setattr(
        "rational_recipes.scrape.pass3_titles.urllib.request.urlopen",
        fake_urlopen,
    )

    title = _ollama_title_call(
        "pecan pie",
        frozenset({"pecan", "bourbon"}),
        frozenset(),
        [frozenset({"pecan", "maple"})],
        model="gemma4:e2b",
    )
    assert title == "Bourbon Pecan Pie"
    options = captured.payload["options"]
    assert isinstance(options, dict)
    assert options["temperature"] == 0.0
    assert options["seed"] == 42


def test_build_default_title_fn_returns_callable() -> None:
    """Smoke-test: factory returns a TitleFn signature without invoking Ollama."""
    fn = build_default_title_fn("gemma4:e2b", base_url="http://nowhere.invalid:1")
    assert callable(fn)


# --- Profiling instrumentation (vwt.29) ---


class TestTimingCollector:
    """The Pass 3 profiling hook plumbs Pass3CallTiming records out of the
    Ollama call path so a profiling driver can inspect per-call timings
    without changing the TitleFn contract."""

    def _fake_response(self, body: bytes) -> object:
        class _R:
            def __enter__(self_inner):  # type: ignore[no-untyped-def]
                return self_inner

            def __exit__(self_inner, *_: object) -> None:
                return None

            def read(self_inner) -> bytes:
                return body

        return _R()

    def test_collector_called_once_per_call_with_ollama_fields(
        self, monkeypatch  # type: ignore[no-untyped-def]
    ) -> None:
        body = (
            b'{"response": "{\\"title\\": \\"Maple Pecan Pie\\"}", '
            b'"total_duration": 1500000000, '
            b'"load_duration": 100000000, '
            b'"prompt_eval_count": 850, '
            b'"prompt_eval_duration": 700000000, '
            b'"eval_count": 9, '
            b'"eval_duration": 200000000}'
        )
        monkeypatch.setattr(
            "rational_recipes.scrape.pass3_titles.urllib.request.urlopen",
            lambda req, timeout: self._fake_response(body),
        )

        collected: list[Pass3CallTiming] = []
        title = _ollama_title_call(
            "pecan pie",
            frozenset({"pecan", "maple", "egg"}),
            frozenset(),
            [frozenset({"pecan", "bourbon", "egg"})],
            model="gemma4:e2b",
            timing_collector=collected.append,
        )
        assert title == "Maple Pecan Pie"
        assert len(collected) == 1
        rec = collected[0]
        assert rec.family == "pecan pie"
        assert rec.sibling_count == 1
        assert rec.success is True
        # Wall-clock fields are populated (>= 0; can't assert specifics).
        assert rec.request_seconds >= 0
        assert rec.prompt_chars > 0
        # Ollama-reported fields are converted from ns to seconds.
        assert rec.ollama_total_seconds == 1.5
        assert rec.ollama_prompt_eval_count == 850
        assert rec.ollama_prompt_eval_seconds == 0.7
        assert rec.ollama_eval_count == 9
        assert rec.ollama_eval_seconds == 0.2
        # db_write_seconds is left for run_pass3 to populate; standalone
        # _ollama_title_call use leaves it at 0.
        assert rec.db_write_seconds == 0.0

    def test_collector_called_on_failure(
        self, monkeypatch  # type: ignore[no-untyped-def]
    ) -> None:
        """A request that errors still yields a timing record so failures
        show up in the histogram (with success=False)."""
        import urllib.error

        def boom(req, timeout):  # type: ignore[no-untyped-def]
            raise urllib.error.URLError("boom")

        monkeypatch.setattr(
            "rational_recipes.scrape.pass3_titles.urllib.request.urlopen", boom
        )

        collected: list[Pass3CallTiming] = []
        title = _ollama_title_call(
            "pecan pie",
            frozenset({"pecan"}),
            frozenset(),
            [frozenset({"pecan", "bourbon"})],
            model="gemma4:e2b",
            timing_collector=collected.append,
        )
        assert title is None
        assert len(collected) == 1
        rec = collected[0]
        assert rec.success is False
        assert rec.ollama_total_seconds is None  # nothing came back
        assert rec.ollama_eval_count is None

    def test_build_default_title_fn_forwards_collector(
        self, monkeypatch  # type: ignore[no-untyped-def]
    ) -> None:
        body = (
            b'{"response": "{\\"title\\": \\"X\\"}", '
            b'"total_duration": 100000000}'
        )
        monkeypatch.setattr(
            "rational_recipes.scrape.pass3_titles.urllib.request.urlopen",
            lambda req, timeout: self._fake_response(body),
        )
        collected: list[Pass3CallTiming] = []
        fn = build_default_title_fn(
            "gemma4:e2b",
            base_url="http://nowhere.invalid:1",
            timing_collector=collected.append,
        )
        fn(
            "pecan pie",
            frozenset({"pecan"}),
            frozenset(),
            [frozenset({"pecan", "maple"})],
        )
        assert len(collected) == 1

class TestPass3Stats:
    def test_run_pass3_records_db_write_time(self) -> None:
        """Even with a stub TitleFn, DB writes are timed and counted."""
        db = CatalogDB.in_memory()
        _make_variant(
            db,
            l1_title="pecan pie",
            canonical_ingredients=frozenset({"pecan", "egg", "bourbon"}),
        )
        _make_variant(
            db,
            l1_title="pecan pie",
            canonical_ingredients=frozenset({"pecan", "egg", "maple"}),
        )

        stats = run_pass3(db=db, title_fn=_stub_title_fn())
        # Two multi-variant rows got DB writes; singletons would also count
        # but we don't have any in this fixture.
        assert stats.db_write_count == 2
        assert stats.db_write_seconds_total >= 0


class TestSummarizePass3Timings:
    def _make(
        self,
        *,
        sibling_count: int,
        request_seconds: float,
        prompt_chars: int = 1000,
        prompt_eval_seconds: float | None = 0.5,
        prompt_eval_count: int | None = 800,
        success: bool = True,
    ) -> Pass3CallTiming:
        return Pass3CallTiming(
            family="x",
            sibling_count=sibling_count,
            prompt_chars=prompt_chars,
            prompt_build_seconds=0.001,
            request_seconds=request_seconds,
            response_parse_seconds=0.001,
            db_write_seconds=0.001,
            success=success,
            ollama_total_seconds=request_seconds,
            ollama_load_seconds=0.0,
            ollama_prompt_eval_count=prompt_eval_count,
            ollama_prompt_eval_seconds=prompt_eval_seconds,
            ollama_eval_count=10,
            ollama_eval_seconds=0.05,
        )

    def test_empty_input(self) -> None:
        assert summarize_pass3_timings([]) == {"count": 0}

    def test_basic_percentiles(self) -> None:
        timings = [
            self._make(sibling_count=2, request_seconds=0.5),
            self._make(sibling_count=10, request_seconds=1.0),
            self._make(sibling_count=50, request_seconds=2.0),
            self._make(sibling_count=100, request_seconds=4.0),
        ]
        s = summarize_pass3_timings(timings)
        assert s["count"] == 4
        assert s["successes"] == 4
        assert s["request_seconds_max"] == 4.0
        assert s["request_seconds_p50"] == 1.5  # midpoint of 1.0 and 2.0
        assert s["request_seconds_total"] == 7.5

    def test_failure_count_separated(self) -> None:
        timings = [
            self._make(sibling_count=2, request_seconds=0.5),
            self._make(sibling_count=2, request_seconds=0.5, success=False),
        ]
        s = summarize_pass3_timings(timings)
        assert s["successes"] == 1
        assert s["failures"] == 1

    def test_sibling_buckets_split_groups(self) -> None:
        timings = [
            self._make(sibling_count=2, request_seconds=0.5),
            self._make(sibling_count=4, request_seconds=0.5),
            self._make(sibling_count=100, request_seconds=4.0),
        ]
        s = summarize_pass3_timings(timings)
        buckets = s["by_sibling_bucket"]
        assert isinstance(buckets, list)
        labels = [b["label"] for b in buckets]
        assert "2-5" in labels
        assert "51-100" in labels
        big = next(b for b in buckets if b["label"] == "51-100")
        assert big["count"] == 1
        assert big["request_seconds_mean"] == 4.0


class TestFormatPass3Summary:
    def test_empty_timings_yields_no_lines(self) -> None:
        stats = Pass3Stats()
        assert format_pass3_summary(stats) == []

    def test_lines_include_key_metrics(self) -> None:
        stats = Pass3Stats()
        stats.timings.append(
            Pass3CallTiming(
                family="x",
                sibling_count=10,
                prompt_chars=2000,
                prompt_build_seconds=0.001,
                request_seconds=1.0,
                response_parse_seconds=0.001,
                db_write_seconds=0.0001,
                success=True,
                ollama_total_seconds=1.0,
                ollama_load_seconds=0.0,
                ollama_prompt_eval_count=800,
                ollama_prompt_eval_seconds=0.5,
                ollama_eval_count=10,
                ollama_eval_seconds=0.05,
            )
        )
        lines = format_pass3_summary(stats)
        joined = "\n".join(lines)
        assert "pass 3 timing" in joined
        assert "pass 3 prompt" in joined
        assert "pass 3 ollama" in joined
        assert "pass 3 overhead" in joined
        assert "pass 3 by siblings" in joined


# --- Validator + escalation + reconstruction (RationalRecipes-wqy) ---


# The six bad titles seen in the catalog (reproduced verbatim from the bead),
# with their L1 family. These MUST all be rejected by the validator.
_KNOWN_BAD_TITLES: tuple[tuple[str, str], ...] = (
    ("Flour Potato", "scalloped potatoes"),
    ("Egg Potato Salad", "german potato salad"),
    ("Nutmeg Apple Cake", "fresh apple cake"),
    ("Oatmeal Peanut Butter Cookies", "no bake cookies"),
    ("Flour Pie", "sweet potato pie"),
    ("Simple Chicken", "mexican chicken"),
)

# A representative sample from the 844 good titles in the current catalog —
# every one of these MUST pass the validator.
_KNOWN_GOOD_TITLES: tuple[tuple[str, str], ...] = (
    ("Bourbon Pecan Pie", "pecan pie"),
    ("Maple Pecan Pie", "pecan pie"),
    ("Pecan Pie", "pecan pie"),  # bare-family case (legitimate per prompt)
    ("Cocoa Chocolate Cake", "chocolate cake"),
    ("Cream Of Mushroom Scalloped Potatoes", "scalloped potatoes"),
    ("Onion German Potato Salad", "german potato salad"),
    ("Cinnamon Fresh Apple Cake", "fresh apple cake"),
    ("Walnut No Bake Cookies", "no bake cookies"),
    ("Vanilla Sweet Potato Pie", "sweet potato pie"),
    ("Tomato Mexican Chicken", "mexican chicken"),
)


class TestValidateTitleEndsWithFamily:
    def test_known_bad_titles_rejected(self) -> None:
        for title, family in _KNOWN_BAD_TITLES:
            assert not validate_title_ends_with_family(title, family), (
                f"validator should reject {title!r} for family {family!r}"
            )

    def test_known_good_titles_accepted(self) -> None:
        for title, family in _KNOWN_GOOD_TITLES:
            assert validate_title_ends_with_family(title, family), (
                f"validator should accept {title!r} for family {family!r}"
            )

    def test_case_insensitive(self) -> None:
        assert validate_title_ends_with_family("BOURBON PECAN PIE", "pecan pie")
        assert validate_title_ends_with_family("bourbon Pecan Pie", "PECAN PIE")

    def test_none_rejected(self) -> None:
        assert not validate_title_ends_with_family(None, "pecan pie")

    def test_empty_rejected(self) -> None:
        assert not validate_title_ends_with_family("", "pecan pie")

    def test_word_boundary_required(self) -> None:
        """A substring match in the middle of a word must NOT pass —
        otherwise 'Cuttiepie' would falsely validate against family 'pie'."""
        assert not validate_title_ends_with_family("Cuttiepie", "pie")
        assert not validate_title_ends_with_family("Apppie", "pie")
        # But the legitimate single-word case still passes.
        assert validate_title_ends_with_family("Pie", "pie")
        assert validate_title_ends_with_family("Apple Pie", "pie")


class TestExtractDescriptor:
    def test_six_known_bad_titles_salvage(self) -> None:
        """All six bad titles in the bead must reconstruct to the
        intended salvaged form once the family is appended."""
        expected = {
            ("Flour Potato", "scalloped potatoes"): "Flour Scalloped Potatoes",
            ("Egg Potato Salad", "german potato salad"): (
                "Egg German Potato Salad"
            ),
            ("Nutmeg Apple Cake", "fresh apple cake"): (
                "Nutmeg Fresh Apple Cake"
            ),
            ("Oatmeal Peanut Butter Cookies", "no bake cookies"): (
                "Oatmeal Peanut Butter No Bake Cookies"
            ),
            ("Flour Pie", "sweet potato pie"): "Flour Sweet Potato Pie",
            ("Simple Chicken", "mexican chicken"): "Simple Mexican Chicken",
        }
        for (title, family), reconstructed in expected.items():
            descriptor = _extract_descriptor(title, family)
            assert descriptor is not None, f"failed to salvage {title!r}"
            assert (
                f"{descriptor} {family.title()}" == reconstructed
            ), f"unexpected salvage for {title!r}"

    def test_no_descriptor_returns_none(self) -> None:
        """When the bad title is just a family word (e.g. 'Potato' for
        family 'scalloped potatoes'), descriptor extraction returns None
        and the dedup pipeline takes over."""
        assert _extract_descriptor("Potato", "scalloped potatoes") is None
        assert _extract_descriptor("Pie", "pecan pie") is None
        assert _extract_descriptor("Pecan Pie", "pecan pie") is None

    def test_plural_singular_match(self) -> None:
        """'potato' should match 'potatoes' (and vice versa)."""
        assert _extract_descriptor("Flour Potato", "scalloped potatoes") == "Flour"
        assert _extract_descriptor("Flour Potatoes", "scalloped potato") == "Flour"


# A configurable stub that lets tests pretend to be the LLM. It returns
# whatever the test sets ``output`` to without inspecting context.
def _make_constant_title_fn(output: str | None) -> TitleFn:
    def fn(
        family: str,
        my: frozenset[str],
        methods: frozenset[str],
        siblings: Sequence[frozenset[str]],
    ) -> str | None:
        return output

    return fn


class TestEscalationAndReconstruction:
    """Wires up two stub TitleFns to exercise the full validate→escalate
    →salvage pipeline added in RationalRecipes-wqy."""

    def _make_collision_db(self) -> CatalogDB:
        """Two variants in the same L1 family — enough for run_pass3 to
        actually invoke the title fns (singletons skip the LLM)."""
        db = CatalogDB.in_memory()
        _make_variant(
            db,
            l1_title="scalloped potatoes",
            canonical_ingredients=frozenset({"potato", "flour", "milk"}),
        )
        _make_variant(
            db,
            l1_title="scalloped potatoes",
            canonical_ingredients=frozenset({"potato", "cheese", "cream"}),
        )
        return db

    def test_primary_valid_no_escalation(self) -> None:
        """When primary returns a title that passes the validator, the
        fallback must NOT be invoked."""
        db = self._make_collision_db()
        primary = _make_constant_title_fn("Flour Scalloped Potatoes")
        fallback_calls: list[str] = []

        def fallback(
            family: str,
            my: frozenset[str],
            methods: frozenset[str],
            siblings: Sequence[frozenset[str]],
        ) -> str | None:
            fallback_calls.append(family)
            return "should not be invoked"

        stats = run_pass3(
            db=db, title_fn=primary, fallback_title_fn=fallback,
        )
        assert fallback_calls == []
        assert stats.escalations == 0
        assert stats.validation_failures_primary == 0
        assert stats.validation_failures_fallback == 0
        assert stats.reconstructed_titles == 0

    def test_primary_malformed_fallback_succeeds(self) -> None:
        """Primary returns a malformed title, fallback returns a valid
        one — final stored title is the fallback's, escalations == 2
        (one per variant)."""
        db = self._make_collision_db()
        primary = _make_constant_title_fn("Flour Potato")  # invalid
        fallback = _make_constant_title_fn("Flour Scalloped Potatoes")  # valid

        stats = run_pass3(
            db=db, title_fn=primary, fallback_title_fn=fallback,
        )
        # Both variants got the (deduped) fallback title.
        titles = sorted({v.display_title for v in db.list_variants()})
        assert "Scalloped Potatoes" not in titles
        # All titles end with the family.
        for title in titles:
            assert title.lower().endswith("scalloped potatoes")
        assert stats.validation_failures_primary == 2
        assert stats.escalations == 2
        assert stats.validation_failures_fallback == 0
        assert stats.reconstructed_titles == 0

    def test_double_failure_reconstructs(self) -> None:
        """Both primary AND fallback return malformed 'Bourbon Potato' —
        reconstruction must yield 'Bourbon Scalloped Potatoes' and
        increment reconstructed_titles. (Uses a non-stop-list descriptor
        so the 0ki post-pass leaves the reconstruction intact.)"""
        db = self._make_collision_db()
        primary = _make_constant_title_fn("Bourbon Potato")
        fallback = _make_constant_title_fn("Bourbon Potato")

        stats = run_pass3(
            db=db, title_fn=primary, fallback_title_fn=fallback,
        )
        # Every stored title ends with the (Title-cased) family name.
        for v in db.list_variants():
            assert v.display_title.lower().endswith("scalloped potatoes")
            assert "Bourbon" in v.display_title
        assert stats.validation_failures_primary == 2
        assert stats.escalations == 2
        assert stats.validation_failures_fallback == 2
        assert stats.reconstructed_titles == 2
        assert stats.stop_list_substitutions == 0

    def test_double_failure_no_descriptor_falls_through_to_dedup(self) -> None:
        """Both models return just 'Potato' (only a family word, no
        descriptor) → reconstruction returns None → _deduplicate_titles
        picks ingredient-based names."""
        db = self._make_collision_db()
        primary = _make_constant_title_fn("Potato")
        fallback = _make_constant_title_fn("Potato")

        stats = run_pass3(
            db=db, title_fn=primary, fallback_title_fn=fallback,
        )
        titles = sorted({v.display_title for v in db.list_variants()})
        # Two distinct titles, both ending with the family.
        assert len(titles) == 2
        for title in titles:
            assert title.lower().endswith("scalloped potatoes")
        # No reconstruction — descriptor extraction returned None.
        assert stats.reconstructed_titles == 0
        assert stats.validation_failures_primary == 2
        assert stats.validation_failures_fallback == 2
        # Dedup pipeline produced the names — variants_deduped reflects
        # at least one rename relative to the bare family.
        assert stats.variants_deduped > 0

    def test_no_fallback_configured_salvages_from_primary(self) -> None:
        """When no fallback is wired up, a malformed primary title still
        gets salvaged via reconstruction (no escalation though)."""
        db = self._make_collision_db()
        primary = _make_constant_title_fn("Flour Potato")

        stats = run_pass3(db=db, title_fn=primary)
        for v in db.list_variants():
            assert v.display_title.lower().endswith("scalloped potatoes")
        assert stats.validation_failures_primary == 2
        assert stats.escalations == 0
        assert stats.validation_failures_fallback == 0
        assert stats.reconstructed_titles == 2

    def test_primary_none_triggers_fallback(self) -> None:
        """A None response from primary (e.g. HTTP error) escalates to
        the fallback, same as a malformed string would."""
        db = self._make_collision_db()
        primary = _make_constant_title_fn(None)
        fallback = _make_constant_title_fn("Flour Scalloped Potatoes")

        stats = run_pass3(
            db=db, title_fn=primary, fallback_title_fn=fallback,
        )
        for v in db.list_variants():
            assert v.display_title.lower().endswith("scalloped potatoes")
        assert stats.llm_failures == 2  # primary returned None
        assert stats.validation_failures_primary == 0  # not a malformed string
        assert stats.escalations == 2


# --- Ambiguous-family dish-type suffix (RationalRecipes-bt9e) ----------


class TestApplyAmbiguousSuffix:
    """Pure-function checks for the post-LLM dish-type suffix."""

    def test_chili_soup_appends_suffix(self) -> None:
        assert (
            apply_ambiguous_suffix("Celery Chili", "chili", "soup")
            == "Celery Chili Soup"
        )

    def test_no_entry_for_family_unchanged(self) -> None:
        assert (
            apply_ambiguous_suffix(
                "Spice Pumpkin Bread", "pumpkin bread", "bread"
            )
            == "Spice Pumpkin Bread"
        )

    def test_category_not_in_inner_dict_unchanged(self) -> None:
        # Even though 'chili' is an ambiguous family, only the 'soup'
        # category triggers a suffix; other categories pass through.
        assert (
            apply_ambiguous_suffix("Celery Chili", "chili", "condiment")
            == "Celery Chili"
        )

    def test_none_category_returns_unchanged(self) -> None:
        assert (
            apply_ambiguous_suffix("Celery Chili", "chili", None)
            == "Celery Chili"
        )

    def test_idempotent(self) -> None:
        """Re-applying must not double the suffix — Pass 3 re-runs with
        ``force=True`` would otherwise produce 'Chili Soup Soup'."""
        assert (
            apply_ambiguous_suffix("Celery Chili Soup", "chili", "soup")
            == "Celery Chili Soup"
        )

    def test_idempotent_case_insensitive(self) -> None:
        assert (
            apply_ambiguous_suffix("celery chili soup", "chili", "soup")
            == "celery chili soup"
        )

    def test_lookup_contains_chili_soup_seed(self) -> None:
        """The initial seed entry from the bead must remain intact."""
        assert AMBIGUOUS_FAMILY_SUFFIXES["chili"]["soup"] == " Soup"


class TestRunPass3AmbiguousFamilySuffix:
    """End-to-end checks: run_pass3 must apply the suffix to LLM-validated
    titles and to singleton fallbacks alike."""

    def test_chili_soup_multi_variant_gets_suffix(self) -> None:
        """family='chili', category='soup' → titles end in ' Soup' and the
        LLM-picked descriptor is preserved before the suffix."""
        db = CatalogDB.in_memory()
        _make_variant(
            db,
            l1_title="chili",
            canonical_ingredients=frozenset({"celery", "tomato", "beef"}),
            category="soup",
        )
        _make_variant(
            db,
            l1_title="chili",
            canonical_ingredients=frozenset({"pepper", "tomato", "beef"}),
            category="soup",
        )
        run_pass3(db=db, title_fn=_stub_title_fn())
        titles = {v.display_title for v in db.list_variants()}
        for t in titles:
            assert t is not None
            assert t.endswith(" Soup"), f"expected ' Soup' suffix on {t!r}"
        assert "Celery Chili Soup" in titles
        assert "Pepper Chili Soup" in titles

    def test_chili_soup_singleton_gets_suffix(self) -> None:
        """The singleton path skips the LLM but must still suffix."""
        db = CatalogDB.in_memory()
        _make_variant(
            db,
            l1_title="chili",
            canonical_ingredients=frozenset({"tomato", "beef", "bean"}),
            category="soup",
        )
        run_pass3(db=db, title_fn=_stub_title_fn())
        v = db.list_variants()[0]
        assert v.display_title == "Chili Soup"

    def test_pumpkin_bread_no_suffix(self) -> None:
        """family='pumpkin bread' has no entry in the lookup — the title
        must not get a suffix regardless of category."""
        db = CatalogDB.in_memory()
        _make_variant(
            db,
            l1_title="pumpkin bread",
            canonical_ingredients=frozenset({"flour", "pumpkin", "sugar"}),
            category="bread",
        )
        _make_variant(
            db,
            l1_title="pumpkin bread",
            canonical_ingredients=frozenset({"flour", "pumpkin", "molasses"}),
            category="bread",
        )
        run_pass3(db=db, title_fn=_stub_title_fn())
        for v in db.list_variants():
            assert v.display_title is not None
            # No bonus suffix; the title still ends in the family name.
            assert v.display_title.lower().endswith("pumpkin bread")
            assert " Soup" not in v.display_title

    def test_chili_non_soup_category_no_suffix(self) -> None:
        """family='chili' with a non-'soup' category (e.g. condiment) must
        not pick up the suffix — only the {chili: soup} entry triggers."""
        db = CatalogDB.in_memory()
        # categorize('chili') returns 'soup' by default but the catalog
        # may legitimately label some chili variants as condiment/sauce
        # (the very ambiguity that motivates this bead). Force category
        # explicitly so we exercise the inner-key non-match branch.
        _make_variant(
            db,
            l1_title="chili",
            canonical_ingredients=frozenset({"celery", "tomato", "vinegar"}),
            category="condiment",
        )
        _make_variant(
            db,
            l1_title="chili",
            canonical_ingredients=frozenset({"pepper", "tomato", "vinegar"}),
            category="condiment",
        )
        run_pass3(db=db, title_fn=_stub_title_fn())
        for v in db.list_variants():
            assert v.display_title is not None
            assert not v.display_title.endswith(" Soup")
            # The LLM-picked descriptor + family is intact.
            assert v.display_title.lower().endswith("chili")


# --- Stop-list descriptor substitution (RationalRecipes-0ki) -----------


class TestStopListDescriptorsSeed:
    """The stop-list must contain every descriptor called out in the bead."""

    def test_known_stop_list_entries(self) -> None:
        for descriptor in (
            "water",
            "flour",
            "sugar",
            "white sugar",
            "soda",
            "baking soda",
            "baking powder",
            "salt",
            "oil",
            "vegetable oil",
            "shortening",
        ):
            assert descriptor in STOP_LIST_DESCRIPTORS


class TestSubstituteStopListDescriptor:
    """Pure-function checks for the post-LLM stop-list cleanup."""

    def test_water_substituted_with_lemon(self) -> None:
        title, substituted = _substitute_stop_list_descriptor(
            "Water Punch", "punch", ("water", "lemon"),
        )
        assert title == "Lemon Punch"
        assert substituted is True

    def test_only_water_no_alternative_falls_back(self) -> None:
        """No non-stop-list ingredient available → keep the original title.
        A generic descriptor still beats a bare-family collision."""
        title, substituted = _substitute_stop_list_descriptor(
            "Water Punch", "punch", ("water",),
        )
        assert title == "Water Punch"
        assert substituted is False

    def test_baking_soda_substituted_with_pecan(self) -> None:
        """Multi-word stop-list phrase replaced wholesale by a stats
        ingredient when nothing else differentiates."""
        title, substituted = _substitute_stop_list_descriptor(
            "Baking Soda Zucchini Bread",
            "zucchini bread",
            ("baking soda", "pecan"),
        )
        assert title == "Pecan Zucchini Bread"
        assert substituted is True

    def test_drops_stop_list_keeps_distinctive_remainder(self) -> None:
        """When the descriptor mixes a stop-list phrase with a real
        descriptor, drop just the generic part. Mirrors the actual
        production case 'Pecan Baking Soda Zucchini Bread'."""
        title, substituted = _substitute_stop_list_descriptor(
            "Pecan Baking Soda Zucchini Bread",
            "zucchini bread",
            ("pecan", "baking soda"),
        )
        assert title == "Pecan Zucchini Bread"
        assert substituted is True

    def test_white_sugar_keeps_butter(self) -> None:
        title, substituted = _substitute_stop_list_descriptor(
            "White Sugar Butter Peanut Butter Cookies",
            "peanut butter cookies",
            ("white sugar", "butter"),
        )
        assert title == "Butter Peanut Butter Cookies"
        assert substituted is True

    def test_vegetable_oil_substituted(self) -> None:
        title, substituted = _substitute_stop_list_descriptor(
            "Vegetable Oil Fresh Apple Cake",
            "fresh apple cake",
            ("vegetable oil", "cinnamon", "apple"),
        )
        assert title == "Cinnamon Fresh Apple Cake"
        assert substituted is True

    def test_non_stop_list_descriptor_unchanged(self) -> None:
        """A title with a distinctive descriptor passes through intact."""
        title, substituted = _substitute_stop_list_descriptor(
            "Bourbon Pecan Pie", "pecan pie", ("bourbon", "pecan"),
        )
        assert title == "Bourbon Pecan Pie"
        assert substituted is False

    def test_bare_family_title_unchanged(self) -> None:
        title, substituted = _substitute_stop_list_descriptor(
            "Pecan Pie", "pecan pie", ("pecan",),
        )
        assert title == "Pecan Pie"
        assert substituted is False

    def test_stop_list_only_with_only_family_in_stats(self) -> None:
        """When the only non-stop-list ingredients are family words
        (e.g. 'potato' for family 'scalloped potatoes'), there's no
        valid alternative — fall back to the original title."""
        title, substituted = _substitute_stop_list_descriptor(
            "Flour Scalloped Potatoes",
            "scalloped potatoes",
            ("flour", "potato"),
        )
        assert title == "Flour Scalloped Potatoes"
        assert substituted is False

    def test_skips_replacement_already_in_descriptor(self) -> None:
        """If the only candidate replacement is already present elsewhere
        in the descriptor, skip it and try the next one."""
        title, substituted = _substitute_stop_list_descriptor(
            "Pecan Flour Pie", "pie", ("pecan", "flour", "walnut"),
        )
        # 'flour' (stop-list) gets dropped → cleaned=['Pecan'] → no
        # need to dip into stats since something distinctive survives.
        assert title == "Pecan Pie"
        assert substituted is True

    def test_soda_alias_for_baking_soda(self) -> None:
        """'soda' alone is on the stop-list (covers the LLM emitting the
        canonicalized short form before the 0hq fold normalizes it)."""
        title, substituted = _substitute_stop_list_descriptor(
            "Soda Peanut Butter Cookies",
            "peanut butter cookies",
            ("baking soda", "molasses"),
        )
        assert title == "Molasses Peanut Butter Cookies"
        assert substituted is True


class TestRunPass3StopListSubstitution:
    """End-to-end checks: run_pass3 wires the substitution into the
    pipeline AFTER ``_resolve_title`` and BEFORE ``apply_ambiguous_suffix``,
    and the counter on Pass3Stats tracks each fired substitution."""

    def test_counter_increments_per_substitution(self) -> None:
        """Two variants with stop-list descriptors → two substitutions."""
        db = CatalogDB.in_memory()
        _make_variant(
            db,
            l1_title="punch",
            canonical_ingredients=frozenset({"water", "lemon"}),
        )
        _make_variant(
            db,
            l1_title="punch",
            canonical_ingredients=frozenset({"water", "orange"}),
        )

        # Stub returns "Water Punch" for every variant — exactly the
        # production failure mode the bead targets.
        def water_fn(
            family: str,
            my: frozenset[str],
            methods: frozenset[str],
            siblings: Sequence[frozenset[str]],
        ) -> str | None:
            return f"Water {family.title()}"

        stats = run_pass3(db=db, title_fn=water_fn)
        assert stats.stop_list_substitutions == 2
        titles = {v.display_title for v in db.list_variants()}
        for t in titles:
            assert "Water" not in t, f"stop-list 'Water' should be substituted ({t})"
            assert t.endswith("Punch")
        assert "Lemon Punch" in titles
        assert "Orange Punch" in titles

    def test_no_substitution_when_descriptor_distinctive(self) -> None:
        """Distinctive descriptors → counter stays at zero."""
        db = CatalogDB.in_memory()
        _make_variant(
            db,
            l1_title="pecan pie",
            canonical_ingredients=frozenset({"pecan", "egg", "bourbon"}),
        )
        _make_variant(
            db,
            l1_title="pecan pie",
            canonical_ingredients=frozenset({"pecan", "egg", "maple"}),
        )

        stats = run_pass3(db=db, title_fn=_stub_title_fn())
        assert stats.stop_list_substitutions == 0
        titles = {v.display_title for v in db.list_variants()}
        assert "Bourbon Pecan Pie" in titles
        assert "Maple Pecan Pie" in titles

    def test_substitution_runs_before_ambiguous_suffix(self) -> None:
        """For an ambiguous family ('chili' → ' Soup' suffix), the 0ki
        substitution must fire on the descriptor BEFORE bt9e adds the
        suffix — so the final title is e.g. 'Beef Chili Soup', not
        'Water Chili Soup'."""
        db = CatalogDB.in_memory()
        _make_variant(
            db,
            l1_title="chili",
            canonical_ingredients=frozenset({"water", "beef", "tomato"}),
            category="soup",
        )
        _make_variant(
            db,
            l1_title="chili",
            canonical_ingredients=frozenset({"water", "pepper", "tomato"}),
            category="soup",
        )

        # Stub returns "Water Chili" — the bt9e suffix would turn that
        # into "Water Chili Soup" without 0ki.
        def water_fn(
            family: str,
            my: frozenset[str],
            methods: frozenset[str],
            siblings: Sequence[frozenset[str]],
        ) -> str | None:
            return f"Water {family.title()}"

        stats = run_pass3(db=db, title_fn=water_fn)
        assert stats.stop_list_substitutions == 2
        titles = {v.display_title for v in db.list_variants()}
        for t in titles:
            assert t.endswith(" Soup")
            assert "Water" not in t


# --- Family-name display overrides (RationalRecipes-ec1a) -------------


class TestApplyFamilyDisplayOverride:
    """Pure-function checks for the post-bt9e family-name display rewrite."""

    def test_descriptor_prefixed_title_hyphenated(self) -> None:
        assert (
            apply_family_display_override(
                "Ginger Bread And Butter Pickles", "bread and butter pickles"
            )
            == "Ginger Bread-and-Butter Pickles"
        )

    def test_two_word_descriptor_prefix_preserved(self) -> None:
        assert (
            apply_family_display_override(
                "White Onion Bread And Butter Pickles",
                "bread and butter pickles",
            )
            == "White Onion Bread-and-Butter Pickles"
        )

    def test_singleton_bare_family_overridden(self) -> None:
        """Singleton path hands ``family.title()`` to the override; the
        result is the canonical hyphenated form with no descriptor."""
        assert (
            apply_family_display_override(
                "Bread And Butter Pickles", "bread and butter pickles"
            )
            == "Bread-and-Butter Pickles"
        )

    def test_family_not_in_override_map_unchanged(self) -> None:
        assert (
            apply_family_display_override(
                "Spice Pumpkin Bread", "pumpkin bread"
            )
            == "Spice Pumpkin Bread"
        )

    def test_idempotent(self) -> None:
        """Re-applying must be a no-op — Pass 3 re-runs with ``force=True``
        would otherwise mangle already-overridden titles."""
        once = apply_family_display_override(
            "Ginger Bread And Butter Pickles", "bread and butter pickles"
        )
        twice = apply_family_display_override(
            once, "bread and butter pickles"
        )
        assert once == twice == "Ginger Bread-and-Butter Pickles"

    def test_idempotent_singleton(self) -> None:
        once = apply_family_display_override(
            "Bread And Butter Pickles", "bread and butter pickles"
        )
        twice = apply_family_display_override(
            once, "bread and butter pickles"
        )
        assert once == twice == "Bread-and-Butter Pickles"

    def test_lookup_contains_seed_entry(self) -> None:
        """The initial seed entry from the bead must remain intact."""
        assert (
            FAMILY_DISPLAY_OVERRIDES["bread and butter pickles"]
            == "Bread-and-Butter Pickles"
        )


class TestRunPass3FamilyDisplayOverride:
    """End-to-end checks: run_pass3 wires ``apply_family_display_override``
    as the final step in both the multi-variant and singleton paths."""

    def test_multi_variant_descriptor_lands_hyphenated(self) -> None:
        db = CatalogDB.in_memory()
        _make_variant(
            db,
            l1_title="bread and butter pickles",
            canonical_ingredients=frozenset(
                {"cucumber", "vinegar", "ginger"}
            ),
            category="condiment",
        )
        _make_variant(
            db,
            l1_title="bread and butter pickles",
            canonical_ingredients=frozenset(
                {"cucumber", "vinegar", "white onion"}
            ),
            category="condiment",
        )
        run_pass3(db=db, title_fn=_stub_title_fn())
        titles = {v.display_title for v in db.list_variants()}
        for t in titles:
            assert t is not None
            assert "Bread-and-Butter Pickles" in t, (
                f"expected hyphenated family in {t!r}"
            )
            assert "Bread And Butter Pickles" not in t
        assert "Ginger Bread-and-Butter Pickles" in titles
        assert "White Onion Bread-and-Butter Pickles" in titles

    def test_singleton_gets_hyphenated_family(self) -> None:
        """Singleton path skips the LLM but must still apply the override."""
        db = CatalogDB.in_memory()
        _make_variant(
            db,
            l1_title="bread and butter pickles",
            canonical_ingredients=frozenset(
                {"cucumber", "vinegar", "sugar"}
            ),
            category="condiment",
        )
        run_pass3(db=db, title_fn=_stub_title_fn())
        v = db.list_variants()[0]
        assert v.display_title == "Bread-and-Butter Pickles"

    def test_unaffected_family_passes_through(self) -> None:
        """A family with no override entry must render with the default
        Title-Case form — the override is opt-in per family."""
        db = CatalogDB.in_memory()
        _make_variant(
            db,
            l1_title="pumpkin bread",
            canonical_ingredients=frozenset({"flour", "pumpkin", "spice"}),
            category="bread",
        )
        _make_variant(
            db,
            l1_title="pumpkin bread",
            canonical_ingredients=frozenset(
                {"flour", "pumpkin", "molasses"}
            ),
            category="bread",
        )
        run_pass3(db=db, title_fn=_stub_title_fn())
        for v in db.list_variants():
            assert v.display_title is not None
            assert v.display_title.endswith("Pumpkin Bread")
            assert "-" not in v.display_title


# Suppress unused-import lint guards for fixtures shared with other suites.
_ = MergedRecipe
_ = Pass3Stats
