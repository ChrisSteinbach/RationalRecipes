"""Tests for Pass 3 distinctive title generation (bead vwt.24)."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

from rational_recipes.catalog_db import CatalogDB
from rational_recipes.scrape.merge import MergedRecipe
from rational_recipes.scrape.pass3_titles import (
    Pass3CallTiming,
    Pass3Stats,
    TitleFn,
    _deduplicate_titles,
    _ollama_title_call,
    _variants_to_slots,
    _VariantSlot,
    build_default_title_fn,
    build_title_prompt,
    format_pass3_summary,
    parse_title_response,
    run_pass3,
    summarize_pass3_timings,
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
    db.upsert_variant(variant, l1_key=l1_title)
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


# Suppress unused-import lint guards for fixtures shared with other suites.
_ = MergedRecipe
_ = Pass3Stats
