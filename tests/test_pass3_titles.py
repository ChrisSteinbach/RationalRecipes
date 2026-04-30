"""Tests for Pass 3 distinctive title generation (bead vwt.24, v97)."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

from rational_recipes.catalog_db import CatalogDB
from rational_recipes.scrape.merge import MergedRecipe
from rational_recipes.scrape.pass3_titles import (
    BatchTitleFn,
    Pass3CallTiming,
    Pass3Stats,
    TitleFn,
    _batched_titles_with_fallback,
    _ollama_batched_title_call,
    _ollama_title_call,
    _VariantSlot,
    build_batched_title_prompt,
    build_default_batch_title_fn,
    build_default_title_fn,
    build_title_prompt,
    format_pass3_summary,
    parse_batched_title_response,
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


def _stub_batch_title_fn() -> BatchTitleFn:
    """Batched stub: pick the first ingredient unique to each variant."""

    def fn(
        family: str,
        slots: Sequence[_VariantSlot],
        all_slots: Sequence[_VariantSlot],
    ) -> list[str | None] | None:
        # Build union of all other variants' ingredients for context.
        all_ingredients = [s.ingredients for s in all_slots]
        titles: list[str | None] = []
        for slot in slots:
            others_union: set[str] = set()
            for other in all_ingredients:
                if other is not slot.ingredients:
                    others_union.update(other)
            distinctive = sorted(slot.ingredients - others_union)
            if distinctive:
                titles.append(f"{distinctive[0].title()} {family.title()}")
            else:
                titles.append(family.title())
        return titles

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


class TestBuildBatchedTitlePrompt:
    def test_includes_family_and_all_variants(self) -> None:
        slots = [
            _VariantSlot("v1", frozenset({"bourbon", "pecan"}), frozenset()),
            _VariantSlot("v2", frozenset({"maple", "pecan"}), frozenset()),
        ]
        prompt = build_batched_title_prompt("pecan pie", slots, slots)
        assert "pecan pie" in prompt
        assert "bourbon" in prompt
        assert "maple" in prompt
        assert "variants" in prompt
        # Full group = no context field.
        assert "other_variants_in_group" not in prompt

    def test_chunked_includes_context(self) -> None:
        all_slots = [
            _VariantSlot("v1", frozenset({"bourbon", "pecan"}), frozenset()),
            _VariantSlot("v2", frozenset({"maple", "pecan"}), frozenset()),
            _VariantSlot("v3", frozenset({"chocolate", "pecan"}), frozenset()),
        ]
        chunk = all_slots[:2]
        prompt = build_batched_title_prompt("pecan pie", chunk, all_slots)
        assert "other_variants_in_group" in prompt
        assert "chocolate" in prompt

    def test_deterministic_serialization(self) -> None:
        slots = [
            _VariantSlot("v1", frozenset({"a", "b"}), frozenset({"c"})),
            _VariantSlot("v2", frozenset({"d", "e"}), frozenset()),
        ]
        a = build_batched_title_prompt("x", slots, slots)
        b = build_batched_title_prompt("x", slots, slots)
        assert a == b

    def test_cooking_methods_included_when_present(self) -> None:
        slots = [
            _VariantSlot("v1", frozenset({"a"}), frozenset({"bake"})),
        ]
        prompt = build_batched_title_prompt("x", slots, slots)
        assert "bake" in prompt
        assert "cooking_methods" in prompt

    def test_cooking_methods_omitted_when_empty(self) -> None:
        slots = [
            _VariantSlot("v1", frozenset({"a"}), frozenset()),
        ]
        prompt = build_batched_title_prompt("x", slots, slots)
        assert "cooking_methods" not in prompt


class TestParseBatchedTitleResponse:
    def test_extracts_titles(self) -> None:
        raw = '{"results": [{"title": "Bourbon Pie"}, {"title": "Maple Pie"}]}'
        assert parse_batched_title_response(raw, 2) == [
            "Bourbon Pie",
            "Maple Pie",
        ]

    def test_returns_none_on_count_mismatch(self) -> None:
        raw = '{"results": [{"title": "X"}]}'
        assert parse_batched_title_response(raw, 2) is None

    def test_returns_none_on_missing_results(self) -> None:
        assert parse_batched_title_response('{"foo": []}', 1) is None

    def test_returns_none_on_empty(self) -> None:
        assert parse_batched_title_response("", 1) is None

    def test_returns_none_on_non_json(self) -> None:
        assert parse_batched_title_response("not json", 1) is None

    def test_handles_extra_text_around_json(self) -> None:
        raw = 'ok: {"results": [{"title": "X"}]} done'
        assert parse_batched_title_response(raw, 1) == ["X"]

    def test_individual_bad_entry_becomes_none(self) -> None:
        raw = '{"results": [{"title": "Good"}, {"bad": true}]}'
        assert parse_batched_title_response(raw, 2) == ["Good", None]

    def test_collapses_whitespace(self) -> None:
        raw = '{"results": [{"title": "  Maple   Pie  "}]}'
        assert parse_batched_title_response(raw, 1) == ["Maple Pie"]


class TestBatchedTitlesWithFallback:
    def test_success_returns_titles(self) -> None:
        slots = [
            _VariantSlot("v1", frozenset({"a"}), frozenset()),
            _VariantSlot("v2", frozenset({"b"}), frozenset()),
        ]

        def ok_fn(
            family: str,
            s: Sequence[_VariantSlot],
            a: Sequence[_VariantSlot],
        ) -> list[str | None] | None:
            return [f"{sl.variant_id} Title" for sl in s]

        result = _batched_titles_with_fallback("x", slots, slots, ok_fn)
        assert result == ["v1 Title", "v2 Title"]

    def test_failure_bisects(self) -> None:
        slots = [
            _VariantSlot("v1", frozenset({"a"}), frozenset()),
            _VariantSlot("v2", frozenset({"b"}), frozenset()),
        ]
        call_count = 0

        def fail_then_ok(
            family: str,
            s: Sequence[_VariantSlot],
            a: Sequence[_VariantSlot],
        ) -> list[str | None] | None:
            nonlocal call_count
            call_count += 1
            if len(s) > 1:
                return None  # Fail on batch
            return [f"{s[0].variant_id} Title"]

        result = _batched_titles_with_fallback("x", slots, slots, fail_then_ok)
        assert result == ["v1 Title", "v2 Title"]
        assert call_count == 3  # 1 failed batch + 2 successful singles

    def test_total_failure_returns_nones(self) -> None:
        slots = [_VariantSlot("v1", frozenset({"a"}), frozenset())]

        def always_fail(
            family: str,
            s: Sequence[_VariantSlot],
            a: Sequence[_VariantSlot],
        ) -> list[str | None] | None:
            return None

        result = _batched_titles_with_fallback("x", slots, slots, always_fail)
        assert result == [None]

    def test_empty_slots(self) -> None:
        def noop(
            family: str,
            s: Sequence[_VariantSlot],
            a: Sequence[_VariantSlot],
        ) -> list[str | None] | None:
            return []

        result = _batched_titles_with_fallback("x", [], [], noop)
        assert result == []


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
        for v in db.list_variants():
            assert v.display_title == "X Pecan Pie"

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
        for v in db.list_variants():
            # Fallback is the L1 family name in Title Case.
            assert v.display_title == "Pecan Pie"

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


# --- Batched run_pass3 tests (v97) ---


class TestRunPass3Batched:
    def test_singleton_skips_llm(self) -> None:
        db = CatalogDB.in_memory()
        _make_variant(
            db,
            l1_title="banana bread",
            canonical_ingredients=frozenset({"flour", "banana"}),
        )

        calls: list[str] = []

        def fn(
            family: str,
            slots: Sequence[_VariantSlot],
            all_slots: Sequence[_VariantSlot],
        ) -> list[str | None] | None:
            calls.append(family)
            return [f"X {family.title()}" for _ in slots]

        stats = run_pass3(db=db, batch_title_fn=fn)
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

        stats = run_pass3(db=db, batch_title_fn=_stub_batch_title_fn())

        variants = db.list_variants()
        titles = {v.display_title for v in variants}
        assert len(titles) == 3
        assert "Bourbon Pecan Pie" in titles
        assert "Maple Pecan Pie" in titles
        assert "Chocolate Pecan Pie" in titles
        assert stats.variants_titled == 3
        # 1 LLM call for the whole group (< _MAX_BATCH_SIZE).
        assert stats.llm_calls == 1
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

        first = run_pass3(db=db, batch_title_fn=_stub_batch_title_fn())

        def bad_fn(
            family: str,
            slots: Sequence[_VariantSlot],
            all_slots: Sequence[_VariantSlot],
        ) -> list[str | None] | None:
            return ["SHOULD NOT APPEAR" for _ in slots]

        second = run_pass3(db=db, batch_title_fn=bad_fn)
        after = {v.variant_id: v.display_title for v in db.list_variants()}
        assert all("SHOULD NOT APPEAR" not in t for t in after.values())
        assert first.variants_titled == 2
        assert second.variants_titled == 0
        assert second.variants_skipped == 2

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

        run_pass3(db=db, batch_title_fn=_stub_batch_title_fn())

        def override_fn(
            family: str,
            slots: Sequence[_VariantSlot],
            all_slots: Sequence[_VariantSlot],
        ) -> list[str | None] | None:
            return [f"X {family.title()}" for _ in slots]

        stats = run_pass3(db=db, batch_title_fn=override_fn, force=True)
        assert stats.variants_titled == 2
        for v in db.list_variants():
            assert v.display_title == "X Pecan Pie"

    def test_falls_back_to_family_on_total_failure(self) -> None:
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

        def always_fail(
            family: str,
            slots: Sequence[_VariantSlot],
            all_slots: Sequence[_VariantSlot],
        ) -> list[str | None] | None:
            return None

        stats = run_pass3(db=db, batch_title_fn=always_fail)
        assert stats.llm_failures == 2
        for v in db.list_variants():
            assert v.display_title == "Pecan Pie"

    def test_parallel_workers_match_serial(self) -> None:
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
        run_pass3(
            db=db_serial, batch_title_fn=_stub_batch_title_fn(), max_workers=1,
        )
        serial = {v.variant_id: v.display_title for v in db_serial.list_variants()}

        db_parallel = build()
        run_pass3(
            db=db_parallel, batch_title_fn=_stub_batch_title_fn(), max_workers=4,
        )
        parallel = {
            v.variant_id: v.display_title for v in db_parallel.list_variants()
        }

        assert serial == parallel

    def test_batched_matches_legacy_titles(self) -> None:
        """Batched and legacy per-variant paths produce equivalent titles."""
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
            return db

        db_legacy = build()
        run_pass3(db=db_legacy, title_fn=_stub_title_fn())
        legacy = {v.variant_id: v.display_title for v in db_legacy.list_variants()}

        db_batched = build()
        run_pass3(db=db_batched, batch_title_fn=_stub_batch_title_fn())
        batched = {v.variant_id: v.display_title for v in db_batched.list_variants()}

        assert legacy == batched


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


def test_batched_ollama_call_pins_temperature_and_seed(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """Batched LLM path must use temperature=0 + seed=42 (v97 contract)."""
    import json as _json

    captured = _RecordingHTTPCall()

    class _FakeResponse:
        def __enter__(self) -> _FakeResponse:
            return self

        def __exit__(self, *_: object) -> None:
            return None

        def read(self) -> bytes:
            return (
                b'{"response": "{\\"results\\": '
                b'[{\\"title\\": \\"Bourbon Pecan Pie\\"}, '
                b'{\\"title\\": \\"Maple Pecan Pie\\"}]}"}'
            )

    def fake_urlopen(req, timeout):  # type: ignore[no-untyped-def]
        captured.payload = _json.loads(req.data)
        return _FakeResponse()

    monkeypatch.setattr(
        "rational_recipes.scrape.pass3_titles.urllib.request.urlopen",
        fake_urlopen,
    )

    slots = [
        _VariantSlot("v1", frozenset({"pecan", "bourbon"}), frozenset()),
        _VariantSlot("v2", frozenset({"pecan", "maple"}), frozenset()),
    ]
    titles = _ollama_batched_title_call(
        "pecan pie",
        slots,
        slots,
        model="gemma4:e2b",
    )
    assert titles == ["Bourbon Pecan Pie", "Maple Pecan Pie"]
    options = captured.payload["options"]
    assert isinstance(options, dict)
    assert options["temperature"] == 0.0
    assert options["seed"] == 42


def test_build_default_batch_title_fn_returns_callable() -> None:
    fn = build_default_batch_title_fn(
        "gemma4:e2b", base_url="http://nowhere.invalid:1",
    )
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

    def test_batched_collector_called_with_ollama_fields(
        self, monkeypatch  # type: ignore[no-untyped-def]
    ) -> None:
        body = (
            b'{"response": "{\\"results\\": [{\\"title\\": \\"Maple Pie\\"}]}", '
            b'"total_duration": 2000000000, '
            b'"prompt_eval_count": 500, '
            b'"prompt_eval_duration": 800000000, '
            b'"eval_count": 20, '
            b'"eval_duration": 400000000}'
        )
        monkeypatch.setattr(
            "rational_recipes.scrape.pass3_titles.urllib.request.urlopen",
            lambda req, timeout: self._fake_response(body),
        )
        collected: list[Pass3CallTiming] = []
        slots = [_VariantSlot("v1", frozenset({"maple"}), frozenset())]
        titles = _ollama_batched_title_call(
            "pie",
            slots,
            slots,
            model="gemma4:e2b",
            timing_collector=collected.append,
        )
        assert titles == ["Maple Pie"]
        assert len(collected) == 1
        rec = collected[0]
        assert rec.success is True
        assert rec.ollama_total_seconds == 2.0
        assert rec.ollama_prompt_eval_count == 500
        assert rec.ollama_eval_count == 20

    def test_build_default_batch_title_fn_forwards_collector(
        self, monkeypatch  # type: ignore[no-untyped-def]
    ) -> None:
        body = (
            b'{"response": "{\\"results\\": [{\\"title\\": \\"X\\"}]}", '
            b'"total_duration": 100000000}'
        )
        monkeypatch.setattr(
            "rational_recipes.scrape.pass3_titles.urllib.request.urlopen",
            lambda req, timeout: self._fake_response(body),
        )
        collected: list[Pass3CallTiming] = []
        fn = build_default_batch_title_fn(
            "gemma4:e2b",
            base_url="http://nowhere.invalid:1",
            timing_collector=collected.append,
        )
        slots = [_VariantSlot("v1", frozenset({"pecan"}), frozenset())]
        fn("pecan pie", slots, slots)
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
