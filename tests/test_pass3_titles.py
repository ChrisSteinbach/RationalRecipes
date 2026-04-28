"""Tests for Pass 3 distinctive title generation (bead vwt.24)."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

from rational_recipes.catalog_db import CatalogDB
from rational_recipes.scrape.merge import MergedRecipe
from rational_recipes.scrape.pass3_titles import (
    Pass3Stats,
    TitleFn,
    _ollama_title_call,
    build_default_title_fn,
    build_title_prompt,
    parse_title_response,
    run_pass3,
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
        assert v.display_title == "banana bread"

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
            # Fallback is the L1 family name.
            assert v.display_title == "pecan pie"

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


# Suppress unused-import lint guards for fixtures shared with other suites.
_ = MergedRecipe
_ = Pass3Stats
