"""Round-trip tests for the SQLite catalog backing store (bead vwt.6)."""

from __future__ import annotations

from pathlib import Path

import pytest

from rational_recipes.catalog_db import (
    CatalogDB,
    ListFilters,
    ParsedIngredientRow,
    ParsedLineRow,
    parsed_from_json,
    parsed_to_json,
)
from rational_recipes.scrape.parse import ParsedIngredient
from rational_recipes.scrape.pipeline_merged import (
    MergedNormalizedRow,
    MergedVariantResult,
)


def _row(
    url: str,
    cells: dict[str, str],
    proportions: dict[str, float],
    *,
    corpus: str = "recipenlg",
    title: str = "pannkakor",
) -> MergedNormalizedRow:
    return MergedNormalizedRow(
        url=url,
        title=title,
        corpus=corpus,
        cells=cells,
        proportions=proportions,
    )


def _variant(
    *,
    n_rows: int = 3,
    title: str = "pannkakor",
    cooking_methods: frozenset[str] = frozenset(),
) -> MergedVariantResult:
    rows = [
        _row(
            f"https://example.com/r/{i}",
            {"flour": "100 g", "milk": "250 ml"},
            {"flour": 28.5 + i * 0.01, "milk": 71.5 - i * 0.01},
            corpus="recipenlg" if i % 2 == 0 else "wdc",
            title=title,
        )
        for i in range(n_rows)
    ]
    return MergedVariantResult(
        variant_title=title,
        canonical_ingredients=frozenset({"flour", "milk"}),
        cooking_methods=cooking_methods,
        normalized_rows=rows,
        header_ingredients=["flour", "milk"],
    )


class TestSchema:
    def test_open_is_idempotent(self, tmp_path: Path) -> None:
        path = tmp_path / "recipes.db"
        CatalogDB.open(path).close()
        # Re-opening must succeed without a schema conflict.
        db = CatalogDB.open(path)
        db.close()

    def test_min_sample_size_filter_uses_index(self) -> None:
        db = CatalogDB.in_memory()
        plan = list(
            db.connection.execute(
                "EXPLAIN QUERY PLAN"
                " SELECT * FROM variants WHERE n_recipes >= ?"
                " ORDER BY n_recipes DESC",
                (10,),
            )
        )
        assert any("USING INDEX" in row[3] for row in plan), plan

    def test_category_filter_uses_index(self) -> None:
        db = CatalogDB.in_memory()
        plan = list(
            db.connection.execute(
                "EXPLAIN QUERY PLAN"
                " SELECT * FROM variants WHERE category = ?",
                ("crepes",),
            )
        )
        assert any("USING INDEX" in row[3] for row in plan), plan

    def test_title_order_uses_index(self) -> None:
        db = CatalogDB.in_memory()
        plan = list(
            db.connection.execute(
                "EXPLAIN QUERY PLAN"
                " SELECT * FROM variants ORDER BY normalized_title ASC"
            )
        )
        assert any("USING INDEX" in row[3] for row in plan), plan


class TestUpsertVariant:
    def test_round_trip_preserves_variant_core(self) -> None:
        db = CatalogDB.in_memory()
        variant = _variant(n_rows=4, cooking_methods=frozenset({"stekt"}))

        db.upsert_variant(
            variant,
            l1_key="pannkakor",
            category="crepes",
            description="Thin pancakes.",
            base_ingredient="flour",
            confidence_level=0.95,
        )

        listed = db.list_variants()
        assert len(listed) == 1
        v = listed[0]
        assert v.variant_id == variant.variant_id
        assert v.normalized_title == "pannkakor"
        assert v.display_title == "pannkakor"
        assert v.category == "crepes"
        assert v.description == "Thin pancakes."
        assert v.base_ingredient == "flour"
        assert v.n_recipes == 4
        assert v.cooking_methods == ("stekt",)
        assert v.canonical_ingredient_set == ("flour", "milk")
        assert v.confidence_level == 0.95

    def test_round_trip_writes_member_rows(self) -> None:
        db = CatalogDB.in_memory()
        variant = _variant(n_rows=3)
        db.upsert_variant(variant, l1_key="pannkakor", base_ingredient="flour")

        members = db.get_variant_members(variant.variant_id)
        assert len(members) == 3
        urls = {m.url for m in members}
        assert urls == {
            "https://example.com/r/0",
            "https://example.com/r/1",
            "https://example.com/r/2",
        }

    def test_round_trip_computes_stats_in_fraction_form(self) -> None:
        db = CatalogDB.in_memory()
        variant = _variant(n_rows=3)
        db.upsert_variant(variant, l1_key="pannkakor", base_ingredient="flour")

        stats = db.get_ingredient_stats(variant.variant_id)
        assert [s.canonical_name for s in stats] == ["flour", "milk"]
        # Proportions are fractions (0..1) not percents.
        assert 0.2 < stats[0].mean_proportion < 0.4
        assert 0.6 < stats[1].mean_proportion < 0.8
        assert stats[0].ratio == pytest.approx(1.0)
        assert stats[1].ratio == pytest.approx(
            stats[1].mean_proportion / stats[0].mean_proportion
        )

    def test_upsert_is_idempotent(self) -> None:
        db = CatalogDB.in_memory()
        variant = _variant(n_rows=3)
        db.upsert_variant(variant, l1_key="pannkakor", base_ingredient="flour")
        db.upsert_variant(variant, l1_key="pannkakor", base_ingredient="flour")

        assert len(db.list_variants()) == 1
        assert len(db.get_variant_members(variant.variant_id)) == 3
        assert len(db.get_ingredient_stats(variant.variant_id)) == 2

    def test_upsert_replaces_old_members(self) -> None:
        db = CatalogDB.in_memory()
        first = _variant(n_rows=4)
        db.upsert_variant(first, l1_key="pannkakor", base_ingredient="flour")

        smaller = MergedVariantResult(
            variant_title=first.variant_title,
            canonical_ingredients=first.canonical_ingredients,
            cooking_methods=first.cooking_methods,
            normalized_rows=first.normalized_rows[:2],
            header_ingredients=first.header_ingredients,
        )
        db.upsert_variant(smaller, l1_key="pannkakor", base_ingredient="flour")

        members = db.get_variant_members(first.variant_id)
        assert len(members) == 2

    def test_filter_by_min_sample_size(self) -> None:
        db = CatalogDB.in_memory()
        big = _variant(n_rows=5, title="pannkakor")
        small = _variant(n_rows=2, title="crepes")
        db.upsert_variant(big, l1_key="pannkakor", base_ingredient="flour")
        db.upsert_variant(small, l1_key="crepes", base_ingredient="flour")

        listed = db.list_variants(ListFilters(min_sample_size=3))
        assert {v.normalized_title for v in listed} == {"pannkakor"}

    def test_filter_by_category(self) -> None:
        db = CatalogDB.in_memory()
        a = _variant(title="pannkakor")
        b = _variant(title="bread")
        db.upsert_variant(a, l1_key="pannkakor", category="crepes")
        db.upsert_variant(b, l1_key="bread", category="bread")

        listed = db.list_variants(ListFilters(category="crepes"))
        assert {v.normalized_title for v in listed} == {"pannkakor"}

    def test_filter_by_title_substring_case_insensitive(self) -> None:
        db = CatalogDB.in_memory()
        db.upsert_variant(
            _variant(title="Pannkakor"), l1_key="pannkakor", category="crepes"
        )
        db.upsert_variant(
            _variant(title="sourdough"), l1_key="sourdough", category="bread"
        )

        listed = db.list_variants(ListFilters(title_search="PANN"))
        assert len(listed) == 1
        assert listed[0].normalized_title == "pannkakor"

    def test_drop_variant_hidden_by_default(self) -> None:
        db = CatalogDB.in_memory()
        v = _variant(n_rows=3)
        db.upsert_variant(v, l1_key="pannkakor", base_ingredient="flour")
        with db.connection:
            db.connection.execute(
                "UPDATE variants SET review_status='drop' WHERE variant_id = ?",
                (v.variant_id,),
            )
        assert db.list_variants() == []
        assert len(db.list_variants(ListFilters(include_dropped=True))) == 1


class TestComputeStatsNoLongerFilters:
    """RationalRecipes-70o: the filter moved upstream to pipeline_merged.

    Stats writer now passes through whatever ingredients the variant
    declares — duplicating the filter here would diverge from
    canonical_ingredient_set.
    """

    def test_low_freq_ingredient_in_canonical_yields_stat_row(self) -> None:
        db = CatalogDB.in_memory()
        rows: list[MergedNormalizedRow] = []
        for i in range(20):
            cells = {"flour": "100 g", "milk": "250 ml"}
            props = {"flour": 28.5 + i * 0.01, "milk": 71.5 - i * 0.01}
            if i == 0:
                cells["ketchup"] = "5 g"
                props["ketchup"] = 0.01
            rows.append(_row(f"https://example.com/r/{i}", cells, props))
        # Caller hands us a variant that still has ketchup in canonical
        # (e.g. a test/legacy variant). The writer must not silently
        # drop it — it goes into the stats table as-is.
        variant = MergedVariantResult(
            variant_title="pecan pie",
            canonical_ingredients=frozenset({"flour", "milk", "ketchup"}),
            cooking_methods=frozenset(),
            normalized_rows=rows,
            header_ingredients=["flour", "milk"],
        )
        db.upsert_variant(variant, l1_key="pecan pie", base_ingredient="flour")

        stats = db.get_ingredient_stats(variant.variant_id)
        names = {s.canonical_name for s in stats}
        assert names == {"flour", "milk", "ketchup"}


class TestL1RunTracking:
    def test_is_l1_fresh_false_before_run(self) -> None:
        db = CatalogDB.in_memory()
        assert db.is_l1_fresh("pannkakor", corpus_revisions="abc") is False

    def test_record_l1_run_then_fresh(self) -> None:
        db = CatalogDB.in_memory()
        db.record_l1_run(
            "pannkakor",
            corpus_revisions="rev-abc",
            variants_produced=2,
            dry=False,
            run_at="2026-04-24T10:00:00Z",
        )
        assert db.is_l1_fresh("pannkakor", corpus_revisions="rev-abc") is True

    def test_fingerprint_mismatch_reports_stale(self) -> None:
        db = CatalogDB.in_memory()
        db.record_l1_run(
            "pannkakor",
            corpus_revisions="rev-abc",
            variants_produced=2,
            dry=False,
            run_at="2026-04-24T10:00:00Z",
        )
        assert db.is_l1_fresh("pannkakor", corpus_revisions="rev-xyz") is False

    def test_record_is_idempotent(self) -> None:
        db = CatalogDB.in_memory()
        db.record_l1_run(
            "pannkakor",
            corpus_revisions="rev",
            variants_produced=1,
            dry=False,
            run_at="2026-04-24T10:00:00Z",
        )
        db.record_l1_run(
            "pannkakor",
            corpus_revisions="rev",
            variants_produced=3,
            dry=True,
            run_at="2026-04-24T11:00:00Z",
        )
        row = db.connection.execute(
            "SELECT variants_produced, dry FROM query_runs WHERE l1_group_key = ?",
            ("pannkakor",),
        ).fetchone()
        assert row == (3, 1)


class TestDeleteStaleVariantsForL1:
    """RationalRecipes-dos: re-cluster must drop variants the new policy
    no longer produces, not accumulate them."""

    def test_drops_variants_not_in_keep_set(self) -> None:
        db = CatalogDB.in_memory()
        keeper = _variant(title="pecan pie", n_rows=10)
        # Two variants that survived a prior run but the new policy drops.
        stale_a = MergedVariantResult(
            variant_title="pecan pie",
            canonical_ingredients=frozenset({"flour", "egg"}),
            cooking_methods=frozenset(),
            normalized_rows=[
                _row(
                    "https://x/a/1",
                    {"flour": "100 g", "egg": "1 unit"},
                    {"flour": 90.0, "egg": 10.0},
                    title="pecan pie",
                ),
            ],
            header_ingredients=["flour", "egg"],
        )
        stale_b = MergedVariantResult(
            variant_title="pecan pie",
            canonical_ingredients=frozenset({"flour", "butter"}),
            cooking_methods=frozenset(),
            normalized_rows=[
                _row(
                    "https://x/b/1",
                    {"flour": "100 g", "butter": "50 g"},
                    {"flour": 67.0, "butter": 33.0},
                    title="pecan pie",
                ),
            ],
            header_ingredients=["flour", "butter"],
        )
        db.upsert_variant(keeper, l1_key="pecan pie")
        db.upsert_variant(stale_a, l1_key="pecan pie")
        db.upsert_variant(stale_b, l1_key="pecan pie")

        removed = db.delete_stale_variants_for_l1(
            "pecan pie", keep_variant_ids=[keeper.variant_id]
        )
        assert removed == 2

        remaining = {v.variant_id for v in db.list_variants()}
        assert remaining == {keeper.variant_id}
        # Cascade: stale variants' member rows are gone too.
        assert db.get_variant_members(stale_a.variant_id) == []
        assert db.get_variant_members(stale_b.variant_id) == []

    def test_noop_when_all_kept(self) -> None:
        db = CatalogDB.in_memory()
        v = _variant(title="brownie", n_rows=5)
        db.upsert_variant(v, l1_key="brownie")
        removed = db.delete_stale_variants_for_l1(
            "brownie", keep_variant_ids=[v.variant_id]
        )
        assert removed == 0
        assert len(db.list_variants()) == 1

    def test_other_l1_keys_untouched(self) -> None:
        db = CatalogDB.in_memory()
        a = _variant(title="brownie", n_rows=5)
        b = _variant(title="muffin", n_rows=5)
        db.upsert_variant(a, l1_key="brownie")
        db.upsert_variant(b, l1_key="muffin")
        # Wipe brownie's variants entirely; muffin must survive.
        db.delete_stale_variants_for_l1("brownie", keep_variant_ids=[])
        remaining = {v.variant_id for v in db.list_variants()}
        assert remaining == {b.variant_id}


class TestUpsertRecipe:
    def test_recipe_with_raw_and_parsed_rows(self) -> None:
        db = CatalogDB.in_memory()
        db.upsert_recipe(
            recipe_id="abc123",
            url="https://example.com/r",
            title="Pannkakor",
            corpus="wdc",
            language="sv",
            source_type="url",
            raw_lines=("2 dl vetemjöl", "5 dl mjölk", "3 ägg"),
            parsed=(
                ParsedIngredientRow(
                    canonical_name="flour",
                    quantity=200.0,
                    unit="ml",
                    grams=105.66,
                ),
                ParsedIngredientRow(
                    canonical_name="milk",
                    quantity=500.0,
                    unit="ml",
                    grams=515.6,
                ),
            ),
        )
        raw = db.connection.execute(
            "SELECT raw_line FROM raw_ingredients WHERE recipe_id = ?"
            " ORDER BY line_index",
            ("abc123",),
        ).fetchall()
        assert [r[0] for r in raw] == ["2 dl vetemjöl", "5 dl mjölk", "3 ägg"]

        parsed = db.connection.execute(
            "SELECT canonical_name, quantity, unit, grams"
            " FROM parsed_ingredients WHERE recipe_id = ?"
            " ORDER BY canonical_name",
            ("abc123",),
        ).fetchall()
        assert parsed == [
            ("flour", 200.0, "ml", 105.66),
            ("milk", 500.0, "ml", 515.6),
        ]

    def test_upsert_recipe_replaces_children(self) -> None:
        db = CatalogDB.in_memory()
        db.upsert_recipe(
            recipe_id="r",
            url=None,
            title="x",
            corpus="curated",
            raw_lines=("a", "b"),
            parsed=(ParsedIngredientRow(canonical_name="flour", quantity=1.0),),
        )
        db.upsert_recipe(
            recipe_id="r",
            url=None,
            title="x",
            corpus="curated",
            raw_lines=("c",),
            parsed=(),
        )
        raw = db.connection.execute(
            "SELECT raw_line FROM raw_ingredients WHERE recipe_id = ?",
            ("r",),
        ).fetchall()
        assert [r[0] for r in raw] == ["c"]
        parsed = db.connection.execute(
            "SELECT count(*) FROM parsed_ingredients WHERE recipe_id = ?",
            ("r",),
        ).fetchone()
        assert parsed[0] == 0


class TestUpdateReviewStatus:
    """Bead vwt.9: CLI review persistence via UPDATE."""

    def test_set_drop_hides_from_default_list(self) -> None:
        db = CatalogDB.in_memory()
        v = _variant(n_rows=3)
        db.upsert_variant(v, l1_key="pannkakor", base_ingredient="flour")

        db.update_review_status(v.variant_id, "drop", note="category bleed")

        row = db.connection.execute(
            "SELECT review_status, review_note, reviewed_at"
            " FROM variants WHERE variant_id = ?",
            (v.variant_id,),
        ).fetchone()
        assert row[0] == "drop"
        assert row[1] == "category bleed"
        assert row[2] is not None and row[2].endswith("+00:00")

        # Default list filter hides the dropped variant.
        assert db.list_variants() == []
        assert len(db.list_variants(ListFilters(include_dropped=True))) == 1

    def test_accept_keeps_variant_visible(self) -> None:
        db = CatalogDB.in_memory()
        v = _variant(n_rows=3)
        db.upsert_variant(v, l1_key="pannkakor", base_ingredient="flour")
        db.update_review_status(v.variant_id, "accept")
        assert len(db.list_variants()) == 1

    def test_pending_only_filter_hides_reviewed(self) -> None:
        db = CatalogDB.in_memory()
        a = _variant(n_rows=3, title="pannkakor")
        b = _variant(n_rows=3, title="crepes")
        db.upsert_variant(a, l1_key="pannkakor", base_ingredient="flour")
        db.upsert_variant(b, l1_key="crepes", base_ingredient="flour")
        db.update_review_status(a.variant_id, "accept")
        pending = db.list_variants(ListFilters(pending_only=True))
        assert {v.normalized_title for v in pending} == {"crepes"}

    def test_status_none_clears_decision(self) -> None:
        db = CatalogDB.in_memory()
        v = _variant(n_rows=3)
        db.upsert_variant(v, l1_key="pannkakor", base_ingredient="flour")
        db.update_review_status(v.variant_id, "drop", note="temp")
        db.update_review_status(v.variant_id, None)
        row = db.connection.execute(
            "SELECT review_status, review_note, reviewed_at"
            " FROM variants WHERE variant_id = ?",
            (v.variant_id,),
        ).fetchone()
        assert row == (None, None, None)

    def test_invalid_status_rejected(self) -> None:
        db = CatalogDB.in_memory()
        v = _variant(n_rows=3)
        db.upsert_variant(v, l1_key="pannkakor", base_ingredient="flour")
        with pytest.raises(ValueError, match="invalid review status"):
            db.update_review_status(v.variant_id, "bogus")  # type: ignore[arg-type]


class TestParsedLineCache:
    """vwt.16: parsed_ingredient_lines schema + reader/writer."""

    def _row(
        self,
        *,
        corpus: str = "wdc",
        recipe_id: str = "https://example.com/r/1",
        line_index: int = 0,
        raw_line: str = "1 cup flour",
        parsed_json: str | None = '{"quantity": 1.0, "unit": "cup", '
        '"ingredient": "flour", "preparation": ""}',
        model: str = "qwen3.6:35b-a3b",
        seed: int = 42,
    ) -> ParsedLineRow:
        return ParsedLineRow(
            corpus=corpus,
            recipe_id=recipe_id,
            line_index=line_index,
            raw_line=raw_line,
            parsed_json=parsed_json,
            model=model,
            seed=seed,
        )

    def test_upsert_and_read_back(self) -> None:
        db = CatalogDB.in_memory()
        rows = [
            self._row(line_index=0, raw_line="1 cup flour"),
            self._row(
                line_index=1,
                raw_line="2 eggs",
                parsed_json='{"quantity": 2.0, "unit": "MEDIUM", '
                '"ingredient": "egg", "preparation": ""}',
            ),
        ]
        db.upsert_parsed_lines(rows)

        fetched = db.get_parsed_lines_for_recipe(
            "wdc", "https://example.com/r/1"
        )
        assert [r.line_index for r in fetched] == [0, 1]
        assert fetched[0].raw_line == "1 cup flour"

    def test_upsert_replaces_existing_row(self) -> None:
        db = CatalogDB.in_memory()
        db.upsert_parsed_lines([self._row(line_index=0, raw_line="orig")])
        db.upsert_parsed_lines(
            [self._row(line_index=0, raw_line="overwritten")]
        )

        fetched = db.get_parsed_lines_for_recipe(
            "wdc", "https://example.com/r/1"
        )
        assert len(fetched) == 1
        assert fetched[0].raw_line == "overwritten"

    def test_lookup_cached_parse_hit(self) -> None:
        db = CatalogDB.in_memory()
        db.upsert_parsed_lines([self._row(raw_line="1 cup flour")])

        found, payload = db.lookup_cached_parse(
            "1 cup flour", "qwen3.6:35b-a3b", 42
        )
        assert found is True
        assert payload is not None
        assert "flour" in payload

    def test_lookup_cached_parse_miss(self) -> None:
        db = CatalogDB.in_memory()
        found, payload = db.lookup_cached_parse(
            "never seen", "qwen3.6:35b-a3b", 42
        )
        assert found is False
        assert payload is None

    def test_lookup_distinguishes_cached_failure_from_miss(self) -> None:
        db = CatalogDB.in_memory()
        # NULL parsed_json = cached failure; should NOT trigger LLM retry.
        db.upsert_parsed_lines(
            [self._row(raw_line="garbled line", parsed_json=None)]
        )
        found, payload = db.lookup_cached_parse(
            "garbled line", "qwen3.6:35b-a3b", 42
        )
        assert found is True
        assert payload is None

    def test_lookup_respects_model_and_seed(self) -> None:
        db = CatalogDB.in_memory()
        db.upsert_parsed_lines([self._row(model="qwen3.6:35b-a3b", seed=42)])

        # Different model — must miss.
        assert db.lookup_cached_parse("1 cup flour", "gemma4:e2b", 42) == (
            False,
            None,
        )
        # Different seed — must miss.
        assert db.lookup_cached_parse("1 cup flour", "qwen3.6:35b-a3b", 99) == (
            False,
            None,
        )

    def test_has_parsed_lines_for_recipe(self) -> None:
        db = CatalogDB.in_memory()
        assert not db.has_parsed_lines_for_recipe("wdc", "rid-1")

        db.upsert_parsed_lines(
            [
                self._row(
                    corpus="wdc",
                    recipe_id="rid-1",
                    raw_line="x",
                    parsed_json=None,
                )
            ]
        )
        assert db.has_parsed_lines_for_recipe("wdc", "rid-1")
        assert not db.has_parsed_lines_for_recipe(
            "wdc", "rid-1", model="other-model"
        )

    def test_count_parsed_lines_with_filters(self) -> None:
        db = CatalogDB.in_memory()
        db.upsert_parsed_lines(
            [
                self._row(
                    corpus="wdc", recipe_id="r1", line_index=0, raw_line="a"
                ),
                self._row(
                    corpus="wdc", recipe_id="r1", line_index=1, raw_line="b"
                ),
                self._row(
                    corpus="recipenlg", recipe_id="r2", line_index=0, raw_line="c"
                ),
            ]
        )
        assert db.count_parsed_lines() == 3
        assert db.count_parsed_lines(corpus="wdc") == 2
        assert db.count_parsed_lines(corpus="recipenlg") == 1


class TestInvalidateNonEnglishParses:
    """Bead e4s: drop cached parses whose payload contains non-ASCII bytes."""

    def _row(
        self,
        *,
        recipe_id: str,
        line_index: int,
        raw_line: str,
        parsed_json: str | None,
    ) -> ParsedLineRow:
        return ParsedLineRow(
            corpus="wdc",
            recipe_id=recipe_id,
            line_index=line_index,
            raw_line=raw_line,
            parsed_json=parsed_json,
            model="gemma4:e2b",
            seed=42,
        )

    def test_deletes_only_non_ascii_rows(self) -> None:
        db = CatalogDB.in_memory()
        rows = [
            self._row(
                recipe_id="r-en",
                line_index=0,
                raw_line="1 cup flour",
                parsed_json='{"quantity": 1.0, "unit": "cup", '
                '"ingredient": "flour", "preparation": ""}',
            ),
            self._row(
                recipe_id="r-sv",
                line_index=0,
                raw_line="3 dl vetemjöl",
                parsed_json='{"quantity": 3.0, "unit": "dl", '
                '"ingredient": "vetemjöl", "preparation": ""}',
            ),
            self._row(
                recipe_id="r-ru",
                line_index=0,
                raw_line="молоко 500 мл",
                parsed_json='{"quantity": 500.0, "unit": "мл", '
                '"ingredient": "молоко", "preparation": ""}',
            ),
            self._row(
                recipe_id="r-jp",
                line_index=0,
                raw_line="卵 3個",
                parsed_json='{"quantity": 3.0, "unit": "個", '
                '"ingredient": "卵", "preparation": ""}',
            ),
        ]
        db.upsert_parsed_lines(rows)
        assert db.count_parsed_lines() == 4

        deleted = db.invalidate_non_english_parses()

        assert deleted == 3
        remaining = db.get_parsed_lines_for_recipe("wdc", "r-en")
        assert len(remaining) == 1
        assert remaining[0].raw_line == "1 cup flour"
        # Non-English rows are gone.
        assert db.get_parsed_lines_for_recipe("wdc", "r-sv") == []
        assert db.get_parsed_lines_for_recipe("wdc", "r-ru") == []
        assert db.get_parsed_lines_for_recipe("wdc", "r-jp") == []
        assert db.count_parsed_lines() == 1

    def test_returns_zero_when_no_non_ascii_rows(self) -> None:
        db = CatalogDB.in_memory()
        db.upsert_parsed_lines(
            [
                self._row(
                    recipe_id="r-en",
                    line_index=0,
                    raw_line="1 cup flour",
                    parsed_json='{"quantity": 1.0, "unit": "cup", '
                    '"ingredient": "flour", "preparation": ""}',
                ),
            ]
        )
        assert db.invalidate_non_english_parses() == 0
        assert db.count_parsed_lines() == 1

    def test_skips_cached_failure_rows(self) -> None:
        """NULL parsed_json rows (cached failures) are not deleted."""
        db = CatalogDB.in_memory()
        db.upsert_parsed_lines(
            [
                self._row(
                    recipe_id="r-fail",
                    line_index=0,
                    raw_line="garbled 卵",
                    parsed_json=None,
                ),
            ]
        )
        assert db.invalidate_non_english_parses() == 0
        assert db.count_parsed_lines() == 1


class TestParsedSerialization:
    """vwt.16: parsed_to_json / parsed_from_json round-trip."""

    def test_round_trip_preserves_fields(self) -> None:
        original = ParsedIngredient(
            quantity=1.5,
            unit="cup",
            ingredient="all-purpose flour",
            preparation="sifted",
            raw="1 1/2 cups all-purpose flour, sifted",
        )
        payload = parsed_to_json(original)
        assert payload is not None
        recovered = parsed_from_json(payload, original.raw)
        assert recovered == original

    def test_none_round_trips_to_none(self) -> None:
        assert parsed_to_json(None) is None
        assert parsed_from_json(None, "1 cup flour") is None

    def test_malformed_json_yields_none(self) -> None:
        assert parsed_from_json("not-json", "1 cup flour") is None

    def test_missing_required_keys_yields_none(self) -> None:
        # Missing "quantity".
        assert parsed_from_json('{"unit": "cup"}', "1 cup flour") is None


class TestVariantSources:
    def test_add_and_retrieve(self) -> None:
        db = CatalogDB.in_memory()
        v = _variant(n_rows=2)
        db.upsert_variant(v, l1_key="pannkakor", base_ingredient="flour")
        db.add_variant_source(
            v.variant_id,
            ordinal=0,
            source_type="text",
            ref="Swedish pannkakor recipes.",
            title="Aggregated Swedish recipes",
        )
        sources = db.get_variant_sources(v.variant_id)
        assert len(sources) == 1
        assert sources[0].source_type == "text"
        assert sources[0].title == "Aggregated Swedish recipes"
