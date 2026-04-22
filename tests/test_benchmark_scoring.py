"""Tests for the pure scoring / aggregation logic in scripts/benchmark_models.py.

No network calls — every test synthesizes ModelRun fixtures in-memory. The goal
is to lock down the scoring contract (what counts as a match, how retries
collapse into bands) so that follow-on benchmark sweeps compare apples to
apples.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from benchmark_models import (
    GoldItem,
    LineRun,
    MetricBand,
    ModelBand,
    ModelRun,
    NameSetScore,
    PerFieldScore,
    Score,
    _norm_ingredient_en,
    _norm_ingredient_generic,
    _norm_prep,
    _norm_unit_en,
    _norm_unit_generic,
    _prep_exact_match,
    _prep_jaccard,
    _prep_tokens,
    _score_language,
    _score_pair,
    _score_swedish_name_set,
    aggregate,
    format_per_field_breakdown,
    format_summary_table,
    load_runs,
    save_runs,
    score_run,
)

# ---------------------------------------------------------------------------
# Field normalizers
# ---------------------------------------------------------------------------


class TestNormUnitEn:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("c", "cup"),
            ("c.", "cup"),
            ("cups", "cup"),
            ("tbsp.", "tbsp"),
            ("tablespoon", "tbsp"),
            ("Tsp.", "tsp"),
            ("OUNCES", "oz"),
            ("medium", "MEDIUM"),
            ("LARGE", "LARGE"),
            ("  cup  ", "cup"),
            ("", ""),
            (None, ""),
        ],
    )
    def test_normalizes(self, raw: str | None, expected: str) -> None:
        assert _norm_unit_en(raw) == expected

    def test_unknown_unit_falls_through_lowercased(self) -> None:
        assert _norm_unit_en("Zehe") == "zehe"


class TestNormUnitGeneric:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("TL", "tl"),
            ("EL", "el"),
            ("  г  ", "г"),
            ("大さじ", "大さじ"),
            ("", ""),
            (None, ""),
        ],
    )
    def test_just_strips_lowercases(self, raw: str | None, expected: str) -> None:
        assert _norm_unit_generic(raw) == expected


class TestNormIngredientEn:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("eggs", "egg"),
            ("Eggs ", "egg"),
            ("onions", "onion"),
            ("peas", "pea"),  # vowel+s: regular plural, strip
            ("bananas", "banana"),  # regular plural via -s
            ("mushroom", "mushroom"),  # already singular
            ("cream cheese", "cream cheese"),  # compound, no stripping
            ("tomatoes", "tomato"),  # -oes → -o
            ("dishes", "dish"),  # -shes → -sh
            ("boxes", "box"),  # -xes → -x
            ("cherries", "cherry"),  # -ies → -y
            ("classes", "class"),  # -sses → -ss (via -ses match)
            ("bass", "bass"),  # -ss → keep (not plural)
            ("bus", "bus"),  # len 3 → keep
            ("asparagus", "asparagus"),  # -us → keep
            ("analysis", "analysis"),  # -is → keep
            (None, ""),
            ("", ""),
        ],
    )
    def test_depluralization(self, raw: str | None, expected: str) -> None:
        assert _norm_ingredient_en(raw) == expected


class TestNormIngredientGeneric:
    def test_preserves_source_language(self) -> None:
        assert _norm_ingredient_generic("Knoblauch") == "knoblauch"
        assert _norm_ingredient_generic("молоко") == "молоко"
        assert _norm_ingredient_generic("赤パプリカ") == "赤パプリカ"

    def test_no_depluralization(self) -> None:
        # German plural: 'Eier' is already plural, don't strip
        assert _norm_ingredient_generic("eier") == "eier"


class TestNormPrep:
    def test_strip_lowercase(self) -> None:
        assert _norm_prep("  Chopped  ") == "chopped"
        assert _norm_prep(None) == ""

    def test_tokens_remove_stopwords(self) -> None:
        assert _prep_tokens("peeled and chopped") == frozenset({"peeled", "chopped"})
        assert _prep_tokens("peeled, seeded and cubed") == frozenset(
            {"peeled", "seeded", "cubed"}
        )
        assert _prep_tokens(None) == frozenset()


class TestPrepExactMatch:
    def test_identical(self) -> None:
        assert _prep_exact_match("chopped", "chopped")

    def test_case_insensitive(self) -> None:
        assert _prep_exact_match("CHOPPED", "chopped")

    def test_whitespace_normalized(self) -> None:
        assert _prep_exact_match("  chopped  ", "chopped")

    def test_word_order_matters(self) -> None:
        # Strict: different order = not equal
        assert not _prep_exact_match("sliced thinly", "thinly sliced")

    def test_both_empty(self) -> None:
        assert _prep_exact_match("", "")
        assert _prep_exact_match(None, "")


class TestPrepJaccard:
    def test_both_empty_is_one(self) -> None:
        assert _prep_jaccard("", "") == 1.0
        assert _prep_jaccard(None, "") == 1.0

    def test_one_empty_is_zero(self) -> None:
        assert _prep_jaccard("", "chopped") == 0.0
        assert _prep_jaccard("chopped", "") == 0.0

    def test_exact_match_is_one(self) -> None:
        assert _prep_jaccard("chopped", "chopped") == 1.0

    def test_word_order_equivalent(self) -> None:
        # Jaccard is set-based, so order doesn't matter
        assert _prep_jaccard("sliced thinly", "thinly sliced") == 1.0

    def test_stopwords_ignored(self) -> None:
        # "and" is a stopword, so these should match fully
        assert _prep_jaccard("peeled and chopped", "peeled, chopped") == 1.0

    def test_partial_overlap(self) -> None:
        # {peeled, chopped} vs {peeled, seeded, chopped} → 2/3
        j = _prep_jaccard("peeled, chopped", "peeled, seeded, chopped")
        assert abs(j - 2 / 3) < 1e-9


# ---------------------------------------------------------------------------
# Single-line scoring
# ---------------------------------------------------------------------------


def _run(
    line: str = "x",
    quantity: float | None = 1.0,
    unit: str | None = "cup",
    ingredient: str | None = "flour",
    preparation: str | None = "",
    failed: bool = False,
    latency_s: float = 0.5,
) -> LineRun:
    return LineRun(
        line=line,
        quantity=quantity,
        unit=unit,
        ingredient=ingredient,
        preparation=preparation,
        failed=failed,
        latency_s=latency_s,
    )


def _expected(
    quantity: float = 1.0,
    unit: str = "cup",
    ingredient: str = "flour",
    preparation: str = "",
) -> dict[str, Any]:
    return {
        "quantity": quantity,
        "unit": unit,
        "ingredient": ingredient,
        "preparation": preparation,
    }


class TestScorePair:
    def test_all_match(self) -> None:
        q, u, i_, pe, pj = _score_pair(
            _run(),
            _expected(),
            _norm_unit_en,
            _norm_ingredient_en,
        )
        assert (q, u, i_, pe, pj) == (True, True, True, True, 1.0)

    def test_unit_mismatch(self) -> None:
        q, u, i_, pe, _ = _score_pair(
            _run(unit="tbsp"),
            _expected(unit="cup"),
            _norm_unit_en,
            _norm_ingredient_en,
        )
        assert (q, u, i_, pe) == (True, False, True, True)

    def test_failed_line_scores_zero(self) -> None:
        q, u, i_, pe, pj = _score_pair(
            _run(failed=True),
            _expected(),
            _norm_unit_en,
            _norm_ingredient_en,
        )
        assert (q, u, i_, pe, pj) == (False, False, False, False, 0.0)

    def test_qty_fraction_equality(self) -> None:
        q, _, _, _, _ = _score_pair(
            _run(quantity=0.5),
            _expected(quantity=0.5),
            _norm_unit_en,
            _norm_ingredient_en,
        )
        assert q

    def test_ingredient_depluralization_applies(self) -> None:
        # model says 'eggs', gold says 'egg' — both normalize to 'egg'
        _, _, i_, _, _ = _score_pair(
            _run(ingredient="eggs"),
            _expected(ingredient="egg"),
            _norm_unit_en,
            _norm_ingredient_en,
        )
        assert i_

    def test_prep_jaccard_word_order(self) -> None:
        _, _, _, pe, pj = _score_pair(
            _run(preparation="sliced thinly"),
            _expected(preparation="thinly sliced"),
            _norm_unit_en,
            _norm_ingredient_en,
        )
        assert pe is False  # exact match fails
        assert pj == 1.0  # Jaccard succeeds


# ---------------------------------------------------------------------------
# Corpus scoring
# ---------------------------------------------------------------------------


class TestScoreLanguage:
    def test_empty_corpus_returns_zero(self) -> None:
        score = _score_language([], [], _norm_unit_en, _norm_ingredient_en)
        assert score == PerFieldScore(0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)

    def test_all_correct(self) -> None:
        items = [
            GoldItem(line="1 cup flour", expected=_expected()),
            GoldItem(
                line="2 eggs",
                expected=_expected(quantity=2.0, unit="MEDIUM", ingredient="egg"),
            ),
        ]
        runs = [
            _run(quantity=1.0, unit="cup", ingredient="flour"),
            _run(quantity=2.0, unit="MEDIUM", ingredient="egg"),
        ]
        score = _score_language(items, runs, _norm_unit_en, _norm_ingredient_en)
        assert score.n == 2
        assert score.failures == 0
        assert score.qty_acc == 1.0
        assert score.unit_acc == 1.0
        assert score.ing_acc == 1.0
        assert score.line_f1 == 1.0

    def test_one_failure_counts(self) -> None:
        items = [
            GoldItem(line="1 cup flour", expected=_expected()),
            GoldItem(line="2 eggs", expected=_expected(quantity=2, unit="MEDIUM")),
        ]
        runs = [_run(), _run(failed=True)]
        score = _score_language(items, runs, _norm_unit_en, _norm_ingredient_en)
        assert score.failures == 1
        assert score.qty_acc == 0.5
        assert score.line_f1 == 0.5

    def test_prep_scoring_separates_exact_and_jaccard(self) -> None:
        items = [
            GoldItem(
                line="1 cup flour",
                expected=_expected(preparation="sifted and cooled"),
            )
        ]
        runs = [_run(preparation="cooled, sifted")]
        score = _score_language(items, runs, _norm_unit_en, _norm_ingredient_en)
        assert score.prep_exact_acc == 0.0  # order differs
        assert score.prep_jaccard_mean == 1.0  # same token set


class TestScoreSwedishNameSet:
    def test_perfect_recall_and_precision(self) -> None:
        items = [
            GoldItem(line="x", expected=_expected(ingredient="mjölk"), row_id=1),
            GoldItem(line="y", expected=_expected(ingredient="mjöl"), row_id=1),
            GoldItem(line="z", expected=_expected(ingredient="socker"), row_id=2),
        ]
        runs = [
            _run(ingredient="mjölk"),
            _run(ingredient="mjöl"),
            _run(ingredient="socker"),
        ]
        score = _score_swedish_name_set(items, runs)
        assert score.f1 == 1.0
        assert score.precision == 1.0
        assert score.recall == 1.0

    def test_missing_ingredient(self) -> None:
        items = [
            GoldItem(line="x", expected=_expected(ingredient="mjölk"), row_id=1),
            GoldItem(line="y", expected=_expected(ingredient="mjöl"), row_id=1),
        ]
        runs = [_run(ingredient="mjölk"), _run(failed=True)]
        score = _score_swedish_name_set(items, runs)
        # 1 TP, 0 FP, 1 FN → P=1, R=0.5, F1=0.667
        assert score.tp == 1
        assert score.precision == 1.0
        assert score.recall == 0.5

    def test_hallucinated_ingredient(self) -> None:
        items = [
            GoldItem(line="x", expected=_expected(ingredient="mjölk"), row_id=1),
        ]
        runs = [_run(ingredient="lönnsirap")]
        # 0 TP, 1 FP, 1 FN
        score = _score_swedish_name_set(items, runs)
        assert score.tp == 0
        assert score.precision == 0.0
        assert score.recall == 0.0
        assert score.f1 == 0.0

    def test_no_row_id_skipped(self) -> None:
        # GoldItems without row_id are silently skipped (shouldn't happen for
        # Swedish gold, but don't crash if it does).
        items = [GoldItem(line="x", expected=_expected(ingredient="mjölk"))]
        runs = [_run(ingredient="mjölk")]
        score = _score_swedish_name_set(items, runs)
        assert score.tp == 0
        assert score.f1 == 0.0


# ---------------------------------------------------------------------------
# End-to-end score_run (mixed English + Swedish + multilingual)
# ---------------------------------------------------------------------------


class TestScoreRun:
    def test_all_three_corpora(self) -> None:
        english = [GoldItem(line="1 cup flour", expected=_expected(), language="en")]
        swedish = [
            GoldItem(
                line="3 dl vetemjöl",
                expected=_expected(quantity=3.0, unit="dl", ingredient="vetemjöl"),
                language="sv",
                row_id=0,
            )
        ]
        multilingual = [
            GoldItem(
                line="1 Zehe Knoblauch",
                expected=_expected(quantity=1.0, unit="zehe", ingredient="knoblauch"),
                language="de",
                host="chefkoch.de",
            ),
            GoldItem(
                line="赤パプリカ 1/2個",
                expected=_expected(quantity=0.5, unit="個", ingredient="赤パプリカ"),
                language="ja",
                host="delishkitchen.tv",
            ),
        ]
        run = ModelRun(
            model="m1",
            ollama_url="http://x",
            retry_idx=0,
            english=[_run(quantity=1.0, unit="cup", ingredient="flour")],
            swedish=[_run(quantity=3.0, unit="dl", ingredient="vetemjöl")],
            multilingual=[
                _run(quantity=1.0, unit="zehe", ingredient="knoblauch"),
                _run(quantity=0.5, unit="個", ingredient="赤パプリカ"),
            ],
        )
        score = score_run(run, english, swedish, multilingual)
        assert set(score.per_language) == {"en", "sv", "de", "ja"}
        assert score.per_language["en"].line_f1 == 1.0
        assert score.per_language["sv"].line_f1 == 1.0
        assert score.per_language["de"].line_f1 == 1.0
        assert score.per_language["ja"].line_f1 == 1.0
        assert score.sv_name_set.f1 == 1.0

    def test_empty_corpora_skip_cleanly(self) -> None:
        run = ModelRun(model="m1", ollama_url="http://x", retry_idx=0)
        score = score_run(run, [], [], [])
        assert score.per_language == {}
        assert score.sv_name_set.f1 == 0.0


# ---------------------------------------------------------------------------
# Aggregation (mean ± stdev across retries)
# ---------------------------------------------------------------------------


class TestMetricBand:
    def test_empty_values_zero(self) -> None:
        b = MetricBand.from_values([])
        assert b.mean == 0.0 and b.stdev == 0.0

    def test_single_value_stdev_zero(self) -> None:
        b = MetricBand.from_values([0.5])
        assert b.mean == 0.5 and b.stdev == 0.0

    def test_multi_value(self) -> None:
        b = MetricBand.from_values([0.4, 0.5, 0.6])
        assert abs(b.mean - 0.5) < 1e-9
        assert b.stdev > 0.0  # sample stdev of three distinct values


class TestAggregate:
    def _score(
        self, model: str, retry: int, en_line_f1: float, sv_name_f1: float = 0.9
    ) -> Score:
        return Score(
            model=model,
            retry_idx=retry,
            per_language={
                "en": PerFieldScore(
                    n=10,
                    failures=0,
                    qty_acc=1.0,
                    unit_acc=0.9,
                    ing_acc=0.8,
                    prep_exact_acc=0.7,
                    prep_jaccard_mean=0.85,
                    line_f1=en_line_f1,
                    avg_latency_s=0.5,
                )
            },
            sv_name_set=NameSetScore(
                precision=1.0,
                recall=sv_name_f1,
                f1=sv_name_f1,
                tp=9,
                extracted=9,
                gold=10,
            ),
        )

    def test_single_retry(self) -> None:
        bands = aggregate([self._score("m1", 0, 0.8, 0.9)])
        assert len(bands) == 1
        assert bands[0].retries == 1
        assert bands[0].per_language["en"].line_f1.mean == 0.8
        assert bands[0].per_language["en"].line_f1.stdev == 0.0

    def test_multi_retry_computes_stdev(self) -> None:
        scores = [
            self._score("m1", 0, 0.8, 0.9),
            self._score("m1", 1, 0.82, 0.88),
            self._score("m1", 2, 0.78, 0.91),
        ]
        bands = aggregate(scores)
        assert len(bands) == 1
        assert bands[0].retries == 3
        en = bands[0].per_language["en"].line_f1
        assert abs(en.mean - 0.8) < 1e-9
        assert en.stdev > 0.0

    def test_multiple_models_grouped(self) -> None:
        scores = [
            self._score("m1", 0, 0.8),
            self._score("m2", 0, 0.6),
        ]
        bands = aggregate(scores)
        models = {b.model for b in bands}
        assert models == {"m1", "m2"}

    def test_sv_name_f1_aggregates(self) -> None:
        scores = [
            self._score("m1", 0, 0.8, 0.9),
            self._score("m1", 1, 0.8, 0.85),
        ]
        bands = aggregate(scores)
        assert abs(bands[0].sv_name_f1.mean - 0.875) < 1e-9
        assert bands[0].sv_name_f1.stdev > 0.0


# ---------------------------------------------------------------------------
# Reporting (smoke — no crashes)
# ---------------------------------------------------------------------------


class TestFormatters:
    def _band(self) -> ModelBand:
        # build via aggregate to avoid hand-constructing nested bands
        score = Score(
            model="m1",
            retry_idx=0,
            per_language={
                "en": PerFieldScore(10, 0, 1.0, 0.9, 0.8, 0.7, 0.85, 0.75, 0.5),
                "sv": PerFieldScore(20, 1, 0.95, 0.85, 0.8, 0.6, 0.8, 0.7, 1.0),
                "de": PerFieldScore(5, 0, 0.9, 0.9, 0.9, 0.5, 0.7, 0.6, 0.8),
            },
            sv_name_set=NameSetScore(1.0, 0.9, 0.94, 18, 18, 20),
        )
        return aggregate([score])[0]

    def test_summary_table_single_retry(self) -> None:
        bands = [self._band()]
        out = format_summary_table(bands)
        assert "m1" in out
        assert "en F1" in out
        assert "sv F1" in out
        assert "de F1" in out
        # Single retry → no ± in cells
        assert " ± " not in out

    def test_summary_table_with_retries_shows_plusminus(self) -> None:
        score_a = Score(
            model="m1",
            retry_idx=0,
            per_language={"en": PerFieldScore(10, 0, 1, 1, 1, 1, 1, 0.8, 0.5)},
            sv_name_set=NameSetScore(0.0, 0.0, 0.0, 0, 0, 0),
        )
        score_b = Score(
            model="m1",
            retry_idx=1,
            per_language={"en": PerFieldScore(10, 0, 1, 1, 1, 1, 1, 0.9, 0.5)},
            sv_name_set=NameSetScore(0.0, 0.0, 0.0, 0, 0, 0),
        )
        out = format_summary_table(aggregate([score_a, score_b]))
        assert " ± " in out

    def test_per_field_breakdown_has_prep_columns(self) -> None:
        out = format_per_field_breakdown([self._band()])
        assert "prep (exact)" in out
        assert "prep (jaccard)" in out


# ---------------------------------------------------------------------------
# Persistence round-trip
# ---------------------------------------------------------------------------


class TestPersistence:
    def test_save_and_load_round_trip(self, tmp_path: Path) -> None:
        runs = [
            ModelRun(
                model="m1",
                ollama_url="http://x",
                retry_idx=0,
                english=[_run(preparation="chopped")],
                swedish=[_run(ingredient="mjölk")],
                multilingual=[],
                errors=["boom"],
            )
        ]
        path = tmp_path / "r.json"
        save_runs(runs, path)
        loaded = load_runs(path)
        assert len(loaded) == 1
        assert loaded[0].model == "m1"
        assert loaded[0].english[0].preparation == "chopped"
        assert loaded[0].errors == ["boom"]

    def test_v1_schema_rejected(self, tmp_path: Path) -> None:
        # v1 payload had no schema field and a different shape.
        path = tmp_path / "old.json"
        path.write_text('{"runs": []}')  # no schema key → defaults to "v1"
        with pytest.raises(ValueError, match="schema"):
            load_runs(path)
