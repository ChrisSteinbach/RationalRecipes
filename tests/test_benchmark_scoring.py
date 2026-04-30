"""Tests for the pure scoring / aggregation logic in scripts/benchmark_models.py.

No network calls — every test synthesizes ModelRun fixtures in-memory. The goal
is to lock down the scoring contract (what counts as a match, how retries
collapse into bands) so that follow-on benchmark sweeps compare apples to
apples.

Field normalization (unit synonyms, ingredient depluralization, prep tokenizing)
is exercised through the scoring boundary in TestScorePair rather than tested
on the private helpers directly — the contract callers depend on is "does this
pair score as a match?", not "what does the private normalizer return?".
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
    _norm_unit_en,
    _norm_unit_generic,
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


def _score_en(
    out: LineRun, exp: dict[str, Any]
) -> tuple[bool, bool, bool, bool, float]:
    return _score_pair(out, exp, _norm_unit_en, _norm_ingredient_en)


def _score_generic(
    out: LineRun, exp: dict[str, Any]
) -> tuple[bool, bool, bool, bool, float]:
    return _score_pair(out, exp, _norm_unit_generic, _norm_ingredient_generic)


class TestScorePair:
    def test_all_match(self) -> None:
        assert _score_en(_run(), _expected()) == (True, True, True, True, 1.0)

    def test_unit_mismatch(self) -> None:
        q, u, i_, pe, _ = _score_en(_run(unit="tbsp"), _expected(unit="cup"))
        assert (q, u, i_, pe) == (True, False, True, True)

    def test_failed_line_scores_zero(self) -> None:
        assert _score_en(_run(failed=True), _expected()) == (
            False,
            False,
            False,
            False,
            0.0,
        )

    @pytest.mark.parametrize(
        ("model_unit", "gold_unit"),
        [
            ("cups", "cup"),  # plural and case-canonicalization
            ("OUNCES", "oz"),  # cross-spelling + case
            ("medium", "MEDIUM"),  # size sentinel uppercased
        ],
    )
    def test_english_unit_synonyms_match(
        self, model_unit: str, gold_unit: str
    ) -> None:
        _, u, *_ = _score_en(_run(unit=model_unit), _expected(unit=gold_unit))
        assert u

    @pytest.mark.parametrize(
        ("model_ing", "gold_ing"),
        [
            ("eggs", "egg"),  # regular -s
            ("cherries", "cherry"),  # -ies → -y
            ("cream cheese", "cream cheese"),  # compound, no stripping
        ],
    )
    def test_english_ingredient_depluralization(
        self, model_ing: str, gold_ing: str
    ) -> None:
        _, _, i_, *_ = _score_en(
            _run(ingredient=model_ing), _expected(ingredient=gold_ing)
        )
        assert i_

    def test_generic_normalization_preserves_source_language(self) -> None:
        # Generic norm just lowercases + strips. Without it, German "Knoblauch"
        # wouldn't match its lowercase canonical form.
        _, u, i_, *_ = _score_generic(
            _run(unit="Zehe", ingredient="Knoblauch"),
            _expected(unit="zehe", ingredient="knoblauch"),
        )
        assert u and i_

    def test_generic_normalization_skips_depluralization(self) -> None:
        # German "eier" is already plural; English depluralization would corrupt it.
        _, _, i_, *_ = _score_generic(
            _run(ingredient="eier"), _expected(ingredient="eier")
        )
        assert i_

    def test_size_sentinel_canonicalized_under_generic(self) -> None:
        # Cross-language sentinel contract: model returns lowercase "medium"
        # but gold has uppercase MEDIUM (per prompt contract); generic norm
        # must uppercase the sentinel rather than lowercase everything.
        _, u, *_ = _score_generic(_run(unit="medium"), _expected(unit="MEDIUM"))
        assert u

    def test_prep_exact_is_word_order_strict(self) -> None:
        # Order differs → exact fails, Jaccard treats it as the same set.
        _, _, _, pe, pj = _score_en(
            _run(preparation="sliced thinly"),
            _expected(preparation="thinly sliced"),
        )
        assert pe is False
        assert pj == 1.0

    def test_prep_jaccard_ignores_stopwords_and_commas(self) -> None:
        _, _, _, _, pj = _score_en(
            _run(preparation="peeled and chopped"),
            _expected(preparation="peeled, chopped"),
        )
        assert pj == 1.0

    def test_prep_jaccard_partial_overlap(self) -> None:
        # {peeled, chopped} vs {peeled, seeded, chopped} → 2/3
        _, _, _, _, pj = _score_en(
            _run(preparation="peeled, chopped"),
            _expected(preparation="peeled, seeded, chopped"),
        )
        assert abs(pj - 2 / 3) < 1e-9

    def test_prep_both_empty_is_match(self) -> None:
        _, _, _, pe, pj = _score_en(_run(preparation=""), _expected(preparation=""))
        assert pe is True
        assert pj == 1.0

    def test_prep_one_empty_jaccard_zero(self) -> None:
        _, _, _, _, pj = _score_en(
            _run(preparation=""), _expected(preparation="chopped")
        )
        assert pj == 0.0


class TestScoreLanguage:
    def test_empty_corpus_returns_zero(self) -> None:
        score = _score_language([], [], _norm_unit_en, _norm_ingredient_en)
        assert score == PerFieldScore(0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)

    def test_one_failure_counts(self) -> None:
        items = [
            GoldItem(line="1 cup flour", expected=_expected()),
            GoldItem(line="2 eggs", expected=_expected(quantity=2, unit="MEDIUM")),
        ]
        runs = [_run(), _run(failed=True)]
        score = _score_language(items, runs, _norm_unit_en, _norm_ingredient_en)
        assert (score.failures, score.qty_acc, score.line_f1) == (1, 0.5, 0.5)


class TestScoreSwedishNameSet:
    def _gold(self, ing: str, row_id: int | None = 1) -> GoldItem:
        return GoldItem(line="x", expected=_expected(ingredient=ing), row_id=row_id)

    def test_perfect_recall_and_precision(self) -> None:
        items = [
            self._gold("mjölk"),
            self._gold("mjöl"),
            self._gold("socker", row_id=2),
        ]
        runs = [_run(ingredient=i) for i in ("mjölk", "mjöl", "socker")]
        score = _score_swedish_name_set(items, runs)
        assert (score.precision, score.recall, score.f1) == (1.0, 1.0, 1.0)

    def test_missing_ingredient(self) -> None:
        # 1 TP, 0 FP, 1 FN → P=1, R=0.5
        items = [self._gold("mjölk"), self._gold("mjöl")]
        runs = [_run(ingredient="mjölk"), _run(failed=True)]
        score = _score_swedish_name_set(items, runs)
        assert (score.tp, score.precision, score.recall) == (1, 1.0, 0.5)

    def test_hallucinated_ingredient(self) -> None:
        # 0 TP, 1 FP, 1 FN
        score = _score_swedish_name_set(
            [self._gold("mjölk")], [_run(ingredient="lönnsirap")]
        )
        assert (score.tp, score.precision, score.recall, score.f1) == (0, 0.0, 0.0, 0.0)


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
            ),
            GoldItem(
                line="赤パプリカ 1/2個",
                expected=_expected(quantity=0.5, unit="個", ingredient="赤パプリカ"),
                language="ja",
            ),
        ]
        run = ModelRun(
            model="m1",
            ollama_url="http://x",
            retry_idx=0,
            english=[_run(unit="cup", ingredient="flour")],
            swedish=[_run(quantity=3.0, unit="dl", ingredient="vetemjöl")],
            multilingual=[
                _run(unit="zehe", ingredient="knoblauch"),
                _run(quantity=0.5, unit="個", ingredient="赤パプリカ"),
            ],
        )
        score = score_run(run, english, swedish, multilingual)
        assert set(score.per_language) == {"en", "sv", "de", "ja"}
        for lang in ("en", "sv", "de", "ja"):
            assert score.per_language[lang].line_f1 == 1.0
        assert score.sv_name_set.f1 == 1.0

    def test_empty_corpora_skip_cleanly(self) -> None:
        run = ModelRun(model="m1", ollama_url="http://x", retry_idx=0)
        score = score_run(run, [], [], [])
        assert score.per_language == {}
        assert score.sv_name_set.f1 == 0.0


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
        # PerFieldScore positional: n, failures, qty, unit, ing, prep_exact,
        # prep_jaccard, line_f1, latency.
        return Score(
            model=model,
            retry_idx=retry,
            per_language={
                "en": PerFieldScore(10, 0, 1.0, 0.9, 0.8, 0.7, 0.85, en_line_f1, 0.5),
            },
            sv_name_set=NameSetScore(1.0, sv_name_f1, sv_name_f1, 9, 9, 10),
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
        bands = aggregate([self._score("m1", 0, 0.8), self._score("m2", 0, 0.6)])
        assert {b.model for b in bands} == {"m1", "m2"}


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
        empty_sv = NameSetScore(0.0, 0.0, 0.0, 0, 0, 0)
        scores = [
            Score(
                "m1",
                retry,
                {"en": PerFieldScore(10, 0, 1, 1, 1, 1, 1, f1, 0.5)},
                empty_sv,
            )
            for retry, f1 in enumerate([0.8, 0.9])
        ]
        out = format_summary_table(aggregate(scores))
        assert " ± " in out

    def test_per_field_breakdown_has_prep_columns(self) -> None:
        out = format_per_field_breakdown([self._band()])
        assert "prep (exact)" in out and "prep (jaccard)" in out


class TestPersistence:
    def test_save_and_load_round_trip(self, tmp_path: Path) -> None:
        original = ModelRun(
            model="m1",
            ollama_url="http://x",
            retry_idx=0,
            english=[_run(preparation="chopped")],
            errors=["boom"],
        )
        path = tmp_path / "r.json"
        save_runs([original], path)
        loaded = load_runs(path)
        assert len(loaded) == 1 and loaded[0].model == "m1"
        assert loaded[0].english[0].preparation == "chopped"
        assert loaded[0].errors == ["boom"]

    def test_v1_schema_rejected(self, tmp_path: Path) -> None:
        # v1 payload had no schema field and a different shape.
        path = tmp_path / "old.json"
        path.write_text('{"runs": []}')  # no schema key → defaults to "v1"
        with pytest.raises(ValueError, match="schema"):
            load_runs(path)


class TestScoreRunWithLimits:
    # --rescore path: gold corpus may be larger than stored LineRuns when a
    # run used --english-limit. Callers must clip gold to LineRun count;
    # score_run uses zip(..., strict=True) so mismatch raises ValueError.

    def test_mismatched_lengths_raise(self) -> None:
        english = [
            GoldItem(line="1 cup flour", expected=_expected(), language="en"),
            GoldItem(line="2 eggs", expected=_expected(quantity=2.0), language="en"),
        ]
        run = ModelRun(
            model="m1",
            ollama_url="http://x",
            retry_idx=0,
            english=[_run()],  # only one LineRun, gold has two
        )
        with pytest.raises(ValueError, match="shorter"):
            score_run(run, english, [], [])

    def test_clipped_gold_scores_cleanly(self) -> None:
        english_full = [
            GoldItem(line="1 cup flour", expected=_expected(), language="en"),
            GoldItem(line="2 eggs", expected=_expected(quantity=2.0), language="en"),
        ]
        run = ModelRun(
            model="m1",
            ollama_url="http://x",
            retry_idx=0,
            english=[_run()],
        )
        # Caller clips gold to match run length — this is the pattern
        # main() applies on the rescore path.
        english = english_full[: len(run.english)]
        score = score_run(run, english, [], [])
        assert score.per_language["en"].n == 1
        assert score.per_language["en"].line_f1 == 1.0
