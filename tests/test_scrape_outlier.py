"""Tests for per-recipe outlier scoring (bead 0g3)."""

from __future__ import annotations

import math

from rational_recipes.scrape.outlier import compute_outlier_scores


class TestComputeOutlierScores:
    def test_empty_input(self) -> None:
        assert compute_outlier_scores([], ["flour", "milk"]) == []

    def test_single_row_is_zero(self) -> None:
        """With N=1 there is no meaningful median to deviate from."""
        scores = compute_outlier_scores(
            [{"flour": 50.0, "milk": 50.0}],
            ["flour", "milk"],
        )
        assert scores == [0.0]

    def test_empty_axis_set_returns_zeros(self) -> None:
        scores = compute_outlier_scores(
            [{"flour": 50.0}, {"flour": 40.0}],
            [],
        )
        assert scores == [0.0, 0.0]

    def test_identical_rows_all_zero(self) -> None:
        rows = [
            {"flour": 50.0, "milk": 50.0},
            {"flour": 50.0, "milk": 50.0},
            {"flour": 50.0, "milk": 50.0},
        ]
        scores = compute_outlier_scores(rows, ["flour", "milk"])
        assert scores == [0.0, 0.0, 0.0]

    def test_outlier_row_gets_higher_score(self) -> None:
        """Median is computed from three close rows; the fourth deviates."""
        rows = [
            {"flour": 50.0, "milk": 50.0},
            {"flour": 51.0, "milk": 49.0},
            {"flour": 49.0, "milk": 51.0},
            {"flour": 80.0, "milk": 20.0},  # outlier
        ]
        scores = compute_outlier_scores(rows, ["flour", "milk"])
        assert scores[3] > scores[0]
        assert scores[3] > scores[1]
        assert scores[3] > scores[2]

    def test_missing_ingredient_treated_as_zero(self) -> None:
        """A row omitting an ingredient counts as 0.0 on that axis."""
        rows = [
            {"flour": 50.0, "milk": 50.0, "sugar": 5.0},
            {"flour": 50.0, "milk": 50.0, "sugar": 5.0},
            {"flour": 50.0, "milk": 50.0},  # omits sugar → treated as 0.0
        ]
        scores = compute_outlier_scores(rows, ["flour", "milk", "sugar"])
        # First two are median; third deviates by 5 on sugar axis.
        assert math.isclose(scores[0], 0.0)
        assert math.isclose(scores[1], 0.0)
        assert math.isclose(scores[2], 5.0)

    def test_euclidean_distance_matches_manual(self) -> None:
        """Two rows; median is their midpoint so each sits √(Δ²) from center."""
        rows = [
            {"flour": 40.0, "milk": 60.0},
            {"flour": 60.0, "milk": 40.0},
        ]
        scores = compute_outlier_scores(rows, ["flour", "milk"])
        # Median of [40, 60] per axis = 50. Each row is (10, -10) or (-10, 10)
        # from median → distance = sqrt(100+100) = sqrt(200) ≈ 14.142
        assert math.isclose(scores[0], math.sqrt(200), rel_tol=1e-9)
        assert math.isclose(scores[1], math.sqrt(200), rel_tol=1e-9)

    def test_order_preserved(self) -> None:
        """Scores align with input row order, not any sort."""
        rows = [
            {"flour": 80.0, "milk": 20.0},  # outlier — first
            {"flour": 50.0, "milk": 50.0},
            {"flour": 50.0, "milk": 50.0},
            {"flour": 50.0, "milk": 50.0},
        ]
        scores = compute_outlier_scores(rows, ["flour", "milk"])
        assert scores[0] > scores[1]
        assert scores[0] > scores[2]
        assert scores[0] > scores[3]

    def test_axis_set_with_duplicates_deduped(self) -> None:
        """Duplicated ingredient names in the axis iterable are deduped."""
        rows = [
            {"flour": 50.0, "milk": 50.0},
            {"flour": 60.0, "milk": 40.0},
        ]
        a = compute_outlier_scores(rows, ["flour", "milk"])
        b = compute_outlier_scores(rows, ["flour", "milk", "flour"])
        assert a == b

    def test_non_ascii_ingredient_names(self) -> None:
        """Canonicalization isn't required — raw names work fine."""
        rows = [
            {"mjölk": 50.0, "ägg": 10.0},
            {"mjölk": 52.0, "ägg": 10.0},
            {"mjölk": 48.0, "ägg": 10.0},
        ]
        scores = compute_outlier_scores(rows, ["mjölk", "ägg"])
        # Median of mjölk = 50, ägg = 10. First row is on median → score 0.
        assert math.isclose(scores[0], 0.0, abs_tol=1e-9)
        assert math.isclose(scores[1], 2.0, abs_tol=1e-9)
        assert math.isclose(scores[2], 2.0, abs_tol=1e-9)
