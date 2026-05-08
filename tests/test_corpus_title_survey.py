"""Tests for the cross-corpus title-frequency survey (vwt.1)."""

from __future__ import annotations

import csv
import gzip
import io
import json
import zipfile
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

import pytest

from rational_recipes.corpus_title_survey import (
    LANGUAGE_FILTER_ALL,
    LANGUAGE_FILTER_EN_SV,
    SURVEY_VERSION,
    accept_en_sv,
    build_survey,
    merge_tallies,
    tally_titles,
)

# --- Language filter ---


class TestAcceptEnSv:
    def test_accepts_ascii_english(self) -> None:
        assert accept_en_sv("chocolate cake")

    def test_accepts_swedish_with_diacritics(self) -> None:
        assert accept_en_sv("pannkakor")
        assert accept_en_sv("kötbullar")
        assert accept_en_sv("räksallad")
        assert accept_en_sv("äppelpaj")

    def test_accepts_uppercase_swedish_diacritics(self) -> None:
        assert accept_en_sv("Ärtsoppa")

    def test_rejects_cyrillic(self) -> None:
        assert not accept_en_sv("борщ")
        assert not accept_en_sv("блины")

    def test_rejects_cjk(self) -> None:
        assert not accept_en_sv("親子丼")
        assert not accept_en_sv("饺子")
        assert not accept_en_sv("김치찌개")

    def test_rejects_arabic(self) -> None:
        assert not accept_en_sv("كبسة")

    def test_rejects_mixed_swedish_and_cyrillic(self) -> None:
        # Pure-Swedish marker is fine; Cyrillic mixed in is not.
        assert not accept_en_sv("pannkakor борщ")

    def test_accepts_digits_and_punctuation(self) -> None:
        # Normalization can leave digits, ampersands, hyphens behind.
        assert accept_en_sv("3-ingredient cookies")
        assert accept_en_sv("mac & cheese")


# --- Tally + merge ---


class TestTallyTitles:
    def test_counts_normalized_titles(self) -> None:
        counts, rows = tally_titles(
            ["Chocolate Cake", "chocolate cake", "Banana Bread"]
        )
        assert counts == Counter({"chocolate cake": 2, "banana bread": 1})
        assert rows == 3

    def test_skips_empty_normalized_titles(self) -> None:
        # normalize_title strips trailing "recipes"; "Recipe" alone -> empty.
        counts, rows = tally_titles(["Recipe", "  ", "Pancakes"])
        assert counts == Counter({"pancakes": 1})
        assert rows == 3

    def test_language_filter_drops_rejected(self) -> None:
        counts, rows = tally_titles(
            ["chocolate cake", "борщ", "pannkakor"],
            accept=accept_en_sv,
        )
        assert counts == Counter({"chocolate cake": 1, "pannkakor": 1})
        assert rows == 3


class TestMergeTallies:
    def test_combines_and_sorts(self) -> None:
        rnlg = Counter({"chocolate cake": 100, "banana bread": 30})
        wdc = Counter({"chocolate cake": 5, "pannkakor": 12})
        merged = merge_tallies(rnlg, wdc)
        # combined desc, then title asc on ties.
        assert [(t.title, t.recipenlg, t.wdc, t.combined) for t in merged] == [
            ("chocolate cake", 100, 5, 105),
            ("banana bread", 30, 0, 30),
            ("pannkakor", 0, 12, 12),
        ]

    def test_min_combined_filter(self) -> None:
        rnlg = Counter({"a": 5, "b": 1})
        wdc = Counter({"a": 1, "c": 2})
        merged = merge_tallies(rnlg, wdc, min_combined=3)
        assert [t.title for t in merged] == ["a"]

    def test_tie_break_alphabetical(self) -> None:
        rnlg: Counter[str] = Counter({"zebra": 3, "apple": 3})
        wdc: Counter[str] = Counter()
        merged = merge_tallies(rnlg, wdc)
        assert [t.title for t in merged] == ["apple", "zebra"]


# --- CLI round-trip on tiny fixture corpora ---


_RECIPENLG_FIELDS = [
    "",
    "title",
    "ingredients",
    "directions",
    "link",
    "source",
    "NER",
]


def _write_recipenlg_csv(path: Path, titles: list[str]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_RECIPENLG_FIELDS)
        writer.writeheader()
        for i, title in enumerate(titles):
            writer.writerow(
                {
                    "": str(i),
                    "title": title,
                    "ingredients": "[]",
                    "directions": "[]",
                    "link": "",
                    "source": "",
                    "NER": "[]",
                }
            )


def _write_wdc_zip(path: Path, host_to_titles: dict[str, list[str]]) -> None:
    with zipfile.ZipFile(path, "w") as zf:
        for host, titles in host_to_titles.items():
            entry = f"Recipe_{host}_October2023.json.gz"
            rows = [{"row_id": i, "name": t} for i, t in enumerate(titles)]
            payload = "\n".join(json.dumps(r) for r in rows).encode()
            zf.writestr(entry, gzip.compress(payload))


@pytest.fixture()
def tiny_corpora(tmp_path: Path) -> tuple[Path, Path]:
    csv_path = tmp_path / "recipenlg.csv"
    _write_recipenlg_csv(
        csv_path,
        [
            "Chocolate Cake",
            "Chocolate Cake",
            "Chocolate Cake",
            "Banana Bread",
            "Banana Bread",
            "borscht",  # ASCII transliteration; survives en+sv filter
        ],
    )
    zip_path = tmp_path / "wdc.zip"
    _write_wdc_zip(
        zip_path,
        {
            "ica.se": [
                "Pannkakor",
                "Pannkakor",
                "Chocolate Cake",
                "борщ",  # Cyrillic; rejected by en+sv filter
            ],
        },
    )
    return csv_path, zip_path


class TestBuildSurvey:
    def test_merge_across_corpora(self, tiny_corpora: tuple[Path, Path]) -> None:
        csv_path, zip_path = tiny_corpora
        survey = build_survey(
            recipenlg_path=csv_path,
            wdc_zip_path=zip_path,
            generated_at=datetime(2026, 4, 24, 12, 0, 0, tzinfo=UTC),
        )
        assert survey["version"] == SURVEY_VERSION
        assert survey["language_filter"] == LANGUAGE_FILTER_ALL
        assert survey["min_combined_count"] is None
        assert survey["corpus_revisions"]["recipenlg_rows"] == 6
        assert survey["corpus_revisions"]["wdc_rows"] == 4

        by_title = {t["title"]: t for t in survey["titles"]}
        assert by_title["chocolate cake"] == {
            "title": "chocolate cake",
            "recipenlg": 3,
            "wdc": 1,
            "combined": 4,
        }
        assert by_title["pannkakor"]["recipenlg"] == 0
        assert by_title["pannkakor"]["wdc"] == 2
        assert by_title["banana bread"]["recipenlg"] == 2
        assert by_title["banana bread"]["wdc"] == 0
        # Top entry is the highest-combined: chocolate cake (4).
        assert survey["titles"][0]["title"] == "chocolate cake"

    def test_language_filter_drops_cyrillic(
        self, tiny_corpora: tuple[Path, Path]
    ) -> None:
        csv_path, zip_path = tiny_corpora
        survey = build_survey(
            recipenlg_path=csv_path,
            wdc_zip_path=zip_path,
            language_filter=LANGUAGE_FILTER_EN_SV,
            generated_at=datetime(2026, 4, 24, 12, 0, 0, tzinfo=UTC),
        )
        titles = {t["title"] for t in survey["titles"]}
        assert "борщ" not in titles
        # Pannkakor (ä-free, ASCII-only) and chocolate cake survive.
        assert "pannkakor" in titles
        assert "chocolate cake" in titles

    def test_min_combined_filter(self, tiny_corpora: tuple[Path, Path]) -> None:
        csv_path, zip_path = tiny_corpora
        survey = build_survey(
            recipenlg_path=csv_path,
            wdc_zip_path=zip_path,
            min_combined=3,
            generated_at=datetime(2026, 4, 24, 12, 0, 0, tzinfo=UTC),
        )
        assert [t["title"] for t in survey["titles"]] == ["chocolate cake"]
        assert survey["min_combined_count"] == 3

    def test_rejects_unknown_language_filter(
        self, tiny_corpora: tuple[Path, Path]
    ) -> None:
        csv_path, zip_path = tiny_corpora
        with pytest.raises(ValueError):
            build_survey(
                recipenlg_path=csv_path,
                wdc_zip_path=zip_path,
                language_filter="klingon",
            )


# --- CLI round-trip: byte-identical reruns ---


class TestCliRoundtrip:
    def test_byte_identical_reruns(
        self, tiny_corpora: tuple[Path, Path], tmp_path: Path
    ) -> None:
        import sys

        from rational_recipes.cli import corpus_title_survey as cli

        csv_path, zip_path = tiny_corpora
        out_a = tmp_path / "a.json"
        out_b = tmp_path / "b.json"
        captured = io.StringIO()
        argv_a = [
            "--recipenlg",
            str(csv_path),
            "--wdc-zip",
            str(zip_path),
            "--output",
            str(out_a),
        ]
        argv_b = list(argv_a)
        argv_b[-1] = str(out_b)

        # Redirect the script's stderr summary line so it doesn't pollute.
        old_stderr = sys.stderr
        sys.stderr = captured
        try:
            assert cli.run(argv_a) == 0
            assert cli.run(argv_b) == 0
        finally:
            sys.stderr = old_stderr

        assert out_a.read_bytes() == out_b.read_bytes()

        # Also confirm the JSON parses and contains expected top entry.
        payload = json.loads(out_a.read_text(encoding="utf-8"))
        assert payload["version"] == SURVEY_VERSION
        assert payload["titles"][0]["title"] == "chocolate cake"
