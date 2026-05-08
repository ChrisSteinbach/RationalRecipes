"""Tests for the component-structure diagnostic (RationalRecipes-0x1z)."""

from __future__ import annotations

import csv
import sys
from pathlib import Path

# Allow importing scripts/diagnose_component_structure.py directly.
SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import diagnose_component_structure as dcs  # noqa: E402

# --- find_markers_in_text -------------------------------------------------


class TestForTheMarker:
    def test_basic_for_the(self) -> None:
        markers = dcs.find_markers_in_text(
            "For the crust: Combine the graham cracker crumbs and butter."
        )
        assert len(markers) == 1
        assert markers[0].component_name == "crust"
        assert markers[0].pattern == "for_the"

    def test_case_insensitive(self) -> None:
        markers = dcs.find_markers_in_text("FOR THE FILLING: Whisk the eggs.")
        assert len(markers) == 1
        assert markers[0].component_name == "filling"

    def test_for_without_the(self) -> None:
        markers = dcs.find_markers_in_text(
            "For pigs-in-a-blanket: Preheat the oven to 400 degrees F."
        )
        assert len(markers) == 1
        assert markers[0].component_name == "pigs-in-a-blanket"

    def test_multiple_components_in_one_recipe(self) -> None:
        text = (
            "For the pigs-in-a-blanket: Preheat the oven to 400.\n"
            "Cut each piece of puff pastry in half.\n"
            "For the relish: Add the cherry peppers and garlic.\n"
            "For the mustard: Stir together the mustard and honey.\n"
            "For the everything spice: Combine sesame, poppy, and salt."
        )
        markers = dcs.find_markers_in_text(text)
        assert len(markers) == 4
        names = {m.component_name for m in markers}
        assert names == {
            "pigs-in-a-blanket",
            "relish",
            "mustard",
            "everything spice",
        }

    def test_numbered_subsection(self) -> None:
        # Numbered prefix "1. For the X:" should still match.
        markers = dcs.find_markers_in_text(
            "1. For the crust: Mix flour, butter, and sugar."
        )
        assert len(markers) == 1
        assert markers[0].component_name == "crust"

    def test_bulleted_for_the(self) -> None:
        markers = dcs.find_markers_in_text(
            "- For the topping: Whip the cream and sugar."
        )
        assert len(markers) == 1
        assert markers[0].component_name == "topping"


class TestAllCapsHeader:
    def test_all_caps_filling(self) -> None:
        text = "FILLING:\nWhisk the cream cheese until smooth."
        markers = dcs.find_markers_in_text(text)
        assert len(markers) == 1
        assert markers[0].component_name == "filling"
        assert markers[0].pattern == "header"

    def test_all_caps_for_the_crust(self) -> None:
        text = "FOR THE CRUST:\nCombine graham crumbs with melted butter."
        markers = dcs.find_markers_in_text(text)
        # "FOR THE CRUST:" matches the for_the regex first (case-insensitive).
        assert any(m.component_name == "crust" for m in markers)

    def test_imperative_stir_rejected(self) -> None:
        # "STIR:" looks like a header but it's an imperative — don't flag.
        text = "STIR:\nStir the mixture for 2 minutes."
        markers = dcs.find_markers_in_text(text)
        assert markers == []

    def test_imperative_bake_rejected(self) -> None:
        markers = dcs.find_markers_in_text("BAKE:\nBake at 350 for 30 minutes.")
        assert markers == []

    def test_preheat_rejected(self) -> None:
        markers = dcs.find_markers_in_text("PREHEAT:\nPreheat oven to 400.")
        assert markers == []

    def test_lowercase_noun_header_accepted(self) -> None:
        # "Frosting:" alone on a line, mixed case, contains a component noun.
        text = "Frosting:\nBeat butter with powdered sugar."
        markers = dcs.find_markers_in_text(text)
        assert len(markers) == 1
        assert markers[0].component_name == "frosting"

    def test_imperative_in_lone_header_form_rejected(self) -> None:
        # "Stir well:" is imperative even though the pattern looks header-ish.
        markers = dcs.find_markers_in_text("Stir well:\nUntil smooth.")
        assert markers == []


class TestSchemaOrgHowToSection:
    def test_normalize_list_of_how_to_sections(self) -> None:
        instr = [
            {
                "@type": "HowToSection",
                "name": "Crust",
                "itemListElement": [
                    {"text": "Mix flour and butter."},
                    {"text": "Press into pan."},
                ],
            },
            {
                "@type": "HowToSection",
                "name": "Filling",
                "itemListElement": [{"text": "Whisk eggs and sugar."}],
            },
        ]
        text, has_section = dcs.normalize_wdc_instructions(instr)
        assert has_section is True
        markers = dcs.find_markers_in_text(text)
        names = {m.component_name for m in markers}
        assert names == {"crust", "filling"}

    def test_normalize_mixed_list_with_section(self) -> None:
        instr = [
            {"text": "Preheat oven."},
            {
                "@type": "HowToSection",
                "name": "Topping",
                "itemListElement": [{"text": "Whip cream."}],
            },
        ]
        text, has_section = dcs.normalize_wdc_instructions(instr)
        assert has_section is True
        # The synthesized "For the Topping:" should be detected.
        markers = dcs.find_markers_in_text(text)
        assert any(m.component_name == "topping" for m in markers)

    def test_normalize_list_of_text_dicts(self) -> None:
        instr = [
            {"text": "Step 1."},
            {"text": "Step 2."},
        ]
        text, has_section = dcs.normalize_wdc_instructions(instr)
        assert has_section is False
        assert "Step 1." in text and "Step 2." in text

    def test_normalize_list_of_strings(self) -> None:
        instr = ["Step 1.", "Step 2."]
        text, has_section = dcs.normalize_wdc_instructions(instr)
        assert has_section is False
        assert "Step 1." in text

    def test_normalize_string(self) -> None:
        text, has_section = dcs.normalize_wdc_instructions("Just one blob.")
        assert text == "Just one blob."
        assert has_section is False

    def test_normalize_none(self) -> None:
        text, has_section = dcs.normalize_wdc_instructions(None)
        assert text == ""
        assert has_section is False


class TestPlainSingleComponent:
    def test_no_markers_in_simple_directions(self) -> None:
        text = (
            "Boil and debone chicken.\n"
            "Discard shells.\n"
            "Mix soup and cream together."
        )
        markers = dcs.find_markers_in_text(text)
        assert markers == []

    def test_no_markers_when_only_imperatives(self) -> None:
        text = (
            "Preheat oven to 350.\n"
            "Combine flour and sugar.\n"
            "Bake for 30 minutes."
        )
        markers = dcs.find_markers_in_text(text)
        assert markers == []


class TestRecipeDetection:
    def test_is_component_structured_requires_two_distinct(self) -> None:
        text = (
            "For the crust: Mix flour.\n"
            "Stir.\n"
            "For the filling: Whisk eggs."
        )
        markers = tuple(dcs.find_markers_in_text(text))
        det = dcs.RecipeDetection(
            title="Cheesecake",
            source_id="x",
            directions=text,
            markers=markers,
            has_how_to_section=False,
        )
        assert det.is_component_structured is True

    def test_single_marker_is_not_component_structured(self) -> None:
        text = "For the topping: Whip cream and sugar."
        markers = tuple(dcs.find_markers_in_text(text))
        det = dcs.RecipeDetection(
            title="Cake",
            source_id="x",
            directions=text,
            markers=markers,
            has_how_to_section=False,
        )
        assert det.is_component_structured is False
        assert det.has_any_marker is True

    def test_how_to_section_alone_is_component_structured(self) -> None:
        det = dcs.RecipeDetection(
            title="Pie",
            source_id="x",
            directions="",
            markers=(),
            has_how_to_section=True,
        )
        assert det.is_component_structured is True


# --- normalize_recipenlg_directions --------------------------------------


class TestRecipeNLGDirectionsNormalization:
    def test_basic_list(self) -> None:
        raw = '["Step one.", "Step two."]'
        text = dcs.normalize_recipenlg_directions(raw)
        assert text == "Step one.\nStep two."

    def test_empty(self) -> None:
        assert dcs.normalize_recipenlg_directions("") == ""

    def test_malformed(self) -> None:
        assert dcs.normalize_recipenlg_directions("not a list") == ""

    def test_for_the_inside_list_step(self) -> None:
        raw = (
            '["Preheat oven to 400.", '
            '"For the crust: combine graham crumbs with butter.", '
            '"For the filling: whisk eggs and sugar."]'
        )
        text = dcs.normalize_recipenlg_directions(raw)
        markers = dcs.find_markers_in_text(text)
        names = {m.component_name for m in markers}
        assert names == {"crust", "filling"}


# --- CLI smoke test -------------------------------------------------------


class TestCLISmoke:
    def _write_synthetic_recipenlg(self, path: Path) -> None:
        """Three rows: one component-structured, two single-component."""
        rows = [
            {
                "": "0",
                "title": "Boring Soup",
                "ingredients": '["water", "salt"]',
                "directions": '["Boil water.", "Add salt."]',
                "link": "boring-soup",
                "source": "Test",
                "NER": "[]",
            },
            {
                "": "1",
                "title": "Cheesecake",
                "ingredients": '["graham crumbs", "butter", "cream cheese"]',
                "directions": (
                    '["For the crust: mix graham crumbs with butter.", '
                    '"Press into pan.", '
                    '"For the filling: whisk cream cheese and sugar.", '
                    '"Pour into crust and bake."]'
                ),
                "link": "cheesecake",
                "source": "Test",
                "NER": "[]",
            },
            {
                "": "2",
                "title": "Cookies",
                "ingredients": '["flour", "sugar"]',
                "directions": '["Combine and bake."]',
                "link": "cookies",
                "source": "Test",
                "NER": "[]",
            },
        ]
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "",
                    "title",
                    "ingredients",
                    "directions",
                    "link",
                    "source",
                    "NER",
                ],
            )
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

    def _write_synthetic_wdc(self, path: Path) -> None:
        """One host with two recipes — one HowToSection, one plain."""
        rows_by_host = {
            "test.example.com": [
                {
                    "row_id": 1,
                    "name": "Sectioned Pie",
                    "page_url": "https://test.example.com/pie",
                    "recipeingredient": ["crust ingredients", "filling ingredients"],
                    "recipeinstructions": [
                        {
                            "@type": "HowToSection",
                            "name": "Crust",
                            "itemListElement": [
                                {"text": "Mix flour and butter."},
                            ],
                        },
                        {
                            "@type": "HowToSection",
                            "name": "Filling",
                            "itemListElement": [
                                {"text": "Whisk eggs and sugar."},
                            ],
                        },
                    ],
                },
                {
                    "row_id": 2,
                    "name": "Plain Pasta",
                    "page_url": "https://test.example.com/pasta",
                    "recipeingredient": ["pasta", "salt"],
                    "recipeinstructions": [
                        {"text": "Boil water."},
                        {"text": "Add pasta and salt."},
                    ],
                },
            ]
        }
        path.write_bytes(dcs.build_synthetic_wdc_zip(rows_by_host))

    def test_main_runs_on_synthetic_corpora(self, tmp_path, capsys) -> None:
        rnlg_path = tmp_path / "rnlg.csv"
        wdc_path = tmp_path / "wdc.zip"
        self._write_synthetic_recipenlg(rnlg_path)
        self._write_synthetic_wdc(wdc_path)

        json_out = tmp_path / "out.json"
        rc = dcs.main(
            [
                "--recipenlg", str(rnlg_path),
                "--wdc", str(wdc_path),
                "--sample-size", "10",
                "--wdc-sample-size", "10",
                "--wdc-hosts", "1",
                "--examples-to-show", "5",
                "--seed", "42",
                "--json", str(json_out),
            ]
        )
        assert rc == 0

        out = capsys.readouterr().out
        assert "recipenlg" in out
        assert "wdc" in out
        assert "pilot recommendation" in out

        import json as _json

        payload = _json.loads(json_out.read_text())
        # RecipeNLG: 3 sampled, 1 component-structured (Cheesecake).
        assert payload["recipenlg"]["n_sampled"] == 3
        assert payload["recipenlg"]["n_component_structured"] == 1
        # WDC: 2 sampled, 1 HowToSection-driven structured (Sectioned Pie).
        assert payload["wdc"]["n_sampled"] == 2
        assert payload["wdc"]["n_component_structured"] == 1
        assert payload["wdc"]["n_how_to_section"] == 1

    def test_missing_corpus_path_returns_error(self, tmp_path, capsys) -> None:
        rc = dcs.main(
            [
                "--recipenlg", str(tmp_path / "missing.csv"),
                "--skip-wdc",
            ]
        )
        assert rc == 1
        err = capsys.readouterr().err
        assert "RecipeNLG CSV not found" in err


# --- recommend_pilot_corpus -----------------------------------------------


class TestRecommendation:
    def test_picks_higher_rate(self) -> None:
        rnlg = dcs.CorpusReport(
            name="recipenlg", n_sampled=1000, n_component_structured=5
        )
        wdc = dcs.CorpusReport(
            name="wdc", n_sampled=1000, n_component_structured=120
        )
        winner, reason = dcs.recommend_pilot_corpus(rnlg, wdc)
        assert winner == "wdc"
        assert "natural pilot" in reason

    def test_tie_when_equal(self) -> None:
        rnlg = dcs.CorpusReport(
            name="recipenlg", n_sampled=100, n_component_structured=10
        )
        wdc = dcs.CorpusReport(
            name="wdc", n_sampled=100, n_component_structured=10
        )
        winner, _ = dcs.recommend_pilot_corpus(rnlg, wdc)
        assert winner == "tie"
