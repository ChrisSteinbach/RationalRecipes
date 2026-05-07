"""Tests for ``scripts/render_drop.py`` covering the ia1x and ie1a integrations.

Existing drops with NULL ``canonical_instructions`` must render unchanged
from the pre-ia1x baseline (the per-source median path); drops with a
populated value must include the ``Canonical instructions (generative
consensus)`` section. The ie1a refinement replaces the literal-median
picker with a top-N completeness ranker — see ``TestTopNCentralPicker``
and ``TestInstructionScorer`` below.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

# scripts/ is on pythonpath via pyproject.toml's pytest config.
import render_drop  # noqa: E402

from rational_recipes.catalog_db import CatalogDB
from rational_recipes.render.instruction_picker import (
    pick_median_source,
    score_instructions,
)
from rational_recipes.scrape.pipeline_merged import (
    MergedNormalizedRow,
    MergedVariantResult,
)


def _seed_variant(db: CatalogDB) -> str:
    rows = [
        MergedNormalizedRow(
            url=f"https://example.com/r/{i}",
            title="pannkakor",
            corpus="recipenlg",
            cells={"flour": "100 g", "milk": "200 ml"},
            proportions={"flour": 33.0 + i, "milk": 67.0 - i},
        )
        for i in range(3)
    ]
    variant = MergedVariantResult(
        variant_title="pannkakor",
        canonical_ingredients=frozenset({"flour", "milk"}),
        cooking_methods=frozenset(),
        normalized_rows=rows,
        header_ingredients=["flour", "milk"],
    )
    db.upsert_variant(variant, l1_key="pannkakor", base_ingredient="flour")
    return variant.variant_id


class TestRenderBaseline:
    """NULL canonical_instructions: render falls back to the median-source
    path, unchanged from pre-ia1x output."""

    def test_uses_median_source_section(self, tmp_path: Path) -> None:
        db_path = tmp_path / "recipes.db"
        db = CatalogDB.open(db_path)
        try:
            vid = _seed_variant(db)
        finally:
            db.close()

        md = render_drop.render(db_path, vid)

        assert "## Instructions" in md
        # The pre-ia1x median-source path is intact.
        assert "RationalRecipes-r8hx option 1" in md
        # The new section is NOT present.
        assert "Canonical instructions" not in md

    def test_baseline_includes_quantities_table(self, tmp_path: Path) -> None:
        """Sanity: the ingredient table is unaffected by the ia1x edits."""
        db_path = tmp_path / "recipes.db"
        db = CatalogDB.open(db_path)
        try:
            vid = _seed_variant(db)
        finally:
            db.close()

        md = render_drop.render(db_path, vid)
        assert "Ingredients (mass fractions)" in md
        assert "flour" in md
        assert "milk" in md


class TestNaturalLanguageQuantities:
    """RationalRecipes-4ba4 / F4: density + whole-unit metadata renders
    a human-friendly Quantity column alongside the gram count."""

    def test_table_has_quantity_column(self, tmp_path: Path) -> None:
        db_path = tmp_path / "recipes.db"
        db = CatalogDB.open(db_path)
        try:
            vid = _seed_variant(db)
        finally:
            db.close()
        md = render_drop.render(db_path, vid)
        # New column header lands in the markdown table.
        assert "Quantity" in md

    def test_flour_renders_volume_form(self, tmp_path: Path) -> None:
        """flour has density data; the Quantity column should show a
        cup / tbsp / tsp form, not just the gram count."""
        db_path = tmp_path / "recipes.db"
        db = CatalogDB.open(db_path)
        try:
            vid = _seed_variant(db)
        finally:
            db.close()
        md = render_drop.render(db_path, vid)
        # The rendered drop must mention a volume unit somewhere in the
        # Quantity column. "cup" is the most likely for a 1 kg batch.
        assert "cup" in md

    def test_null_metadata_renders_em_dash_fallback(
        self, tmp_path: Path
    ) -> None:
        """When a canonical has no density / whole-unit data, the
        Quantity cell renders an em-dash so the column stays aligned
        rather than collapsing the table."""
        from rational_recipes.scrape.pipeline_merged import (
            MergedNormalizedRow as _Row,
        )
        from rational_recipes.scrape.pipeline_merged import (
            MergedVariantResult as _Variant,
        )

        # Use the pipeline's stat-write path but blank out the metadata
        # afterward so we exercise the render-side fallback.
        db_path = tmp_path / "recipes.db"
        db = CatalogDB.open(db_path)
        try:
            rows = [
                _Row(
                    url=f"https://example.com/n/{i}",
                    title="cookies",
                    corpus="recipenlg",
                    cells={"flour": "100 g"},
                    proportions={"flour": 100.0},
                )
                for i in range(3)
            ]
            variant = _Variant(
                variant_title="cookies",
                canonical_ingredients=frozenset({"flour"}),
                cooking_methods=frozenset(),
                normalized_rows=rows,
                header_ingredients=["flour"],
            )
            db.upsert_variant(variant, l1_key="cookies", base_ingredient="flour")
            # Force NULL metadata to test the fallback branch.
            db.connection.execute(
                "UPDATE variant_ingredient_stats "
                "SET density_g_per_ml = NULL, whole_unit_name = NULL, "
                "whole_unit_grams = NULL"
            )
            db.connection.commit()
            vid = variant.variant_id
        finally:
            db.close()
        md = render_drop.render(db_path, vid)
        # An em-dash appears in the Quantity column; the gram column
        # still renders normally.
        assert "—" in md

    def test_format_natural_quantity_whole_unit_wins(self) -> None:
        from render_drop import _format_natural_quantity

        # Egg-like ingredient: density is also populated, but whole-unit
        # rendering should win because "3 medium" reads better than "0.6 cup".
        out = _format_natural_quantity(150.0, 1.03, "medium", 50.0)
        assert "3 medium" == out

    def test_format_natural_quantity_density_form(self) -> None:
        from render_drop import _format_natural_quantity

        # 200 g flour at 0.55 g/ml ≈ 363 ml ≈ 1.5 cups → "1½ cup".
        out = _format_natural_quantity(200.0, 0.55, None, None)
        assert "cup" in out
        # Should be a fractional-cup form, not a tbsp.
        assert "tbsp" not in out

    def test_format_natural_quantity_empty_when_metadata_null(self) -> None:
        from render_drop import _format_natural_quantity

        assert _format_natural_quantity(100.0, None, None, None) == ""


class TestRenderWithCanonicalInstructions:
    """Populated canonical_instructions: render emits the new section
    with the exact label the bead requires."""

    def test_section_label_signals_generative_consensus(
        self, tmp_path: Path
    ) -> None:
        db_path = tmp_path / "recipes.db"
        db = CatalogDB.open(db_path)
        try:
            vid = _seed_variant(db)
            db.set_canonical_instructions(
                vid,
                "1. Whisk flour into milk.\n2. Cook on a hot griddle.",
            )
        finally:
            db.close()

        md = render_drop.render(db_path, vid)

        # Exact bead-required label, plus the explicit "generative" cue.
        assert "## Canonical instructions (generative consensus)" in md
        assert "generative consensus" in md.lower()

    def test_synthesized_steps_appear_verbatim(self, tmp_path: Path) -> None:
        db_path = tmp_path / "recipes.db"
        db = CatalogDB.open(db_path)
        try:
            vid = _seed_variant(db)
            db.set_canonical_instructions(
                vid,
                "1. Whisk flour into milk.\n2. Cook on a hot griddle.",
            )
        finally:
            db.close()

        md = render_drop.render(db_path, vid)
        assert "Whisk flour into milk." in md
        assert "Cook on a hot griddle." in md

    def test_canonical_path_replaces_median_source_section(
        self, tmp_path: Path
    ) -> None:
        """Once canonical_instructions is set, the median-source
        placeholder section steps aside — they describe two different
        things and we don't want both at once."""
        db_path = tmp_path / "recipes.db"
        db = CatalogDB.open(db_path)
        try:
            vid = _seed_variant(db)
            db.set_canonical_instructions(vid, "1. Mix.\n2. Bake.")
        finally:
            db.close()

        md = render_drop.render(db_path, vid)
        assert "RationalRecipes-r8hx option 1" not in md

    def test_reviewed_at_timestamp_renders(self, tmp_path: Path) -> None:
        db_path = tmp_path / "recipes.db"
        db = CatalogDB.open(db_path)
        try:
            vid = _seed_variant(db)
            db.set_canonical_instructions(
                vid,
                "1. Mix.",
                reviewed_at="2026-05-07T12:00:00+00:00",
            )
        finally:
            db.close()

        md = render_drop.render(db_path, vid)
        assert "2026-05-07T12:00:00+00:00" in md

    def test_unknown_variant_raises_systemexit(self, tmp_path: Path) -> None:
        db_path = tmp_path / "recipes.db"
        db = CatalogDB.open(db_path)
        try:
            _seed_variant(db)
        finally:
            db.close()
        with pytest.raises(SystemExit, match="not found"):
            render_drop.render(db_path, "nope")


class TestRenderHonorsFilterOverrides:
    """bz2e: a filter override removes a recipe from the source list,
    fixes the 'Averaged across N sources' count, and prevents the
    filtered recipe from being picked as the median-source for
    instructions. variant_members rows are intentionally preserved
    (sj18: filter is reversible) so render must derive the active set
    client-side from variant_overrides."""

    def test_filtered_recipe_omitted_from_source_list(
        self, tmp_path: Path
    ) -> None:
        db_path = tmp_path / "recipes.db"
        db = CatalogDB.open(db_path)
        try:
            vid = _seed_variant(db)
            members = db.get_variant_members(vid)
            assert len(members) == 3
            target = members[0].recipe_id
            target_url = members[0].url
            db.add_filter_override(vid, target, reason="test")
        finally:
            db.close()

        md = render_drop.render(db_path, vid)

        # The filtered URL must not appear under the source-recipes list.
        assert target_url not in md
        # The remaining two URLs do appear.
        survivors = [m.url for m in members if m.recipe_id != target]
        for url in survivors:
            assert url in md

    def test_averaged_count_matches_post_filter_member_set(
        self, tmp_path: Path
    ) -> None:
        db_path = tmp_path / "recipes.db"
        db = CatalogDB.open(db_path)
        try:
            vid = _seed_variant(db)
            members = db.get_variant_members(vid)
            db.add_filter_override(vid, members[0].recipe_id, reason="test")
        finally:
            db.close()

        md = render_drop.render(db_path, vid)
        assert "Averaged across 2 sources" in md
        assert "Averaged across 3 sources" not in md

    def test_filter_redirects_median_source_pick(
        self, tmp_path: Path
    ) -> None:
        """The median-source path must not name a filtered recipe.
        The seeded variant orders members by outlier_score; filtering
        the lowest-outlier member must shift the rendered link to the
        next-best survivor."""
        db_path = tmp_path / "recipes.db"
        db = CatalogDB.open(db_path)
        try:
            vid = _seed_variant(db)
            members = db.get_variant_members(vid)
            # Members come back in best-outlier-first order — same
            # ordering render_drop's sources query uses. The first
            # is the median-source candidate by default.
            top = members[0]
            db.add_filter_override(vid, top.recipe_id, reason="outlier")
        finally:
            db.close()

        md = render_drop.render(db_path, vid)
        # The filtered recipe's URL must not appear as the bracketed
        # median-source link.
        assert top.url not in md

    def test_substitute_override_leaves_source_list_intact(
        self, tmp_path: Path
    ) -> None:
        """Substitute overrides reshape variant_ingredient_stats but
        contribute no excluded recipes — the source list must remain
        complete."""
        db_path = tmp_path / "recipes.db"
        db = CatalogDB.open(db_path)
        try:
            vid = _seed_variant(db)
            members = db.get_variant_members(vid)
            db.add_substitute_override(vid, "milk", "buttermilk")
        finally:
            db.close()

        md = render_drop.render(db_path, vid)
        assert "Averaged across 3 sources" in md
        for m in members:
            assert m.url in md


_TERSE_INSTRUCTIONS = (
    "1. Cream shortening, margarine and sugar.\n"
    "2. Add eggs and vanilla. Add dry ingredients. "
    "Add chocolate chips last.\n"
    "3. Bake at 350° for 10 to 12 minutes."
)

_COMPLETE_INSTRUCTIONS = (
    "1. Preheat oven to 375°F.\n"
    "2. Cream butter and sugars together until light and fluffy, "
    "about 3 minutes.\n"
    "3. Add eggs one at a time, beating well after each addition.\n"
    "4. Stir in vanilla extract.\n"
    "5. In a separate bowl, whisk together flour, baking soda, "
    "and salt.\n"
    "6. Gradually add dry ingredients to the butter mixture, "
    "mixing until just combined.\n"
    "7. Fold in chocolate chips.\n"
    "8. Drop by rounded teaspoons onto an ungreased cookie sheet.\n"
    "9. Bake for 10 to 12 minutes, until edges are golden brown.\n"
    "10. Cool on a wire rack for 5 minutes before transferring."
)


def _add_directions_column(db_path: Path) -> None:
    """Simulate F5 (RationalRecipes-15g4): add ``directions_text`` to recipes.

    F5 lands the column for real in a later wave. Until then, the
    picker reads the column defensively (catches OperationalError);
    these tests opt in by adding the column themselves so they can
    exercise the populated-text path."""
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("ALTER TABLE recipes ADD COLUMN directions_text TEXT")
        conn.commit()
    except sqlite3.OperationalError:
        # Already added — fine.
        pass
    finally:
        conn.close()


def _set_directions(db_path: Path, recipe_id: str, text: str) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "UPDATE recipes SET directions_text = ? WHERE recipe_id = ?",
            (text, recipe_id),
        )
        conn.commit()
    finally:
        conn.close()


def _seed_wide_variant(db: CatalogDB, n: int = 7) -> str:
    """Seed a variant with ``n`` source recipes (≥6 to exercise top-5 gate)."""
    rows = [
        MergedNormalizedRow(
            url=f"https://example.com/wide/{i}",
            title="cookies",
            corpus="recipenlg",
            cells={"flour": "100 g", "sugar": "50 g"},
            proportions={"flour": 60.0 + i * 0.5, "sugar": 40.0 - i * 0.5},
        )
        for i in range(n)
    ]
    variant = MergedVariantResult(
        variant_title="cookies",
        canonical_ingredients=frozenset({"flour", "sugar"}),
        cooking_methods=frozenset(),
        normalized_rows=rows,
        header_ingredients=["flour", "sugar"],
    )
    db.upsert_variant(variant, l1_key="cookies", base_ingredient="flour")
    return variant.variant_id


class TestInstructionScorer:
    """Pure-function scoring: deterministic, no DB, no LLM."""

    def test_empty_text_scores_zero(self) -> None:
        assert score_instructions("") == 0.0
        assert score_instructions(None) == 0.0

    def test_complete_beats_terse(self) -> None:
        # The F10 failure case: literal-median is terse, runner-up
        # is complete. Scorer must order complete > terse.
        assert score_instructions(_COMPLETE_INSTRUCTIONS) > score_instructions(
            _TERSE_INSTRUCTIONS
        )

    def test_preheat_keyword_adds_score(self) -> None:
        without = "Mix the dough. Bake until done."
        with_preheat = "Preheat the oven. Mix the dough. Bake until done."
        assert score_instructions(with_preheat) > score_instructions(without)

    def test_cooling_keyword_adds_score(self) -> None:
        without = "Mix the dough. Bake at 350°F for 12 minutes."
        with_cooling = (
            "Mix the dough. Bake at 350°F for 12 minutes. "
            "Cool on a wire rack."
        )
        assert score_instructions(with_cooling) > score_instructions(without)

    def test_timing_keyword_adds_score(self) -> None:
        without = "Mix. Bake."
        with_timing = "Mix. Bake for 10 minutes."
        assert score_instructions(with_timing) > score_instructions(without)

    def test_step_count_caps(self) -> None:
        # 50 short numbered steps must not blow up the score; the cap
        # keeps the heuristic stable against pathological prose.
        many = "\n".join(f"{i}. step" for i in range(1, 51))
        score = score_instructions(many)
        # 20 (step cap) + 5.5 (length) + 0 (no keywords) ≈ 25.5
        assert score < 30.0

    def test_deterministic(self) -> None:
        for _ in range(5):
            assert score_instructions(_COMPLETE_INSTRUCTIONS) == score_instructions(
                _COMPLETE_INSTRUCTIONS
            )


class TestTopNCentralPicker:
    """ie1a: pick the most-complete instructions among the top-N central
    sources, falling back to literal-median when no candidate has
    directions_text (e.g. before F5 lands or every candidate is NULL)."""

    def test_falls_back_to_literal_median_when_column_missing(
        self, tmp_path: Path
    ) -> None:
        """Pre-F5 path: directions_text column doesn't exist on the DB.

        The picker must catch the OperationalError, treat all candidates
        as 0-scoring, and return active_sources[0] without raising."""
        db_path = tmp_path / "recipes.db"
        db = CatalogDB.open(db_path)
        try:
            vid = _seed_variant(db)
            members = db.get_variant_members(vid)
        finally:
            db.close()

        md = render_drop.render(db_path, vid)
        # Lowest-outlier (members[0]) is named in the bracketed link.
        assert members[0].url in md

    def test_falls_back_to_literal_median_when_all_directions_null(
        self, tmp_path: Path
    ) -> None:
        """Post-F5 but every candidate's directions_text is NULL: same
        fallback — pick the lowest-outlier source unchanged."""
        db_path = tmp_path / "recipes.db"
        db = CatalogDB.open(db_path)
        try:
            vid = _seed_variant(db)
            members = db.get_variant_members(vid)
        finally:
            db.close()
        _add_directions_column(db_path)
        # Leave directions_text NULL for every recipe.

        md = render_drop.render(db_path, vid)
        assert members[0].url in md

    def test_picks_better_instructions_within_top_n(
        self, tmp_path: Path
    ) -> None:
        """The F10 fix: when a top-N runner-up has visibly better
        instructions, it wins over the literal median."""
        db_path = tmp_path / "recipes.db"
        db = CatalogDB.open(db_path)
        try:
            vid = _seed_variant(db)
            members = db.get_variant_members(vid)
        finally:
            db.close()
        _add_directions_column(db_path)
        # Make the literal-median (lowest outlier_score) terse and a
        # near-central runner-up complete. Picker must select the runner-up.
        _set_directions(db_path, members[0].recipe_id, _TERSE_INSTRUCTIONS)
        _set_directions(db_path, members[1].recipe_id, _COMPLETE_INSTRUCTIONS)

        md = render_drop.render(db_path, vid)
        # Find the bracketed median-source line — the literal-median
        # URL must NOT be it; the runner-up's URL must be. (Both URLs
        # still appear in the source-recipes list at the bottom — that
        # surface is unaffected.)
        bracketed = [
            line for line in md.splitlines()
            if line.startswith("> [") and "](" in line
        ]
        assert any(members[1].url in line for line in bracketed)
        assert not any(members[0].url in line for line in bracketed)

    def test_top_n_gate_excludes_far_outliers_with_great_text(
        self, tmp_path: Path
    ) -> None:
        """A non-top-N candidate's instructions, no matter how complete,
        must NOT win — the top-N gate clamps the candidate pool to the
        most-central sources."""
        db_path = tmp_path / "recipes.db"
        db = CatalogDB.open(db_path)
        try:
            vid = _seed_wide_variant(db, n=7)
            members = db.get_variant_members(vid)
        finally:
            db.close()
        assert len(members) >= 7
        _add_directions_column(db_path)
        # Every top-5 candidate gets terse text; the 6th (outside the
        # top-5) gets the complete text.
        for m in members[:5]:
            _set_directions(db_path, m.recipe_id, _TERSE_INSTRUCTIONS)
        _set_directions(db_path, members[5].recipe_id, _COMPLETE_INSTRUCTIONS)

        chosen = pick_median_source(
            [
                {"recipe_id": m.recipe_id, "outlier_score": 0.0 + i, "url": m.url}
                for i, m in enumerate(members)
            ],
            sqlite3.connect(db_path),
            top_n=5,
        )
        assert chosen["recipe_id"] != members[5].recipe_id
        # Concretely: every top-5 candidate has identical text, so the
        # tie-break by outlier_score picks the lowest-outlier — members[0].
        assert chosen["recipe_id"] == members[0].recipe_id

    def test_pick_is_deterministic_under_score_ties(
        self, tmp_path: Path
    ) -> None:
        """When all top-N candidates score identically, the pick falls
        back to outlier_score ascending — same input, same output."""
        db_path = tmp_path / "recipes.db"
        db = CatalogDB.open(db_path)
        try:
            vid = _seed_variant(db)
            members = db.get_variant_members(vid)
        finally:
            db.close()
        _add_directions_column(db_path)
        # All three get the same complete text; they tie on score.
        for m in members:
            _set_directions(db_path, m.recipe_id, _COMPLETE_INSTRUCTIONS)

        for _ in range(3):
            md = render_drop.render(db_path, vid)
            # Tie-break by outlier_score: members[0] still wins.
            bracketed = [
                line for line in md.splitlines()
                if line.startswith("> [") and "](" in line
            ]
            assert any(members[0].url in line for line in bracketed)


# Mirror test_synthesize_instructions.py: be defensive about scripts/
# being on sys.path even when this file is invoked outside pytest.
def _ensure_scripts_on_path() -> None:
    scripts = Path(__file__).resolve().parent.parent / "scripts"
    if str(scripts) not in sys.path:
        sys.path.insert(0, str(scripts))


_ensure_scripts_on_path()
