"""Tests for ``scripts/render_drop.py`` covering the ia1x integration.

Existing drops with NULL ``canonical_instructions`` must render unchanged
from the pre-ia1x baseline (the per-source median path); drops with a
populated value must include the new ``Canonical instructions
(generative consensus)`` section.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# scripts/ is on pythonpath via pyproject.toml's pytest config.
import render_drop  # noqa: E402

from rational_recipes.catalog_db import CatalogDB
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


# Mirror test_synthesize_instructions.py: be defensive about scripts/
# being on sys.path even when this file is invoked outside pytest.
def _ensure_scripts_on_path() -> None:
    scripts = Path(__file__).resolve().parent.parent / "scripts"
    if str(scripts) not in sys.path:
        sys.path.insert(0, str(scripts))


_ensure_scripts_on_path()
