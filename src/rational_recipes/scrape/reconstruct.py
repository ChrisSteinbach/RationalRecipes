"""Reconstruct merged-pipeline variants from on-disk CSV+manifest artifacts.

The inverse of ``pipeline_merged.emit_variants``. Built for
``RationalRecipes-v61w``: an existing scrape_merged.py extraction can be
imported into recipes.db without re-running the LLM. The reconstructed
``MergedVariantResult`` flows through the same ``CatalogDB.upsert_variant``
path the live pipeline now uses, so stats are computed identically.

Limitation: the CSV serialization in ``emit_variants`` only emits header
ingredients (those present in >= 50% of recipes). Non-header ingredients
in the manifest's ``canonical_ingredients`` are not in any CSV row and
reconstruct as zero across all rows. Acceptable for hand-cycle drops
where the dominant ingredients carry the story; new pipeline runs
write to recipes.db directly and don't go through this lossy CSV step.
"""

from __future__ import annotations

import csv
import logging
from collections.abc import Iterable
from pathlib import Path

from rational_recipes.ingredient import Factory as IngredientFactory
from rational_recipes.scrape.manifest import Manifest, VariantManifestEntry
from rational_recipes.scrape.pipeline_merged import (
    MergedNormalizedRow,
    MergedVariantResult,
    _resolve_unit,
)
from rational_recipes.units import BadUnitException

logger = logging.getLogger(__name__)


def parse_cell(cell: str) -> tuple[float, str] | None:
    """Parse a display-string cell like ``"12 tbsp"`` into ``(quantity, unit)``.

    Returns ``None`` for empty/unparseable input. ``"0"`` parses as
    ``(0.0, "g")`` so callers can normalize zero through the standard
    unit machinery (zero grams under any unit). Bare numbers without a
    unit ("12") are interpreted as grams — a conservative fallback for
    lossy CSV roundtrips where the unit was elided.
    """
    cell = cell.strip()
    if not cell:
        return None
    if cell == "0":
        return 0.0, "g"
    parts = cell.split(None, 1)
    try:
        qty = float(parts[0])
    except ValueError:
        return None
    if len(parts) < 2:
        return qty, "g"
    return qty, parts[1].strip()


def normalize_row_from_cells(
    *,
    url: str,
    title: str,
    header: list[str],
    cell_values: list[str],
    corpus: str = "recipenlg",
) -> MergedNormalizedRow:
    """Rebuild one ``MergedNormalizedRow`` from CSV cells alone.

    Mirrors ``pipeline_merged.normalize_merged_row`` but operates on the
    already-stringified cells from the on-disk CSV instead of parsed-LLM
    rows. Cells whose unit/ingredient can't be resolved contribute zero
    to the proportion total, matching the original pipeline's
    ``BadUnitException``/``KeyError`` handling.

    The returned row preserves the original cell strings verbatim in
    its ``cells`` dict so downstream insertion into ``parsed_ingredients``
    (via ``catalog_db._split_cell``) sees the same quantity/unit pairs
    the original run produced.
    """
    cells: dict[str, str] = {}
    grams: dict[str, float] = {}

    for ing_name, raw_cell in zip(header, cell_values, strict=False):
        cell = raw_cell.strip()
        cells[ing_name] = cell
        parsed = parse_cell(cell)
        if parsed is None:
            continue
        qty, unit_name = parsed

        try:
            ingredient = IngredientFactory.get_by_name(ing_name)
        except KeyError:
            continue

        resolved = _resolve_unit(unit_name)
        if resolved is None:
            continue
        unit, _ = resolved

        if qty == 0:
            grams[ing_name] = 0.0
            continue

        try:
            g = unit.norm(qty, ingredient)  # type: ignore[attr-defined]
        except BadUnitException:
            continue
        grams[ing_name] = float(g)

    total = sum(grams.values())
    if total > 0:
        proportions = {k: v / total * 100 for k, v in grams.items()}
    else:
        proportions = {}

    return MergedNormalizedRow(
        url=url,
        title=title,
        corpus=corpus,
        cells=cells,
        proportions=proportions,
    )


def reconstruct_variant(
    entry: VariantManifestEntry,
    csv_path: Path,
    *,
    corpus: str = "recipenlg",
) -> MergedVariantResult:
    """Rebuild a ``MergedVariantResult`` from one variant's CSV+manifest entry.

    Re-derives proportions per row by parsing the display-string cells
    through the same unit/ingredient machinery the live pipeline uses.
    The variant's ``canonical_ingredients`` and ``cooking_methods`` come
    straight from the manifest entry — these would otherwise be lost
    (the CSV header is the post-50%-frequency subset, not the full
    canonical set).
    """
    rows: list[MergedNormalizedRow] = []
    with csv_path.open(encoding="utf-8") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            return MergedVariantResult(
                variant_title=entry.title,
                canonical_ingredients=frozenset(entry.canonical_ingredients),
                cooking_methods=frozenset(entry.cooking_methods),
                normalized_rows=[],
                header_ingredients=[],
            )
        for i, csv_row in enumerate(reader):
            url = (
                entry.source_urls[i]
                if i < len(entry.source_urls)
                else ""
            )
            rows.append(
                normalize_row_from_cells(
                    url=url,
                    title=entry.title,
                    header=header,
                    cell_values=list(csv_row),
                    corpus=corpus,
                )
            )

    return MergedVariantResult(
        variant_title=entry.title,
        canonical_ingredients=frozenset(entry.canonical_ingredients),
        cooking_methods=frozenset(entry.cooking_methods),
        normalized_rows=rows,
        header_ingredients=list(header),
    )


def reconstruct_variants(
    directory: Path,
    *,
    corpus: str = "recipenlg",
) -> Iterable[MergedVariantResult]:
    """Yield reconstructed variants for every entry in ``manifest.json``.

    Skips entries whose advertised CSV path is missing from disk — logs
    a warning so the user sees the lossy import rather than getting a
    silent half-restore.
    """
    manifest = Manifest.read(directory / "manifest.json")
    for entry in manifest.variants:
        csv_path = directory / entry.csv_path
        if not csv_path.exists():
            logger.warning(
                "manifest references %s but file not found; skipping",
                csv_path,
            )
            continue
        yield reconstruct_variant(entry, csv_path, corpus=corpus)
