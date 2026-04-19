"""Merged-pipeline emission layer.

Turns per-variant normalized rows (produced upstream by the
LLM-parse + normalize path, which already exists in ``pipeline.py``
for the RecipeNLG case and needs extending for the merged case)
into the on-disk artifact the downstream review shell
(``RationalRecipes-eco``) and SQLite writer (``RationalRecipes-5ub``)
consume:

- One CSV per variant in the ``rr-stats``-compatible format.
- A single ``manifest.json`` indexing all variants by stable
  ``variant_id``.

The ``MergedVariantResult`` dataclass carries the data a variant
contributes to both artifacts. ``emit_variants()`` writes them to disk.
Both are LLM-free so the orchestration logic is testable without a
running Ollama instance.
"""

from __future__ import annotations

import csv
import re
from collections.abc import Sequence
from dataclasses import dataclass
from io import StringIO
from pathlib import Path

from rational_recipes.scrape.grouping import normalize_title
from rational_recipes.scrape.manifest import (
    Manifest,
    VariantManifestEntry,
    compute_variant_id,
)
from rational_recipes.scrape.merge import (
    DEFAULT_BUCKET_SIZE,
    proportion_bucket_dedup,
)


@dataclass(frozen=True, slots=True)
class MergedNormalizedRow:
    """One recipe's normalized ingredients, corpus-agnostic.

    ``cells`` maps canonical ingredient name to a ``"value unit"`` string
    compatible with ``rr-stats``. ``proportions`` maps the same names to
    grams-per-100g floats (the input to ``proportion_bucket_dedup``).
    """

    url: str
    title: str
    corpus: str
    cells: dict[str, str]
    proportions: dict[str, float]


@dataclass
class MergedVariantResult:
    """One variant's contribution to the merged-pipeline output.

    Built by the upstream orchestrator after merge + regrouping. Knows
    how to compute its own stable ``variant_id`` and emit itself as a
    CSV + manifest entry.
    """

    variant_title: str
    canonical_ingredients: frozenset[str]
    cooking_methods: frozenset[str]
    normalized_rows: list[MergedNormalizedRow]
    header_ingredients: list[str]

    @property
    def variant_id(self) -> str:
        return compute_variant_id(
            normalize_title(self.variant_title),
            self.canonical_ingredients,
            self.cooking_methods,
        )

    @property
    def source_urls(self) -> list[str]:
        return [row.url for row in self.normalized_rows if row.url]

    def dedup_in_place(self, *, bucket_size: float = DEFAULT_BUCKET_SIZE) -> int:
        """Apply proportion-bucket dedup to ``normalized_rows``.

        Returns the count of rows dropped.
        """
        before = len(self.normalized_rows)
        self.normalized_rows = proportion_bucket_dedup(
            self.normalized_rows,
            lambda r: r.proportions,
            bucket_size=bucket_size,
        )
        return before - len(self.normalized_rows)

    def to_csv(self) -> str:
        buf = StringIO()
        writer = csv.writer(buf)
        writer.writerow(self.header_ingredients)
        for row in self.normalized_rows:
            writer.writerow(row.cells.get(ing, "0") for ing in self.header_ingredients)
        return buf.getvalue()

    def csv_filename(self) -> str:
        """A filesystem-safe CSV name derived from title + variant_id.

        Non-alphanumeric characters are replaced with ``_`` to survive
        filesystems and URL encoders; ``variant_id`` suffix keeps
        names unique when title normalization collides.
        """
        slug = re.sub(r"[^\w-]+", "_", normalize_title(self.variant_title)).strip("_")
        if not slug:
            slug = "variant"
        return f"{slug}_{self.variant_id}.csv"

    def to_manifest_entry(self, csv_path: str) -> VariantManifestEntry:
        return VariantManifestEntry(
            variant_id=self.variant_id,
            title=normalize_title(self.variant_title),
            canonical_ingredients=tuple(sorted(self.canonical_ingredients)),
            cooking_methods=tuple(sorted(self.cooking_methods)),
            n_recipes=len(self.normalized_rows),
            csv_path=csv_path,
            source_urls=tuple(self.source_urls),
        )


def emit_variants(
    variants: Sequence[MergedVariantResult],
    output_dir: Path,
) -> Manifest:
    """Write per-variant CSVs + ``manifest.json`` to ``output_dir``.

    The directory is created if it doesn't exist. Variants with empty
    ``normalized_rows`` are skipped rather than written — they can't be
    meaningfully averaged and carry no reviewable information.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    entries: list[VariantManifestEntry] = []
    for variant in variants:
        if not variant.normalized_rows:
            continue
        csv_name = variant.csv_filename()
        csv_path = output_dir / csv_name
        csv_path.write_text(variant.to_csv(), encoding="utf-8")
        entries.append(variant.to_manifest_entry(csv_name))

    manifest = Manifest(variants=entries)
    manifest.write(output_dir / "manifest.json")
    return manifest
