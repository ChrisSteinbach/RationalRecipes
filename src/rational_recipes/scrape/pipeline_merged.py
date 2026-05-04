"""Merged-pipeline emission + orchestration.

The module owns two kinds of work:

- **Emission layer** (``MergedVariantResult``, ``emit_variants``): pure
  data transforms that turn per-variant normalized rows into the
  on-disk artifacts consumed by downstream beads (review shell ``eco``,
  SQLite writer ``5ub``). One CSV per variant in the ``rr-stats``-
  compatible format, plus one ``manifest.json`` indexing all variants
  by stable ``variant_id``.

- **Orchestration** (``build_variants``, ``run_merged_pipeline``): end
  to end wiring that loads both corpora, LLM-extracts WDC ingredient
  names, merges them at the row level, groups into variants, LLM-
  parses each surviving row's ingredient lines, normalizes to rr-stats
  cells + proportion dicts, applies within-variant proportion-bucket
  dedup, and emits. The pure helpers accept injectable parse/extract
  callbacks so orchestration logic is testable without a running
  Ollama instance.
"""

from __future__ import annotations

import csv
import logging
import re
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from io import StringIO
from pathlib import Path

from rational_recipes.ingredient import Factory as IngredientFactory
from rational_recipes.scrape.canonical import canonicalize_name
from rational_recipes.scrape.grouping import (
    DEFAULT_MAX_VARIANTS_PER_L1,
    DEFAULT_MIN_VARIANT_SIZE,
    group_by_ingredients,
    group_by_title,
    normalize_title,
)
from rational_recipes.scrape.manifest import (
    Manifest,
    VariantManifestEntry,
    compute_variant_id,
)
from rational_recipes.scrape.merge import (
    DEFAULT_BUCKET_SIZE,
    MergedRecipe,
    MergeStats,
    merge_corpora,
    proportion_bucket_dedup,
)
from rational_recipes.scrape.outlier import compute_outlier_scores
from rational_recipes.scrape.parse import (
    OLLAMA_BASE_URL,
    ParsedIngredient,
    parse_ingredient_lines,
)
from rational_recipes.scrape.recipenlg import Recipe, RecipeNLGLoader
from rational_recipes.scrape.wdc import WDCLoader, WDCRecipe, extract_batch
from rational_recipes.units import BadUnitException
from rational_recipes.units import Factory as UnitFactory

logger = logging.getLogger(__name__)

# Drop ingredients appearing in fewer than this fraction of a variant's
# recipes (filter only fires when the variant has at least
# _INGREDIENT_FREQ_MIN_N recipes so tiny variants aren't over-pruned).
# Applied at variant-formation time so variant_id, canonical_ingredients
# and stats agree by construction (RationalRecipes-70o).
INGREDIENT_FREQ_THRESHOLD: float = 0.10
_INGREDIENT_FREQ_MIN_N: int = 5

# Unit-name aliases the LLM may emit that aren't directly registered on
# UnitFactory. Mirrors pipeline.py's private table for RecipeNLG output.
_UNIT_ALIASES = {
    "cup": "cup",
    "cups": "cup",
    "tablespoon": "tbsp",
    "tablespoons": "tbsp",
    "teaspoon": "tsp",
    "teaspoons": "tsp",
    "ounce": "oz",
    "ounces": "oz",
    "pound": "lb",
    "pounds": "lb",
}


ParseFn = Callable[[list[str]], list[ParsedIngredient | None]]
"""Callback type for LLM-parsing one recipe's ingredient lines.

Shape matches ``parse_ingredient_lines(lines, model=..., base_url=...)``
with the LLM params bound. Tests inject a stub that returns canned
``ParsedIngredient``s without touching Ollama.
"""


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

    def outlier_scores(self) -> list[float]:
        """Per-row Euclidean distance from the variant's median (bead 0g3)."""
        return compute_outlier_scores(
            [row.proportions for row in self.normalized_rows],
            self.canonical_ingredients,
        )

    def to_manifest_entry(self, csv_path: str) -> VariantManifestEntry:
        scores = self.outlier_scores()
        return VariantManifestEntry(
            variant_id=self.variant_id,
            title=normalize_title(self.variant_title),
            canonical_ingredients=tuple(sorted(self.canonical_ingredients)),
            cooking_methods=tuple(sorted(self.cooking_methods)),
            n_recipes=len(self.normalized_rows),
            csv_path=csv_path,
            source_urls=tuple(self.source_urls),
            row_outlier_scores=tuple(scores),
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


# --- Orchestration: merged-pipeline end-to-end ---


def _resolve_unit(unit_name: str) -> tuple[object, str] | None:
    """Look up a unit by name, applying LLM-alias normalization.

    Returns ``(unit, canonical_name)`` or ``None`` if unresolvable.
    """
    name = unit_name.strip()
    unit = UnitFactory.get_by_name(name)
    if unit is not None:
        return unit, name
    alias = _UNIT_ALIASES.get(name.lower())
    if alias:
        unit = UnitFactory.get_by_name(alias)
        if unit is not None:
            return unit, alias
    return None


def normalize_merged_row(
    url: str,
    title: str,
    corpus: str,
    parsed_ingredients: Iterable[ParsedIngredient],
) -> tuple[MergedNormalizedRow | None, list[str]]:
    """Normalize one recipe's parsed ingredients into a merged row.

    Returns ``(row, skipped)``. ``row`` is ``None`` if fewer than one
    ingredient resolved to both a known unit and a DB entry — that row
    carries no information and shouldn't enter a variant. ``skipped``
    lists each ingredient that couldn't be normalized (for miss-rate
    tracking in calling code).
    """
    cells: dict[str, str] = {}
    grams: dict[str, float] = {}
    skipped: list[str] = []

    for parsed in parsed_ingredients:
        canonical = canonicalize_name(parsed.ingredient)
        if not canonical:
            continue
        try:
            ingredient = IngredientFactory.get_by_name(canonical)
        except KeyError:
            skipped.append(canonical)
            continue

        resolved = _resolve_unit(parsed.unit)
        if resolved is None:
            skipped.append(f"{canonical} (unknown unit: {parsed.unit})")
            continue
        unit, unit_name = resolved

        quantity = parsed.quantity
        if quantity == 0:
            cells[canonical] = "0"
            grams[canonical] = 0.0
            continue

        try:
            g = unit.norm(quantity, ingredient)  # type: ignore[attr-defined]
        except BadUnitException:
            skipped.append(
                f"{canonical} (incompatible unit {unit_name} for this ingredient)"
            )
            continue

        cells[canonical] = f"{quantity:g} {unit_name}"
        grams[canonical] = float(g)

    if not cells:
        return None, skipped

    total = sum(grams.values())
    if total > 0:
        proportions = {k: v / total * 100 for k, v in grams.items()}
    else:
        # All-zero row (every ingredient resolved but quantity 0). Keep
        # cells for CSV fidelity, but proportion vector is empty so the
        # row contributes nothing to outlier / dedup work.
        proportions = {}

    return (
        MergedNormalizedRow(
            url=url,
            title=title,
            corpus=corpus,
            cells=cells,
            proportions=proportions,
        ),
        skipped,
    )


@dataclass(frozen=True)
class PipelineRunStats:
    """Summary numbers from a merged-pipeline run."""

    recipenlg_in: int
    wdc_in: int
    merge_stats: MergeStats
    l1_groups_kept: int
    l2_variants_kept: int
    rows_parsed: int
    rows_normalized: int
    rows_dedup_dropped: int
    db_misses: dict[str, int]


def _apply_freq_filter(
    canonical: Iterable[str],
    normalized_rows: Sequence[MergedNormalizedRow],
) -> set[str]:
    """Drop low-frequency ingredients from a candidate variant's set.

    Presence is non-zero proportion (matches the previous catalog_db
    semantics — zero-quantity ingredients don't count as "present").
    Filter only fires when the variant has at least
    ``_INGREDIENT_FREQ_MIN_N`` recipes.
    """
    canonical_set = set(canonical)
    n = len(normalized_rows)
    if n < _INGREDIENT_FREQ_MIN_N:
        return canonical_set
    kept: set[str] = set()
    for name in canonical_set:
        present = sum(
            1 for row in normalized_rows if row.proportions.get(name, 0.0) > 0.0
        )
        if present / n >= INGREDIENT_FREQ_THRESHOLD:
            kept.add(name)
    return kept


def _derive_header(
    normalized_rows: Sequence[MergedNormalizedRow],
    canonical: Iterable[str],
) -> list[str]:
    """Header: ingredients in canonical and present in >= half the rows."""
    canonical_set = set(canonical)
    min_appearance = max(1, len(normalized_rows) // 2)
    counts: dict[str, int] = {}
    for row in normalized_rows:
        for name in row.cells:
            counts[name] = counts.get(name, 0) + 1
    return sorted(
        name
        for name, c in counts.items()
        if c >= min_appearance and name in canonical_set
    )


def _merge_duplicate_variants(
    variants: Sequence[MergedVariantResult],
    *,
    bucket_size: float,
) -> tuple[list[MergedVariantResult], int]:
    """Combine variants sharing a ``variant_id`` into one.

    Two L2 clusters in the same L1 group can produce the same
    post-filter ``variant_id`` when their pre-filter ingredient sets
    differ only by low-frequency noise. Merge their rows (union by URL),
    re-derive the header, and re-run dedup so the merged set behaves
    like a single variant.
    """
    by_id: dict[str, MergedVariantResult] = {}
    touched: set[str] = set()
    for v in variants:
        vid = v.variant_id
        existing = by_id.get(vid)
        if existing is None:
            by_id[vid] = v
            continue
        existing_urls = {row.url for row in existing.normalized_rows}
        for row in v.normalized_rows:
            if row.url in existing_urls:
                continue
            existing.normalized_rows.append(row)
            existing_urls.add(row.url)
        touched.add(vid)

    dropped = 0
    for vid in touched:
        v = by_id[vid]
        v.header_ingredients = _derive_header(
            v.normalized_rows, v.canonical_ingredients
        )
        dropped += v.dedup_in_place(bucket_size=bucket_size)
    return list(by_id.values()), dropped


def _cap_per_l1(
    variants: Sequence[MergedVariantResult],
    *,
    max_per_l1: int,
) -> list[MergedVariantResult]:
    """Keep only the top-N largest variants per L1 (normalized title).

    Sort within each L1 by ``len(normalized_rows)`` descending; ties are
    broken by ``variant_id`` so the cap is deterministic across runs.
    Returns variants in the same group-relative order as the input,
    flattened across L1s.
    """
    if max_per_l1 <= 0:
        return list(variants)
    by_l1: dict[str, list[MergedVariantResult]] = {}
    order: list[str] = []
    for v in variants:
        key = normalize_title(v.variant_title)
        if key not in by_l1:
            by_l1[key] = []
            order.append(key)
        by_l1[key].append(v)
    out: list[MergedVariantResult] = []
    for key in order:
        members = by_l1[key]
        members.sort(
            key=lambda v: (-len(v.normalized_rows), v.variant_id),
        )
        out.extend(members[:max_per_l1])
    return out


def build_variants(
    merged_recipes: Sequence[MergedRecipe],
    *,
    parse_fn: ParseFn,
    l1_min_group_size: int,
    l2_similarity_threshold: float,
    l2_min_group_size: int,
    min_variant_size: int = DEFAULT_MIN_VARIANT_SIZE,
    max_variants_per_l1: int = DEFAULT_MAX_VARIANTS_PER_L1,
    bucket_size: float = DEFAULT_BUCKET_SIZE,
) -> tuple[list[MergedVariantResult], PipelineRunStats]:
    """Group merged recipes, LLM-parse each, normalize, and dedup.

    Runs L1 (title) then L2 (ingredient set), LLM-parses and normalizes
    each surviving L2 cluster into one variant. Pure orchestration over
    injectable ``parse_fn`` — tests pass a stub that returns canned
    parsed lines, so this function exercises full variant-building
    without Ollama.

    L3 cookingMethod partitioning was removed in RationalRecipes-gc7;
    ``cooking_methods`` on the resulting variant is always
    ``frozenset()``. Per-recipe ``cooking_methods`` data still flows
    through ``MergedRecipe`` for downstream PWA filtering.

    Variant proliferation is capped two ways (RationalRecipes-dos):

    - ``min_variant_size`` (default 5) drops variants whose recipe count
      falls below the threshold — kills the long tail of low-confidence
      averages.
    - ``max_variants_per_l1`` (default 5) keeps only the top-N largest
      variants within each L1 group, ranked by ``n_recipes``. Pass 0 to
      disable the cap entirely.
    """
    l1_groups = group_by_title(merged_recipes, min_group_size=l1_min_group_size)
    logger.info("L1: %d title groups kept", len(l1_groups))

    variants: list[MergedVariantResult] = []
    rows_parsed = 0
    rows_normalized = 0
    rows_dedup_dropped = 0
    db_misses: dict[str, int] = {}

    for title_key, l1_members in l1_groups.items():
        l2_clusters = group_by_ingredients(
            l1_members,
            similarity_threshold=l2_similarity_threshold,
            min_group_size=l2_min_group_size,
        )
        logger.info(
            "  L1 %r (%d recipes) → %d L2 cluster(s)",
            title_key,
            len(l1_members),
            len(l2_clusters),
        )

        for cluster in l2_clusters:
            canonical_ingredients: set[str] = set()
            normalized_rows: list[MergedNormalizedRow] = []

            for recipe in cluster.recipes:
                rows_parsed += 1
                raw_parsed = parse_fn(list(recipe.ingredients))
                parsed = [p for p in raw_parsed if p is not None]
                if not parsed:
                    continue

                row, skipped = normalize_merged_row(
                    url=recipe.url,
                    title=recipe.title,
                    corpus=recipe.corpus,
                    parsed_ingredients=parsed,
                )
                for miss in skipped:
                    base = miss.split(" (")[0]
                    db_misses[base] = db_misses.get(base, 0) + 1
                if row is None:
                    continue

                normalized_rows.append(row)
                canonical_ingredients.update(row.cells.keys())
                rows_normalized += 1

            if len(normalized_rows) < min_variant_size:
                continue

            filtered_canonical = _apply_freq_filter(
                canonical_ingredients, normalized_rows
            )
            if not filtered_canonical:
                continue

            header = _derive_header(normalized_rows, filtered_canonical)
            if not header:
                continue

            variant = MergedVariantResult(
                variant_title=title_key,
                canonical_ingredients=frozenset(filtered_canonical),
                cooking_methods=frozenset(),
                normalized_rows=normalized_rows,
                header_ingredients=header,
            )
            dropped = variant.dedup_in_place(bucket_size=bucket_size)
            rows_dedup_dropped += dropped
            if len(variant.normalized_rows) >= min_variant_size:
                variants.append(variant)

    variants, merge_dedup_dropped = _merge_duplicate_variants(
        variants, bucket_size=bucket_size
    )
    rows_dedup_dropped += merge_dedup_dropped
    variants = [
        v for v in variants if len(v.normalized_rows) >= min_variant_size
    ]
    variants = _cap_per_l1(variants, max_per_l1=max_variants_per_l1)

    stats = PipelineRunStats(
        recipenlg_in=0,  # filled by caller
        wdc_in=0,
        merge_stats=MergeStats(0, 0, 0, 0, 0),
        l1_groups_kept=len(l1_groups),
        l2_variants_kept=len(variants),
        rows_parsed=rows_parsed,
        rows_normalized=rows_normalized,
        rows_dedup_dropped=rows_dedup_dropped,
        db_misses=db_misses,
    )
    return variants, stats


def run_merged_pipeline(
    *,
    recipenlg_path: Path,
    wdc_zip_path: Path,
    title_query: str,
    output_dir: Path,
    wdc_hosts: Sequence[str] | None = None,
    l1_min_group_size: int = 3,
    l2_similarity_threshold: float = 0.6,
    l2_min_group_size: int = 3,
    min_variant_size: int = DEFAULT_MIN_VARIANT_SIZE,
    max_variants_per_l1: int = DEFAULT_MAX_VARIANTS_PER_L1,
    bucket_size: float = DEFAULT_BUCKET_SIZE,
    llm_model: str = "gemma4:e2b",
    ollama_url: str = OLLAMA_BASE_URL,
) -> tuple[Manifest, PipelineRunStats]:
    """End-to-end: load both corpora, merge, LLM-parse, normalize, emit.

    Calls Ollama twice per recipe (once for WDC ingredient-name
    extraction before merge, once for ingredient-line parsing after
    merge); start with a small title_query and tight min-group-size
    when exercising on real data.
    """
    rnlg_loader = RecipeNLGLoader(path=recipenlg_path)
    rnlg_matching: list[Recipe] = list(rnlg_loader.search_title(title_query))
    logger.info("RecipeNLG: %d recipes match %r", len(rnlg_matching), title_query)

    wdc_loader = WDCLoader(zip_path=wdc_zip_path)
    wdc_raw: list[WDCRecipe] = list(
        wdc_loader.search_title(title_query, hosts=wdc_hosts)
    )
    logger.info("WDC: %d recipes match %r", len(wdc_raw), title_query)

    # LLM-extract ingredient names on WDC (needed for the near-dup
    # Jaccard step of merge_corpora).
    wdc_populated = extract_batch(wdc_raw, model=llm_model, base_url=ollama_url)

    merged, merge_stats = merge_corpora(rnlg_matching, wdc_populated)
    logger.info(
        "Merge: rnlg=%d wdc=%d → merged=%d (url_dups=%d, near_dups=%d)",
        merge_stats.recipenlg_in,
        merge_stats.wdc_in,
        merge_stats.merged_out,
        merge_stats.url_duplicates,
        merge_stats.near_dup_duplicates,
    )

    def _parse(lines: list[str]) -> list[ParsedIngredient | None]:
        return parse_ingredient_lines(lines, model=llm_model, base_url=ollama_url)

    variants, partial_stats = build_variants(
        merged,
        parse_fn=_parse,
        l1_min_group_size=l1_min_group_size,
        l2_similarity_threshold=l2_similarity_threshold,
        l2_min_group_size=l2_min_group_size,
        min_variant_size=min_variant_size,
        max_variants_per_l1=max_variants_per_l1,
        bucket_size=bucket_size,
    )

    manifest = emit_variants(variants, output_dir)
    logger.info(
        "Emitted %d variant(s) to %s (dropped %d rows in within-variant dedup)",
        len(manifest.variants),
        output_dir,
        partial_stats.rows_dedup_dropped,
    )

    stats = PipelineRunStats(
        recipenlg_in=len(rnlg_matching),
        wdc_in=len(wdc_raw),
        merge_stats=merge_stats,
        l1_groups_kept=partial_stats.l1_groups_kept,
        l2_variants_kept=partial_stats.l2_variants_kept,
        rows_parsed=partial_stats.rows_parsed,
        rows_normalized=partial_stats.rows_normalized,
        rows_dedup_dropped=partial_stats.rows_dedup_dropped,
        db_misses=partial_stats.db_misses,
    )
    return manifest, stats
