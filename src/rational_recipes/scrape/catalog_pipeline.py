"""Whole-corpus extraction pipeline (bead vwt.2).

Streams both corpora into title-keyed L1 groups, thresholds, and for each
surviving group runs LLM name-extraction (WDC), cross-corpus merge, L2
Jaccard clustering, L3 cookingMethod partitioning, LLM line-parsing,
normalization, and within-variant dedup. Each variant is written to
``recipes.db`` via ``CatalogDB.upsert_variant``; each L1 group gets a
``query_runs`` row so a killed run resumes at the next group.

The LLM boundaries (``parse_fn`` for ingredient-line parsing, ``extract_fn``
for WDC name extraction) are injectable — tests pass stubs so full
orchestration runs without Ollama. Defaults bind the live Ollama endpoints
at CLI time.
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from rational_recipes.catalog_db import CatalogDB
from rational_recipes.corpus_title_survey import (
    LANGUAGE_FILTER_ALL,
    LANGUAGE_FILTER_PREDICATES,
)
from rational_recipes.scrape.grouping import (
    DEFAULT_L3_MIN_VARIANT_SIZE,
    normalize_title,
)
from rational_recipes.scrape.merge import (
    DEFAULT_BUCKET_SIZE,
    merge_corpora,
)
from rational_recipes.scrape.parse import ParsedIngredient
from rational_recipes.scrape.pipeline_merged import (
    MergedVariantResult,
    build_variants,
)
from rational_recipes.scrape.recipenlg import Recipe, RecipeNLGLoader
from rational_recipes.scrape.wdc import WDCLoader, WDCRecipe

logger = logging.getLogger(__name__)


ParseFn = Callable[[list[str]], list[ParsedIngredient | None]]
"""Bound-LLM callback shaping ``parse_ingredient_lines``."""

ExtractFn = Callable[[Sequence[WDCRecipe]], list[WDCRecipe]]
"""Bound-LLM callback shaping ``extract_batch``."""


# Lowercase Swedish diacritics — language detection sentinel for the
# recipes.language column. Sufficient for the en+sv scope; broader locale
# detection belongs elsewhere.
_SWEDISH_DIACRITICS = frozenset("åäö")


def detect_language(title: str) -> str:
    """Coarse en/sv bucket for the ``recipes.language`` column."""
    for ch in title.lower():
        if ch in _SWEDISH_DIACRITICS:
            return "sv"
    return "en"


def compute_corpus_revisions(recipenlg_path: Path, wdc_zip_path: Path) -> str:
    """Fingerprint both corpora for the ``query_runs.corpus_revisions`` column.

    Size+mtime is coarse but stable across identical file copies on the same
    filesystem. A mismatch forces re-processing; a match allows incremental
    resume at the L1-group boundary.
    """
    rnlg_stat = recipenlg_path.stat()
    wdc_stat = wdc_zip_path.stat()
    return (
        f"recipenlg:size={rnlg_stat.st_size},mtime={int(rnlg_stat.st_mtime)}|"
        f"wdc:size={wdc_stat.st_size},mtime={int(wdc_stat.st_mtime)}"
    )


@dataclass
class L1Group:
    """Pre-extraction bucket of recipes sharing a normalized title."""

    key: str
    recipenlg: list[Recipe] = field(default_factory=list)
    wdc: list[WDCRecipe] = field(default_factory=list)

    @property
    def size(self) -> int:
        return len(self.recipenlg) + len(self.wdc)


def stream_l1_groups(
    *,
    rnlg_loader: RecipeNLGLoader,
    wdc_loader: WDCLoader,
    wdc_hosts: Sequence[str] | None = None,
    accept: Callable[[str], bool],
) -> dict[str, L1Group]:
    """Stream both corpora into ``{normalized_title: L1Group}``.

    ``accept`` is the language predicate from ``corpus_title_survey`` — keys
    rejected here never surface to downstream stages, saving the LLM pass.
    """
    groups: dict[str, L1Group] = defaultdict(lambda: L1Group(key=""))
    for recipe in rnlg_loader.iter_recipes():
        key = normalize_title(recipe.title)
        if not key or not accept(key):
            continue
        group = groups[key]
        if not group.key:
            group.key = key
        group.recipenlg.append(recipe)
    for wdc_recipe in wdc_loader.iter_all(hosts=wdc_hosts):
        key = normalize_title(wdc_recipe.title)
        if not key or not accept(key):
            continue
        group = groups[key]
        if not group.key:
            group.key = key
        group.wdc.append(wdc_recipe)
    return dict(groups)


@dataclass
class CatalogRunStats:
    """Summary numbers from a whole-corpus run."""

    l1_groups_total: int = 0
    l1_groups_processed: int = 0
    l1_groups_skipped: int = 0
    l1_groups_dry: int = 0
    variants_produced: int = 0
    llm_parse_calls: int = 0
    llm_extract_calls: int = 0
    wallclock_seconds: float = 0.0


def _utcnow_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


def run_catalog_pipeline(
    *,
    db: CatalogDB,
    rnlg_loader: RecipeNLGLoader,
    wdc_loader: WDCLoader,
    parse_fn: ParseFn,
    extract_fn: ExtractFn,
    corpus_revisions: str,
    wdc_hosts: Sequence[str] | None = None,
    language_filter: str = LANGUAGE_FILTER_ALL,
    l1_min: int = 5,
    l2_threshold: float = 0.6,
    l2_min: int = 3,
    l3_min: int = DEFAULT_L3_MIN_VARIANT_SIZE,
    bucket_size: float = DEFAULT_BUCKET_SIZE,
    title_filter: str | None = None,
    now_fn: Callable[[], str] = _utcnow_iso,
    on_group_done: Callable[[str, list[MergedVariantResult]], None] | None = None,
) -> CatalogRunStats:
    """Drive the whole-corpus → recipes.db pipeline.

    Per L1-group commit boundary: if ``is_l1_fresh`` reports the group is
    already done for this corpus fingerprint, it's skipped. Otherwise each
    surviving variant is written inside its own transaction via
    ``upsert_variant``, and a ``query_runs`` row marks the group done.

    ``title_filter`` restricts processed keys to ones containing the
    substring — dev-loop slice knob. ``language_filter`` is consulted
    before L1 size thresholding so rejected titles don't consume any LLM
    time. ``on_group_done`` is a caller hook invoked after each group for
    side-effects (cache persistence, progress reporting).
    """
    if language_filter not in LANGUAGE_FILTER_PREDICATES:
        raise ValueError(
            f"Unknown language_filter {language_filter!r}; "
            f"expected one of {sorted(LANGUAGE_FILTER_PREDICATES)}"
        )
    accept = LANGUAGE_FILTER_PREDICATES[language_filter]

    stats = CatalogRunStats()
    start_t = time.monotonic()

    logger.info("Streaming corpora into L1 groups…")
    groups = stream_l1_groups(
        rnlg_loader=rnlg_loader,
        wdc_loader=wdc_loader,
        wdc_hosts=wdc_hosts,
        accept=accept,
    )
    # Stable order: alphabetical by normalized title. Resumability relies on
    # processing groups in a deterministic order so a killed run's "next"
    # group is also the next run's "next" group.
    keys = sorted(groups.keys())
    if title_filter:
        keys = [k for k in keys if title_filter in k]
    keys = [k for k in keys if groups[k].size >= l1_min]
    stats.l1_groups_total = len(keys)
    logger.info(
        "L1: %d groups meet size>=%d (language=%s, title_filter=%r)",
        len(keys),
        l1_min,
        language_filter,
        title_filter,
    )

    for key in keys:
        if db.is_l1_fresh(key, corpus_revisions):
            stats.l1_groups_skipped += 1
            logger.info("  skip %r — fresh in query_runs", key)
            continue

        group = groups[key]
        logger.info(
            "  process %r (recipenlg=%d, wdc=%d)",
            key,
            len(group.recipenlg),
            len(group.wdc),
        )

        variants = _process_group(
            group=group,
            parse_fn=parse_fn,
            extract_fn=extract_fn,
            l2_threshold=l2_threshold,
            l2_min=l2_min,
            l3_min=l3_min,
            bucket_size=bucket_size,
            stats=stats,
        )

        for variant in variants:
            if not variant.normalized_rows:
                continue
            language = detect_language(variant.variant_title)
            db.upsert_variant(
                variant,
                l1_key=key,
                language=language,
            )

        db.record_l1_run(
            key,
            corpus_revisions=corpus_revisions,
            variants_produced=len(variants),
            dry=len(variants) == 0,
            run_at=now_fn(),
        )
        stats.l1_groups_processed += 1
        stats.variants_produced += len(variants)
        if not variants:
            stats.l1_groups_dry += 1

        if on_group_done is not None:
            on_group_done(key, variants)

    stats.wallclock_seconds = time.monotonic() - start_t
    return stats


def _process_group(
    *,
    group: L1Group,
    parse_fn: ParseFn,
    extract_fn: ExtractFn,
    l2_threshold: float,
    l2_min: int,
    l3_min: int,
    bucket_size: float,
    stats: CatalogRunStats,
) -> list[MergedVariantResult]:
    """Extract → merge → L2/L3 → parse/normalize → dedup for one L1 group."""
    wdc_populated: list[WDCRecipe]
    if group.wdc:
        stats.llm_extract_calls += len(group.wdc)
        wdc_populated = list(extract_fn(group.wdc))
    else:
        wdc_populated = []

    merged, _merge_stats = merge_corpora(group.recipenlg, wdc_populated)
    if not merged:
        return []

    def _counting_parse(lines: list[str]) -> list[ParsedIngredient | None]:
        stats.llm_parse_calls += 1
        return parse_fn(lines)

    # l1_min_group_size=1: pre-thresholded at the whole-corpus level, so a
    # single-group build_variants call must not re-drop the group.
    variants, _ = build_variants(
        merged,
        parse_fn=_counting_parse,
        l1_min_group_size=1,
        l2_similarity_threshold=l2_threshold,
        l2_min_group_size=l2_min,
        l3_min_variant_size=l3_min,
        bucket_size=bucket_size,
    )
    return variants
