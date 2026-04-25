"""Two-pass whole-corpus extraction pipeline (beads vwt.2 + vwt.16).

Pass 1 (LLM-bound, slow, resumable): for every recipe in surviving L1
groups, persist parsed ingredient lines into ``parsed_ingredient_lines``
keyed by ``(corpus, recipe_id, line_index)``. Line-text dedup (baked-in
vwt.15: same ``hash(raw_line + model + seed)``) skips the LLM whenever a
parse for that exact text is already on disk.

Pass 2 (no LLM, fast, repeatable): hydrate parses back from the table,
derive WDC ``ingredient_names`` from them, run merge / L2 / L3 / stats /
write. Threshold sweeps on ``l1_min``, ``l2_threshold``,
``near_dup_threshold`` etc. re-run Pass 2 only — no re-parse cost.

The legacy single-pass shape stays as the default of
``run_catalog_pipeline`` (do_pass1=True, do_pass2=True) — the change is
internal: even one-shot runs now write parsed_ingredient_lines, so a
later threshold sweep can re-run Pass 2 alone.

The LLM boundary (``parse_fn`` for ingredient-line parsing) stays
injectable — tests pass stubs so full orchestration runs without Ollama.
``extract_fn`` is accepted for backward compatibility but is no longer
called by Pass 2 (parsed_ingredient_lines provides the ingredient names
WDC needs for cross-corpus dedup).
"""

from __future__ import annotations

import dataclasses
import logging
import time
from collections import defaultdict
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from rational_recipes.catalog_db import (
    CatalogDB,
    ParsedLineRow,
    parsed_from_json,
    parsed_to_json,
)
from rational_recipes.corpus_title_survey import (
    LANGUAGE_FILTER_ALL,
    LANGUAGE_FILTER_PREDICATES,
)
from rational_recipes.scrape.canonical import canonicalize_names
from rational_recipes.scrape.grouping import (
    DEFAULT_L3_MIN_VARIANT_SIZE,
    normalize_title,
)
from rational_recipes.scrape.merge import (
    DEFAULT_BUCKET_SIZE,
    DEFAULT_NEAR_DUP_THRESHOLD,
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
"""Bound-LLM callback shaping ``extract_batch``.

Accepted by ``run_catalog_pipeline`` for backward compatibility but no
longer invoked under the two-pass architecture (vwt.16) — Pass 2
derives ``WDCRecipe.ingredient_names`` from ``parsed_ingredient_lines``
populated by Pass 1, so the separate WDC name-extraction LLM call is
gone. Callers that still pass it incur no cost; the parameter remains
in the public signature so existing CLI integrations don't break.
"""


# Lowercase Swedish diacritics — language detection sentinel for the
# recipes.language column. Sufficient for the en+sv scope; broader locale
# detection belongs elsewhere.
_SWEDISH_DIACRITICS = frozenset("åäö")

DEFAULT_PARSE_MODEL = "gemma4:e2b"
"""Production model name persisted in ``parsed_ingredient_lines.model``.

Mirrors ``parse.py``'s default. Pass 1 records (model, seed) so a model
swap (e.g. vwt.18 to gemma4:e2b) automatically invalidates the cache.
"""

DEFAULT_PARSE_SEED = 42
"""Production LLM seed (matches ``parse.py::_ollama_generate``)."""


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


# --- Recipe-identity helpers ---


def recipenlg_recipe_id(recipe: Recipe) -> str:
    """Stable recipe_id for parsed_ingredient_lines on a RecipeNLG row.

    row_index is canonical for a given CSV; if missing/-1 (unknown CSV
    layout), fall back to the link so we still get something unique
    enough to scope a parse.
    """
    if recipe.row_index >= 0:
        return f"rnlg:{recipe.row_index}"
    return f"rnlg:{recipe.link}"


def wdc_recipe_id(recipe: WDCRecipe) -> str:
    """Stable recipe_id for parsed_ingredient_lines on a WDC row.

    page_url is preferred (matches the existing extract_batch cache
    key). When absent, fall back to host:row_id.
    """
    if recipe.page_url:
        return f"wdc:{recipe.page_url}"
    return f"wdc:{recipe.host}:{recipe.row_id}"


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
    # vwt.16 Pass 1 counters.
    pass1_recipes_seen: int = 0
    pass1_recipes_skipped: int = 0
    pass1_lines_parsed: int = 0
    pass1_lines_cache_hits: int = 0
    pass1_llm_batches: int = 0
    wallclock_seconds: float = 0.0


def _utcnow_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


def _select_keys(
    groups: dict[str, L1Group],
    *,
    l1_min: int,
    title_filter: str | None,
    title_exact: str | None,
) -> list[str]:
    """Apply the three slicing knobs over L1 keys in stable order."""
    keys = sorted(groups.keys())
    if title_exact is not None:
        keys = [k for k in keys if k == title_exact]
    elif title_filter:
        keys = [k for k in keys if title_filter in k]
    return [k for k in keys if groups[k].size >= l1_min]


# --- Pass 1: LLM-bound line parsing into the cache table ---


def _pass1_recipe(
    *,
    db: CatalogDB,
    corpus: str,
    recipe_id: str,
    raw_lines: Sequence[str],
    parse_fn: ParseFn,
    model: str,
    seed: int,
    line_text_cache: dict[str, str | None],
    stats: CatalogRunStats,
) -> None:
    """Parse one recipe's lines and persist them, applying line-text dedup.

    ``line_text_cache`` is the in-process dedup map shared across the
    whole Pass 1 run — keyed by raw_line text (model+seed pinned by the
    surrounding call), value is the parsed_json (str) or None for a
    cached failure. Mutated in-place as new texts are parsed.
    """
    if not raw_lines:
        return

    if db.has_parsed_lines_for_recipe(corpus, recipe_id, model=model, seed=seed):
        stats.pass1_recipes_skipped += 1
        return

    stats.pass1_recipes_seen += 1
    indexed_lines = list(enumerate(raw_lines))
    needs_llm: list[tuple[int, str]] = []
    pre_resolved: list[ParsedLineRow] = []

    for idx, line in indexed_lines:
        # Local cache hit (already parsed during this run).
        if line in line_text_cache:
            stats.pass1_lines_cache_hits += 1
            pre_resolved.append(
                ParsedLineRow(
                    corpus=corpus,
                    recipe_id=recipe_id,
                    line_index=idx,
                    raw_line=line,
                    parsed_json=line_text_cache[line],
                    model=model,
                    seed=seed,
                )
            )
            continue
        # DB cache hit (line-text dedup across recipes / prior runs).
        found, payload = db.lookup_cached_parse(line, model, seed)
        if not found:
            needs_llm.append((idx, line))
            continue
        stats.pass1_lines_cache_hits += 1
        line_text_cache[line] = payload
        pre_resolved.append(
            ParsedLineRow(
                corpus=corpus,
                recipe_id=recipe_id,
                line_index=idx,
                raw_line=line,
                parsed_json=payload,
                model=model,
                seed=seed,
            )
        )

    new_rows: list[ParsedLineRow] = []
    if needs_llm:
        # Dedup by line text for the LLM call too — same recipe can repeat
        # a line, and a single batch can include the same text twice.
        unique_texts: list[str] = []
        seen: set[str] = set()
        for _, line in needs_llm:
            if line not in seen:
                seen.add(line)
                unique_texts.append(line)

        stats.pass1_llm_batches += 1
        results = parse_fn(unique_texts)
        if len(results) != len(unique_texts):
            # Defensive: should not happen with the documented contract,
            # but if it does, mark every uncovered line as a failure.
            results = list(results) + [None] * (len(unique_texts) - len(results))

        text_to_payload: dict[str, str | None] = {}
        for text, parsed in zip(unique_texts, results, strict=False):
            text_to_payload[text] = parsed_to_json(parsed)

        for idx, line in needs_llm:
            payload = text_to_payload.get(line)
            line_text_cache[line] = payload
            new_rows.append(
                ParsedLineRow(
                    corpus=corpus,
                    recipe_id=recipe_id,
                    line_index=idx,
                    raw_line=line,
                    parsed_json=payload,
                    model=model,
                    seed=seed,
                )
            )

    all_rows = sorted(pre_resolved + new_rows, key=lambda r: r.line_index)
    if all_rows:
        db.upsert_parsed_lines(all_rows)
        stats.pass1_lines_parsed += len(all_rows)


def _run_pass1(
    *,
    db: CatalogDB,
    keys: Sequence[str],
    groups: dict[str, L1Group],
    parse_fn: ParseFn,
    model: str,
    seed: int,
    stats: CatalogRunStats,
) -> None:
    """Phase 1: parse + persist every line in every recipe of the surviving
    L1 groups. Resumable per-recipe; idempotent on re-run."""
    line_text_cache: dict[str, str | None] = {}
    for key in keys:
        group = groups[key]
        for r in group.recipenlg:
            _pass1_recipe(
                db=db,
                corpus="recipenlg",
                recipe_id=recipenlg_recipe_id(r),
                raw_lines=r.ingredients,
                parse_fn=parse_fn,
                model=model,
                seed=seed,
                line_text_cache=line_text_cache,
                stats=stats,
            )
        for w in group.wdc:
            _pass1_recipe(
                db=db,
                corpus="wdc",
                recipe_id=wdc_recipe_id(w),
                raw_lines=w.ingredients,
                parse_fn=parse_fn,
                model=model,
                seed=seed,
                line_text_cache=line_text_cache,
                stats=stats,
            )


# --- Pass 2: No-LLM clustering + variant assembly from the cache table ---


def _build_db_parse_fn(db: CatalogDB, model: str, seed: int) -> ParseFn:
    """A parse_fn that hydrates parses from parsed_ingredient_lines.

    ``None`` for cache miss (recipe was not run through Pass 1) so the
    row gets dropped at the same place an LLM failure would.
    """

    def parse(lines: list[str]) -> list[ParsedIngredient | None]:
        out: list[ParsedIngredient | None] = []
        for line in lines:
            found, payload = db.lookup_cached_parse(line, model, seed)
            if not found or payload is None:
                out.append(None)
                continue
            out.append(parsed_from_json(payload, line))
        return out

    return parse


def _populate_wdc_names_from_db(
    db: CatalogDB,
    recipes: Sequence[WDCRecipe],
) -> list[WDCRecipe]:
    """Fill in WDCRecipe.ingredient_names from parsed_ingredient_lines.

    Mirrors what ``extract_batch`` used to do (one LLM call per recipe)
    but pulls every name from cached parses — no Ollama traffic. Recipes
    with no parsed lines come through with empty ``ingredient_names`` and
    will fall through cross-corpus near-dup since Jaccard against an
    empty set is 0.
    """
    out: list[WDCRecipe] = []
    for recipe in recipes:
        recipe_id = wdc_recipe_id(recipe)
        names: list[str] = []
        for row in db.get_parsed_lines_for_recipe("wdc", recipe_id):
            parsed = row.to_parsed()
            if parsed and parsed.ingredient:
                names.append(parsed.ingredient)
        out.append(
            dataclasses.replace(recipe, ingredient_names=canonicalize_names(names))
        )
    return out


def _run_pass2(
    *,
    db: CatalogDB,
    keys: Sequence[str],
    groups: dict[str, L1Group],
    model: str,
    seed: int,
    corpus_revisions: str,
    l2_threshold: float,
    l2_min: int,
    l3_min: int,
    bucket_size: float,
    near_dup_threshold: float,
    now_fn: Callable[[], str],
    on_group_done: Callable[[str, list[MergedVariantResult]], None] | None,
    stats: CatalogRunStats,
) -> None:
    """Phase 2: cluster + write variants from the parsed_ingredient_lines
    table. No LLM; a re-run with new thresholds is seconds, not hours."""
    db_parse_fn = _build_db_parse_fn(db, model, seed)

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

        wdc_populated = _populate_wdc_names_from_db(db, group.wdc)

        merged, _merge_stats = merge_corpora(
            group.recipenlg,
            wdc_populated,
            near_dup_threshold=near_dup_threshold,
        )
        if not merged:
            variants: list[MergedVariantResult] = []
        else:
            def _counting_parse(
                lines: list[str],
            ) -> list[ParsedIngredient | None]:
                stats.llm_parse_calls += 1
                return db_parse_fn(lines)

            variants, _ = build_variants(
                merged,
                parse_fn=_counting_parse,
                l1_min_group_size=1,
                l2_similarity_threshold=l2_threshold,
                l2_min_group_size=l2_min,
                l3_min_variant_size=l3_min,
                bucket_size=bucket_size,
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


# --- Top-level orchestrator ---


def run_catalog_pipeline(
    *,
    db: CatalogDB,
    rnlg_loader: RecipeNLGLoader,
    wdc_loader: WDCLoader,
    parse_fn: ParseFn,
    extract_fn: ExtractFn | None = None,
    corpus_revisions: str,
    wdc_hosts: Sequence[str] | None = None,
    language_filter: str = LANGUAGE_FILTER_ALL,
    l1_min: int = 5,
    l2_threshold: float = 0.6,
    l2_min: int = 3,
    l3_min: int = DEFAULT_L3_MIN_VARIANT_SIZE,
    bucket_size: float = DEFAULT_BUCKET_SIZE,
    near_dup_threshold: float = DEFAULT_NEAR_DUP_THRESHOLD,
    title_filter: str | None = None,
    title_exact: str | None = None,
    model: str = DEFAULT_PARSE_MODEL,
    seed: int = DEFAULT_PARSE_SEED,
    do_pass1: bool = True,
    do_pass2: bool = True,
    now_fn: Callable[[], str] = _utcnow_iso,
    on_group_done: Callable[[str, list[MergedVariantResult]], None] | None = None,
) -> CatalogRunStats:
    """Drive the two-pass whole-corpus → recipes.db pipeline.

    Default ``do_pass1=True, do_pass2=True`` is the legacy single-call
    behavior (parse + cluster + write in one invocation). Passing one
    flag false enables the threshold-sweep workflow:

      1. ``do_pass2=False`` warms parsed_ingredient_lines once.
      2. ``do_pass1=False`` re-runs cluster + write under different
         ``l2_threshold`` / ``near_dup_threshold`` etc., reading parses
         from the cache.

    Per L1-group commit boundary in Pass 2: if ``is_l1_fresh`` reports
    the group is already done for this corpus fingerprint, it's skipped.
    Otherwise each surviving variant is written inside its own
    transaction via ``upsert_variant``, and a ``query_runs`` row marks
    the group done.

    ``extract_fn`` is accepted for backward compatibility but is no
    longer called — Pass 2 derives WDC ingredient_names from
    parsed_ingredient_lines populated by Pass 1.
    """
    if language_filter not in LANGUAGE_FILTER_PREDICATES:
        raise ValueError(
            f"Unknown language_filter {language_filter!r}; "
            f"expected one of {sorted(LANGUAGE_FILTER_PREDICATES)}"
        )
    if not (do_pass1 or do_pass2):
        raise ValueError("at least one of do_pass1, do_pass2 must be True")
    accept = LANGUAGE_FILTER_PREDICATES[language_filter]
    # extract_fn signature is preserved for backward compat; suppress
    # unused-arg lint.
    _ = extract_fn

    stats = CatalogRunStats()
    start_t = time.monotonic()

    logger.info("Streaming corpora into L1 groups…")
    groups = stream_l1_groups(
        rnlg_loader=rnlg_loader,
        wdc_loader=wdc_loader,
        wdc_hosts=wdc_hosts,
        accept=accept,
    )
    keys = _select_keys(
        groups,
        l1_min=l1_min,
        title_filter=title_filter,
        title_exact=title_exact,
    )
    stats.l1_groups_total = len(keys)
    logger.info(
        "L1: %d groups meet size>=%d (language=%s, title_filter=%r)",
        len(keys),
        l1_min,
        language_filter,
        title_filter,
    )

    if do_pass1:
        logger.info("Pass 1: parsing ingredient lines into cache table")
        _run_pass1(
            db=db,
            keys=keys,
            groups=groups,
            parse_fn=parse_fn,
            model=model,
            seed=seed,
            stats=stats,
        )

    if do_pass2:
        logger.info("Pass 2: clustering and writing variants from cache")
        _run_pass2(
            db=db,
            keys=keys,
            groups=groups,
            model=model,
            seed=seed,
            corpus_revisions=corpus_revisions,
            l2_threshold=l2_threshold,
            l2_min=l2_min,
            l3_min=l3_min,
            bucket_size=bucket_size,
            near_dup_threshold=near_dup_threshold,
            now_fn=now_fn,
            on_group_done=on_group_done,
            stats=stats,
        )

    stats.wallclock_seconds = time.monotonic() - start_t
    return stats
