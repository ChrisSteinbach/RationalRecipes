"""Pass 3: distinctive variant titles via LLM (vwt.24).

Pass 2 leaves every variant in an L1 group sharing ``display_title``
with the L1 normalized title, so the PWA list view shows e.g. four
'pecan pie' rows differing only by ``n_recipes``. Pass 3 issues one
LLM call per variant, supplying a sample of sibling ingredient sets
so the model can pick a distinctive descriptor ('Maple Pecan Pie',
'Bourbon Pecan Pie', etc.). A post-LLM dedup step (vwt.32) resolves
any collisions via ingredient-based disambiguation or numeric suffix.

Singletons skip the LLM — ``display_title`` is title-cased
``normalized_title``.

Determinism: the LLM call uses temperature=0 + seed=42 (same convention
as ``parse.py::_ollama_generate``) so a re-run produces byte-identical
titles for unchanged variant content.
"""

from __future__ import annotations

import contextlib
import json
import logging
import statistics
import threading
import time
import urllib.error
import urllib.request
from collections import defaultdict
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field

from rational_recipes.catalog_db import CatalogDB, VariantRow
from rational_recipes.scrape.parse import OLLAMA_BASE_URL

logger = logging.getLogger(__name__)


# RationalRecipes-bt9e: some L1 family names are intrinsically ambiguous
# — they refer to multiple dish types. When Pass 3 picks just
# `[descriptor] [family]`, the resulting title strips category context
# and reads as nonsense (e.g. 'Celery Chili' could be a soup, sauce,
# dip, or just 'celery chilli pepper'). For known-ambiguous families we
# append a category-specific suffix as a post-step on the LLM-validated
# title.
#
# Outer key: normalized family name (lowercased L1 key). Inner key: the
# category whose suffix to apply. Value: the literal suffix string with
# its leading space. Add new entries here as feedback surfaces them.
AMBIGUOUS_FAMILY_SUFFIXES: dict[str, dict[str, str]] = {
    "chili": {"soup": " Soup"},
}


def apply_ambiguous_suffix(
    title: str, family: str, category: str | None
) -> str:
    """Append a dish-type suffix when ``family`` is a known-ambiguous L1
    name and ``category`` matches an entry in ``AMBIGUOUS_FAMILY_SUFFIXES``.

    Returns ``title`` unchanged when there's no entry for ``family``,
    when ``category`` is ``None``, when ``category`` doesn't match any
    inner key, or when ``title`` already ends with the suffix
    (idempotent — re-running Pass 3 over an already-suffixed catalog
    must not double-suffix)."""
    if not category:
        return title
    suffixes = AMBIGUOUS_FAMILY_SUFFIXES.get(family.lower())
    if not suffixes:
        return title
    suffix = suffixes.get(category)
    if not suffix:
        return title
    if title.lower().endswith(suffix.lower()):
        return title
    return title + suffix


# RationalRecipes-0ki: descriptors the LLM picker tends to default to when
# nothing better surfaces in its prompt. They give the user no signal —
# every recipe in the corpus contains water/flour/sugar/leavening, so
# 'Water Punch' or 'Pecan Baking Soda Zucchini Bread' is just noise. The
# post-LLM substitution step strips these tokens from the descriptor and,
# if nothing distinctive remains, replaces the whole descriptor with the
# highest-ordinal non-stop-list ingredient from the variant's stats. The
# stop-list is overridden when no alternative is available — a generic
# descriptor still beats a bare family name.
#
# 'shortening' is a judgment call: as a generic fat it rarely
# differentiates, but a shortening-vs-butter pair is sometimes the most
# distinctive split between two cookie variants. Keeping it on the list
# for now; revisit if real titles regress.
STOP_LIST_DESCRIPTORS: frozenset[str] = frozenset({
    "water",
    "flour",
    "sugar",
    "white sugar",
    "soda",
    "baking soda",
    "baking powder",
    "salt",
    "oil",
    "vegetable oil",
    "shortening",
})


def _substitute_stop_list_descriptor(
    title: str,
    family: str,
    ingredients_ordered: Sequence[str],
) -> tuple[str, bool]:
    """Strip stop-list tokens from ``title``'s descriptor; if nothing
    distinctive remains, swap in the highest-ordinal non-stop-list
    ingredient from ``ingredients_ordered``. Returns ``(new_title,
    substituted)``; ``new_title == title`` when ``substituted`` is False.

    No-op when the title has no descriptor (bare-family case), the
    descriptor contains no stop-list tokens, or every descriptor token is
    stop-list AND no non-stop-list alternative exists in stats.

    ``ingredients_ordered`` is the variant's canonical ingredient list in
    ordinal order (most-prevalent first), as produced by
    ``CatalogDB.bulk_ingredient_names``. Substitution picks the first
    candidate that is not stop-list, not part of the family name, and
    not already in the descriptor — i.e. the highest-coverage informative
    alternative.

    Re-validates the result against ``validate_title_ends_with_family``
    so a malformed substitution falls back to the original title rather
    than regressing the wqy contract.
    """
    descriptor = _extract_descriptor(title, family)
    if descriptor is None:
        return title, False

    desc_words = descriptor.split()
    multi_phrases = sorted(
        (s for s in STOP_LIST_DESCRIPTORS if " " in s),
        key=lambda s: -len(s.split()),
    )
    single_words = frozenset(s for s in STOP_LIST_DESCRIPTORS if " " not in s)

    cleaned: list[str] = []
    dropped_any = False
    i = 0
    while i < len(desc_words):
        matched_n: int | None = None
        for phrase in multi_phrases:
            phrase_words = phrase.split()
            n = len(phrase_words)
            if i + n > len(desc_words):
                continue
            if [w.lower() for w in desc_words[i:i + n]] == phrase_words:
                matched_n = n
                break
        if matched_n is not None:
            dropped_any = True
            i += matched_n
            continue
        if desc_words[i].lower() in single_words:
            dropped_any = True
            i += 1
            continue
        cleaned.append(desc_words[i])
        i += 1

    if not dropped_any:
        return title, False

    # Preserve the original casing/spacing of the family suffix.
    family_suffix = title[len(descriptor):]

    if cleaned:
        candidate = " ".join(cleaned) + family_suffix
        if validate_title_ends_with_family(candidate, family):
            return candidate, True
        return title, False

    # Whole descriptor was stop-list — try a stats-driven replacement.
    # Use ``_stem_match`` for the family-word check so that singular
    # canonical names like 'potato' are recognized as the family word
    # 'potatoes' (otherwise we'd produce 'Potato Scalloped Potatoes').
    family_words = [w.lower() for w in family.split()]
    desc_words_lc = {w.lower() for w in desc_words}
    for ing in ingredients_ordered:
        ing_lc = ing.lower()
        if ing_lc in STOP_LIST_DESCRIPTORS:
            continue
        if any(_stem_match(ing_lc, fw) for fw in family_words):
            continue
        if ing_lc in desc_words_lc:
            continue
        candidate = ing.title() + family_suffix
        if validate_title_ends_with_family(candidate, family):
            return candidate, True

    return title, False


TITLE_SYSTEM_PROMPT = """\
You name dish variants. Given a dish family name and the ingredients of
ONE variant alongside its sibling variants' ingredients, choose a SHORT
distinctive title for this variant — one that calls out what makes it
different from its siblings.

Rules:
- Output ONLY a JSON object: {"title": "..."}.
- Title is at most 5 words, Title Case, English.
- The title MUST end with the dish family name (e.g. for family
  "pecan pie" the title is "<Descriptor> Pecan Pie").
- Pick ONE descriptor (an ingredient, method, or modifier) that is
  present in this variant but absent from every sibling. If multiple
  qualify, pick the most distinctive single word.
- Avoid generic descriptors like "water", "flour", "sugar", "baking
  soda", "baking powder", "salt", "oil", "vegetable oil", or
  "shortening" unless they are the only thing distinguishing this
  variant from its siblings.
- If nothing distinguishes this variant from its siblings, return the
  family name unchanged in Title Case.

Examples:
Family: "pecan pie"
This variant: {"ingredients": ["bourbon", "butter", "egg", "pecan", "sugar"]}
Siblings: [{"ingredients": ["butter", "egg", "maple syrup", "pecan"]}, \
{"ingredients": ["chocolate", "egg", "pecan", "sugar"]}]
Output: {"title": "Bourbon Pecan Pie"}

Family: "chocolate cake"
This variant: {"ingredients": ["butter", "cocoa", "egg", "flour", "sugar"]}
Siblings: [{"ingredients": ["butter", "cream cheese", "egg", "flour", "sugar"]}]
Output: {"title": "Cocoa Chocolate Cake"}
"""

TitleFn = Callable[
    [str, frozenset[str], frozenset[str], Sequence[frozenset[str]]],
    str | None,
]
"""LLM callback: (family, variant_ingredients, variant_methods, siblings) -> title.

Returning None signals "no usable title" — the caller falls back to the
family name. Tests inject deterministic stubs that bypass Ollama.
"""


@dataclass(frozen=True, slots=True)
class _VariantSlot:
    """Lightweight carrier for one variant's data inside a batched call.

    ``ingredients_ordered`` mirrors ``ingredients`` but preserves the
    ordinal ordering from ``variant_ingredient_stats`` (most-prevalent
    first). Used by the stop-list descriptor substitution
    (RationalRecipes-0ki) to pick the highest-coverage alternative.
    Defaults to an empty tuple so legacy test fixtures that only set
    ``ingredients``/``methods`` keep working.
    """

    variant_id: str
    ingredients: frozenset[str]
    methods: frozenset[str]
    ingredients_ordered: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class Pass3CallTiming:
    """Per-call instrumentation record for one Pass 3 LLM invocation (vwt.29).

    Two clocks live side by side so we can pin down where time goes:

    - **Wall-clock** breakdown (``prompt_build_seconds`` + ``request_seconds``
      + ``response_parse_seconds``) is what the Python process spends on
      this call. ``request_seconds`` covers HTTP transit + server-side
      inference + response read; the others isolate local overhead.
    - **Ollama-reported** numbers (``ollama_*``) come from the
      ``/api/generate`` response body and split server-side time into
      prompt-eval (token-by-token "reading" the prompt) and eval
      (generating output tokens). ``ollama_total_seconds`` plus the
      transit gap gives the network slice.

    Set every value the call captures; ``None`` means "Ollama didn't
    return that field" (e.g. because the request errored before any
    inference happened).
    """

    family: str
    sibling_count: int
    prompt_chars: int
    """``len(system_prompt) + len(user_prompt)``. Cheap proxy for token count
    that doesn't require a tokenizer; correlates strongly with
    ``ollama_prompt_eval_count`` for a fixed model."""
    prompt_build_seconds: float
    request_seconds: float
    response_parse_seconds: float
    db_write_seconds: float
    """Set by ``run_pass3._process`` after the title is resolved. ``0.0``
    when the timing record is consumed before the DB write (e.g. via a
    timing_collector outside ``run_pass3``)."""
    success: bool
    ollama_total_seconds: float | None = None
    ollama_load_seconds: float | None = None
    ollama_prompt_eval_count: int | None = None
    ollama_prompt_eval_seconds: float | None = None
    ollama_eval_count: int | None = None
    ollama_eval_seconds: float | None = None

    @property
    def total_seconds(self) -> float:
        """Sum of the wall-clock phases, excluding DB write."""
        return (
            self.prompt_build_seconds
            + self.request_seconds
            + self.response_parse_seconds
        )

    def to_dict(self) -> dict[str, object]:
        """JSONL-friendly dict (no None elision; analysis tools want fixed schema)."""
        return {
            "family": self.family,
            "sibling_count": self.sibling_count,
            "prompt_chars": self.prompt_chars,
            "prompt_build_seconds": self.prompt_build_seconds,
            "request_seconds": self.request_seconds,
            "response_parse_seconds": self.response_parse_seconds,
            "db_write_seconds": self.db_write_seconds,
            "total_seconds": self.total_seconds,
            "success": self.success,
            "ollama_total_seconds": self.ollama_total_seconds,
            "ollama_load_seconds": self.ollama_load_seconds,
            "ollama_prompt_eval_count": self.ollama_prompt_eval_count,
            "ollama_prompt_eval_seconds": self.ollama_prompt_eval_seconds,
            "ollama_eval_count": self.ollama_eval_count,
            "ollama_eval_seconds": self.ollama_eval_seconds,
        }


TimingCollector = Callable[[Pass3CallTiming], None]
"""Callback invoked once per LLM call (success or failure).

Plumbed through ``build_default_title_fn`` so a profiling driver can
collect per-call records. Thread-safety is the collector's
responsibility — under ``run_pass3(max_workers > 1)`` it's called from
worker threads concurrently.
"""


@dataclass
class Pass3Stats:
    """Per-run counters for the Pass 3 stage.

    ``timings`` is populated when an instrumented ``title_fn`` is in use
    (e.g. via ``build_default_title_fn(timing_collector=...)``). It stays
    empty for stub fns used in unit tests, which keeps the existing test
    contract intact.

    ``validation_failures_primary``, ``escalations``,
    ``validation_failures_fallback``, and ``reconstructed_titles`` track
    the tiered validate-and-escalate path (RationalRecipes-wqy): when
    the small primary model returns a title that drops the L1 family
    name, we retry with a stronger fallback model, and salvage by
    appending the family if that also fails.

    ``stop_list_substitutions`` counts how often the post-LLM stop-list
    cleanup (RationalRecipes-0ki) replaced or stripped a generic
    descriptor (water, flour, baking soda, etc.) on a successfully-titled
    variant."""

    variants_total: int = 0
    variants_singleton: int = 0
    variants_titled: int = 0
    variants_skipped: int = 0
    variants_deduped: int = 0
    llm_calls: int = 0
    llm_failures: int = 0
    validation_failures_primary: int = 0
    escalations: int = 0
    validation_failures_fallback: int = 0
    reconstructed_titles: int = 0
    stop_list_substitutions: int = 0
    db_write_seconds_total: float = 0.0
    db_write_count: int = 0
    timings: list[Pass3CallTiming] = field(default_factory=list)


def _ollama_title_call(
    family: str,
    variant_ingredients: frozenset[str],
    variant_methods: frozenset[str],
    siblings: Sequence[frozenset[str]],
    *,
    model: str,
    base_url: str = OLLAMA_BASE_URL,
    timeout: float = 60.0,
    num_ctx: int | None = 16384,
    timing_collector: TimingCollector | None = None,
) -> str | None:
    """Single Ollama /api/generate call shaped for title generation.

    temperature=0 + seed=42 pin determinism (same convention as
    parse.py); re-runs of an unchanged variant produce identical titles.

    When ``timing_collector`` is provided, it's invoked exactly once per
    call (success or failure) with a ``Pass3CallTiming`` record carrying
    a wall-clock breakdown plus the Ollama-reported timing fields. The
    collector runs unlocked — under ``run_pass3(max_workers > 1)`` it's
    called from worker threads, and the collector must guard its own
    state if shared.
    """
    build_start = time.monotonic()
    prompt = build_title_prompt(
        family, variant_ingredients, variant_methods, siblings
    )
    prompt_build_seconds = time.monotonic() - build_start
    prompt_chars = len(TITLE_SYSTEM_PROMPT) + len(prompt)

    options: dict[str, object] = {
        "num_predict": 64,
        "temperature": 0.0,
        "seed": 42,
    }
    if num_ctx is not None:
        options["num_ctx"] = num_ctx

    payload = json.dumps(
        {
            "model": model,
            "system": TITLE_SYSTEM_PROMPT,
            "prompt": prompt,
            "format": "json",
            "stream": False,
            "options": options,
        }
    ).encode()

    req = urllib.request.Request(
        f"{base_url}/api/generate",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    title: str | None = None
    request_seconds = 0.0
    response_parse_seconds = 0.0
    body: dict[str, object] = {}
    success = False

    request_start = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
        request_seconds = time.monotonic() - request_start

        parse_start = time.monotonic()
        body = json.loads(raw)
        visible = body.get("response") or ""
        if isinstance(visible, str) and visible.strip():
            title = parse_title_response(visible)
        else:
            thinking = body.get("thinking") or ""
            if isinstance(thinking, str) and thinking:
                title = parse_title_response(thinking)
        response_parse_seconds = time.monotonic() - parse_start
        success = title is not None
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
        request_seconds = time.monotonic() - request_start
        logger.warning("Ollama title call failed: %s", e)

    if timing_collector is not None:
        timing_collector(
            Pass3CallTiming(
                family=family,
                sibling_count=len(siblings),
                prompt_chars=prompt_chars,
                prompt_build_seconds=prompt_build_seconds,
                request_seconds=request_seconds,
                response_parse_seconds=response_parse_seconds,
                db_write_seconds=0.0,
                success=success,
                ollama_total_seconds=_ns_to_seconds(body.get("total_duration")),
                ollama_load_seconds=_ns_to_seconds(body.get("load_duration")),
                ollama_prompt_eval_count=_as_int(body.get("prompt_eval_count")),
                ollama_prompt_eval_seconds=_ns_to_seconds(
                    body.get("prompt_eval_duration")
                ),
                ollama_eval_count=_as_int(body.get("eval_count")),
                ollama_eval_seconds=_ns_to_seconds(body.get("eval_duration")),
            )
        )

    return title


def _ns_to_seconds(value: object) -> float | None:
    """Ollama returns durations as integer nanoseconds; coerce to float seconds."""
    if isinstance(value, int | float):
        return float(value) / 1_000_000_000.0
    return None


def _as_int(value: object) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return None


def build_default_title_fn(
    model: str,
    base_url: str = OLLAMA_BASE_URL,
    *,
    num_ctx: int | None = 16384,
    timing_collector: TimingCollector | None = None,
) -> TitleFn:
    """Bind model + base_url into a TitleFn for the production LLM path.

    When ``timing_collector`` is provided, every produced TitleFn
    invocation forwards a ``Pass3CallTiming`` record to it (vwt.29).
    """

    def fn(
        family: str,
        variant_ingredients: frozenset[str],
        variant_methods: frozenset[str],
        siblings: Sequence[frozenset[str]],
    ) -> str | None:
        return _ollama_title_call(
            family,
            variant_ingredients,
            variant_methods,
            siblings,
            model=model,
            base_url=base_url,
            num_ctx=num_ctx,
            timing_collector=timing_collector,
        )

    return fn


def build_title_prompt(
    family: str,
    variant_ingredients: frozenset[str],
    variant_methods: frozenset[str],
    siblings: Sequence[frozenset[str]],
) -> str:
    """Pack the prompt payload as a deterministic JSON blob."""
    payload = {
        "family": family,
        "this_variant": {
            "ingredients": sorted(variant_ingredients),
            "cooking_methods": sorted(variant_methods),
        },
        "siblings": [{"ingredients": sorted(sib)} for sib in siblings],
    }
    return (
        "Choose a distinctive title for this variant:\n"
        f"{json.dumps(payload, ensure_ascii=False, sort_keys=True)}\nOutput:"
    )


def validate_title_ends_with_family(title: str | None, family: str) -> bool:
    """Title is valid iff it ends with ``family`` on a word boundary
    (case-insensitive). RationalRecipes-wqy: the system prompt instructs
    the LLM to end every title with the family name, but gemma4:e2b
    (2B params) drops it ~5.6% of the time. This is the rejection
    predicate that gates the escalation path."""
    if not title:
        return False
    title_lc = title.lower()
    family_lc = family.lower()
    if title_lc == family_lc:
        return True
    return title_lc.endswith(" " + family_lc)


def _stem_match(a: str, b: str) -> bool:
    """Equal or off-by-one-suffix (s/es) comparison for crude singular/plural
    tolerance. Used by ``_extract_descriptor`` to recognize that 'potato'
    and 'potatoes' refer to the same family word when the LLM
    singularized the family in its output."""
    a_lc = a.lower()
    b_lc = b.lower()
    if a_lc == b_lc:
        return True
    short, long = (a_lc, b_lc) if len(a_lc) <= len(b_lc) else (b_lc, a_lc)
    if not long.startswith(short):
        return False
    suffix = long[len(short):]
    return suffix in ("s", "es")


def _extract_descriptor(title: str, family: str) -> str | None:
    """Strip the family-overlapping suffix from ``title``; return the
    remaining prefix, or ``None`` when nothing remains.

    Walks backwards over ``title`` and ``family`` together, popping
    word pairs that match (with simple plural tolerance via ``_stem_match``).
    The remaining title prefix is the descriptor — e.g.
    'Flour Potato' / 'scalloped potatoes' → 'Flour' (the trailing
    'Potato' matches 'potatoes' as a plural variant, then 'Flour' has
    no counterpart in the family). Pure salvage path (RationalRecipes-wqy);
    only invoked when both primary and fallback models returned a title
    that fails ``validate_title_ends_with_family``."""
    title_words = title.split()
    family_words = family.split()
    while (
        title_words
        and family_words
        and _stem_match(title_words[-1], family_words[-1])
    ):
        title_words.pop()
        family_words.pop()
    if not title_words:
        return None
    return " ".join(title_words)


def parse_title_response(raw: str) -> str | None:
    """Pull a clean title string out of the LLM's JSON response."""
    text = (raw or "").strip()
    if not text:
        return None
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}") + 1
        if start < 0 or end <= start:
            return None
        try:
            data = json.loads(text[start:end])
        except json.JSONDecodeError:
            return None
    if not isinstance(data, dict):
        return None
    title = data.get("title")
    if not isinstance(title, str):
        return None
    cleaned = " ".join(title.split())
    return cleaned or None


def _group_by_l1(variants: Sequence[VariantRow]) -> dict[str, list[VariantRow]]:
    """Bucket variants by ``normalized_title`` (= L1 group key)."""
    groups: dict[str, list[VariantRow]] = defaultdict(list)
    for v in variants:
        groups[v.normalized_title].append(v)
    # Stable order so siblings list is deterministic across runs.
    for key in groups:
        groups[key].sort(key=lambda v: v.variant_id)
    return groups


def _variants_to_slots(
    variants: Sequence[VariantRow],
    ingredient_names: dict[str, tuple[str, ...]],
) -> list[_VariantSlot]:
    """Convert VariantRows to lightweight slots for the batched call.

    ``ingredient_names`` maps variant_id → frequency-filtered canonical
    names from ``variant_ingredient_stats``; variants missing from the
    map (no stats rows) get an empty ingredient set.
    """
    return [
        _VariantSlot(
            variant_id=v.variant_id,
            ingredients=frozenset(ingredient_names.get(v.variant_id, ())),
            methods=frozenset(v.cooking_methods),
            ingredients_ordered=ingredient_names.get(v.variant_id, ()),
        )
        for v in variants
    ]


def _deduplicate_titles(
    family: str,
    slots: Sequence[_VariantSlot],
    titles: Sequence[str | None],
    existing_titles: frozenset[str] = frozenset(),
) -> list[str]:
    """Resolve duplicate display_titles within an L1 group (vwt.32).

    Three phases:
    1. Resolve None titles to the family name (Title Case).
    2. For each collision set (variants sharing a title), try to
       disambiguate by inserting a second ingredient or cooking method
       that is unique to that variant within the collision set.
    3. Any remaining collisions get a numeric suffix ``(2)``, ``(3)``, etc.

    ``existing_titles`` holds titles from already-titled variants that
    were skipped (``force=False``); new titles must not collide with them.
    """
    family_tc = family.title()
    family_lc = family.lower()
    resolved = [t if t else family_tc for t in titles]

    # Phase 2: ingredient/method-based disambiguation for collisions.
    by_title: dict[str, list[int]] = defaultdict(list)
    for i, t in enumerate(resolved):
        by_title[t].append(i)

    for title, indices in by_title.items():
        extra = 1 if title in existing_titles else 0
        if len(indices) + extra <= 1:
            continue
        if len(indices) <= 1:
            # Single new variant collides only with existing titles;
            # ingredient comparison needs peers, so skip to phase 3.
            continue
        # Find ingredients unique to each variant within the collision set.
        collision_ings = [slots[i].ingredients for i in indices]
        collision_methods = [slots[i].methods for i in indices]
        for pos, global_idx in enumerate(indices):
            others_ings: set[str] = set()
            for j, ings in enumerate(collision_ings):
                if j != pos:
                    others_ings.update(ings)
            unique = sorted(slots[global_idx].ingredients - others_ings)
            if not unique:
                others_methods: set[str] = set()
                for j, methods in enumerate(collision_methods):
                    if j != pos:
                        others_methods.update(methods)
                unique = sorted(slots[global_idx].methods - others_methods)
            if unique:
                disamb = unique[0].title()
                if title.lower().endswith(family_lc):
                    prefix = title[: len(title) - len(family)].strip()
                    new_title = (
                        f"{prefix} {disamb} {family_tc}".strip()
                        if prefix
                        else f"{disamb} {family_tc}"
                    )
                else:
                    new_title = f"{disamb} {title}"
                resolved[global_idx] = new_title

    # Phase 3: numeric suffix for any remaining collisions.
    taken: set[str] = set(existing_titles)
    for i in range(len(resolved)):
        title = resolved[i]
        if title not in taken:
            taken.add(title)
            continue
        counter = 2
        while f"{title} ({counter})" in taken:
            counter += 1
        resolved[i] = f"{title} ({counter})"
        taken.add(resolved[i])

    return resolved


def _resolve_title(
    *,
    family: str,
    slot: _VariantSlot,
    sibling_sets: Sequence[frozenset[str]],
    title_fn: TitleFn,
    fallback_title_fn: TitleFn | None,
    stats: Pass3Stats,
) -> str | None:
    """Per-variant tiered resolver (RationalRecipes-wqy).

    1. Call ``title_fn`` (primary, e.g. gemma4:e2b). Validate.
    2. If invalid and ``fallback_title_fn`` is configured, call it
       (escalate to e.g. qwen3.6:35b-a3b). Validate.
    3. If both fail, salvage by extracting the descriptor from the
       fallback's output (or primary's, if no fallback) and reconstructing
       as ``f"{descriptor} {family.title()}"``.
    4. If no descriptor remains, return None — the dedup pipeline in
       ``_deduplicate_titles`` produces ingredient-based names from
       there.

    All counters on ``stats`` are mutated in-place. Thread-safety: this
    function runs unlocked under ``run_pass3(max_workers > 1)``; counters
    are plain ints so concurrent ``+=`` can lose updates. Acceptable for
    the production path because the totals are observability rather
    than control flow — if a stricter contract is needed later, wrap
    the increments under ``run_pass3``'s existing lock.
    """
    stats.llm_calls += 1
    primary = title_fn(family, slot.ingredients, slot.methods, sibling_sets)

    primary_valid = validate_title_ends_with_family(primary, family)
    if primary_valid:
        return primary

    if primary is None:
        stats.llm_failures += 1
    else:
        stats.validation_failures_primary += 1

    fallback: str | None = None
    if fallback_title_fn is not None:
        stats.escalations += 1
        fallback = fallback_title_fn(
            family, slot.ingredients, slot.methods, sibling_sets,
        )
        if validate_title_ends_with_family(fallback, family):
            return fallback
        if fallback is None:
            stats.llm_failures += 1
        else:
            stats.validation_failures_fallback += 1

    # Salvage: prefer the fallback's output (it had more capacity), then
    # the primary's. ``_extract_descriptor`` returns None when nothing
    # remains after stripping the family overlap, in which case we let
    # the variant fall through to ``_deduplicate_titles``.
    candidate = fallback if fallback else primary
    if candidate:
        descriptor = _extract_descriptor(candidate, family)
        if descriptor:
            stats.reconstructed_titles += 1
            return f"{descriptor} {family.title()}"
    return None


def run_pass3(
    *,
    db: CatalogDB,
    title_fn: TitleFn,
    fallback_title_fn: TitleFn | None = None,
    max_workers: int = 1,
    max_siblings: int = 20,
    force: bool = False,
    stats: Pass3Stats | None = None,
    on_group_done: Callable[[int, int], None] | None = None,
) -> Pass3Stats:
    """Generate ``display_title`` for every variant in the catalog DB.

    Issues one LLM call per variant, supplying up to ``max_siblings``
    sibling ingredient sets so the model can pick a distinctive
    descriptor. A post-LLM dedup step (vwt.32) resolves any collisions.

    - Variants in singleton L1 groups keep ``display_title = L1 title``
      (no LLM call).
    - If ``force=False``, variants whose ``display_title`` already
      differs from ``normalized_title`` are skipped (already titled by
      a prior Pass 3). Set ``force=True`` to retitle regardless.
    - ``max_workers > 1`` runs L1 groups concurrently; DB writes are
      serialized via a shared lock.
    - ``max_siblings`` caps the number of sibling ingredient sets sent
      to the LLM prompt. Groups larger than this still get complete
      dedup coverage via the post-LLM disambiguation step.
    - ``fallback_title_fn`` is the escalation hop (RationalRecipes-wqy):
      when the primary returns a title that fails
      ``validate_title_ends_with_family``, the fallback is invoked
      with the same context. If the fallback's output is also
      malformed, ``_extract_descriptor`` salvages by appending the
      family name; if no descriptor remains, the variant falls through
      to ``_deduplicate_titles`` (treated as if the LLM returned None).
    """
    if stats is None:
        stats = Pass3Stats()

    variants = db.list_variants()
    stats.variants_total = len(variants)
    groups = _group_by_l1(variants)
    ingredient_names = db.bulk_ingredient_names()

    # Build per-group work items. The trailing ``str | None`` carries the
    # group's category (shared across all members because ``categorize``
    # is keyed off the L1 ``normalized_title``); ``apply_ambiguous_suffix``
    # uses it post-LLM (RationalRecipes-bt9e).
    _WorkItem = tuple[
        str,
        list[_VariantSlot],
        list[_VariantSlot],
        frozenset[str],
        str | None,
    ]
    work: list[_WorkItem] = []
    for family, members in groups.items():
        if len(members) <= 1:
            stats.variants_singleton += len(members)
            for v in members:
                titled = apply_ambiguous_suffix(
                    family.title(), family, v.category
                )
                if v.display_title != titled:
                    db.update_display_title(v.variant_id, titled)
            continue
        all_slots = _variants_to_slots(members, ingredient_names)
        needs_title: list[_VariantSlot] = []
        existing: set[str] = set()
        for v, slot in zip(members, all_slots, strict=True):
            if not force and v.display_title and v.display_title != family:
                stats.variants_skipped += 1
                existing.add(v.display_title)
                continue
            needs_title.append(slot)
        if needs_title:
            category = members[0].category
            work.append(
                (family, needs_title, all_slots, frozenset(existing), category)
            )

    if not work:
        return stats

    lock = threading.Lock() if max_workers > 1 else None
    hold: contextlib.AbstractContextManager[object] = (
        lock if lock is not None else contextlib.nullcontext()
    )

    def _process_group(item: _WorkItem) -> None:
        family, needs_title, all_slots, existing_titles, category = item

        # One primary LLM call per variant, with capped sibling context.
        # Validation + escalation + reconstruction live inside
        # ``_resolve_title`` so the dedup step downstream sees only
        # already-cleaned titles or None.
        raw_titles: list[str | None] = []
        for slot in needs_title:
            other_slots = [s for s in all_slots if s.variant_id != slot.variant_id]
            capped = other_slots[:max_siblings]
            sibling_sets: Sequence[frozenset[str]] = [s.ingredients for s in capped]
            title = _resolve_title(
                family=family,
                slot=slot,
                sibling_sets=sibling_sets,
                title_fn=title_fn,
                fallback_title_fn=fallback_title_fn,
                stats=stats,
            )
            # Strip / substitute generic stop-list descriptors BEFORE the
            # ambiguous-family suffix runs (so the suffix sees the
            # post-substitution title) but AFTER ``_resolve_title``'s wqy
            # validator (so we never substitute on a malformed title).
            # RationalRecipes-0ki.
            if title is not None:
                substituted_title, substituted = (
                    _substitute_stop_list_descriptor(
                        title, family, slot.ingredients_ordered,
                    )
                )
                if substituted:
                    stats.stop_list_substitutions += 1
                    title = substituted_title
            # Append the dish-type suffix AFTER the wqy validator
            # inside ``_resolve_title`` has approved the LLM output (so
            # the validator sees the LLM's raw string) and BEFORE dedup
            # below (so collisions are computed on the final
            # user-facing title). RationalRecipes-bt9e.
            if title is not None:
                title = apply_ambiguous_suffix(title, family, category)
            raw_titles.append(title)

        # Deduplicate titles within this group (vwt.32).
        deduped = _deduplicate_titles(
            family, needs_title, raw_titles, existing_titles,
        )
        for raw, final in zip(raw_titles, deduped, strict=True):
            original = raw if raw else family.title()
            if final != original:
                stats.variants_deduped += 1

        # Write resolved titles to DB.
        for slot, title in zip(needs_title, deduped, strict=True):
            write_start = time.monotonic()
            with hold:
                db.update_display_title(slot.variant_id, title)
                db_write_seconds = time.monotonic() - write_start
                stats.variants_titled += 1
                stats.db_write_seconds_total += db_write_seconds
                stats.db_write_count += 1

    total_groups = len(work)
    _beat = on_group_done or (lambda _pos, _tot: None)

    if max_workers <= 1:
        for i, item in enumerate(work, start=1):
            _process_group(item)
            _beat(i, total_groups)
    else:
        logger.info(
            "  pass3: %d groups across %d workers", len(work), max_workers
        )
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(_process_group, item) for item in work]
            for i, future in enumerate(as_completed(futures), start=1):
                future.result()
                _beat(i, total_groups)

    return stats


# --- Profiling helpers (vwt.29) -------------------------------------------------


def summarize_pass3_timings(
    timings: Sequence[Pass3CallTiming],
) -> dict[str, object]:
    """Compute aggregate stats over a collected timing list.

    Returns a flat dict keyed for easy JSON dump or log printing. Empty
    input yields a dict with ``count=0`` and no other keys — callers
    should special-case that.

    Percentiles are p50/p90/p99 (linear interpolation, statistics.quantiles
    style) over ``request_seconds``, ``ollama_prompt_eval_seconds``,
    ``ollama_eval_seconds``, and ``prompt_chars`` — the four most
    informative dimensions for diagnosing where time goes.
    """
    n = len(timings)
    if n == 0:
        return {"count": 0}

    request_secs = sorted(t.request_seconds for t in timings)
    prompt_chars = sorted(t.prompt_chars for t in timings)
    sibling_counts = sorted(t.sibling_count for t in timings)

    prompt_eval_secs = sorted(
        t.ollama_prompt_eval_seconds
        for t in timings
        if t.ollama_prompt_eval_seconds is not None
    )
    eval_secs = sorted(
        t.ollama_eval_seconds
        for t in timings
        if t.ollama_eval_seconds is not None
    )
    prompt_eval_counts = sorted(
        t.ollama_prompt_eval_count
        for t in timings
        if t.ollama_prompt_eval_count is not None
    )
    eval_counts = sorted(
        t.ollama_eval_count
        for t in timings
        if t.ollama_eval_count is not None
    )

    successes = sum(1 for t in timings if t.success)

    out: dict[str, object] = {
        "count": n,
        "successes": successes,
        "failures": n - successes,
        "request_seconds_mean": statistics.fmean(request_secs),
        "request_seconds_p50": _percentile(request_secs, 0.50),
        "request_seconds_p90": _percentile(request_secs, 0.90),
        "request_seconds_p99": _percentile(request_secs, 0.99),
        "request_seconds_max": request_secs[-1],
        "request_seconds_total": sum(request_secs),
        "prompt_chars_mean": statistics.fmean(prompt_chars),
        "prompt_chars_p50": _percentile(prompt_chars, 0.50),
        "prompt_chars_p90": _percentile(prompt_chars, 0.90),
        "prompt_chars_p99": _percentile(prompt_chars, 0.99),
        "prompt_chars_max": prompt_chars[-1],
        "sibling_count_mean": statistics.fmean(sibling_counts),
        "sibling_count_max": sibling_counts[-1],
        "prompt_build_seconds_total": sum(t.prompt_build_seconds for t in timings),
        "response_parse_seconds_total": sum(
            t.response_parse_seconds for t in timings
        ),
        "db_write_seconds_total": sum(t.db_write_seconds for t in timings),
    }
    if prompt_eval_secs:
        out["ollama_prompt_eval_seconds_p50"] = _percentile(
            prompt_eval_secs, 0.50
        )
        out["ollama_prompt_eval_seconds_p99"] = _percentile(
            prompt_eval_secs, 0.99
        )
        out["ollama_prompt_eval_seconds_total"] = sum(prompt_eval_secs)
    if eval_secs:
        out["ollama_eval_seconds_p50"] = _percentile(eval_secs, 0.50)
        out["ollama_eval_seconds_p99"] = _percentile(eval_secs, 0.99)
        out["ollama_eval_seconds_total"] = sum(eval_secs)
    if prompt_eval_counts:
        out["ollama_prompt_eval_count_p50"] = _percentile(
            prompt_eval_counts, 0.50
        )
        out["ollama_prompt_eval_count_p99"] = _percentile(
            prompt_eval_counts, 0.99
        )
        out["ollama_prompt_eval_count_max"] = prompt_eval_counts[-1]
    if eval_counts:
        out["ollama_eval_count_p50"] = _percentile(eval_counts, 0.50)
        out["ollama_eval_count_p99"] = _percentile(eval_counts, 0.99)

    # Bucket by sibling count to expose whether large groups are slow.
    buckets = _bucket_by_siblings(timings)
    out["by_sibling_bucket"] = [
        {
            "label": label,
            "count": data["count"],
            "request_seconds_mean": data["request_seconds_mean"],
            "prompt_chars_mean": data["prompt_chars_mean"],
            "ollama_prompt_eval_seconds_mean": data[
                "ollama_prompt_eval_seconds_mean"
            ],
        }
        for label, data in buckets
    ]
    return out


def _percentile(values: Sequence[float | int], q: float) -> float:
    """Linear-interpolated quantile over a *pre-sorted* sequence."""
    if not values:
        raise ValueError("_percentile on empty sequence")
    if q <= 0:
        return float(values[0])
    if q >= 1:
        return float(values[-1])
    idx = q * (len(values) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(values) - 1)
    frac = idx - lo
    return float(values[lo]) * (1 - frac) + float(values[hi]) * frac


_SIBLING_BUCKETS: tuple[tuple[str, int, int], ...] = (
    ("1", 1, 1),
    ("2-5", 2, 5),
    ("6-10", 6, 10),
    ("11-20", 11, 20),
    ("21-50", 21, 50),
    ("51-100", 51, 100),
    ("101-200", 101, 200),
    (">200", 201, 10**9),
)


def _bucket_by_siblings(
    timings: Sequence[Pass3CallTiming],
) -> list[tuple[str, dict[str, float]]]:
    """Aggregate request_seconds / prompt_chars by sibling-count bucket.

    Returns rows for non-empty buckets only, in ascending bucket order.
    """
    rows: list[tuple[str, dict[str, float]]] = []
    for label, lo, hi in _SIBLING_BUCKETS:
        members = [t for t in timings if lo <= t.sibling_count <= hi]
        if not members:
            continue
        prompt_eval = [
            t.ollama_prompt_eval_seconds
            for t in members
            if t.ollama_prompt_eval_seconds is not None
        ]
        rows.append(
            (
                label,
                {
                    "count": float(len(members)),
                    "request_seconds_mean": statistics.fmean(
                        t.request_seconds for t in members
                    ),
                    "prompt_chars_mean": statistics.fmean(
                        t.prompt_chars for t in members
                    ),
                    "ollama_prompt_eval_seconds_mean": (
                        statistics.fmean(prompt_eval) if prompt_eval else 0.0
                    ),
                },
            )
        )
    return rows


def format_pass3_summary(stats: Pass3Stats) -> list[str]:
    """Multi-line printable summary derived from ``stats.timings`` (vwt.29).

    Computes everything inline rather than going through
    ``summarize_pass3_timings`` so the formatter has fully-typed
    intermediates (the summary dict is for JSON dumps, not printing).
    """
    timings = stats.timings
    if not timings:
        return []

    request_secs = sorted(t.request_seconds for t in timings)
    prompt_chars = sorted(t.prompt_chars for t in timings)
    sibling_counts = sorted(t.sibling_count for t in timings)
    failures = sum(1 for t in timings if not t.success)

    lines: list[str] = [
        "pass 3 timing: "
        f"calls={len(timings)} "
        f"failures={failures} "
        f"request_total={sum(request_secs):.1f}s "
        f"request_mean={statistics.fmean(request_secs):.3f}s "
        f"p50={_percentile(request_secs, 0.50):.3f}s "
        f"p90={_percentile(request_secs, 0.90):.3f}s "
        f"p99={_percentile(request_secs, 0.99):.3f}s "
        f"max={request_secs[-1]:.3f}s",
        "pass 3 prompt: "
        f"chars_mean={int(statistics.fmean(prompt_chars))} "
        f"chars_p99={int(_percentile(prompt_chars, 0.99))} "
        f"chars_max={prompt_chars[-1]} "
        f"siblings_mean={statistics.fmean(sibling_counts):.1f} "
        f"siblings_max={sibling_counts[-1]}",
    ]

    prompt_eval_secs = sorted(
        t.ollama_prompt_eval_seconds
        for t in timings
        if t.ollama_prompt_eval_seconds is not None
    )
    eval_secs = sorted(
        t.ollama_eval_seconds
        for t in timings
        if t.ollama_eval_seconds is not None
    )
    prompt_eval_counts = sorted(
        t.ollama_prompt_eval_count
        for t in timings
        if t.ollama_prompt_eval_count is not None
    )
    if prompt_eval_secs and eval_secs:
        lines.append(
            "pass 3 ollama: "
            f"prompt_eval_total={sum(prompt_eval_secs):.1f}s "
            f"prompt_eval_p99={_percentile(prompt_eval_secs, 0.99):.3f}s "
            f"eval_total={sum(eval_secs):.1f}s "
            f"eval_p99={_percentile(eval_secs, 0.99):.3f}s "
            f"prompt_tokens_p99="
            f"{int(_percentile(prompt_eval_counts, 0.99)) if prompt_eval_counts else 0}"
        )

    prompt_build_total = sum(t.prompt_build_seconds for t in timings)
    response_parse_total = sum(t.response_parse_seconds for t in timings)
    db_write_total = sum(t.db_write_seconds for t in timings)
    overhead_total = prompt_build_total + response_parse_total + db_write_total
    lines.append(
        "pass 3 overhead: "
        f"prompt_build={prompt_build_total:.2f}s "
        f"response_parse={response_parse_total:.2f}s "
        f"db_writes={db_write_total:.2f}s "
        f"total_overhead={overhead_total:.2f}s"
    )

    buckets = _bucket_by_siblings(timings)
    if buckets:
        bucket_strs = [
            f"{label}:n={int(data['count'])}/"
            f"req={data['request_seconds_mean']:.2f}s"
            for label, data in buckets
        ]
        lines.append("pass 3 by siblings: " + " ".join(bucket_strs))
    return lines
