"""Pass 3: distinctive variant titles via LLM (vwt.24, batched v97).

Pass 2 leaves every variant in an L1 group sharing ``display_title``
with the L1 normalized title, so the PWA list view shows e.g. four
'pecan pie' rows differing only by ``n_recipes``. Pass 3 issues one
batched LLM call per L1 group (v97), supplying the dish family name
plus every variant's canonicals + cooking_methods so the model can
assign distinctive titles in one shot ('Maple Pecan Pie', 'Bourbon
Pecan Pie', etc.).

Groups larger than ``_MAX_BATCH_SIZE`` are chunked; each chunk still
sees the full group's ingredient lists as context so the model can
pick globally-distinctive descriptors. On parse failure, the chunk
bisects (mirroring Pass 1's ``_parse_batch_with_fallback``), degrading
to per-variant calls at the leaves.

Singletons skip the LLM — ``display_title`` is left equal to
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

BATCHED_TITLE_SYSTEM_PROMPT = """\
You name dish variants. Given a dish family name and an array of
variants (each with its ingredients and cooking methods), choose a
SHORT distinctive title for EACH variant — one that calls out what
makes it different from all the others.

Rules:
- Output ONLY a JSON object: {"results": [{"title": "..."}, ...]}.
- The results array MUST have exactly as many entries as the input
  variants array, in the same order.
- Each title is at most 5 words, Title Case, English.
- Each title MUST end with the dish family name (e.g. for family
  "pecan pie" a title is "<Descriptor> Pecan Pie").
- Pick ONE descriptor per variant (an ingredient, method, or modifier)
  that is present in that variant but absent from every other variant.
  If multiple qualify, pick the most distinctive single word.
- If nothing distinguishes a variant from all others, return the
  family name unchanged in Title Case.

Example:
Family: "pecan pie"
Variants: [
  {"ingredients": ["bourbon", "butter", "egg", "pecan", "sugar"]},
  {"ingredients": ["butter", "egg", "maple syrup", "pecan"]},
  {"ingredients": ["chocolate", "egg", "pecan", "sugar"]}
]
Output: {"results": [{"title": "Bourbon Pecan Pie"}, \
{"title": "Maple Pecan Pie"}, {"title": "Chocolate Pecan Pie"}]}
"""

_MAX_BATCH_SIZE = 50
"""Max variants to title in a single LLM call (v97).

Groups larger than this are chunked; each chunk still receives the full
group as context so the model picks globally-distinctive descriptors.
Matches Pass 1's chunking strategy for reliability.
"""

_TOKENS_PER_TITLE = 15
"""Estimated output tokens per title (5 words + JSON overhead)."""

_BATCH_OVERHEAD_TOKENS = 50
"""Fixed overhead tokens for the JSON wrapper in batched output."""


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
    """Lightweight carrier for one variant's data inside a batched call."""

    variant_id: str
    ingredients: frozenset[str]
    methods: frozenset[str]


BatchTitleFn = Callable[
    [str, Sequence[_VariantSlot], Sequence[_VariantSlot]],
    list[str | None] | None,
]
"""LLM callback: (family, slots_to_title, all_group_slots) -> titles.

Returns a list of titles parallel to ``slots_to_title``, or ``None``
on structural failure (triggering bisect). Individual titles can be
``None`` to signal per-variant fallback. Tests inject deterministic
stubs that bypass Ollama.
"""


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

Plumbed through ``build_default_batch_title_fn`` (v97) so a profiling
driver can collect per-call records. Thread-safety is the collector's
responsibility — under ``run_pass3(max_workers > 1)`` it's called from
worker threads concurrently.
"""


@dataclass
class Pass3Stats:
    """Per-run counters for the Pass 3 stage.

    ``timings`` is populated when an instrumented ``batch_title_fn`` is
    in use (e.g. via ``build_default_batch_title_fn(timing_collector=...)``).
    It stays empty for stub fns used in unit tests, which keeps the
    existing test contract intact."""

    variants_total: int = 0
    variants_singleton: int = 0
    variants_titled: int = 0
    variants_skipped: int = 0
    variants_deduped: int = 0
    llm_calls: int = 0
    llm_failures: int = 0
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

    payload = json.dumps(
        {
            "model": model,
            "system": TITLE_SYSTEM_PROMPT,
            "prompt": prompt,
            "format": "json",
            "stream": False,
            "options": {
                "num_ctx": 16384,
                "num_predict": 64,
                "temperature": 0.0,
                "seed": 42,
            },
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


def build_batched_title_prompt(
    family: str,
    slots: Sequence[_VariantSlot],
    all_slots: Sequence[_VariantSlot],
) -> str:
    """Build the batched prompt (v97).

    ``slots`` is the chunk to title; ``all_slots`` is the full group
    (used as context when chunking). When len(slots) == len(all_slots)
    the entire group is being titled in one call.
    """
    variants_json = [
        {
            "ingredients": sorted(s.ingredients),
            **({"cooking_methods": sorted(s.methods)} if s.methods else {}),
        }
        for s in slots
    ]
    payload: dict[str, object] = {
        "family": family,
        "variants": variants_json,
    }
    # When chunking, include the full group as context so the model can
    # see what other variants exist and avoid colliding titles.
    if len(slots) < len(all_slots):
        context_json = [
            {"ingredients": sorted(s.ingredients)}
            for s in all_slots
            if s not in slots
        ]
        payload["other_variants_in_group"] = context_json
    return (
        f"Choose a distinctive title for each of these {len(slots)} variants:\n"
        f"{json.dumps(payload, ensure_ascii=False, sort_keys=True)}\nOutput:"
    )


def parse_batched_title_response(
    raw: str, expected_count: int
) -> list[str | None] | None:
    """Parse the batched LLM output into a list of titles.

    Returns ``None`` on structural failure (wrong count, no results
    array, unparseable JSON) — the caller should bisect. Individual
    title entries that are malformed become ``None`` in the list.
    """
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
    items = data.get("results")
    if not isinstance(items, list):
        return None
    if len(items) != expected_count:
        logger.warning(
            "Batched title output length mismatch: got %d, expected %d",
            len(items),
            expected_count,
        )
        return None

    titles: list[str | None] = []
    for item in items:
        if isinstance(item, dict):
            t = item.get("title")
            if isinstance(t, str):
                cleaned = " ".join(t.split())
                titles.append(cleaned or None)
            else:
                titles.append(None)
        else:
            titles.append(None)
    return titles


def _ollama_batched_title_call(
    family: str,
    slots: Sequence[_VariantSlot],
    all_slots: Sequence[_VariantSlot],
    *,
    model: str,
    base_url: str = OLLAMA_BASE_URL,
    timeout: float = 120.0,
    timing_collector: TimingCollector | None = None,
) -> list[str | None] | None:
    """One batched Ollama call for a chunk of variants (v97).

    Returns a list of titles parallel to ``slots``, or ``None`` on
    structural failure. Individual entries can be ``None`` for
    per-variant fallback.
    """
    build_start = time.monotonic()
    prompt = build_batched_title_prompt(family, slots, all_slots)
    prompt_build_seconds = time.monotonic() - build_start
    prompt_chars = len(BATCHED_TITLE_SYSTEM_PROMPT) + len(prompt)

    num_predict = _TOKENS_PER_TITLE * len(slots) + _BATCH_OVERHEAD_TOKENS

    payload = json.dumps(
        {
            "model": model,
            "system": BATCHED_TITLE_SYSTEM_PROMPT,
            "prompt": prompt,
            "format": "json",
            "stream": False,
            "options": {
                "num_ctx": 16384,
                "num_predict": num_predict,
                "temperature": 0.0,
                "seed": 42,
            },
        }
    ).encode()

    req = urllib.request.Request(
        f"{base_url}/api/generate",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    titles: list[str | None] | None = None
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
            titles = parse_batched_title_response(visible, len(slots))
        else:
            thinking = body.get("thinking") or ""
            if isinstance(thinking, str) and thinking:
                titles = parse_batched_title_response(thinking, len(slots))
        response_parse_seconds = time.monotonic() - parse_start
        success = titles is not None
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
        request_seconds = time.monotonic() - request_start
        logger.warning("Ollama batched title call failed: %s", e)

    if timing_collector is not None:
        timing_collector(
            Pass3CallTiming(
                family=family,
                sibling_count=len(all_slots),
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

    return titles


def _batched_titles_with_fallback(
    family: str,
    slots: Sequence[_VariantSlot],
    all_slots: Sequence[_VariantSlot],
    batch_fn: BatchTitleFn,
) -> list[str | None]:
    """Bisect-on-failure wrapper around the batched LLM call (v97).

    Mirrors Pass 1's ``_parse_batch_with_fallback``: on structural
    failure the chunk is split in half and each half retried. At the
    leaf (single variant), a ``None`` title signals per-variant
    fallback to the family name.
    """
    if len(slots) == 0:
        return []

    titles = batch_fn(family, slots, all_slots)
    if titles is not None:
        return titles

    if len(slots) <= 1:
        # Leaf: can't bisect further; signal per-variant fallback.
        return [None]

    mid = len(slots) // 2
    logger.warning(
        "Batched title call failed for %d variants in %r; bisecting into %d + %d",
        len(slots),
        family,
        mid,
        len(slots) - mid,
    )
    left = _batched_titles_with_fallback(family, slots[:mid], all_slots, batch_fn)
    right = _batched_titles_with_fallback(family, slots[mid:], all_slots, batch_fn)
    return left + right


def build_default_batch_title_fn(
    model: str,
    base_url: str = OLLAMA_BASE_URL,
    *,
    timing_collector: TimingCollector | None = None,
) -> BatchTitleFn:
    """Bind model + base_url into a BatchTitleFn for the production LLM path."""

    def fn(
        family: str,
        slots: Sequence[_VariantSlot],
        all_slots: Sequence[_VariantSlot],
    ) -> list[str | None] | None:
        return _ollama_batched_title_call(
            family,
            slots,
            all_slots,
            model=model,
            base_url=base_url,
            timing_collector=timing_collector,
        )

    return fn


def _group_by_l1(variants: Sequence[VariantRow]) -> dict[str, list[VariantRow]]:
    """Bucket variants by ``normalized_title`` (= L1 group key)."""
    groups: dict[str, list[VariantRow]] = defaultdict(list)
    for v in variants:
        groups[v.normalized_title].append(v)
    # Stable order so siblings list is deterministic across runs.
    for key in groups:
        groups[key].sort(key=lambda v: v.variant_id)
    return groups


def _resolve_title(
    family: str,
    variant: VariantRow,
    siblings: Sequence[VariantRow],
    title_fn: TitleFn,
    stats: Pass3Stats,
) -> str:
    """One LLM call → distinctive title; family-name fallback on failure."""
    sibling_sets = [
        frozenset(s.canonical_ingredient_set) for s in siblings
    ]
    stats.llm_calls += 1
    title = title_fn(
        family,
        frozenset(variant.canonical_ingredient_set),
        frozenset(variant.cooking_methods),
        sibling_sets,
    )
    if not title:
        stats.llm_failures += 1
        return family.title()
    return title


def _variants_to_slots(variants: Sequence[VariantRow]) -> list[_VariantSlot]:
    """Convert VariantRows to lightweight slots for the batched call."""
    return [
        _VariantSlot(
            variant_id=v.variant_id,
            ingredients=frozenset(v.canonical_ingredient_set),
            methods=frozenset(v.cooking_methods),
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


def run_pass3(
    *,
    db: CatalogDB,
    title_fn: TitleFn | None = None,
    batch_title_fn: BatchTitleFn | None = None,
    max_workers: int = 1,
    force: bool = False,
    stats: Pass3Stats | None = None,
) -> Pass3Stats:
    """Generate ``display_title`` for every variant in the catalog DB (v97).

    Uses batched LLM calls: one call per L1 group (or per chunk for
    groups larger than ``_MAX_BATCH_SIZE``). On failure, bisects down
    to single-variant granularity with family-name fallback.

    - Variants in singleton L1 groups keep ``display_title = L1 title``
      (no LLM call).
    - If ``force=False``, variants whose ``display_title`` already
      differs from ``normalized_title`` are skipped (already titled by
      a prior Pass 3). Set ``force=True`` to retitle regardless.
    - ``max_workers > 1`` runs L1 groups concurrently; DB writes are
      serialized via a shared lock.

    Either ``batch_title_fn`` (v97 batched path) or ``title_fn`` (legacy
    per-variant path) must be provided. When ``batch_title_fn`` is given
    it takes precedence.
    """
    if batch_title_fn is None and title_fn is None:
        raise ValueError("one of batch_title_fn or title_fn must be provided")

    if stats is None:
        stats = Pass3Stats()

    variants = db.list_variants()
    stats.variants_total = len(variants)
    groups = _group_by_l1(variants)

    # Build per-group work items.
    _WorkItem = tuple[str, list[_VariantSlot], list[_VariantSlot], frozenset[str]]
    work: list[_WorkItem] = []
    for family, members in groups.items():
        if len(members) <= 1:
            stats.variants_singleton += len(members)
            titled = family.title()
            for v in members:
                if v.display_title != titled:
                    db.update_display_title(v.variant_id, titled)
            continue
        all_slots = _variants_to_slots(members)
        needs_title: list[_VariantSlot] = []
        existing: set[str] = set()
        for v, slot in zip(members, all_slots, strict=True):
            if not force and v.display_title and v.display_title != family:
                stats.variants_skipped += 1
                existing.add(v.display_title)
                continue
            needs_title.append(slot)
        if needs_title:
            work.append((family, needs_title, all_slots, frozenset(existing)))

    if not work:
        return stats

    lock = threading.Lock() if max_workers > 1 else None
    hold: contextlib.AbstractContextManager[object] = (
        lock if lock is not None else contextlib.nullcontext()
    )

    def _process_group(item: _WorkItem) -> None:
        family, needs_title, all_slots, existing_titles = item

        # Collect raw titles from the LLM.
        raw_titles: list[str | None]
        if batch_title_fn is not None:
            raw_titles = []
            for i in range(0, len(needs_title), _MAX_BATCH_SIZE):
                chunk = needs_title[i : i + _MAX_BATCH_SIZE]
                stats.llm_calls += 1
                chunk_titles = _batched_titles_with_fallback(
                    family, chunk, all_slots, batch_title_fn,
                )
                raw_titles.extend(chunk_titles)
        else:
            assert title_fn is not None
            all_members = [
                v for v in db.list_variants()
                if v.normalized_title == family
            ]
            all_members.sort(key=lambda v: v.variant_id)
            raw_titles = []
            for slot in needs_title:
                variant = next(
                    v for v in all_members if v.variant_id == slot.variant_id
                )
                siblings = [
                    s for s in all_members if s.variant_id != slot.variant_id
                ]
                title = _resolve_title(
                    family, variant, siblings, title_fn, stats,
                )
                raw_titles.append(title)

        # Count LLM failures (None titles).
        for t in raw_titles:
            if not t:
                stats.llm_failures += 1

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

    if max_workers <= 1:
        for item in work:
            _process_group(item)
    else:
        logger.info(
            "  pass3: %d groups across %d workers", len(work), max_workers
        )
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(_process_group, item) for item in work]
            for future in as_completed(futures):
                future.result()

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
