"""Pass 3: distinctive variant titles via LLM (vwt.24).

Pass 2 leaves every variant in an L1 group sharing ``display_title``
with the L1 normalized title, so the PWA list view shows e.g. four
'pecan pie' rows differing only by ``n_recipes``. Pass 3 issues one
LLM call per variant in a multi-variant L1 group, supplying the dish
family name plus the variant's canonicals + cooking_methods alongside
its sibling variants' canonicals so the model can pick a
distinguishing descriptor ('Maple Pecan Pie', 'Bourbon Pecan Pie',
etc.).

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
import threading
import urllib.error
import urllib.request
from collections import defaultdict
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

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


TitleFn = Callable[
    [str, frozenset[str], frozenset[str], Sequence[frozenset[str]]],
    str | None,
]
"""LLM callback: (family, variant_ingredients, variant_methods, siblings) -> title.

Returning None signals "no usable title" — the caller falls back to the
family name. Tests inject deterministic stubs that bypass Ollama.
"""


@dataclass
class Pass3Stats:
    """Per-run counters for the Pass 3 stage."""

    variants_total: int = 0
    variants_singleton: int = 0
    variants_titled: int = 0
    variants_skipped: int = 0
    llm_calls: int = 0
    llm_failures: int = 0


def _ollama_title_call(
    family: str,
    variant_ingredients: frozenset[str],
    variant_methods: frozenset[str],
    siblings: Sequence[frozenset[str]],
    *,
    model: str,
    base_url: str = OLLAMA_BASE_URL,
    timeout: float = 60.0,
) -> str | None:
    """Single Ollama /api/generate call shaped for title generation.

    temperature=0 + seed=42 pin determinism (same convention as
    parse.py); re-runs of an unchanged variant produce identical titles.
    """
    prompt = build_title_prompt(
        family, variant_ingredients, variant_methods, siblings
    )
    payload = json.dumps(
        {
            "model": model,
            "system": TITLE_SYSTEM_PROMPT,
            "prompt": prompt,
            "format": "json",
            "stream": False,
            "options": {
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

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = json.loads(resp.read())
            visible = body.get("response") or ""
            if visible.strip():
                return parse_title_response(visible)
            thinking = body.get("thinking") or ""
            return parse_title_response(thinking) if thinking else None
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
        logger.warning("Ollama title call failed: %s", e)
        return None


def build_default_title_fn(model: str, base_url: str = OLLAMA_BASE_URL) -> TitleFn:
    """Bind model + base_url into a TitleFn for the production LLM path."""

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
        return family
    return title


def run_pass3(
    *,
    db: CatalogDB,
    title_fn: TitleFn,
    max_workers: int = 1,
    force: bool = False,
    stats: Pass3Stats | None = None,
) -> Pass3Stats:
    """Generate ``display_title`` for every variant in the catalog DB.

    - Variants in singleton L1 groups keep ``display_title = L1 title``
      (no LLM call).
    - Variants in multi-variant L1 groups get one LLM call apiece, with
      the other group members' canonical ingredients passed as
      siblings.
    - If ``force=False``, variants whose ``display_title`` already
      differs from ``normalized_title`` are skipped (already titled by
      a prior Pass 3). Set ``force=True`` to retitle regardless — useful
      when sibling membership has changed.
    - ``max_workers > 1`` runs LLM calls concurrently; DB writes are
      serialized via a shared lock.
    """
    if stats is None:
        stats = Pass3Stats()

    variants = db.list_variants()
    stats.variants_total = len(variants)
    groups = _group_by_l1(variants)

    work: list[tuple[VariantRow, list[VariantRow]]] = []
    for family, members in groups.items():
        if len(members) <= 1:
            stats.variants_singleton += len(members)
            for v in members:
                # Make sure the singleton's display_title is the L1
                # title (Pass 2 already does this, but fix any stragglers).
                if v.display_title != family:
                    db.update_display_title(v.variant_id, family)
            continue
        for v in members:
            if not force and v.display_title and v.display_title != family:
                stats.variants_skipped += 1
                continue
            siblings = [s for s in members if s.variant_id != v.variant_id]
            work.append((v, siblings))

    if not work:
        return stats

    lock = threading.Lock() if max_workers > 1 else None
    hold: contextlib.AbstractContextManager[object] = (
        lock if lock is not None else contextlib.nullcontext()
    )

    def _process(item: tuple[VariantRow, list[VariantRow]]) -> None:
        variant, siblings = item
        family = variant.normalized_title
        title = _resolve_title(family, variant, siblings, title_fn, stats)
        with hold:
            db.update_display_title(variant.variant_id, title)
            stats.variants_titled += 1

    if max_workers <= 1:
        for item in work:
            _process(item)
    else:
        logger.info(
            "  pass3: %d variants across %d workers", len(work), max_workers
        )
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(_process, item) for item in work]
            for future in as_completed(futures):
                future.result()

    return stats
