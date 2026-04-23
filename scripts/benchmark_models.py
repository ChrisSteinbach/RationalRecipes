#!/usr/bin/env python3
"""Benchmark LLM candidates against gold standards for ingredient extraction.

Runs three corpora through a list of Ollama-hosted models and reports
per-field accuracy, per-language F1, retry variance bands, and latency.

Corpora
-------
- **English**: hand-labeled per-line structured gold
  (``english_messy_gold.jsonl``). Scored per-field (quantity, unit,
  ingredient, preparation-exact, preparation-jaccard) using
  ``parse.py``'s default ``_SYSTEM_PROMPT``.

- **Swedish**: hand-labeled per-line structured gold
  (``swedish_ica_se_structured_gold.jsonl``) drawn from ~20 ica.se
  recipes. Scored per-field plus a recipe-grouped name-set F1 derived
  from the ingredient field. Uses ``NEUTRAL_PROMPT``.

- **Multilingual**: hand-labeled per-line gold
  (``multilingual_gold.jsonl``) covering German (chefkoch.de), Russian
  (the-challenger.ru), and Japanese (delishkitchen.tv). Scored
  per-field and per-language. Uses ``NEUTRAL_PROMPT``.

Retries
-------
``--retries N`` runs every corpus N times per model and reports each
metric as ``mean ± stdev``. Needed for variance-aware ranking when
cross-model gaps are small (<= 0.10 F1).

Cross-language unit asymmetry (known; not a bug)
------------------------------------------------
Countable items with no explicit unit parse differently across
languages because the prompts say different things:

- English ``_SYSTEM_PROMPT`` (parse.py) teaches MEDIUM/LARGE/SMALL
  sentinels: ``'3 eggs'`` → ``unit='MEDIUM'``,
  ``'2 large eggs'`` → ``unit='LARGE'``.
- ``NEUTRAL_PROMPT`` (wdc.py) says "if no unit, use empty string":
  ``'3 ägg'`` → ``unit=''``, ``'1 stort ägg'`` → ``unit='stort'``.

Both golds match their respective prompt contracts. Downstream
canonicalization has to treat ``MEDIUM`` and ``''`` as equivalent
"countable with default size" when comparing ratios across languages.
Fixing this properly means aligning the two prompts — out of scope for
the v2 gold, flagged for jpp's close-note as an open question.

Results file schema
-------------------
``results.json`` is a ``v2`` payload: a list of ``ModelRun`` records
(one per model per retry), each carrying per-corpus ``LineRun`` lists.
Scoring is re-runnable from cached runs via ``--rescore``.

Usage
-----
::

    python3 scripts/benchmark_models.py \\
        --ollama-url http://192.168.50.189:11434 \\
        --models gemma4:e2b devstral:24b \\
        --retries 3 \\
        --out scripts/benchmark_data/results.json

    # Re-score cached run without re-calling LLMs:
    python3 scripts/benchmark_models.py --rescore scripts/benchmark_data/results.json

    # Variance probe on baseline:
    python3 scripts/benchmark_models.py --models gemma4:e2b --retries 3 \\
        --english-limit 30 --swedish-limit 0 --multilingual-limit 0 \\
        --out scripts/benchmark_data/results-variance.json
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import statistics
import sys
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from rational_recipes.scrape.parse import (  # noqa: E402
    ParsedIngredient,
    parse_ingredient_line,
)
from rational_recipes.scrape.wdc import NEUTRAL_PROMPT  # noqa: E402

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

ENGLISH_GOLD = Path("scripts/benchmark_data/english_messy_gold.jsonl")
SWEDISH_GOLD = Path("scripts/benchmark_data/swedish_ica_se_structured_gold.jsonl")
MULTILINGUAL_GOLD = Path("scripts/benchmark_data/multilingual_gold.jsonl")


# ---------------------------------------------------------------------------
# Field normalization (used at scoring time)
# ---------------------------------------------------------------------------

_UNIT_SYNONYMS_EN = {
    "c": "cup",
    "c.": "cup",
    "cup": "cup",
    "cups": "cup",
    "tbsp": "tbsp",
    "tbsp.": "tbsp",
    "tablespoon": "tbsp",
    "tablespoons": "tbsp",
    "t": "tbsp",
    "tsp": "tsp",
    "tsp.": "tsp",
    "teaspoon": "tsp",
    "teaspoons": "tsp",
    "oz": "oz",
    "oz.": "oz",
    "ounce": "oz",
    "ounces": "oz",
    "lb": "lb",
    "lb.": "lb",
    "pound": "lb",
    "pounds": "lb",
    "g": "g",
    "gram": "g",
    "grams": "g",
    "kg": "kg",
    "kilogram": "kg",
    "kilograms": "kg",
    "ml": "ml",
    "milliliter": "ml",
    "milliliters": "ml",
    "l": "l",
    "liter": "l",
    "liters": "l",
    "pinch": "pinch",
    "pinches": "pinch",
    "dash": "dash",
    "clove": "clove",
    "cloves": "clove",
    "stick": "stick",
    "sticks": "stick",
    "slice": "slice",
    "slices": "slice",
    "cube": "cube",
    "cubes": "cube",
    "stalk": "stalk",
    "stalks": "stalk",
    "can": "can",
    "cans": "can",
    "pkg": "pkg",
    "pkg.": "pkg",
    "package": "pkg",
    "packages": "pkg",
    "jar": "jar",
    "jars": "jar",
    "box": "box",
    "boxes": "box",
    "bag": "bag",
    "bags": "bag",
    "bottle": "bottle",
    "bottles": "bottle",
    "envelope": "envelope",
    "envelopes": "envelope",
    "sleeve": "sleeve",
    "sleeves": "sleeve",
    "handful": "handful",
    "handfuls": "handful",
    "medium": "MEDIUM",
    "med.": "MEDIUM",
    "med": "MEDIUM",
    "large": "LARGE",
    "lg": "LARGE",
    "lg.": "LARGE",
    "small": "SMALL",
    "sm": "SMALL",
    "sm.": "SMALL",
    "": "",
}


def _norm_unit_en(u: str | None) -> str:
    """Normalize English unit tokens (handles abbrev. variants and size sentinels)."""
    if u is None:
        return ""
    u = u.strip()
    if u.upper() in ("MEDIUM", "LARGE", "SMALL"):
        return u.upper()
    return _UNIT_SYNONYMS_EN.get(u.lower(), u.lower())


def _norm_unit_generic(u: str | None) -> str:
    """Strip + lowercase. Used for non-English units where the neutral prompt
    preserves source-language spelling."""
    if u is None:
        return ""
    return u.strip().lower()


_PLURAL_ES_ENDINGS = ("oes", "ses", "shes", "ches", "xes", "zes")


def _norm_ingredient_en(ing: str | None) -> str:
    """Lowercase + conservative de-plural for English countables.

    Handles -s, -es, and -ies plurals without stripping the short
    intrinsic-s words that show up as ingredients: -ss (bass, pass),
    -us (asparagus, couscous), -is (analysis, basis). -as is a more
    common ingredient plural (peas, bananas, tortillas) so we strip
    it; the cost is that non-ingredient -as words (bias, atlas) would
    get stripped too, but those don't appear in recipe text.

    Compared to the v1 rule, this correctly reduces "peas"→"pea",
    "tomatoes"→"tomato", "cherries"→"cherry", "bananas"→"banana"
    which the v1 vowel-before-s exception blocked.
    """
    if ing is None:
        return ""
    ing = ing.strip().lower()
    if len(ing) <= 3 or ing.endswith(("ss", "us", "is")):
        return ing
    if ing.endswith("ies"):
        return ing[:-3] + "y"
    if ing.endswith(_PLURAL_ES_ENDINGS):
        return ing[:-2]
    if ing.endswith("s"):
        return ing[:-1]
    return ing


def _norm_ingredient_generic(ing: str | None) -> str:
    """Strip + lowercase for non-English — plural rules differ per language."""
    if ing is None:
        return ""
    return ing.strip().lower()


# Preparation — free-form natural language; both strict and Jaccard reported.
_PREP_STOPWORDS = frozenset(
    {
        "and",
        "or",
        "then",
        "the",
        "a",
        "und",
        "oder",
        "и",
        "или",
        "と",
        "または",
    }
)
_PREP_SPLIT_RE = re.compile(r"[,\s]+")


def _norm_prep(p: str | None) -> str:
    if p is None:
        return ""
    return p.strip().lower()


def _prep_tokens(p: str | None) -> frozenset[str]:
    """Split a prep string on whitespace/commas, drop stopwords."""
    norm = _norm_prep(p)
    return frozenset(
        t for t in _PREP_SPLIT_RE.split(norm) if t and t not in _PREP_STOPWORDS
    )


def _prep_exact_match(out: str | None, exp: str) -> bool:
    return _norm_prep(out) == _norm_prep(exp)


def _prep_jaccard(out: str | None, exp: str) -> float:
    """Jaccard similarity on tokenized prep. Both-empty == 1.0."""
    a = _prep_tokens(out)
    b = _prep_tokens(exp)
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


# ---------------------------------------------------------------------------
# Corpus loading
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class GoldItem:
    """One labeled ingredient line.

    ``expected`` carries the gold fields: ``quantity``, ``unit``,
    ``ingredient``, ``preparation``. Metadata fields (``category``,
    ``language``, ``host``, ``row_id``) label the line's origin and drive
    per-group reports without affecting scoring correctness.
    """

    line: str
    expected: dict[str, Any]
    category: str = ""
    language: str = "en"
    host: str = ""
    row_id: int | None = None


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    with path.open() as f:
        for raw in f:
            raw = raw.strip()
            if not raw:
                continue
            items.append(json.loads(raw))
    return items


def load_english_corpus(path: Path = ENGLISH_GOLD) -> list[GoldItem]:
    return [
        GoldItem(
            line=d["line"],
            expected=d["expected"],
            category=d.get("category", ""),
            language="en",
        )
        for d in _load_jsonl(path)
    ]


def load_swedish_corpus(path: Path = SWEDISH_GOLD) -> list[GoldItem]:
    if not path.exists():
        return []
    return [
        GoldItem(
            line=d["line"],
            expected=d["expected"],
            category=d.get("category", ""),
            language="sv",
            host="ica.se",
            row_id=d.get("row_id"),
        )
        for d in _load_jsonl(path)
    ]


def load_multilingual_corpus(path: Path = MULTILINGUAL_GOLD) -> list[GoldItem]:
    if not path.exists():
        return []
    return [
        GoldItem(
            line=d["line"],
            expected=d["expected"],
            language=d.get("language", ""),
            host=d.get("host", ""),
            row_id=d.get("row_id"),
        )
        for d in _load_jsonl(path)
    ]


# ---------------------------------------------------------------------------
# Per-model run storage
# ---------------------------------------------------------------------------


@dataclass
class LineRun:
    """One LLM-parsed line, with fields and latency."""

    line: str
    quantity: float | None
    unit: str | None
    ingredient: str | None
    preparation: str | None
    failed: bool
    latency_s: float


@dataclass
class ModelRun:
    """Raw results of running one model once against all three corpora."""

    model: str
    ollama_url: str
    retry_idx: int
    english: list[LineRun] = field(default_factory=list)
    swedish: list[LineRun] = field(default_factory=list)
    multilingual: list[LineRun] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


LLM_TIMEOUT_S = 300.0  # parity with v1; larger models can take >120s on tough lines


def _parse_or_fail(
    line: str, model: str, base_url: str, prompt: str | None
) -> tuple[ParsedIngredient | None, float, str | None]:
    """Call the LLM; surface timeouts/errors as strings rather than raising."""
    t0 = time.monotonic()
    err: str | None = None
    try:
        parsed = parse_ingredient_line(
            line,
            model=model,
            base_url=base_url,
            system_prompt=prompt,
            timeout=LLM_TIMEOUT_S,
        )
    except Exception as e:  # noqa: BLE001 — network / JSON error surface as str
        parsed = None
        err = f"{type(e).__name__}: {e}"
    elapsed = time.monotonic() - t0
    return parsed, elapsed, err


def _run_corpus(
    items: list[GoldItem],
    model: str,
    ollama_url: str,
    prompt: str | None,
    label: str,
) -> tuple[list[LineRun], list[str]]:
    """Run one corpus end-to-end; return parallel LineRuns + error strings."""
    out: list[LineRun] = []
    errors: list[str] = []
    for item in items:
        parsed, elapsed, err = _parse_or_fail(item.line, model, ollama_url, prompt)
        failed = parsed is None
        out.append(
            LineRun(
                line=item.line,
                quantity=None if failed else parsed.quantity,  # type: ignore[union-attr]
                unit=None if failed else parsed.unit,  # type: ignore[union-attr]
                ingredient=None if failed else parsed.ingredient,  # type: ignore[union-attr]
                preparation=None if failed else parsed.preparation,  # type: ignore[union-attr]
                failed=failed,
                latency_s=elapsed,
            )
        )
        if err:
            errors.append(f"{label} {item.line!r}: {err}")
    return out, errors


def run_model(
    model: str,
    ollama_url: str,
    english: list[GoldItem],
    swedish: list[GoldItem],
    multilingual: list[GoldItem],
    retry_idx: int = 0,
) -> ModelRun:
    """Run one model against all three corpora once."""
    run = ModelRun(model=model, ollama_url=ollama_url, retry_idx=retry_idx)

    if english:
        logger.info("[%s r%d] English: %d lines", model, retry_idx, len(english))
        run.english, errs = _run_corpus(english, model, ollama_url, None, "en")
        run.errors.extend(errs)

    if swedish:
        logger.info("[%s r%d] Swedish: %d lines", model, retry_idx, len(swedish))
        run.swedish, errs = _run_corpus(
            swedish, model, ollama_url, NEUTRAL_PROMPT, "sv"
        )
        run.errors.extend(errs)

    if multilingual:
        logger.info(
            "[%s r%d] Multilingual: %d lines", model, retry_idx, len(multilingual)
        )
        run.multilingual, errs = _run_corpus(
            multilingual, model, ollama_url, NEUTRAL_PROMPT, "ml"
        )
        run.errors.extend(errs)

    return run


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------


@dataclass
class PerFieldScore:
    """Per-field accuracy for one language on one retry."""

    n: int
    failures: int
    qty_acc: float
    unit_acc: float
    ing_acc: float
    prep_exact_acc: float
    prep_jaccard_mean: float
    line_f1: float  # all four of (qty, unit, ing, prep-exact) match
    avg_latency_s: float


NormFn = Callable[[str | None], str]


def _score_pair(
    out: LineRun,
    exp: dict[str, Any],
    norm_unit: NormFn,
    norm_ing: NormFn,
) -> tuple[bool, bool, bool, bool, float]:
    """Score one (LineRun, expected) pair. Returns four booleans + prep jaccard."""
    if out.failed:
        return False, False, False, False, 0.0
    try:
        qty_match = abs(float(out.quantity or 0.0) - float(exp["quantity"])) < 1e-6
    except (TypeError, ValueError, KeyError):
        qty_match = False
    unit_match = norm_unit(out.unit) == norm_unit(str(exp.get("unit", "")))
    ing_match = norm_ing(out.ingredient) == norm_ing(str(exp.get("ingredient", "")))
    exp_prep = str(exp.get("preparation", ""))
    prep_exact = _prep_exact_match(out.preparation, exp_prep)
    prep_j = _prep_jaccard(out.preparation, exp_prep)
    return qty_match, unit_match, ing_match, prep_exact, prep_j


def _score_language(
    items: list[GoldItem],
    runs: list[LineRun],
    norm_unit: NormFn,
    norm_ing: NormFn,
) -> PerFieldScore:
    """Aggregate field matches over one (items, runs) pair."""
    n = len(items)
    if n == 0:
        return PerFieldScore(0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
    qty_ok = unit_ok = ing_ok = prep_ok = line_ok = 0
    prep_j_total = 0.0
    failures = 0
    total_lat = 0.0
    for item, out in zip(items, runs, strict=True):
        total_lat += out.latency_s
        if out.failed:
            failures += 1
            continue
        q, u, i_, pe, pj = _score_pair(out, item.expected, norm_unit, norm_ing)
        qty_ok += q
        unit_ok += u
        ing_ok += i_
        prep_ok += pe
        prep_j_total += pj
        if q and u and i_ and pe:
            line_ok += 1
    return PerFieldScore(
        n=n,
        failures=failures,
        qty_acc=qty_ok / n,
        unit_acc=unit_ok / n,
        ing_acc=ing_ok / n,
        prep_exact_acc=prep_ok / n,
        prep_jaccard_mean=prep_j_total / n,
        line_f1=line_ok / n,
        avg_latency_s=total_lat / n,
    )


@dataclass
class NameSetScore:
    """Micro-averaged name-set F1 across recipes (grouped by row_id)."""

    precision: float
    recall: float
    f1: float
    tp: int
    extracted: int
    gold: int


def _score_swedish_name_set(
    items: list[GoldItem],
    runs: list[LineRun],
) -> NameSetScore:
    """Group by row_id, compare gold vs extracted name sets per recipe, micro-F1."""
    gold_by_row: dict[int, set[str]] = {}
    out_by_row: dict[int, set[str]] = {}
    for item, out in zip(items, runs, strict=True):
        rid = item.row_id
        if rid is None:
            continue
        gold_name = str(item.expected.get("ingredient", "")).lower().strip()
        if gold_name:
            gold_by_row.setdefault(rid, set()).add(gold_name)
        else:
            gold_by_row.setdefault(rid, set())
        out_name = (out.ingredient or "").lower().strip()
        if not out.failed and out_name:
            out_by_row.setdefault(rid, set()).add(out_name)
        else:
            out_by_row.setdefault(rid, set())
    tp = fp = fn = 0
    for rid, gold in gold_by_row.items():
        ext = out_by_row.get(rid, set())
        tp += len(ext & gold)
        fp += len(ext - gold)
        fn += len(gold - ext)
    p = tp / (tp + fp) if (tp + fp) else 0.0
    r = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2 * p * r / (p + r) if (p + r) else 0.0
    return NameSetScore(
        precision=p,
        recall=r,
        f1=f1,
        tp=tp,
        extracted=tp + fp,
        gold=tp + fn,
    )


@dataclass
class Score:
    """Single-retry score for one model across all corpora."""

    model: str
    retry_idx: int
    per_language: dict[str, PerFieldScore]
    sv_name_set: NameSetScore


def score_run(
    run: ModelRun,
    english: list[GoldItem],
    swedish: list[GoldItem],
    multilingual: list[GoldItem],
) -> Score:
    """Score one ModelRun against the gold corpora it was run on."""
    per_language: dict[str, PerFieldScore] = {}
    if english:
        per_language["en"] = _score_language(
            english, run.english, _norm_unit_en, _norm_ingredient_en
        )
    if swedish:
        per_language["sv"] = _score_language(
            swedish, run.swedish, _norm_unit_generic, _norm_ingredient_generic
        )
    # Group multilingual by language tag
    if multilingual:
        by_lang: dict[str, list[tuple[GoldItem, LineRun]]] = {}
        for item, out in zip(multilingual, run.multilingual, strict=True):
            by_lang.setdefault(item.language, []).append((item, out))
        for lang, pairs in by_lang.items():
            items_l = [p[0] for p in pairs]
            runs_l = [p[1] for p in pairs]
            per_language[lang] = _score_language(
                items_l, runs_l, _norm_unit_generic, _norm_ingredient_generic
            )

    sv_name_set = (
        _score_swedish_name_set(swedish, run.swedish)
        if swedish
        else NameSetScore(0.0, 0.0, 0.0, 0, 0, 0)
    )
    return Score(
        model=run.model,
        retry_idx=run.retry_idx,
        per_language=per_language,
        sv_name_set=sv_name_set,
    )


# ---------------------------------------------------------------------------
# Aggregation: collapse retries to mean ± stdev bands
# ---------------------------------------------------------------------------


@dataclass
class MetricBand:
    """Mean and sample stdev for one metric across retries."""

    mean: float
    stdev: float

    @classmethod
    def from_values(cls, values: list[float]) -> MetricBand:
        if not values:
            return cls(0.0, 0.0)
        m = statistics.fmean(values)
        s = statistics.stdev(values) if len(values) > 1 else 0.0
        return cls(mean=m, stdev=s)


@dataclass
class LangBand:
    """Per-field MetricBand set for one language across retries."""

    n: int
    retries: int
    qty: MetricBand
    unit: MetricBand
    ing: MetricBand
    prep_exact: MetricBand
    prep_jaccard: MetricBand
    line_f1: MetricBand
    avg_latency_s: MetricBand
    failures_mean: float


@dataclass
class ModelBand:
    """One row of the results table: per-language bands for one model."""

    model: str
    retries: int
    per_language: dict[str, LangBand]
    sv_name_f1: MetricBand


def aggregate(scores: list[Score]) -> list[ModelBand]:
    """Group Scores by model, collapse across retries to mean ± stdev bands."""
    by_model: dict[str, list[Score]] = {}
    for s in scores:
        by_model.setdefault(s.model, []).append(s)
    bands: list[ModelBand] = []
    for model, ss in by_model.items():
        langs: dict[str, LangBand] = {}
        all_langs: set[str] = set()
        for s in ss:
            all_langs.update(s.per_language)
        for lang in sorted(all_langs):
            per = [s.per_language[lang] for s in ss if lang in s.per_language]
            if not per:
                continue
            langs[lang] = LangBand(
                n=per[0].n,
                retries=len(per),
                qty=MetricBand.from_values([p.qty_acc for p in per]),
                unit=MetricBand.from_values([p.unit_acc for p in per]),
                ing=MetricBand.from_values([p.ing_acc for p in per]),
                prep_exact=MetricBand.from_values([p.prep_exact_acc for p in per]),
                prep_jaccard=MetricBand.from_values([p.prep_jaccard_mean for p in per]),
                line_f1=MetricBand.from_values([p.line_f1 for p in per]),
                avg_latency_s=MetricBand.from_values([p.avg_latency_s for p in per]),
                failures_mean=statistics.fmean([float(p.failures) for p in per]),
            )
        sv_vals = [s.sv_name_set.f1 for s in ss if s.sv_name_set.gold > 0]
        sv_name_f1 = MetricBand.from_values(sv_vals)
        bands.append(
            ModelBand(
                model=model,
                retries=len(ss),
                per_language=langs,
                sv_name_f1=sv_name_f1,
            )
        )
    return bands


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

_LANG_PRIORITY = {"en": 0, "sv": 1}


def _lang_order(langs: list[str]) -> list[str]:
    return sorted(langs, key=lambda k: (_LANG_PRIORITY.get(k, 99), k))


def _cell(band: MetricBand, retries: int) -> str:
    """Format a MetricBand as 'mean' or 'mean ± stdev' depending on retries."""
    if retries <= 1:
        return f"{band.mean:.3f}"
    return f"{band.mean:.3f} ± {band.stdev:.3f}"


def format_summary_table(bands: list[ModelBand]) -> str:
    """Top-level table: per-language line-F1 + latency + Swedish name-F1."""
    all_langs: list[str] = []
    for b in bands:
        for lang in b.per_language:
            if lang not in all_langs:
                all_langs.append(lang)
    ordered = _lang_order(all_langs)

    headers = (
        ["Model", "retries"]
        + [f"{lang} F1" for lang in ordered]
        + [f"{lang} lat/line" for lang in ordered]
        + ["sv name-F1"]
    )
    rows = ["| " + " | ".join(headers) + " |"]
    rows.append("|" + "|".join(["---"] * len(headers)) + "|")
    for b in bands:
        cells: list[str] = [f"`{b.model}`", str(b.retries)]
        for lang in ordered:
            if lang in b.per_language:
                cells.append(_cell(b.per_language[lang].line_f1, b.retries))
            else:
                cells.append("—")
        for lang in ordered:
            if lang in b.per_language:
                cells.append(_cell(b.per_language[lang].avg_latency_s, b.retries) + "s")
            else:
                cells.append("—")
        cells.append(_cell(b.sv_name_f1, b.retries))
        rows.append("| " + " | ".join(cells) + " |")
    return "\n".join(rows)


def format_per_field_breakdown(bands: list[ModelBand]) -> str:
    """One per-field table per language."""
    out: list[str] = []
    lang_seen: set[str] = set()
    for b in bands:
        lang_seen.update(b.per_language)
    for lang in _lang_order(list(lang_seen)):
        out.append(f"\n### {lang} per-field accuracy (n per model)\n")
        headers = [
            "Model",
            "n",
            "qty",
            "unit",
            "ing",
            "prep (exact)",
            "prep (jaccard)",
            "line-F1",
            "fails",
        ]
        out.append("| " + " | ".join(headers) + " |")
        out.append("|" + "|".join(["---"] * len(headers)) + "|")
        for b in bands:
            if lang not in b.per_language:
                continue
            lb = b.per_language[lang]
            cells = [
                f"`{b.model}`",
                str(lb.n),
                _cell(lb.qty, b.retries),
                _cell(lb.unit, b.retries),
                _cell(lb.ing, b.retries),
                _cell(lb.prep_exact, b.retries),
                _cell(lb.prep_jaccard, b.retries),
                _cell(lb.line_f1, b.retries),
                f"{lb.failures_mean:.1f}",
            ]
            out.append("| " + " | ".join(cells) + " |")
    return "\n".join(out)


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

RESULTS_SCHEMA = "v2"


def save_runs(runs: list[ModelRun], out: Path) -> None:
    """Write ModelRuns to disk as a v2 JSON payload."""
    out.parent.mkdir(parents=True, exist_ok=True)
    payload = {"schema": RESULTS_SCHEMA, "runs": [asdict(r) for r in runs]}
    with out.open("w") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def load_runs(path: Path) -> list[ModelRun]:
    """Load ModelRuns from a v2 JSON payload."""
    with path.open() as f:
        payload = json.load(f)
    schema = payload.get("schema", "v1")
    if schema != RESULTS_SCHEMA:
        raise ValueError(
            f"{path}: schema {schema!r} not supported (expected {RESULTS_SCHEMA!r}). "
            "Re-run rather than re-score."
        )
    runs: list[ModelRun] = []
    for raw in payload["runs"]:
        english = [LineRun(**r) for r in raw.get("english", [])]
        swedish = [LineRun(**r) for r in raw.get("swedish", [])]
        multilingual = [LineRun(**r) for r in raw.get("multilingual", [])]
        runs.append(
            ModelRun(
                model=raw["model"],
                ollama_url=raw["ollama_url"],
                retry_idx=raw.get("retry_idx", 0),
                english=english,
                swedish=swedish,
                multilingual=multilingual,
                errors=raw.get("errors", []),
            )
        )
    return runs


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

DEFAULT_MODELS = [
    "gemma4:e2b",
    "devstral:24b",
    "qwen3.5:27b",
    "nemotron-3-nano:30b",
    "qwen3-coder:30b",
]


def _limit(items: list[GoldItem], n: int | None) -> list[GoldItem]:
    """None = all; 0 = skip corpus; N = first N items."""
    if n is None:
        return items
    return items[:n]


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--ollama-url", default="http://192.168.50.189:11434")
    parser.add_argument("--models", nargs="+", default=DEFAULT_MODELS)
    parser.add_argument(
        "--retries",
        type=int,
        default=1,
        help="Run each corpus N times per model; report mean ± stdev.",
    )
    parser.add_argument(
        "--swedish-limit",
        type=int,
        default=None,
        help="Cap Swedish lines; use 0 to skip Swedish entirely.",
    )
    parser.add_argument(
        "--english-limit",
        type=int,
        default=None,
        help="Cap English lines; use 0 to skip English entirely.",
    )
    parser.add_argument(
        "--multilingual-limit",
        type=int,
        default=None,
        help="Cap multilingual lines; use 0 to skip.",
    )
    parser.add_argument(
        "--out", type=Path, default=Path("scripts/benchmark_data/results.json")
    )
    parser.add_argument(
        "--rescore",
        type=Path,
        default=None,
        help="Skip LLM calls; load runs from this file and rescore",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(message)s",
    )

    english = _limit(load_english_corpus(), args.english_limit)
    swedish = _limit(load_swedish_corpus(), args.swedish_limit)
    multilingual = _limit(load_multilingual_corpus(), args.multilingual_limit)

    if args.rescore is not None:
        runs = load_runs(args.rescore)
        # A rescore targets whatever subset the original run used; clip the
        # gold corpora to match each run's LineRun count so zip(..., strict=True)
        # in scoring doesn't blow up on limited runs (e.g. --english-limit 30).
        if runs:
            english = english[: len(runs[0].english)]
            swedish = swedish[: len(runs[0].swedish)]
            multilingual = multilingual[: len(runs[0].multilingual)]
    else:
        runs = []
        for retry in range(args.retries):
            for model in args.models:
                print(
                    f"\n=== {model} (retry {retry + 1}/{args.retries}) ===",
                    file=sys.stderr,
                )
                t0 = time.monotonic()
                run = run_model(
                    model,
                    args.ollama_url,
                    english,
                    swedish,
                    multilingual,
                    retry_idx=retry,
                )
                print(f"  wall: {time.monotonic() - t0:.1f}s", file=sys.stderr)
                runs.append(run)
                save_runs(runs, args.out)  # checkpoint after every model

    scores = [score_run(r, english, swedish, multilingual) for r in runs]
    bands = aggregate(scores)

    print("\n## Benchmark results\n")
    print(format_summary_table(bands))
    print()
    print(format_per_field_breakdown(bands))
    if args.rescore is None:
        print(f"\n(saved raw runs to {args.out})")


if __name__ == "__main__":
    main()
