#!/usr/bin/env python3
"""Benchmark LLM candidates against gold standards for ingredient extraction.

Runs two corpora through a list of Ollama-hosted models and reports F1
(name-set for Swedish, line-level for English), per-field accuracy, and
latency. Designed to produce the close-note table for
RationalRecipes-b7t.25 (head-to-head vs gemma4:e2b) and
RationalRecipes-5i1 (e2b accuracy ceiling on messy English lines).

Swedish corpus: name-set gold recovered from a1k spike — one gold
frozenset of head nouns per recipe; extraction is scored by
per-recipe precision/recall/F1 on the name set (matches the a1k
methodology). Uses the NEUTRAL_PROMPT from wdc.py because that is
the production path for non-English WDC data.

English corpus: hand-labeled per-line structured gold (42 lines across
plurals, comma-preps, packaging units). Each field (quantity, unit,
ingredient) scored individually with the normalization rules below.
Uses parse.py's default _SYSTEM_PROMPT because that is what the
RecipeNLG pipeline (pipeline.py → scrape_to_csv.py) uses in production.

Usage:
    python3 scripts/benchmark_models.py \\
        --ollama-url http://192.168.50.189:11434 \\
        --models gemma4:e2b gemma4:26b qwen3.5:27b \\
        --out scripts/benchmark_data/results.json

    # Re-score cached run without re-calling LLMs:
    python3 scripts/benchmark_models.py --rescore scripts/benchmark_data/results.json
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from rational_recipes.scrape.parse import (  # noqa: E402
    ParsedIngredient,
    parse_ingredient_line,
)
from rational_recipes.scrape.wdc import NEUTRAL_PROMPT  # noqa: E402

logger = logging.getLogger(__name__)

ICA_GZ = Path("dataset/wdc/Recipe_ica.se_October2023.json.gz")
SWEDISH_GOLD = Path("scripts/benchmark_data/swedish_ica_se_names_gold.json")
ENGLISH_GOLD = Path("scripts/benchmark_data/english_messy_gold.jsonl")


# ---------------------------------------------------------------------------
# Unit normalization for scoring (English)
# ---------------------------------------------------------------------------

_UNIT_SYNONYMS = {
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


def _norm_unit(u: str) -> str:
    """Normalize unit string to a canonical form for comparison."""
    if u is None:
        return ""
    u = u.strip()
    # MEDIUM/LARGE/SMALL are all-caps sentinels in the prompt
    if u.upper() in ("MEDIUM", "LARGE", "SMALL"):
        return u.upper()
    return _UNIT_SYNONYMS.get(u.lower(), u.lower())


def _norm_ingredient(ing: str) -> str:
    """Normalize ingredient for comparison (strip, lowercase, singularize crude)."""
    if ing is None:
        return ""
    ing = ing.strip().lower()
    # crude de-plural: only the trailing 's' if the noun is >3 chars and doesn't
    # already end in 'ss' / 'us' / 'is'
    if (
        len(ing) > 3
        and ing.endswith("s")
        and not ing.endswith(("ss", "us", "is", "as"))
    ):
        # don't strip obvious intrinsic-s words
        if ing[-2] not in "aeiou":
            ing = ing[:-1]
    return ing


# ---------------------------------------------------------------------------
# Swedish corpus loading
# ---------------------------------------------------------------------------


@dataclass
class SwedishRecipe:
    row_id: int
    name: str
    lines: list[str]
    gold_names: frozenset[str]


def load_swedish_corpus() -> list[SwedishRecipe]:
    if not ICA_GZ.exists():
        raise FileNotFoundError(
            f"ica.se WDC file missing: {ICA_GZ}. "
            "Extract dataset/wdc/Recipe_top100.zip first."
        )
    if not SWEDISH_GOLD.exists():
        raise FileNotFoundError(f"Swedish gold missing: {SWEDISH_GOLD}")

    with SWEDISH_GOLD.open() as f:
        gold = {int(k): frozenset(v) for k, v in json.load(f).items()}

    recipes: dict[int, SwedishRecipe] = {}
    with gzip.open(ICA_GZ, "rt", encoding="utf-8") as f:
        for raw in f:
            rec = json.loads(raw)
            rid = rec.get("row_id")
            if rid is None or rid not in gold:
                continue
            recipes[rid] = SwedishRecipe(
                row_id=rid,
                name=rec.get("name", ""),
                lines=list(rec.get("recipeingredient", [])),
                gold_names=gold[rid],
            )

    missing = set(gold) - set(recipes)
    if missing:
        raise RuntimeError(f"gold refers to missing recipes: {sorted(missing)}")
    return [recipes[rid] for rid in sorted(recipes)]


# ---------------------------------------------------------------------------
# English corpus loading
# ---------------------------------------------------------------------------


@dataclass
class EnglishItem:
    line: str
    category: str
    expected: dict[str, object]


def load_english_corpus() -> list[EnglishItem]:
    items: list[EnglishItem] = []
    with ENGLISH_GOLD.open() as f:
        for raw in f:
            raw = raw.strip()
            if not raw:
                continue
            row = json.loads(raw)
            items.append(EnglishItem(**row))
    return items


# ---------------------------------------------------------------------------
# Per-model run (caches raw results; scoring is separate so it can iterate)
# ---------------------------------------------------------------------------


@dataclass
class LineRun:
    line: str
    quantity: float | None
    unit: str | None
    ingredient: str | None
    preparation: str | None
    failed: bool
    latency_s: float


@dataclass
class RecipeRun:
    row_id: int
    name: str
    extracted_names: list[str]
    line_count: int
    failures: int
    wall_seconds: float


@dataclass
class ModelRun:
    model: str
    ollama_url: str
    swedish: list[RecipeRun] = field(default_factory=list)
    english: list[LineRun] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


LLM_TIMEOUT_S = 300.0  # larger models under Q4 can exceed 120s on long lines


def _parse_or_fail(
    line: str, model: str, base_url: str, prompt: str | None
) -> tuple[ParsedIngredient | None, float, str | None]:
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
    except Exception as e:  # noqa: BLE001 — network / timeout surface as str
        parsed = None
        err = f"{type(e).__name__}: {e}"
    elapsed = time.monotonic() - t0
    return parsed, elapsed, err


def run_model(
    model: str,
    ollama_url: str,
    swedish: list[SwedishRecipe],
    english: list[EnglishItem],
) -> ModelRun:
    run = ModelRun(model=model, ollama_url=ollama_url)

    logger.info("[%s] Swedish: %d recipes", model, len(swedish))
    for rec in swedish:
        t0 = time.monotonic()
        names: list[str] = []
        failures = 0
        for line in rec.lines:
            parsed, _, err = _parse_or_fail(line, model, ollama_url, NEUTRAL_PROMPT)
            if parsed and parsed.ingredient:
                names.append(parsed.ingredient.lower().strip())
            else:
                failures += 1
                if err:
                    run.errors.append(f"sv r{rec.row_id}: {err}")
        elapsed = time.monotonic() - t0
        run.swedish.append(
            RecipeRun(
                row_id=rec.row_id,
                name=rec.name,
                extracted_names=names,
                line_count=len(rec.lines),
                failures=failures,
                wall_seconds=elapsed,
            )
        )
        logger.info(
            "  r%d %r %d/%d lines ok %.1fs",
            rec.row_id,
            rec.name,
            len(rec.lines) - failures,
            len(rec.lines),
            elapsed,
        )

    logger.info("[%s] English: %d lines", model, len(english))
    for item in english:
        parsed, elapsed, err = _parse_or_fail(item.line, model, ollama_url, None)
        failed = parsed is None
        run.english.append(
            LineRun(
                line=item.line,
                quantity=None if failed else parsed.quantity,
                unit=None if failed else parsed.unit,
                ingredient=None if failed else parsed.ingredient,
                preparation=None if failed else parsed.preparation,
                failed=failed,
                latency_s=elapsed,
            )
        )
        if err:
            run.errors.append(f"en {item.line!r}: {err}")

    return run


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------


@dataclass
class Score:
    model: str
    # Swedish: name-set F1 micro-averaged across recipes
    sv_precision: float
    sv_recall: float
    sv_f1: float
    sv_total_gold: int
    sv_total_extracted: int
    sv_total_tp: int
    sv_line_failures: int
    sv_total_lines: int
    sv_avg_latency_s: float
    # English: per-field accuracy and line-level F1
    en_qty_acc: float
    en_unit_acc: float
    en_ing_acc: float
    en_line_f1: float  # a line is "correct" if all three fields match
    en_failures: int
    en_total: int
    en_avg_latency_s: float
    # Error buckets on English
    en_wrong_qty: int
    en_wrong_unit: int
    en_wrong_ingredient: int


def score_swedish(
    run: ModelRun, gold_by_row: dict[int, frozenset[str]]
) -> tuple[float, float, float, int, int, int, float]:
    tp = fp = fn = 0
    total_latency = 0.0
    for rr in run.swedish:
        gold = gold_by_row[rr.row_id]
        extracted = frozenset(n.lower().strip() for n in rr.extracted_names)
        tp += len(extracted & gold)
        fp += len(extracted - gold)
        fn += len(gold - extracted)
        total_latency += rr.wall_seconds
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
    total_lines = sum(r.line_count for r in run.swedish)
    avg_lat = total_latency / total_lines if total_lines else 0.0
    return precision, recall, f1, tp, tp + fp, tp + fn, avg_lat


def score_english(
    run: ModelRun, gold: list[EnglishItem]
) -> tuple[float, float, float, float, int, int, int, float]:
    n = len(gold)
    qty_ok = unit_ok = ing_ok = line_ok = 0
    wrong_qty = wrong_unit = wrong_ing = 0
    failures = 0
    total_latency = 0.0
    for item, out in zip(gold, run.english, strict=True):
        total_latency += out.latency_s
        if out.failed:
            failures += 1
            continue
        exp = item.expected
        qty_match = abs(float(out.quantity) - float(exp["quantity"])) < 1e-6  # type: ignore[arg-type]
        unit_match = _norm_unit(out.unit or "") == _norm_unit(str(exp["unit"]))
        ing_match = _norm_ingredient(out.ingredient or "") == _norm_ingredient(
            str(exp["ingredient"])
        )
        if qty_match:
            qty_ok += 1
        else:
            wrong_qty += 1
        if unit_match:
            unit_ok += 1
        else:
            wrong_unit += 1
        if ing_match:
            ing_ok += 1
        else:
            wrong_ing += 1
        if qty_match and unit_match and ing_match:
            line_ok += 1
    qty_acc = qty_ok / n if n else 0.0
    unit_acc = unit_ok / n if n else 0.0
    ing_acc = ing_ok / n if n else 0.0
    line_f1 = (
        line_ok / n if n else 0.0
    )  # exact-match over all fields, same as accuracy here
    avg_lat = total_latency / n if n else 0.0
    return (
        qty_acc,
        unit_acc,
        ing_acc,
        line_f1,
        failures,
        wrong_qty,
        wrong_unit,
        wrong_ing,
        avg_lat,
    )  # type: ignore[return-value]


def score_run(
    run: ModelRun, swedish: list[SwedishRecipe], english: list[EnglishItem]
) -> Score:
    gold_by_row = {r.row_id: r.gold_names for r in swedish}
    sv_prec, sv_rec, sv_f1, sv_tp, sv_ext, sv_g, sv_lat = score_swedish(
        run, gold_by_row
    )
    (en_qty, en_unit, en_ing, en_f1, en_fail, w_qty, w_unit, w_ing, en_lat) = (
        score_english(run, english)
    )
    sv_failures = sum(r.failures for r in run.swedish)
    sv_total_lines = sum(r.line_count for r in run.swedish)
    return Score(
        model=run.model,
        sv_precision=sv_prec,
        sv_recall=sv_rec,
        sv_f1=sv_f1,
        sv_total_gold=sv_g,
        sv_total_extracted=sv_ext,
        sv_total_tp=sv_tp,
        sv_line_failures=sv_failures,
        sv_total_lines=sv_total_lines,
        sv_avg_latency_s=sv_lat,
        en_qty_acc=en_qty,
        en_unit_acc=en_unit,
        en_ing_acc=en_ing,
        en_line_f1=en_f1,
        en_failures=en_fail,
        en_total=len(english),
        en_avg_latency_s=en_lat,
        en_wrong_qty=w_qty,
        en_wrong_unit=w_unit,
        en_wrong_ingredient=w_ing,
    )


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


_HEADER = (
    "| Model | SwF1 | SwPrec | SwRec | Sw lat/line "
    "| EnF1 | EnQty | EnUnit | EnIng | En lat/line | En fails |"
)
_SEP = "|---|---|---|---|---|---|---|---|---|---|---|"


def format_table(scores: list[Score]) -> str:
    rows = [_HEADER, _SEP]
    for s in scores:
        rows.append(
            f"| `{s.model}` "
            f"| {s.sv_f1:.2f} | {s.sv_precision:.2f} "
            f"| {s.sv_recall:.2f} | {s.sv_avg_latency_s:.2f}s "
            f"| {s.en_line_f1:.2f} | {s.en_qty_acc:.2f} "
            f"| {s.en_unit_acc:.2f} | {s.en_ing_acc:.2f} "
            f"| {s.en_avg_latency_s:.2f}s | {s.en_failures}/{s.en_total} |"
        )
    return "\n".join(rows)


def format_error_breakdown(scores: list[Score]) -> str:
    lines = [
        "\n### English per-class error breakdown (count of incorrect lines per field)\n"
    ]
    lines.append("| Model | wrong qty | wrong unit | wrong ingredient | total lines |")
    lines.append("|---|---|---|---|---|")
    for s in scores:
        lines.append(
            f"| `{s.model}` | {s.en_wrong_qty} | {s.en_wrong_unit} "
            f"| {s.en_wrong_ingredient} | {s.en_total} |"
        )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


def save_runs(runs: list[ModelRun], out: Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    payload = {"runs": [asdict(r) for r in runs]}
    with out.open("w") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def load_runs(path: Path) -> list[ModelRun]:
    with path.open() as f:
        payload = json.load(f)
    runs: list[ModelRun] = []
    for raw in payload["runs"]:
        swedish = [RecipeRun(**r) for r in raw["swedish"]]
        english = [LineRun(**r) for r in raw["english"]]
        runs.append(
            ModelRun(
                model=raw["model"],
                ollama_url=raw["ollama_url"],
                swedish=swedish,
                english=english,
                errors=raw.get("errors", []),
            )
        )
    return runs


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


DEFAULT_MODELS = [
    "gemma4:e2b",
    "gemma4:26b",
    "nemotron-3-nano:30b",
    "qwen3.5:27b",
    "devstral:24b",
    "qwen3-coder:30b",
]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ollama-url", default="http://192.168.50.189:11434")
    parser.add_argument("--models", nargs="+", default=DEFAULT_MODELS)
    parser.add_argument(
        "--swedish-limit",
        type=int,
        default=None,
        help="Limit Swedish recipes (smoke tests)",
    )
    parser.add_argument(
        "--english-limit",
        type=int,
        default=None,
        help="Limit English lines (smoke tests)",
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

    swedish = load_swedish_corpus()
    english = load_english_corpus()
    if args.swedish_limit is not None:
        swedish = swedish[: args.swedish_limit]
    if args.english_limit is not None:
        english = english[: args.english_limit]

    if args.rescore is not None:
        runs = load_runs(args.rescore)
    else:
        runs = []
        for model in args.models:
            print(f"\n=== {model} ===", file=sys.stderr)
            t0 = time.monotonic()
            run = run_model(model, args.ollama_url, swedish, english)
            print(f"  wall: {time.monotonic() - t0:.1f}s", file=sys.stderr)
            runs.append(run)
            save_runs(runs, args.out)  # checkpoint after each model

    scores = [score_run(r, swedish, english) for r in runs]

    print("\n## Benchmark results\n")
    print(format_table(scores))
    print(format_error_breakdown(scores))

    print(f"\n(saved raw runs to {args.out})")


if __name__ == "__main__":
    main()
