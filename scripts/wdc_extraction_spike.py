#!/usr/bin/env python3
"""Spike: WDC ingredient-name extraction bakeoff on ica.se pannkakor.

Compares three extraction strategies on Swedish ingredient lines:
  (a) LLM via parse.py (gemma4:e4b / Ollama)
  (b) Regex stripper (quantity+unit removal, head-noun extraction)
  (c) Raw strings (baseline)

Measures precision/recall vs hand-labelled gold standard, Jaccard
clustering behaviour, and LLM latency/failure rate.

Usage:
    python3 scripts/wdc_extraction_spike.py
    python3 scripts/wdc_extraction_spike.py --skip-llm   # regex-only (fast)
    python3 scripts/wdc_extraction_spike.py --ollama-url http://host:11434
"""

from __future__ import annotations

import argparse
import gzip
import json
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ICA_GZ = Path("dataset/wdc/Recipe_ica.se_October2023.json.gz")
GOLD_PATH = Path("scripts/wdc_gold_standard.json")
JACCARD_THRESHOLD = 0.6

# Swedish units for the regex stripper
_SV_UNITS = (
    r"dl|msk|tsk|krm|st|g|kg|ml|l|cl|port|nypa|bit|skiva|skivor|paket|burk|knippe"
)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class WDCRecipe:
    row_id: int
    name: str
    ingredients_raw: list[str]
    cooking_method: str | None
    page_url: str


@dataclass
class ExtractionResult:
    """Per-recipe extraction output for one strategy."""

    names: frozenset[str]  # extracted ingredient names
    failures: int = 0  # lines that failed to extract
    wall_seconds: float = 0.0  # total wall time for this recipe


@dataclass
class ClusterInfo:
    label: str
    recipes: list[WDCRecipe] = field(default_factory=list)
    canonical: frozenset[str] = frozenset()


# ---------------------------------------------------------------------------
# Step 1: Load ica.se pannkakor recipes
# ---------------------------------------------------------------------------


def load_pannkakor(path: Path) -> list[WDCRecipe]:
    """Load recipes whose title contains 'pannkak' from the ica.se WDC dump."""
    recipes: list[WDCRecipe] = []
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            rec = json.loads(line)
            name = rec.get("name", "")
            if "pannkak" not in name.lower():
                continue
            recipes.append(
                WDCRecipe(
                    row_id=rec["row_id"],
                    name=name,
                    ingredients_raw=rec.get("recipeingredient", []),
                    cooking_method=rec.get("cookingmethod"),
                    page_url=rec.get("page_url", ""),
                )
            )
    return recipes


# ---------------------------------------------------------------------------
# Step 2: Gold standard
# ---------------------------------------------------------------------------


def load_gold_standard(path: Path) -> dict[int, set[str]]:
    """Load gold standard: {row_id: {ingredient_name, ...}}."""
    with open(path) as f:
        data = json.load(f)
    return {int(k): set(v) for k, v in data.items()}


# ---------------------------------------------------------------------------
# Step 3a: LLM extractor (parse.py)
# ---------------------------------------------------------------------------


def extract_llm(
    recipe: WDCRecipe,
    model: str = "gemma4:e4b",
    base_url: str = "http://localhost:11434",
) -> ExtractionResult:
    """Extract ingredient names using parse.py's LLM approach."""
    from rational_recipes.scrape.parse import parse_ingredient_line

    names: list[str] = []
    failures = 0
    t0 = time.monotonic()
    for line in recipe.ingredients_raw:
        parsed = parse_ingredient_line(line, model=model, base_url=base_url)
        if parsed and parsed.ingredient:
            names.append(parsed.ingredient.lower().strip())
        else:
            failures += 1
    elapsed = time.monotonic() - t0
    return ExtractionResult(
        names=frozenset(names), failures=failures, wall_seconds=elapsed
    )


# ---------------------------------------------------------------------------
# Step 3b: Regex stripper
# ---------------------------------------------------------------------------

_QTY_UNIT_RE = re.compile(
    rf"""
    ^                         # start of line
    \s*                       # optional leading whitespace
    (?:\d[\d/.,\s]*)?         # optional quantity (digits, fractions, decimals)
    \s*                       # space between qty and unit
    (?:{_SV_UNITS})           # Swedish unit
    \b                        # word boundary
    \s*                       # trailing space after unit
    """,
    re.VERBOSE | re.IGNORECASE,
)

# Parenthetical notes, trailing commas + text
_TAIL_RE = re.compile(r"\s*\(.*?\)|\s*,\s*.*$")

# Leading quantity without unit: "2 ägg", "3 stora ägg"
_BARE_QTY_RE = re.compile(r"^\d[\d/.,\s]*\s+")

# Size/count adjectives common in Swedish recipes
_SV_ADJ_RE = re.compile(
    r"\b(?:stor|stora|liten|litet|små|lilla|lång|långa|medium|ca|à|circa)\b\.?\s*",
    re.IGNORECASE,
)

# Weight/count in parens: "(à ca 70 g)", "(ca 500 g)"
_PAREN_WEIGHT_RE = re.compile(r"\s*\(.*?\)\s*")


def extract_regex(recipe: WDCRecipe) -> ExtractionResult:
    """Extract ingredient names by stripping quantity+unit via regex."""
    names: list[str] = []
    failures = 0
    for line in recipe.ingredients_raw:
        name = _extract_one_regex(line)
        if name:
            names.append(name)
        else:
            failures += 1
    return ExtractionResult(names=frozenset(names), failures=failures)


def _extract_one_regex(line: str) -> str | None:
    """Try to extract an ingredient head noun from a Swedish line."""
    s = line.strip()
    if not s:
        return None

    # Remove parenthetical weight/notes first: "(à ca 70 g)", "(max 7 %)"
    s = _PAREN_WEIGHT_RE.sub(" ", s).strip()

    # Strip size/approximation adjectives early (before qty+unit, so "ca 4 dl" works)
    s = _SV_ADJ_RE.sub("", s).strip()

    # Strip leading quantity + unit
    s = _QTY_UNIT_RE.sub("", s).strip()

    # Strip bare leading quantity (e.g. "2 ägg" → "ägg")
    s = _BARE_QTY_RE.sub("", s).strip()

    # Second pass: adjective removal may have exposed a new qty+unit
    s = _QTY_UNIT_RE.sub("", s).strip()
    s = _BARE_QTY_RE.sub("", s).strip()

    # Strip trailing comma-clause
    s = _TAIL_RE.sub("", s).strip()

    # Strip leading Swedish preparation/state adjectives
    s = re.sub(
        r"^(?:frysta|färska|färsk|hackad|hackade|strimlad|finhackad|"
        r"kokt|kokta|rökt|riven|rivet|finrivet|smält|torkad|torkade|"
        r"siktat|malen|rumsvarmt|mogen|ljusa|flytande|naturell|"
        r"smälta|halvtinade?|klippt|tärnat|skivad|vispad)\s+",
        "",
        s,
        flags=re.IGNORECASE,
    ).strip()

    # Lowercase, final cleanup
    s = s.lower().strip()

    return s if s else None


# ---------------------------------------------------------------------------
# Step 3c: Raw strings (baseline)
# ---------------------------------------------------------------------------


def extract_raw(recipe: WDCRecipe) -> ExtractionResult:
    """Use the raw ingredient string as the 'name' (baseline)."""
    names = frozenset(
        line.lower().strip() for line in recipe.ingredients_raw if line.strip()
    )
    return ExtractionResult(names=names)


# ---------------------------------------------------------------------------
# Step 4: Metrics
# ---------------------------------------------------------------------------


def precision_recall(
    predicted: frozenset[str], gold: set[str]
) -> tuple[float, float, float]:
    """Compute precision, recall, F1 with fuzzy matching.

    We use substring containment for matching — if a gold name appears as a
    substring of a predicted name or vice versa, it counts.
    """
    if not predicted and not gold:
        return 1.0, 1.0, 1.0

    def fuzzy_match(pred: str, gld: str) -> bool:
        return pred in gld or gld in pred

    tp = 0
    matched_gold: set[str] = set()
    for p in predicted:
        for g in gold:
            if g not in matched_gold and fuzzy_match(p, g):
                tp += 1
                matched_gold.add(g)
                break

    prec = tp / len(predicted) if predicted else 0.0
    rec = tp / len(gold) if gold else 0.0
    f1 = 2 * prec * rec / (prec + rec) if (prec + rec) > 0 else 0.0
    return prec, rec, f1


def jaccard(a: frozenset[str], b: frozenset[str]) -> float:
    if not a and not b:
        return 1.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union > 0 else 0.0


def cluster_recipes(
    extractions: dict[int, ExtractionResult],
    recipes: list[WDCRecipe],
    threshold: float = JACCARD_THRESHOLD,
) -> list[ClusterInfo]:
    """Greedy single-pass Jaccard clustering (same algo as grouping.py)."""
    clusters: list[ClusterInfo] = []
    for r in recipes:
        ext = extractions.get(r.row_id)
        if not ext or not ext.names:
            continue
        placed = False
        for c in clusters:
            if jaccard(ext.names, c.canonical) >= threshold:
                c.recipes.append(r)
                placed = True
                break
        if not placed:
            clusters.append(
                ClusterInfo(
                    label=f"C{len(clusters)}",
                    recipes=[r],
                    canonical=ext.names,
                )
            )
    clusters.sort(key=lambda c: len(c.recipes), reverse=True)
    return clusters


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def print_header(title: str) -> None:
    print(f"\n{'=' * 70}")
    print(f"  {title}")
    print(f"{'=' * 70}\n")


def report_extractor(
    name: str,
    extractions: dict[int, ExtractionResult],
    recipes: list[WDCRecipe],
    gold: dict[int, set[str]],
) -> dict:
    """Print metrics for one extractor and return summary dict."""
    print_header(name)

    # --- Per-recipe gold-standard comparison ---
    gold_ids = set(gold.keys()) & {r.row_id for r in recipes}
    precs, recs, f1s = [], [], []
    for rid in sorted(gold_ids):
        ext = extractions.get(rid)
        if ext is None:
            continue
        p, r, f = precision_recall(ext.names, gold[rid])
        precs.append(p)
        recs.append(r)
        f1s.append(f)
        recipe = next(rc for rc in recipes if rc.row_id == rid)
        print(f"  {recipe.name[:50]:50s}  P={p:.2f}  R={r:.2f}  F1={f:.2f}")
        if p < 1.0 or r < 1.0:
            missed = gold[rid] - {
                g for g in gold[rid] if any(g in e or e in g for e in ext.names)
            }
            spurious = {
                e for e in ext.names if not any(e in g or g in e for g in gold[rid])
            }
            if missed:
                print(f"    missed:   {missed}")
            if spurious:
                print(f"    spurious: {spurious}")

    avg_p = sum(precs) / len(precs) if precs else 0
    avg_r = sum(recs) / len(recs) if recs else 0
    avg_f1 = sum(f1s) / len(f1s) if f1s else 0
    print(
        f"\n  AVERAGE over {len(precs)} recipes:"
        f"  P={avg_p:.3f}  R={avg_r:.3f}  F1={avg_f1:.3f}"
    )

    # --- Failure rate ---
    total_lines = sum(len(r.ingredients_raw) for r in recipes)
    total_failures = sum(ext.failures for ext in extractions.values())
    fail_pct = 100 * total_failures / total_lines if total_lines else 0
    print(f"  Failures: {total_failures}/{total_lines} lines ({fail_pct:.1f}%)")

    # --- Latency (LLM only) ---
    total_time = sum(ext.wall_seconds for ext in extractions.values())
    if total_time > 0:
        per_line = total_time / total_lines if total_lines else 0
        print(f"  Wall time: {total_time:.1f}s total, {per_line:.2f}s/line")

    # --- Jaccard clustering ---
    clusters = cluster_recipes(extractions, recipes)
    print(f"\n  L2 clusters (threshold={JACCARD_THRESHOLD}): {len(clusters)}")
    for c in clusters:
        methods = set()
        for r in c.recipes:
            if r.cooking_method:
                methods.add(r.cooking_method)
        method_str = ", ".join(sorted(methods)) if methods else "—"
        print(f"    {c.label}: {len(c.recipes):2d} recipes  methods=[{method_str}]")
        print(f"           ingredients: {', '.join(sorted(c.canonical))}")

    return {
        "precision": avg_p,
        "recall": avg_r,
        "f1": avg_f1,
        "failures_pct": fail_pct,
        "n_clusters": len(clusters),
        "wall_time": total_time,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--skip-llm", action="store_true", help="Skip the LLM extractor"
    )
    parser.add_argument("--ollama-url", default="http://localhost:11434")
    parser.add_argument("--model", default="gemma4:e4b")
    args = parser.parse_args()

    if not ICA_GZ.exists():
        print(
            f"ERROR: {ICA_GZ} not found. Extract from Recipe_top100.zip first.",
            file=sys.stderr,
        )
        sys.exit(1)

    # --- Load recipes ---
    recipes = load_pannkakor(ICA_GZ)
    print(f"Loaded {len(recipes)} pannkakor recipes from ica.se")

    # --- Load or create gold standard ---
    if not GOLD_PATH.exists():
        print(f"\nGold standard not found at {GOLD_PATH}.")
        print("Run with --build-gold first or create it manually.")
        print("\nShowing raw ingredients for first 20 recipes to help build gold:\n")
        for r in recipes[:20]:
            print(f"  [{r.row_id}] {r.name}")
            for ing in r.ingredients_raw:
                print(f"       {ing}")
            print()
        sys.exit(1)

    gold = load_gold_standard(GOLD_PATH)
    print(f"Gold standard: {len(gold)} recipes labelled")

    # --- Run extractors ---
    summaries: dict[str, dict] = {}

    # (c) Raw baseline — always run first (fast)
    print("\nRunning raw baseline...")
    raw_results = {r.row_id: extract_raw(r) for r in recipes}
    summaries["raw"] = report_extractor(
        "(c) Raw strings (baseline)", raw_results, recipes, gold
    )

    # (b) Regex
    print("\nRunning regex extractor...")
    regex_results = {r.row_id: extract_regex(r) for r in recipes}
    summaries["regex"] = report_extractor(
        "(b) Regex stripper", regex_results, recipes, gold
    )

    # (a) LLM
    if not args.skip_llm:
        print("\nRunning LLM extractor (this may take a few minutes)...")
        llm_results: dict[int, ExtractionResult] = {}
        for i, r in enumerate(recipes):
            print(f"  [{i + 1}/{len(recipes)}] {r.name[:60]}", end="", flush=True)
            result = extract_llm(r, model=args.model, base_url=args.ollama_url)
            llm_results[r.row_id] = result
            print(f"  ({result.wall_seconds:.1f}s, {result.failures} fail)")
        summaries["llm"] = report_extractor(
            "(a) LLM (gemma4:e4b)", llm_results, recipes, gold
        )
    else:
        print("\n  [LLM skipped]")

    # --- Summary comparison ---
    print_header("SUMMARY COMPARISON")
    hdr = f"  {'Extractor':15s}  {'P':>6s}  {'R':>6s}"
    hdr += f"  {'F1':>6s}  {'Fail%':>6s}  {'Clust':>5s}  {'Time':>7s}"
    print(hdr)
    sep = f"  {'-' * 15}  {'-' * 6}  {'-' * 6}"
    sep += f"  {'-' * 6}  {'-' * 6}  {'-' * 5}  {'-' * 7}"
    print(sep)
    for label, s in summaries.items():
        t = f"{s['wall_time']:.1f}s" if s["wall_time"] > 0 else "—"
        print(
            f"  {label:15s}  {s['precision']:6.3f}  {s['recall']:6.3f}  "
            f"{s['f1']:6.3f}  {s['failures_pct']:5.1f}%  {s['n_clusters']:5d}  {t:>7s}"
        )


if __name__ == "__main__":
    main()
