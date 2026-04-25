#!/usr/bin/env python3
"""Shadow-compare two parse models on real WDC ingredient lines.

Decides whether a candidate parse approach (smaller model, regex
implementation, etc.) is safe to swap in for the production model
(vwt.18 + future). Samples N WDC recipes, runs both implementations
through ``parse_ingredient_lines`` (batched), reports per-line
agreement on ``(ingredient, unit-equiv, qty)``, and persists the
baseline outputs as a reusable artifact so future approaches can
be evaluated without re-paying the baseline LLM cost.

Per project policy (memory: project_english_display.md), the PWA
shows English ingredients/units regardless of source language, so
cross-language translations like ``tsk`` → ``tsp`` count as
agreement here.

Usage:
    # Full shadow run (baseline + candidate, write everything):
    python3 scripts/shadow_compare_parse.py \\
        --baseline qwen3.6:35b-a3b \\
        --candidate gemma4:e2b \\
        --recipes 150

    # Save the baseline as a frozen reference (no candidate needed):
    python3 scripts/shadow_compare_parse.py \\
        --baseline qwen3.6:35b-a3b \\
        --save-baseline scripts/benchmark_data/shadow_baseline.json \\
        --recipes 150

    # Reuse a saved baseline for a future approach:
    python3 scripts/shadow_compare_parse.py \\
        --baseline-cache scripts/benchmark_data/shadow_baseline.json \\
        --candidate gemma4:e2b
"""

from __future__ import annotations

import argparse
import json
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from rational_recipes.scrape.parse import (
    ParsedIngredient,
    parse_ingredient_lines,
)
from rational_recipes.scrape.wdc import WDCLoader

# Unit-equivalence classes — members count as identical post-translation.
_EQUIV_GROUPS = [
    {"tsk", "tsp", "teaspoon", "teaspoons"},
    {"msk", "tbsp", "tbs", "tablespoon", "tablespoons"},
    {"dl", "deciliter", "deciliters"},
    {"g", "gram", "grams", ""},
    {"kg", "kilogram", "kilograms"},
    {"ml", "milliliter", "milliliters"},
    {"l", "liter", "litre", "liters"},
    {"cup", "cups", "c"},
    {"oz", "ounce", "ounces"},
    {"lb", "lbs", "pound", "pounds"},
    {"krm", "pinch"},
    {"medium", "large", "small", "piece", "pieces", ""},
]


def _norm_unit(u: str) -> str:
    return u.strip().lower().rstrip(".")


def _unit_equiv(a: str, b: str) -> bool:
    a, b = _norm_unit(a), _norm_unit(b)
    if a == b:
        return True
    for group in _EQUIV_GROUPS:
        if a in group and b in group:
            return True
    return False


def _qty_close(a: float, b: float, tol: float = 0.05) -> bool:
    if a == b:
        return True
    if max(abs(a), abs(b)) == 0:
        return True
    return abs(a - b) / max(abs(a), abs(b)) <= tol


def _ingredient_close(a: str, b: str) -> bool:
    a = a.lower().strip()
    b = b.lower().strip()
    if a == b:
        return True
    return (a in b or b in a) and len(min(a, b, key=len)) >= 3


@dataclass
class Compare:
    line: str
    a: ParsedIngredient | None
    b: ParsedIngredient | None

    def matches(self) -> dict[str, bool]:
        if self.a is None or self.b is None:
            return {
                "both_parsed": False, "ing": False,
                "unit": False, "qty": False, "all": False,
            }
        ing = _ingredient_close(self.a.ingredient, self.b.ingredient)
        unit = _unit_equiv(self.a.unit, self.b.unit)
        qty = _qty_close(self.a.quantity, self.b.quantity)
        return {
            "both_parsed": True,
            "ing": ing,
            "unit": unit,
            "qty": qty,
            "all": ing and unit and qty,
        }


def _sample_wdc(zip_path: Path, n_recipes: int) -> list[list[str]]:
    """Pick the first N WDC recipes that have at least 3 ingredient lines."""
    loader = WDCLoader(zip_path=zip_path)
    out: list[list[str]] = []
    for r in loader.iter_all():
        lines = list(r.ingredients)
        if not lines or len(lines) < 3:
            continue
        out.append(lines)
        if len(out) >= n_recipes:
            break
    return out


def _serialize_parse(p: ParsedIngredient | None) -> dict[str, Any] | None:
    if p is None:
        return None
    return {
        "quantity": p.quantity,
        "unit": p.unit,
        "ingredient": p.ingredient,
        "preparation": p.preparation,
        "raw": p.raw,
    }


def _deserialize_parse(d: dict[str, Any] | None) -> ParsedIngredient | None:
    if d is None:
        return None
    return ParsedIngredient(
        quantity=float(d["quantity"]),
        unit=str(d["unit"]),
        ingredient=str(d["ingredient"]),
        preparation=str(d.get("preparation", "")),
        raw=str(d.get("raw", "")),
    )


def _save_baseline(
    path: Path,
    model: str,
    recipes: list[list[str]],
    parses: list[list[ParsedIngredient | None]],
    elapsed_s: float,
) -> None:
    """Write a baseline artifact reusable via --baseline-cache."""
    payload = {
        "schema_version": 1,
        "model": model,
        "elapsed_s": round(elapsed_s, 1),
        "recipes": [
            {
                "lines": lines,
                "parses": [_serialize_parse(p) for p in plist],
            }
            for lines, plist in zip(recipes, parses, strict=True)
        ],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _load_baseline(
    path: Path,
) -> tuple[str, list[list[str]], list[list[ParsedIngredient | None]], float]:
    """Read a saved baseline; returns (model, recipes, parses, elapsed_s)."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    recipes: list[list[str]] = []
    parses: list[list[ParsedIngredient | None]] = []
    for r in payload["recipes"]:
        recipes.append(list(r["lines"]))
        parses.append([_deserialize_parse(p) for p in r["parses"]])
    return payload["model"], recipes, parses, float(payload.get("elapsed_s", 0.0))


def _run_parse_pass(
    label: str,
    model: str,
    ollama_url: str,
    recipes: list[list[str]],
) -> tuple[list[list[ParsedIngredient | None]], float]:
    print(f"=== {label}: {model} ===", flush=True)
    t0 = time.monotonic()
    out: list[list[ParsedIngredient | None]] = []
    for i, lines in enumerate(recipes):
        out.append(
            parse_ingredient_lines(lines, model=model, base_url=ollama_url)
        )
        if (i + 1) % 10 == 0:
            elapsed = time.monotonic() - t0
            print(f"  {i+1}/{len(recipes)} done, {elapsed:.0f}s elapsed", flush=True)
    elapsed = time.monotonic() - t0
    print(f"{label} total: {elapsed:.1f}s\n", flush=True)
    return out, elapsed


def _summarize(
    recipes: list[list[str]],
    baseline_parses: list[list[ParsedIngredient | None]],
    candidate_parses: list[list[ParsedIngredient | None]],
    baseline_label: str,
    candidate_label: str,
    baseline_t: float,
    candidate_t: float,
) -> dict[str, Any]:
    total_lines = 0
    both_parsed = 0
    ing_match = 0
    unit_match = 0
    qty_match = 0
    full_match = 0
    disagreements: list[dict[str, object]] = []
    per_line: list[dict[str, Any]] = []

    for lines, ba, ca in zip(
        recipes, baseline_parses, candidate_parses, strict=True
    ):
        for line, a, b in zip(lines, ba, ca, strict=True):
            total_lines += 1
            cmp = Compare(line=line, a=a, b=b)
            m = cmp.matches()
            if m["both_parsed"]:
                both_parsed += 1
            if m["ing"]:
                ing_match += 1
            if m["unit"]:
                unit_match += 1
            if m["qty"]:
                qty_match += 1
            if m["all"]:
                full_match += 1
            per_line.append({
                "line": line,
                "baseline": _serialize_parse(a),
                "candidate": _serialize_parse(b),
                "match": m,
            })
            if not m["all"] and m["both_parsed"] and len(disagreements) < 30:
                disagreements.append({
                    "line": line,
                    "baseline": _serialize_parse(a),
                    "candidate": _serialize_parse(b),
                })

    speedup = baseline_t / candidate_t if candidate_t else 0.0
    return {
        "schema_version": 1,
        "baseline": baseline_label,
        "candidate": candidate_label,
        "n_recipes": len(recipes),
        "n_lines": total_lines,
        "baseline_seconds": round(baseline_t, 1),
        "candidate_seconds": round(candidate_t, 1),
        "speedup": round(speedup, 2),
        "both_parsed_rate": round(
            both_parsed / total_lines, 3) if total_lines else 0.0,
        "ing_agreement": round(
            ing_match / total_lines, 3) if total_lines else 0.0,
        "unit_agreement": round(
            unit_match / total_lines, 3) if total_lines else 0.0,
        "qty_agreement": round(
            qty_match / total_lines, 3) if total_lines else 0.0,
        "full_agreement": round(
            full_match / total_lines, 3) if total_lines else 0.0,
        "sample_disagreements": disagreements,
        "per_line": per_line,
    }


def _print_summary(s: dict[str, Any]) -> None:
    total = s["n_lines"]
    print(f"\n{'metric':<25} {'value':>10}")
    print("-" * 40)
    print(f"{'lines':<25} {total:>10}")
    print(f"{'both parsed rate':<25} {s['both_parsed_rate']:>10.3f}")
    print(f"{'ingredient agreement':<25} {s['ing_agreement']:>10.3f}")
    print(f"{'unit agreement':<25} {s['unit_agreement']:>10.3f}")
    print(f"{'quantity agreement':<25} {s['qty_agreement']:>10.3f}")
    print(f"{'full agreement (all 3)':<25} {s['full_agreement']:>10.3f}")
    if s["candidate_seconds"]:
        print(f"{'speedup':<25} {s['speedup']:>10.2f}x")
    print(f"\nSample disagreements (first {len(s['sample_disagreements'])}):")
    for d in s["sample_disagreements"][:10]:
        b = d["baseline"]
        c = d["candidate"]
        if not isinstance(b, dict) or not isinstance(c, dict):
            continue
        print(
            f"  {d['line']!r:<50}\n"
            f"    baseline:  qty={b['quantity']:>5} "
            f"unit={b['unit']!r:<8} ing={b['ingredient']!r}\n"
            f"    candidate: qty={c['quantity']:>5} "
            f"unit={c['unit']!r:<8} ing={c['ingredient']!r}"
        )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--wdc-zip", type=Path,
        default=Path("dataset/wdc/Recipe_top100.zip"),
    )
    parser.add_argument(
        "--ollama-url", default="http://192.168.50.189:11434",
    )
    parser.add_argument(
        "--baseline", default="qwen3.6:35b-a3b",
        help="Trusted production model (ignored if --baseline-cache is set).",
    )
    parser.add_argument(
        "--candidate", default=None,
        help="Candidate model to evaluate. If omitted, only the baseline runs "
             "(useful with --save-baseline to bank a reference).",
    )
    parser.add_argument(
        "--recipes", type=int, default=150,
        help="Number of WDC recipes to sample. Ignored if --baseline-cache "
             "is set (the cache fixes the recipe set).",
    )
    parser.add_argument(
        "--baseline-cache", type=Path, default=None,
        help="Load baseline parses from this JSON instead of re-running. "
             "Saves ~78 min of qwen3.6:35b-a3b time per run.",
    )
    parser.add_argument(
        "--save-baseline", type=Path, default=None,
        help="Write baseline parses to this JSON for future --baseline-cache "
             "reuse.",
    )
    parser.add_argument(
        "--out", type=Path,
        default=Path("scripts/benchmark_data/shadow_compare.json"),
        help="Output JSON for full comparison results (per-line + summary).",
    )
    args = parser.parse_args(argv)

    # Resolve recipes + baseline parses (either freshly or from cache).
    if args.baseline_cache is not None:
        if not args.baseline_cache.exists():
            print(f"Baseline cache not found: {args.baseline_cache}")
            return 1
        print(f"Loading baseline from cache: {args.baseline_cache}")
        baseline_label, recipes, baseline_parses, baseline_t = _load_baseline(
            args.baseline_cache
        )
        print(f"Loaded {len(recipes)} recipes from baseline run "
              f"({baseline_label}, {baseline_t:.0f}s original)")
    else:
        if not args.wdc_zip.exists():
            print(f"WDC zip not found: {args.wdc_zip}")
            return 1
        print(f"Sampling {args.recipes} recipes from {args.wdc_zip} …")
        recipes = _sample_wdc(args.wdc_zip, args.recipes)
        n_lines = sum(len(r) for r in recipes)
        print(f"Got {len(recipes)} recipes / {n_lines} lines\n")
        baseline_label = args.baseline
        baseline_parses, baseline_t = _run_parse_pass(
            "Baseline", args.baseline, args.ollama_url, recipes
        )

    # Optionally save the baseline as a reusable artifact.
    if args.save_baseline is not None:
        _save_baseline(
            args.save_baseline,
            baseline_label,
            recipes,
            baseline_parses,
            baseline_t,
        )
        print(f"Wrote baseline artifact: {args.save_baseline}")

    # Run candidate (if any) and emit the comparison summary.
    if args.candidate is None:
        print("\nNo --candidate specified; baseline-only run complete.")
        return 0

    candidate_parses, candidate_t = _run_parse_pass(
        "Candidate", args.candidate, args.ollama_url, recipes
    )

    summary = _summarize(
        recipes, baseline_parses, candidate_parses,
        baseline_label, args.candidate,
        baseline_t, candidate_t,
    )
    _print_summary(summary)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(f"\nWrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
