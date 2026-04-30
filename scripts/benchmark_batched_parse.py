#!/usr/bin/env python3
"""Speed + accuracy comparison of candidate models under batched parsing.

Groups the Swedish ica.se hand-labeled gold by ``row_id`` into recipe-
sized batches and runs each candidate model through ``parse_ingredient_lines``
(which sends one batched LLM call per recipe). Reports wallclock per
recipe, total wallclock, and quantity/unit/ingredient field-match rate
against the gold.

Skips the full multi-corpus accuracy scoring of ``benchmark_models.py``
intentionally — that suite was per-line. This script answers one
question: under the new batched parse path (vwt.13), which model is
fastest while still producing usable output?

Usage:
    python3 scripts/benchmark_batched_parse.py \\
        --ollama-url http://192.168.50.189:11434 \\
        --models qwen3.6:35b-a3b gemma4:e2b qwen2.5:3b
"""

from __future__ import annotations

import argparse
import json
import time
import urllib.error
import urllib.request
from collections import defaultdict
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from rational_recipes.scrape.parse import (
    ParsedIngredient,
    parse_ingredient_lines,
)

SWEDISH_GOLD = Path("scripts/benchmark_data/swedish_ica_se_structured_gold.jsonl")


def _load_gold(path: Path) -> dict[int, list[dict[str, Any]]]:
    """Group gold lines by row_id (one batch per recipe)."""
    by_recipe: dict[int, list[dict[str, Any]]] = defaultdict(list)
    with path.open() as f:
        for raw in f:
            raw = raw.strip()
            if not raw:
                continue
            d = json.loads(raw)
            rid = d.get("row_id")
            if rid is None:
                continue
            by_recipe[rid].append(d)
    return dict(by_recipe)


def _norm_unit(u: str | None) -> str:
    if not u:
        return ""
    return str(u).strip().lower().rstrip(".")


def _ingredient_match(parsed_ing: str, expected_ing: str) -> bool:
    """Loose match: parsed contains the expected gold token (or vice versa)."""
    p = parsed_ing.lower().strip()
    e = expected_ing.lower().strip()
    return p == e or e in p or p in e


def _score_line(
    parsed: ParsedIngredient | None, expected: dict[str, Any]
) -> dict[str, bool]:
    if parsed is None:
        return {"quantity": False, "unit": False, "ingredient": False, "any": False}
    qty_ok = abs(float(parsed.quantity) - float(expected.get("quantity", 0))) < 1e-3
    unit_ok = _norm_unit(parsed.unit) == _norm_unit(expected.get("unit"))
    ing_ok = _ingredient_match(parsed.ingredient, expected.get("ingredient", ""))
    return {
        "quantity": qty_ok,
        "unit": unit_ok,
        "ingredient": ing_ok,
        "any": qty_ok or unit_ok or ing_ok,
    }


def _safe_div(num: float, denom: float) -> float:
    return num / denom if denom else 0.0


def _model_available(ollama_url: str, model: str) -> bool:
    """Check the remote host has this model (so we fail fast, not 5 min in)."""
    try:
        with urllib.request.urlopen(
            ollama_url.rstrip("/") + "/api/tags", timeout=5
        ) as resp:
            tags = json.loads(resp.read())
    except (urllib.error.URLError, TimeoutError, OSError):
        return False
    names = {m["name"] for m in tags.get("models", [])}
    return model in names


def benchmark_one(
    model: str, ollama_url: str, recipes: dict[int, list[dict[str, Any]]]
) -> dict[str, Any]:
    """Run one model over all recipes; return aggregate metrics."""
    print(f"\n=== {model} ===", flush=True)
    if not _model_available(ollama_url, model):
        print(f"  SKIP — {model} not present on {ollama_url}")
        return {"model": model, "skipped": True}

    total_lines = 0
    total_recipes = 0
    total_wallclock = 0.0
    qty_correct = 0
    unit_correct = 0
    ing_correct = 0
    parse_failures = 0
    per_recipe: list[tuple[int, int, float]] = []  # (row_id, n_lines, wallclock_s)

    for rid in sorted(recipes):
        items = recipes[rid]
        lines = [item["line"] for item in items]
        t0 = time.monotonic()
        parsed_list = parse_ingredient_lines(
            lines, model=model, base_url=ollama_url
        )
        elapsed = time.monotonic() - t0
        total_wallclock += elapsed
        total_recipes += 1
        per_recipe.append((rid, len(lines), elapsed))

        for item, parsed in zip(items, parsed_list, strict=True):
            total_lines += 1
            if parsed is None:
                parse_failures += 1
                continue
            score = _score_line(parsed, item["expected"])
            if score["quantity"]:
                qty_correct += 1
            if score["unit"]:
                unit_correct += 1
            if score["ingredient"]:
                ing_correct += 1

        print(
            f"  recipe {rid:>3} ({len(lines):>2} lines): "
            f"{elapsed:>6.1f}s ({elapsed/len(lines):>5.2f}s/line)",
            flush=True,
        )

    return {
        "model": model,
        "skipped": False,
        "total_recipes": total_recipes,
        "total_lines": total_lines,
        "total_wallclock_s": round(total_wallclock, 2),
        "lines_per_second": round(_safe_div(total_lines, total_wallclock), 2),
        "qty_accuracy": round(_safe_div(qty_correct, total_lines), 3),
        "unit_accuracy": round(_safe_div(unit_correct, total_lines), 3),
        "ingredient_accuracy": round(_safe_div(ing_correct, total_lines), 3),
        "parse_failure_rate": round(_safe_div(parse_failures, total_lines), 3),
    }


def _print_summary(results: list[dict[str, Any]]) -> None:
    rows = [r for r in results if not r.get("skipped")]
    if not rows:
        print("\nNo successful runs.")
        return
    rows.sort(key=lambda r: r["total_wallclock_s"])
    header = (
        f"\n{'model':<28}  {'lines':>5}  {'wall':>7}  "
        f"{'l/s':>5}  {'qty':>5}  {'unit':>5}  {'ing':>5}  {'fail':>5}"
    )
    print(header)
    print("-" * 80)
    for r in rows:
        print(
            f"{r['model']:<28}  {r['total_lines']:>5}  "
            f"{r['total_wallclock_s']:>6.1f}s  "
            f"{r['lines_per_second']:>5.2f}  {r['qty_accuracy']:>5.2f}  "
            f"{r['unit_accuracy']:>5.2f}  {r['ingredient_accuracy']:>5.2f}  "
            f"{r['parse_failure_rate']:>5.2f}"
        )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--ollama-url", default="http://192.168.50.189:11434",
        help="Remote Ollama host (default: %(default)s)",
    )
    parser.add_argument(
        "--models", nargs="+", required=True, help="One or more model tags to test"
    )
    parser.add_argument(
        "--gold", type=Path, default=SWEDISH_GOLD,
        help="Gold JSONL path (must contain row_id per line; default: %(default)s)",
    )
    parser.add_argument(
        "--recipe-limit", type=int, default=None,
        help="Cap recipes per run (smoke testing).",
    )
    parser.add_argument(
        "--out", type=Path, default=None,
        help="Optional JSON output path for the per-model aggregates.",
    )
    args = parser.parse_args(argv)

    if not args.gold.exists():
        print(f"Gold not found: {args.gold}")
        return 1

    recipes = _load_gold(args.gold)
    if args.recipe_limit:
        keep = sorted(recipes)[: args.recipe_limit]
        recipes = {k: recipes[k] for k in keep}
    if not recipes:
        print(f"No recipes loaded from {args.gold}")
        return 1
    n_lines = sum(len(v) for v in recipes.values())
    print(f"Loaded {len(recipes)} recipes / {n_lines} lines")

    results: list[dict[str, Any]] = []
    for model in args.models:
        results.append(benchmark_one(model, args.ollama_url, recipes))

    _print_summary(results)

    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")
        print(f"\nWrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
