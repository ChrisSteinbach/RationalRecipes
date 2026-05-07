#!/usr/bin/env python3
"""Reproducible parsing-side LLM eval driver (RationalRecipes-2n09).

Runs ``parse_ingredient_line`` from ``rational_recipes.scrape.parse``
across a fixed sample of CCC variant ingredient lines for several
candidate models and writes a markdown comparison table to disk.

The sample is curated to surface the three quality axes the bead calls
out:

- **Rare-ingredient handling**: brand names ("Crisco", "Tollhouse",
  "Nestle"), specific cocoa percentages, package descriptors.
- **Unit normalization**: "1 stick" -> butter mass, "1 pkg.", weird
  fractional cups, parenthetical alternatives like "1 c. (2 sticks)".
- **Swedish->English forcing**: a synthesized line ("2 dl mjöl") tests
  the closed-bead vwt.25 guarantee that source-language ingredients
  surface as English canonical names.

The eval is single-line (no batching) to isolate per-model parse
quality from the bisect/batch-tracking dynamics that dominate the full
Pass-1 throughput story. ``temperature=0`` and ``seed=42`` are pinned
inside ``_ollama_generate`` so reruns on the same model produce
identical results.

Usage:
    python3 scripts/eval_models.py \
        --models gemma4:e2b,qwen3.6:35b-a3b,gemma4:31b,mistral-small:24b \
        --output artifacts/2n09_parsing_eval.md
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from rational_recipes.scrape.parse import (
    OLLAMA_BASE_URL,
    ParsedIngredient,
    parse_ingredient_line,
)

# Curated sample of CCC ingredient lines. Hand-picked from
# ``output/catalog/recipes.db`` variant b34c2dce79e2 (chocolate chip
# cookies), 96/98 members recovered from RecipeNLG. Categories tag the
# evaluation axis each line targets so the summary can score
# per-category. The Swedish lines are synthetic (no Swedish source in
# the CCC cluster) — they probe the vwt.25 forcing-English-canonical
# guarantee.
SAMPLE_LINES: tuple[tuple[str, str], ...] = (
    # --- Rare ingredient / brand-name handling ---
    ("brand", "1 c. Crisco"),
    ("brand", "1 c. Crisco shortening"),
    ("brand", "1/2 cup crisco butter flavor"),
    ("brand", "1 (12 oz.) pkg. Nestle's semi-sweet chips"),
    ("brand", "1 bag Nestle chips"),
    ("brand", "1 (10 oz.) pkg. Baker's chocolate chips"),
    ("brand", "1 1/2 c. Gold Medal flour"),
    ("brand", "1 pkg. Jell-O instant pudding (vanilla)"),
    ("brand", "1 pkg. semi-sweet chocolate chips or M&M's, chocolate chunks, etc."),
    ("brand", "1 cup butter flavored Crisco shortening"),
    ("brand", "70% cacao chocolate chips, 1 cup"),
    ("brand", "1 (12 oz.) pkg. semi-sweet chocolate morsels (2 c.)"),
    ("brand", "1 cup Tollhouse semi-sweet morsels"),
    # --- Unit normalization ---
    ("unit", "1 stick margarine"),
    ("unit", "1 1/2 sticks margarine, melted"),
    ("unit", "1 c. (2 sticks) margarine"),
    ("unit", "1 cup (2 sticks) unsalted butter, at room temperature"),
    ("unit", "1 stick unsalted butter"),
    ("unit", "1 lb. butter or margarine"),
    ("unit", "1 lb. dark brown sugar (16 oz. pkg.)"),
    ("unit", "1 (12 oz.) bag chocolate chips"),
    ("unit", "1 pkg. (12 oz.) semi-sweet chocolate"),
    ("unit", "1 (4 serving size) pkg. instant vanilla pudding mix"),
    ("unit", "1 small pkg. vanilla instant pudding"),
    ("unit", "1 pkg. mini chips"),
    ("unit", "1 pkg. chocolate chips"),
    ("unit", "1 12 cups miniature semisweet chocolate chips"),
    ("unit", "1 12 teaspoons baking soda"),
    ("unit", "1 1/4 cups semisweet chocolate chips (about 8 ounces)"),
    ("unit", "1 c. or 6 oz. chocolate chips"),
    # --- Common baseline (sanity) ---
    ("common", "1 cup all-purpose flour"),
    ("common", "1 1/2 c. flour"),
    ("common", "2 large eggs"),
    ("common", "3 eggs"),
    ("common", "1/2 tsp. salt"),
    ("common", "1 tsp. baking soda"),
    ("common", "1 1/2 teaspoons pure vanilla extract"),
    ("common", "1 c. brown sugar, packed"),
    ("common", "1/2 c. granulated sugar"),
    ("common", "2 Tbsp. butter or margarine"),
    # --- Compound / alternatives / preparation noise ---
    ("compound", "1 c. (2 sticks) margarine or butter, softened"),
    ("compound", "1 cup margarine (softened) or 1 cup butter (softened)"),
    ("compound", "1 stick margarine (or 1/2 c. shortening like Crisco)"),
    ("compound", "1 c. or 2 sticks oleo or butter"),
    ("compound", "1 large egg, at room temperature"),
    ("compound", "(optional: another 1/4-1/2"),
    # --- Swedish -> English forcing (synthetic, per vwt.25) ---
    ("swedish", "2 dl mjöl"),
    ("swedish", "1 tsk salt"),
    ("swedish", "200 g smör, rumstempererat"),
    ("swedish", "2 ägg"),
)


@dataclass(frozen=True, slots=True)
class ParseAttempt:
    """One model's attempt at parsing one line."""

    category: str
    line: str
    model: str
    parsed: ParsedIngredient | None
    elapsed_s: float
    error: str | None = None

    def to_dict(self) -> dict[str, object]:
        out: dict[str, object] = {
            "category": self.category,
            "line": self.line,
            "model": self.model,
            "elapsed_s": round(self.elapsed_s, 2),
        }
        if self.parsed is not None:
            out["parsed"] = dataclasses.asdict(self.parsed)
        if self.error is not None:
            out["error"] = self.error
        return out


def check_ollama_reachable(base_url: str, timeout: float = 5.0) -> bool:
    """Ping ``/api/tags`` to confirm the daemon answers."""
    try:
        with urllib.request.urlopen(f"{base_url}/api/tags", timeout=timeout) as resp:
            json.loads(resp.read())
        return True
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
        return False


def list_available_models(base_url: str, timeout: float = 5.0) -> set[str]:
    """Return the set of model names Ollama reports as loaded."""
    try:
        with urllib.request.urlopen(
            f"{base_url}/api/tags", timeout=timeout
        ) as resp:
            body = json.loads(resp.read())
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
        return set()
    return {m.get("name", "") for m in body.get("models", [])}


def run_parse_eval(
    models: Iterable[str],
    sample: Iterable[tuple[str, str]],
    *,
    base_url: str,
    timeout: float,
    skip_unavailable: bool = True,
) -> tuple[list[ParseAttempt], list[str]]:
    """Run each model on each sample line.

    Returns ``(attempts, skipped_models)``. Models absent from
    ``/api/tags`` are skipped (not silently — they're returned in
    ``skipped_models`` so the report can flag the gap).
    """
    available = list_available_models(base_url) if skip_unavailable else set()
    attempts: list[ParseAttempt] = []
    skipped: list[str] = []
    samples = list(sample)
    for model in models:
        if skip_unavailable and available and model not in available:
            skipped.append(model)
            continue
        for category, line in samples:
            t0 = time.monotonic()
            err: str | None = None
            parsed: ParsedIngredient | None = None
            try:
                parsed = parse_ingredient_line(
                    line,
                    model=model,
                    base_url=base_url,
                    timeout=timeout,
                )
            except Exception as e:  # noqa: BLE001 - surface any model crash
                err = f"{type(e).__name__}: {e}"
            elapsed = time.monotonic() - t0
            attempts.append(
                ParseAttempt(
                    category=category,
                    line=line,
                    model=model,
                    parsed=parsed,
                    elapsed_s=elapsed,
                    error=err,
                )
            )
    return attempts, skipped


def _fmt_parsed(p: ParsedIngredient | None) -> str:
    if p is None:
        return "_(parse failed)_"
    parts = [
        f"q={p.quantity}",
        f"u={p.unit!r}",
        f"i={p.ingredient!r}",
    ]
    if p.preparation:
        parts.append(f"prep={p.preparation!r}")
    return ", ".join(parts)


def _summarize_per_model(attempts: list[ParseAttempt]) -> dict[str, dict[str, object]]:
    """Aggregate parse-failure rates and total wallclock per model."""
    by_model: dict[str, list[ParseAttempt]] = {}
    for a in attempts:
        by_model.setdefault(a.model, []).append(a)
    out: dict[str, dict[str, object]] = {}
    for model, rows in by_model.items():
        n = len(rows)
        failed = sum(1 for r in rows if r.parsed is None)
        total_s = sum(r.elapsed_s for r in rows)
        cat_failed: dict[str, int] = {}
        cat_total: dict[str, int] = {}
        for r in rows:
            cat_total[r.category] = cat_total.get(r.category, 0) + 1
            if r.parsed is None:
                cat_failed[r.category] = cat_failed.get(r.category, 0) + 1
        out[model] = {
            "n": n,
            "failed": failed,
            "total_elapsed_s": round(total_s, 1),
            "mean_s_per_line": round(total_s / n, 2) if n else 0.0,
            "failed_by_category": cat_failed,
            "total_by_category": cat_total,
        }
    return out


def render_markdown(
    attempts: list[ParseAttempt],
    skipped: list[str],
    *,
    base_url: str,
) -> str:
    """Render the eval report as a markdown document."""
    lines: list[str] = []
    lines.append("# RationalRecipes-2n09 — Parsing-side LLM eval")
    lines.append("")
    lines.append(
        "Per-model comparison on ~50 hand-picked ingredient lines from the "
        "CCC variant `b34c2dce79e2` (recipes.db). Pinned `temperature=0`, "
        "`seed=42`. Single-line calls (no batching) so per-model parse "
        "quality is isolated from bisect/batching dynamics."
    )
    lines.append("")
    lines.append(f"- Ollama: `{base_url}`")
    lines.append(f"- Sample size: {len({a.line for a in attempts})} lines")
    lines.append(f"- Models compared: {sorted({a.model for a in attempts})}")
    if skipped:
        lines.append(f"- Models skipped (not loaded on server): {skipped}")
    lines.append("")

    summary = _summarize_per_model(attempts)
    lines.append("## Per-model summary")
    lines.append("")
    lines.append(
        "| Model | n | parse_failed | failed_brand | failed_unit | "
        "failed_swedish | total_s | s/line |"
    )
    lines.append(
        "|---|---:|---:|---:|---:|---:|---:|---:|"
    )
    for model in sorted(summary):
        s = summary[model]
        fc = s["failed_by_category"]
        if not isinstance(fc, dict):
            fc = {}
        lines.append(
            f"| `{model}` | {s['n']} | {s['failed']} | "
            f"{fc.get('brand', 0)} | {fc.get('unit', 0)} | "
            f"{fc.get('swedish', 0)} | {s['total_elapsed_s']} | "
            f"{s['mean_s_per_line']} |"
        )
    lines.append("")
    lines.append(
        "_`parse_failed` counts lines where the model returned no usable "
        "JSON; the `failed_*` columns break that down by eval axis. Lower "
        "is better._"
    )
    lines.append("")

    # Per-line table grouped by category, with one row per (model x line).
    by_cat: dict[str, list[ParseAttempt]] = {}
    for a in attempts:
        by_cat.setdefault(a.category, []).append(a)

    cat_order = ["brand", "unit", "common", "compound", "swedish"]
    cat_titles = {
        "brand": "Rare ingredients / brand names",
        "unit": "Unit normalization",
        "common": "Common baseline (sanity)",
        "compound": "Compound / alternatives / preparation",
        "swedish": "Swedish -> English forcing (vwt.25)",
    }

    for cat in cat_order:
        rows = by_cat.get(cat, [])
        if not rows:
            continue
        lines.append(f"## {cat_titles.get(cat, cat)}")
        lines.append("")
        # Group by line so we get one block per line with each model's parse.
        by_line: dict[str, list[ParseAttempt]] = {}
        for r in rows:
            by_line.setdefault(r.line, []).append(r)
        # Preserve sample order
        sample_order = [line for c, line in SAMPLE_LINES if c == cat]
        for line in sample_order:
            if line not in by_line:
                continue
            lines.append(f"### `{line}`")
            lines.append("")
            lines.append("| Model | parse | s |")
            lines.append("|---|---|---:|")
            for r in sorted(by_line[line], key=lambda r: r.model):
                lines.append(
                    f"| `{r.model}` | {_fmt_parsed(r.parsed)} | "
                    f"{r.elapsed_s:.1f} |"
                )
            lines.append("")

    lines.append("## Recommendation")
    lines.append("")
    lines.append(
        "_Reasoning is filled in by the eval driver based on the per-model "
        "summary above. The recommendation is documented in the bead notes._"
    )
    lines.append("")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--models",
        type=str,
        default="gemma4:e2b,qwen3.6:35b-a3b,gemma4:31b,mistral-small:24b",
        help="Comma-separated list of Ollama model names to compare.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("artifacts/2n09_parsing_eval.md"),
        help="Output markdown file (default: artifacts/2n09_parsing_eval.md).",
    )
    parser.add_argument(
        "--json-output",
        type=Path,
        default=None,
        help="If set, also dump raw attempt records as JSON for debugging.",
    )
    parser.add_argument(
        "--ollama-url",
        type=str,
        default=OLLAMA_BASE_URL,
        help=f"Ollama base URL (default: {OLLAMA_BASE_URL}).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=180.0,
        help="Per-call Ollama timeout in seconds (default: 180s).",
    )
    args = parser.parse_args(argv)

    if not check_ollama_reachable(args.ollama_url):
        print(
            f"Ollama unreachable at {args.ollama_url} — aborting eval.",
            file=sys.stderr,
        )
        return 2

    models = [m.strip() for m in args.models.split(",") if m.strip()]
    attempts, skipped = run_parse_eval(
        models,
        SAMPLE_LINES,
        base_url=args.ollama_url,
        timeout=args.timeout,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        render_markdown(attempts, skipped, base_url=args.ollama_url),
        encoding="utf-8",
    )
    print(f"wrote {args.output}", file=sys.stderr)
    if args.json_output:
        args.json_output.write_text(
            json.dumps(
                {
                    "skipped_models": skipped,
                    "attempts": [a.to_dict() for a in attempts],
                },
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        print(f"wrote {args.json_output}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
