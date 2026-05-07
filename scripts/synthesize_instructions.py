#!/usr/bin/env python3
"""Synthesize canonical instructions for a variant (RationalRecipes-ia1x).

Implements the r8hx-resolved approach: "full LLM synthesis with human
review." Reads the variant's averaged ingredient profile + the N source
instruction sequences, assembles a deterministic synthesis prompt, and
calls Ollama (``_llm_synthesize``) with ``temperature=0, seed=42`` so
reruns on the same model + same prompt produce identical text.

The synthesis-side model choice itself is open in ``RationalRecipes-2n09``
— callers must pass ``--model`` explicitly so the eval driver can
re-target candidates without picking a winner here. ``--dry-run`` skips
the LLM call entirely and just prints the assembled prompt.

Usage:
    # Print the synthesis prompt without calling the LLM:
    python3 scripts/synthesize_instructions.py <variant_id> --dry-run

    # Synthesize with a specific candidate model:
    python3 scripts/synthesize_instructions.py <variant_id> --model gemma4:31b

    # Synthesize and persist to recipes.db.canonical_instructions:
    python3 scripts/synthesize_instructions.py <variant_id> --model gemma4:31b --save
"""

from __future__ import annotations

import argparse
import ast
import csv
import json
import sys
import urllib.error
import urllib.request
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from rational_recipes.catalog_db import (
    CatalogDB,
    IngredientStatsRow,
    VariantMemberRow,
    VariantRow,
)

# synth-deep endpoint (NP=1, KEEP_ALIVE=0) — provisioned for long-ctx
# sequential synthesis per ollama-tuning-report.md (2026-05-07).
# Distinct from rational_recipes.scrape.parse.OLLAMA_BASE_URL (parse-fast,
# NP=4) because synthesis wants a single in-flight call with up to 32 k
# context, not a batched pool. Override with `--base-url` for sweeps
# across endpoints.
SYNTHESIS_OLLAMA_BASE_URL = "http://192.168.50.189:11446"

# Match scrape/parse.py — same determinism requirement applies once
# the Ollama call is wired up. Kept here as named constants so the
# eventual call site can reuse them without reaching into another
# module.
SYNTHESIS_TEMPERATURE = 0.0
SYNTHESIS_SEED = 42

# Synthesis is multi-paragraph free-text, not a structured JSON
# extraction. Give the model enough headroom to produce a full
# instruction set and a generous timeout for the larger candidates.
DEFAULT_SYNTHESIS_NUM_PREDICT = 1024
DEFAULT_SYNTHESIS_TIMEOUT = 300.0

# Synthesis model winner per RationalRecipes-2n09 (resolved 2026-05-07):
# only viable candidate on this host. gemma4 family produces empty output
# for instruction-following on Ollama 0.21+ROCm; qwen3.5:27b doesn't exit
# thinking mode; qwen3.6:35b-a3b and nemotron-3-nano:30b overflow the
# 24 GiB ceiling. Override with --model on the CLI for ad-hoc sweeps.
DEFAULT_SYNTHESIS_MODEL = "mistral-small:24b"

DEFAULT_DB_PATH = Path("output/catalog/recipes.db")
DEFAULT_RECIPENLG_PATH = Path("dataset/full_dataset.csv")
# Cap the number of source instruction sequences we send to the LLM.
# Ten is enough for a coherent consensus per r8hx; more inflates the
# prompt without obvious quality lift.
DEFAULT_MAX_SOURCES = 10

SYSTEM_PROMPT = (
    "You synthesize one coherent set of cooking instructions for a "
    "recipe variant by reading multiple independent source recipes "
    "for the same dish and the variant's averaged ingredient profile. "
    "Produce a single canonical instruction set in plain numbered "
    "steps. Use the averaged ingredient quantities (mass percentages) "
    "as the authoritative quantities, not any single source's "
    "amounts. Where sources disagree on technique, prefer the most "
    "common approach across the cluster. For non-additive cookable "
    "parameters (oven temperature, bake time, mixing speed, doneness "
    "cues), prefer the modal consensus — the most-frequent value "
    "across the source recipes — rather than the numerical mean. If "
    "two values are equally frequent, list them as alternatives "
    "(e.g., \"Bake at 350°F or 375°F\"). Do NOT compute averages of "
    "these values; mathematical averaging produces operationally "
    "awkward results (e.g., \"362°F\" is no oven setting anyone "
    "uses). Do NOT invent ingredients absent from the averaged "
    "profile. Do NOT include the averaged mass percentages from the "
    "ingredient profile inside the generated instruction steps; the "
    "ingredient percentages appear separately in the rendered drop's "
    "ingredient table, so the instructions should read like a normal "
    "recipe (e.g., \"add the flour and salt\", not \"add the flour "
    "(32.4%) and salt (0.5%)\"). Output only the instruction steps — "
    "no preamble, no commentary."
)


@dataclass(frozen=True, slots=True)
class SourceInstructions:
    """One source recipe's instruction sequence keyed back to the variant_member."""

    recipe_id: str
    url: str | None
    title: str | None
    corpus: str
    steps: tuple[str, ...]


def _format_pct(value: float | None) -> str:
    if value is None:
        return "—"
    return f"{value * 100:.1f}%"


def _ingredient_block(stats: Iterable[IngredientStatsRow]) -> str:
    """Render the averaged ingredient profile as the prompt's evidence block."""
    lines = ["Averaged ingredient profile (mass fractions across the cluster):"]
    for s in stats:
        lines.append(
            f"- {s.canonical_name}: mean {_format_pct(s.mean_proportion)}"
            f" (n={s.min_sample_size})"
        )
    return "\n".join(lines)


def _instructions_block(sources: Iterable[SourceInstructions]) -> str:
    """Render the per-source instruction sequences as the prompt's evidence block."""
    parts: list[str] = []
    for i, src in enumerate(sources, start=1):
        header = f"Source {i}"
        if src.title:
            header += f" — {src.title}"
        if src.url:
            header += f" ({src.url})"
        parts.append(header)
        if not src.steps:
            parts.append("(no instructions recovered for this source)")
        else:
            for j, step in enumerate(src.steps, start=1):
                parts.append(f"  {j}. {step}")
        parts.append("")
    return "\n".join(parts).rstrip()


def build_synthesis_prompt(
    variant: VariantRow,
    stats: list[IngredientStatsRow],
    sources: list[SourceInstructions],
) -> str:
    """Assemble the LLM prompt from a variant + its sources.

    Pure function so callers can test the prompt shape without touching
    the DB or the corpora. The same string is what eventually goes to
    ``_ollama_generate``.
    """
    title = variant.display_title or variant.normalized_title
    parts = [
        f"Dish: {title}",
        f"Variant id: {variant.variant_id}",
        f"Cluster size: {variant.n_recipes} source recipes",
        "",
        _ingredient_block(stats),
        "",
        f"Source instruction sequences ({len(sources)}):",
        "",
        _instructions_block(sources),
        "",
        "Task: produce a single canonical instruction set for this variant.",
    ]
    return "\n".join(parts)


def _load_recipenlg_directions(
    recipenlg_path: Path,
    target_urls: set[str],
) -> dict[str, tuple[str, ...]]:
    """Stream RecipeNLG once and return ``{url: directions_tuple}`` for hits."""
    out: dict[str, tuple[str, ...]] = {}
    if not target_urls:
        return out
    with open(recipenlg_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            link = row.get("link", "")
            if link not in target_urls:
                continue
            try:
                parsed = ast.literal_eval(row.get("directions", "[]"))
            except (ValueError, SyntaxError):
                parsed = []
            if isinstance(parsed, list):
                out[link] = tuple(str(s) for s in parsed)
            if len(out) == len(target_urls):
                break
    return out


def collect_source_instructions(
    members: list[VariantMemberRow],
    *,
    recipenlg_path: Path | None,
    max_sources: int,
) -> list[SourceInstructions]:
    """Collect per-source instruction sequences for the variant's members.

    Order matches ``members`` (which is already best-outlier-score-first
    per ``CatalogDB.get_variant_members``). Caps at ``max_sources``;
    members beyond the cap are skipped so prompts stay bounded. Lookup
    strategy:

    1. ``recipes.db`` does not currently cache instruction text — if a
       future schema change adds a ``recipes.directions`` column, prefer
       that here.
    2. Fall back to the source corpus by URL. RecipeNLG ``directions``
       reads from the CSV; WDC ``recipeinstructions`` is not yet wired
       up (the loader doesn't carry it). Sources without a recovered
       sequence are still emitted with an empty ``steps`` tuple so the
       prompt records the gap rather than silently dropping the source.
    """
    capped = members[:max_sources]
    rnlg_urls = {
        m.url
        for m in capped
        if m.corpus == "recipenlg" and m.url
    }
    rnlg_lookup: dict[str, tuple[str, ...]] = {}
    if rnlg_urls and recipenlg_path is not None and recipenlg_path.exists():
        rnlg_lookup = _load_recipenlg_directions(recipenlg_path, rnlg_urls)

    out: list[SourceInstructions] = []
    for m in capped:
        steps: tuple[str, ...] = ()
        if m.corpus == "recipenlg" and m.url:
            steps = rnlg_lookup.get(m.url, ())
        # WDC instruction recovery is not yet wired up — leaving steps
        # empty signals "source present, no instructions recovered" to
        # the prompt rather than dropping the source from the list.
        out.append(
            SourceInstructions(
                recipe_id=m.recipe_id,
                url=m.url,
                title=m.title,
                corpus=m.corpus,
                steps=steps,
            )
        )
    return out


class SynthesisError(RuntimeError):
    """Raised when the Ollama synthesis call fails or returns empty output."""


def _llm_synthesize(
    prompt: str,
    *,
    model: str,
    base_url: str = SYNTHESIS_OLLAMA_BASE_URL,
    timeout: float = DEFAULT_SYNTHESIS_TIMEOUT,
    num_predict: int = DEFAULT_SYNTHESIS_NUM_PREDICT,
    num_ctx: int | None = None,
) -> str:
    """Call Ollama /api/generate for free-text synthesis.

    Mirrors ``rational_recipes.scrape.parse._ollama_generate`` but drops
    the ``format=json`` constraint (synthesis output is plain numbered
    steps, not a JSON object) and bumps ``num_predict`` for multi-step
    instruction text. Pins ``temperature=SYNTHESIS_TEMPERATURE`` and
    ``seed=SYNTHESIS_SEED`` so reruns on the same model + same prompt
    return identical text — same determinism guarantee as parsing.
    """
    options: dict[str, object] = {
        "num_predict": num_predict,
        "temperature": SYNTHESIS_TEMPERATURE,
        "seed": SYNTHESIS_SEED,
    }
    if num_ctx is not None:
        options["num_ctx"] = num_ctx
    payload = json.dumps(
        {
            "model": model,
            "system": SYSTEM_PROMPT,
            "prompt": prompt,
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
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = json.loads(resp.read())
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
        raise SynthesisError(
            f"Ollama synthesis call failed for model {model!r}: {e}"
        ) from e

    # Some thinking models (e.g. qwen3.5) emit reasoning into a
    # separate "thinking" field and leave "response" empty; prefer
    # response when non-empty, otherwise fall back to thinking so the
    # caller still gets the model's actual answer.
    visible = (body.get("response") or "").strip()
    if visible:
        return visible
    thinking = (body.get("thinking") or "").strip()
    if thinking:
        return thinking
    raise SynthesisError(
        f"Ollama returned empty response for model {model!r}"
    )


def synthesize(
    variant_id: str,
    *,
    db_path: Path,
    recipenlg_path: Path | None,
    max_sources: int,
    dry_run: bool,
    save: bool,
    model: str | None = None,
    base_url: str = SYNTHESIS_OLLAMA_BASE_URL,
    num_ctx: int | None = None,
) -> str:
    """Top-level orchestration: build the prompt and (when wired) call the LLM.

    Returns the assembled prompt under ``--dry-run`` and the LLM's
    output otherwise. When ``save`` is true and ``dry_run`` is false,
    persists the result to ``variants.canonical_instructions``. Save +
    dry-run is a no-op save to keep the dry-run guarantee that no
    state changes.
    """
    db = CatalogDB.open(db_path)
    try:
        variant = db.get_variant(variant_id)
        if variant is None:
            raise SystemExit(
                f"variant_id {variant_id!r} not found in {db_path}"
            )
        stats = db.get_ingredient_stats(variant_id)
        members = db.get_variant_members(variant_id)
        sources = collect_source_instructions(
            members,
            recipenlg_path=recipenlg_path,
            max_sources=max_sources,
        )
        prompt = build_synthesis_prompt(variant, stats, sources)
        if dry_run:
            return prompt
        if model is None:
            raise SystemExit(
                "synthesize() requires model=... when dry_run is False; "
                "pass --model on the CLI."
            )
        result = _llm_synthesize(
            prompt, model=model, base_url=base_url, num_ctx=num_ctx
        )
        if save:
            db.set_canonical_instructions(variant_id, result)
        return result
    finally:
        db.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("variant_id", help="Variant id (e.g. b34c2dce79e2)")
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB_PATH,
        help=f"Path to recipes.db (default: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--recipenlg",
        type=Path,
        default=DEFAULT_RECIPENLG_PATH,
        help=(
            "Path to RecipeNLG full_dataset.csv "
            "(default: dataset/full_dataset.csv). Used to recover "
            "source instruction sequences not cached in recipes.db."
        ),
    )
    parser.add_argument(
        "--max-sources",
        type=int,
        default=DEFAULT_MAX_SOURCES,
        help=(
            "Cap the number of source instruction sequences sent to "
            f"the LLM (default: {DEFAULT_MAX_SOURCES})."
        ),
    )
    parser.add_argument(
        "--model",
        type=str,
        default=DEFAULT_SYNTHESIS_MODEL,
        help=(
            f"Ollama model name to use for synthesis (default: "
            f"{DEFAULT_SYNTHESIS_MODEL!r} — the 2n09-resolved winner; "
            f"only viable candidate on host trellis at 24 GiB VRAM). "
            f"Override for ad-hoc sweeps."
        ),
    )
    parser.add_argument(
        "--ollama-url",
        "--base-url",
        type=str,
        default=SYNTHESIS_OLLAMA_BASE_URL,
        dest="ollama_url",
        help=(
            f"Ollama base URL (default: {SYNTHESIS_OLLAMA_BASE_URL} — "
            "synth-deep endpoint, NP=1, KEEP_ALIVE=0)"
        ),
    )
    parser.add_argument(
        "--num-ctx",
        type=int,
        default=None,
        help=(
            "Override Ollama num_ctx for the synthesis call. The "
            "synth-deep endpoint (NP=1) is provisioned for 32 k context "
            "with the recommended candidates — pass --num-ctx 32768 to "
            "match the tuning report's measurements. Omit to use the "
            "model's default."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Assemble the prompt and print it to stdout without calling "
            "Ollama. Required while RationalRecipes-2n09 is unresolved."
        ),
    )
    parser.add_argument(
        "--save",
        action="store_true",
        help=(
            "Persist the synthesized result to "
            "variants.canonical_instructions. Has no effect under "
            "--dry-run (which never calls the LLM)."
        ),
    )
    args = parser.parse_args(argv)

    if not args.db.exists():
        print(f"recipes.db not found at {args.db}", file=sys.stderr)
        return 1

    if not args.dry_run and args.model is None:
        print(
            "--model is required unless --dry-run is set",
            file=sys.stderr,
        )
        return 1

    output = synthesize(
        args.variant_id,
        db_path=args.db,
        recipenlg_path=args.recipenlg,
        max_sources=args.max_sources,
        dry_run=args.dry_run,
        save=args.save,
        model=args.model,
        base_url=args.ollama_url,
        num_ctx=args.num_ctx,
    )
    sys.stdout.write(output)
    if not output.endswith("\n"):
        sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
