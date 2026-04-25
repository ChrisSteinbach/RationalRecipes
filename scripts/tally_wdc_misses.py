#!/usr/bin/env python3
"""Tally ingredient-name misses on WDC recipes for a set of hosts.

Sibling to `scripts/tally_recipenlg_misses.py`, but for the Web Data
Commons corpus. WDC ingredient lines need LLM extraction first (the raw
`recipeingredient` field is natural-language prose, not a pre-extracted
name list like RecipeNLG's NER column). This script:

1. Streams WDC recipes for the requested hosts.
2. Extracts raw ingredient names via Ollama (default: production
   extractor `gemma4:e2b` on the remote server).
3. Persists (recipe_url -> [raw_name, ...]) to a JSON cache so re-runs
   after DB additions don't re-pay the LLM cost.
4. Tallies hit/miss against `IngredientFactory.get_by_name`.
5. Reports miss rate plus the top-N unresolved raw names.

Cache path convention: `artifacts/wdc_extraction_cache_<label>.json`.
The `--label` arg is just the identifier used in the cache filename
and the report header — typically a language name or dish family.

Per-language bead flows (b7t.20 SV, bie DE, sdk FR, asq RU, 9oa IT,
lw8 JA): use `--label <lang> --hosts <host1,host2,...>`. Supply hosts
from the WDC language ranking at
`scripts/benchmark_data/wdc_language_ranking.txt`.

Usage:
    # Swedish pilot (small sample to bound LLM cost)
    python3 scripts/tally_wdc_misses.py --label swedish \\
        --hosts ica.se,tasteline.com --limit 100

    # Resume a stopped run (re-uses cache, adds more recipes)
    python3 scripts/tally_wdc_misses.py --label swedish \\
        --hosts ica.se,tasteline.com --limit 300

    # Skip extraction entirely — just retally the cache against the
    # current DB
    python3 scripts/tally_wdc_misses.py --label swedish --no-extract
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_WDC = REPO_ROOT / "dataset" / "wdc" / "Recipe_top100.zip"
DEFAULT_CACHE_DIR = REPO_ROOT / "artifacts"
DB_PATH = REPO_ROOT / "src" / "rational_recipes" / "data" / "ingredients.db"

sys.path.insert(0, str(REPO_ROOT / "src"))
from rational_recipes.scrape.parse import (  # noqa: E402
    parse_ingredient_line,
)
from rational_recipes.scrape.wdc import (  # noqa: E402
    NEUTRAL_PROMPT,
    WDCLoader,
    WDCRecipe,
)


def build_hit_checker(db_path: Path) -> tuple[callable, dict[str, bool]]:
    """Return (is_hit, cache). Mirrors tally_recipenlg_misses.py — bypasses
    IngredientFactory's O(N) miss-suggestion path so the tally scales."""
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    cursor = conn.cursor()
    cache: dict[str, bool] = {}

    def is_hit(raw_name: str) -> bool:
        key = raw_name.lower().strip()
        if not key:
            return False
        if key in cache:
            return cache[key]
        row = cursor.execute(
            "SELECT 1 FROM synonym WHERE name = ? COLLATE NOCASE LIMIT 1",
            (key,),
        ).fetchone()
        hit = row is not None
        cache[key] = hit
        return hit

    return is_hit, cache


def extract_names(
    recipe: WDCRecipe,
    *,
    model: str,
    base_url: str,
) -> list[str]:
    """Extract raw ingredient names (pre-canonicalization) from one recipe.

    Duplicates the inner loop of `wdc.extract_ingredient_names` but
    returns the list of raw LLM-produced names, so tally_wdc_misses can
    surface the language-specific miss vocabulary (e.g. 'vetemjöl' is a
    miss until we alias it).
    """
    out: list[str] = []
    for line in recipe.ingredients:
        parsed = parse_ingredient_line(
            line,
            model=model,
            base_url=base_url,
            system_prompt=NEUTRAL_PROMPT,
        )
        if parsed and parsed.ingredient:
            out.append(parsed.ingredient)
    return out


def _cache_path(label: str, cache_dir: Path) -> Path:
    slug = "".join(c if c.isalnum() or c == "-" else "_" for c in label.lower())
    return cache_dir / f"wdc_extraction_cache_{slug}.json"


def load_cache(path: Path) -> dict[str, list[str]]:
    if not path.exists():
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        return {}
    return {
        str(k): [str(v) for v in vs] for k, vs in data.items() if isinstance(vs, list)
    }


def save_cache(path: Path, cache: dict[str, list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(cache, ensure_ascii=False, indent=0) + "\n",
        encoding="utf-8",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--label",
        required=True,
        help=(
            "Identifier used in the cache filename and report header"
            " (e.g. 'swedish', 'german')."
        ),
    )
    parser.add_argument(
        "--hosts",
        required=True,
        help="Comma-separated list of WDC hosts (e.g. 'ica.se,tasteline.com').",
    )
    parser.add_argument(
        "--wdc-path",
        type=Path,
        default=DEFAULT_WDC,
        help=f"WDC zip path (default: {DEFAULT_WDC.relative_to(REPO_ROOT)}).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Max recipes to extract across all hosts (0 = no limit).",
    )
    parser.add_argument(
        "--no-extract",
        action="store_true",
        help="Retally the cache without running the LLM; useful after DB rebuilds.",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=40,
        help="Show the top-N missing ingredient names (default: 40).",
    )
    parser.add_argument(
        "--model",
        default="gemma4:e2b",
        help="Ollama model (default: gemma4:e2b, production extractor).",
    )
    parser.add_argument(
        "--ollama-url",
        default="http://192.168.50.189:11434",
        help="Ollama server URL (default: remote production server).",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=DEFAULT_CACHE_DIR,
        help=f"Cache directory (default: {DEFAULT_CACHE_DIR.relative_to(REPO_ROOT)}).",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=DB_PATH,
        help=f"ingredients.db path (default: {DB_PATH.relative_to(REPO_ROOT)}).",
    )
    args = parser.parse_args()

    hosts = [h.strip() for h in args.hosts.split(",") if h.strip()]
    if not hosts:
        print("At least one host required via --hosts", file=sys.stderr)
        sys.exit(1)

    cache_path = _cache_path(args.label, args.cache_dir)
    cache = load_cache(cache_path)
    print(f"Cache: {cache_path} ({len(cache):,} recipes already cached)")

    if not args.no_extract:
        if not args.wdc_path.exists():
            print(f"WDC corpus not found at {args.wdc_path}", file=sys.stderr)
            sys.exit(1)
        loader = WDCLoader(zip_path=args.wdc_path)
        # Build the list of candidate recipes up-front so --limit is
        # across-host, not per-host.
        candidates: list[WDCRecipe] = []
        for host in hosts:
            for recipe in loader.iter_host(host):
                candidates.append(recipe)
        print(f"Hosts: {hosts} — {len(candidates):,} recipes in zip")

        # Only extract recipes not already cached; respect --limit on the
        # pool of uncached candidates.
        to_extract = [r for r in candidates if r.page_url not in cache]
        if args.limit and len(to_extract) > args.limit:
            to_extract = to_extract[: args.limit]
        print(f"Will extract {len(to_extract):,} new recipes (skipping cached).")

        extract_start = time.monotonic()
        for idx, recipe in enumerate(to_extract, start=1):
            names = extract_names(
                recipe,
                model=args.model,
                base_url=args.ollama_url,
            )
            cache[recipe.page_url or f"noid:{recipe.host}:{recipe.row_id}"] = names
            # Checkpoint every 10 recipes so Ctrl-C doesn't lose everything.
            if idx % 10 == 0 or idx == len(to_extract):
                save_cache(cache_path, cache)
                elapsed = time.monotonic() - extract_start
                rate = idx / elapsed if elapsed > 0 else 0
                print(
                    f"  ... extracted {idx}/{len(to_extract)}"
                    f" ({rate:.1f} recipes/s, {elapsed:.0f}s elapsed)",
                    file=sys.stderr,
                )

    # --- Tally pass ---
    is_hit, _ = build_hit_checker(args.db)
    mentions = 0
    hits = 0
    miss_counter: Counter[str] = Counter()
    cached_recipes_all_hit = 0
    cached_recipes_with_names = 0
    for names in cache.values():
        if not names:
            continue
        cached_recipes_with_names += 1
        all_hit = True
        for name in names:
            mentions += 1
            if is_hit(name):
                hits += 1
            else:
                miss_counter[name.lower().strip()] += 1
                all_hit = False
        if all_hit:
            cached_recipes_all_hit += 1

    print()
    print(f"=== WDC miss tally: label={args.label} ===")
    if mentions == 0:
        print("No extracted names in cache — run without --no-extract.")
        return
    miss_mentions = mentions - hits
    print(
        f"Ingredient mentions: {mentions:,} "
        f"(hit {hits:,} = {hits / mentions:.1%}, "
        f"miss {miss_mentions:,} = {miss_mentions / mentions:.1%})"
    )
    if cached_recipes_with_names:
        print(
            f"Recipes with every name resolving: "
            f"{cached_recipes_all_hit:,} / {cached_recipes_with_names:,} "
            f"= {cached_recipes_all_hit / cached_recipes_with_names:.1%}"
        )
    print(f"Distinct miss names: {len(miss_counter):,}")
    print()
    print(f"Top {args.top} missing ingredient names:")
    for name, count in miss_counter.most_common(args.top):
        share = count / miss_mentions if miss_mentions else 0
        print(f"  {count:>6,}  {share:>5.1%}  {name}")


if __name__ == "__main__":
    main()
