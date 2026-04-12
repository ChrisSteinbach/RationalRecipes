"""Validate WDC loader + grouping + cross-corpus comparison on pannkakor.

Usage:
    python3 scripts/wdc_pannkakor.py --ollama-url http://host:11434
    python3 scripts/wdc_pannkakor.py --skip-extraction  # no LLM needed
"""

from __future__ import annotations

import argparse
from collections import Counter
from pathlib import Path

from rational_recipes.scrape.comparison import field_complementarity, url_overlap
from rational_recipes.scrape.grouping import group_by_ingredients, group_by_title
from rational_recipes.scrape.recipenlg import RecipeNLGLoader
from rational_recipes.scrape.wdc import WDCLoader, extract_batch


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--ollama-url",
        default="http://localhost:11434",
        help="Ollama server URL",
    )
    parser.add_argument(
        "--model",
        default="gemma4:e2b",
        help="LLM model name",
    )
    parser.add_argument(
        "--wdc-path",
        default="dataset/wdc/Recipe_top100.zip",
        help="Path to WDC recipe corpus zip",
    )
    parser.add_argument(
        "--recipenlg-path",
        default="dataset/full_dataset.csv",
        help="Path to RecipeNLG dataset CSV",
    )
    parser.add_argument(
        "--skip-extraction",
        action="store_true",
        help="Skip LLM extraction (useful for testing without Ollama)",
    )
    args = parser.parse_args()

    # --- Load WDC pannkakor ---
    loader = WDCLoader(zip_path=Path(args.wdc_path))
    wdc_recipes = list(
        loader.search_title("pannkak", hosts=["ica.se", "tasteline.com"])
    )
    print(f"WDC: {len(wdc_recipes)} pannkakor recipes")

    # --- LLM extraction ---
    if not args.skip_extraction:
        cache: dict[str, frozenset[str]] = {}
        wdc_recipes = extract_batch(
            wdc_recipes, model=args.model, base_url=args.ollama_url, cache=cache
        )
        extracted = sum(1 for r in wdc_recipes if r.ingredient_names)
        print(
            f"Extraction: {extracted}/{len(wdc_recipes)} recipes got ingredient names"
        )

    # --- L1 grouping ---
    wdc_l1 = group_by_title(wdc_recipes, min_group_size=2)
    print(f"\nWDC Level 1 groups ({len(wdc_l1)}):")
    for title, recipes in sorted(wdc_l1.items(), key=lambda x: -len(x[1])):
        print(f"  {title}: {len(recipes)} recipes")

    # --- L2 grouping (only with extraction) ---
    if not args.skip_extraction:
        for title, recipes in wdc_l1.items():
            l2_groups = group_by_ingredients(
                recipes, similarity_threshold=0.6, min_group_size=2
            )
            print(f"\n  L2 groups for '{title}' ({len(l2_groups)}):")
            for i, g in enumerate(l2_groups):
                print(
                    f"    Group {i + 1}: {g.size} recipes,"
                    f" ingredients: {sorted(g.canonical_ingredients)}"
                )

    # --- cookingMethod distribution ---
    print("\ncookingMethod distribution:")
    method_counts: Counter[str] = Counter()
    for r in wdc_recipes:
        if r.cooking_methods:
            for m in r.cooking_methods:
                method_counts[m] += 1
        else:
            method_counts["(none)"] += 1
    for method, count in method_counts.most_common():
        print(f"  {method}: {count}")

    # --- Load RecipeNLG pannkakor ---
    recipenlg_path = Path(args.recipenlg_path)
    if not recipenlg_path.exists():
        print(f"\nRecipeNLG dataset not found at {recipenlg_path}, skipping.")
        rnlg_recipes = []
        rnlg_pancake = []
    else:
        rnlg_loader = RecipeNLGLoader(path=recipenlg_path)
        rnlg_recipes = list(rnlg_loader.search_title("pannkak"))
        print(f"\nRecipeNLG: {len(rnlg_recipes)} pannkakor recipes")

        rnlg_pancake = list(rnlg_loader.search_title("pancake"))
        print(f"RecipeNLG: {len(rnlg_pancake)} 'pancake' recipes (broader)")

    # --- Cross-corpus comparison ---
    comparison_recipes = rnlg_recipes
    comparison_label = "pannkakor"
    if not rnlg_recipes and rnlg_pancake:
        print(
            "\nNo RecipeNLG 'pannkak' matches — using 'pancake' results instead."
            "\n(Compares Swedish WDC pannkakor against English RecipeNLG pancakes"
            " — cross-population difference.)"
        )
        comparison_recipes = rnlg_pancake
        comparison_label = "pancake (English) vs pannkakor (Swedish)"

    if comparison_recipes and wdc_recipes:
        fc = field_complementarity(comparison_recipes, wdc_recipes)
        print(f"\nField complementarity ({comparison_label}):")
        for field, fracs in fc.items():
            print(
                f"  {field}: RecipeNLG={fracs['recipenlg']:.1%}, WDC={fracs['wdc']:.1%}"
            )

        overlap = url_overlap(comparison_recipes, wdc_recipes)
        print(
            f"\nURL overlap: {len(overlap.url_matches)} exact,"
            f" {len(overlap.near_dup_matches)} near-dups"
        )
    elif not comparison_recipes:
        print("\nNo RecipeNLG results available — skipping cross-corpus comparison.")


if __name__ == "__main__":
    main()
