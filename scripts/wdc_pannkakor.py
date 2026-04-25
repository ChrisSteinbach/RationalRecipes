"""Validate WDC loader + grouping + cross-corpus comparison on pannkakor.

Usage:
    python3 scripts/wdc_pannkakor.py --ollama-url http://host:11434
    python3 scripts/wdc_pannkakor.py --skip-extraction  # no LLM needed
"""

from __future__ import annotations

import argparse
from collections import Counter
from pathlib import Path

from rational_recipes.scrape.comparison import (
    field_complementarity,
    url_overlap,
    within_variant_comparison,
)
from rational_recipes.scrape.grouping import (
    IngredientGroup,
    group_by_ingredients,
    group_by_title,
    normalize_title,
)
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

        # Default url_overlap threshold (0.8) is strict; we also report
        # matches at a lower threshold so the cross-language
        # canonicalization effect is visible even when partial ingredient
        # coverage keeps Jaccard below 0.8.
        for threshold in (0.8, 0.5):
            overlap = url_overlap(
                comparison_recipes, wdc_recipes, similarity_threshold=threshold
            )
            print(
                f"\nURL overlap (threshold={threshold}): "
                f"{len(overlap.url_matches)} exact,"
                f" {len(overlap.near_dup_matches)} near-dups"
            )
            if overlap.near_dup_matches:
                print("  Near-dup pairs:")
                for rr, wr, sim in overlap.near_dup_matches:
                    print(
                        f"    {sim:.2f}  RecipeNLG {rr.row_index!r} "
                        f"({rr.title!r}) ↔ WDC {wr.host}/{wr.row_id}"
                        f" ({wr.title!r})"
                    )

        # --- Within-variant ingredient-coverage comparison ---
        # For each normalized title shared between the two corpora, compare
        # ingredient coverage. This exercises the canonicalization path:
        # pre-normalization these would show 0 shared ingredients.
        print("\nWithin-variant ingredient coverage:")
        rnlg_by_title: dict[str, list] = {}
        for r in comparison_recipes:
            key = normalize_title(r.title)
            if key:
                rnlg_by_title.setdefault(key, []).append(r)
        wdc_by_title: dict[str, list] = {}
        for w in wdc_recipes:
            key = normalize_title(w.title)
            if key:
                wdc_by_title.setdefault(key, []).append(w)
        shared_titles = sorted(
            set(rnlg_by_title) & set(wdc_by_title),
            key=lambda t: -min(len(rnlg_by_title[t]), len(wdc_by_title[t])),
        )
        if not shared_titles:
            print("  (no shared normalized titles)")
        for title in shared_titles[:5]:
            rnlg_list = rnlg_by_title[title]
            wdc_list = wdc_by_title[title]
            rnlg_ings: set[str] = set()
            for r in rnlg_list:
                rnlg_ings |= r.ingredient_names
            wdc_ings: set[str] = set()
            for w in wdc_list:
                wdc_ings |= w.ingredient_names
            rnlg_group = IngredientGroup(
                canonical_ingredients=frozenset(rnlg_ings),
                recipes=rnlg_list,
            )
            wdc_group = IngredientGroup(
                canonical_ingredients=frozenset(wdc_ings),
                recipes=wdc_list,
            )
            vc = within_variant_comparison(rnlg_group, wdc_group)
            print(
                f"  {title!r}: RecipeNLG={vc.recipenlg_count},"
                f" WDC={vc.wdc_count},"
                f" shared={len(vc.shared_ingredients)}"
            )
            if vc.shared_ingredients:
                print(f"    shared: {sorted(vc.shared_ingredients)}")
    elif not comparison_recipes:
        print("\nNo RecipeNLG results available — skipping cross-corpus comparison.")


if __name__ == "__main__":
    main()
