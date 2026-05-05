# Chocolate Chip Cookies — Hand-Cycle Drop

> Variant identified by RationalRecipes-ehe7 hand-cycle, 2026-05-05/06.
> **This is the artifact's identification + provenance side. The
> averaged-quantities side is unfinished** — see [friction journal
> F1+F9](friction-journal.md) for why; it needs a small bridge between
> `scrape_merged.py`'s CSV+manifest output and the central-tendency
> math in `catalog_db.py` before `render_drop.py` can produce a fully
> averaged drop. The shape that drop will take is shown by
> [`placeholder-brown-sugar-pb-ccc.md`](placeholder-brown-sugar-pb-ccc.md).

## Variant

- **variant_id**: `b34c2dce79e2`
- **n_recipes**: 98 source recipes (largest of the 10 plain-CCC sub-variants found in the corpus, picked as the modal cluster)
- **canonical ingredients (12)**: baking soda, brown sugar, chocolate chips, egg, flour, granulated sugar, margarine, nuts, salt, shortening, sugar, vanilla
- **cooking methods**: not extracted (cooking_methods array was empty in the manifest)

The "granulated sugar" + "sugar" coexistence and the
"shortening" + "margarine" presence suggest this is the Americana /
older-cookbook canonical CCC — the cookbooks.com-dominated branch of
the corpus. A more contemporary CCC (butter-only, no shortening)
would be one of the other 9 variants in
[`output/merged/ehe7-ccc/manifest.json`](../../../output/merged/ehe7-ccc/manifest.json):

| variant_id | n |
|---|---:|
| b34c2dce79e2 (this drop) | 98 |
| 6cac5b0940c5 | 76 |
| 00fe4e0beeec | 61 |
| c02187a1a1c7 | 59 |
| 0ed03768979d | 54 |
| 9b4efe30dad0 | 41 |
| 202f2fd25242 | 33 |
| 9a536a7c8a8a | 31 |
| b55a9b6854b5 (oatmeal CCC) | 28 |
| 5a1b324eb7f5 (oatmeal CCC) | 24 |

## Quantities

*Pending — see friction-journal F1+F9. The CSV at
`output/merged/ehe7-ccc/chocolate_chip_cookies_b34c2dce79e2.csv` has
the parsed cells per source recipe (e.g. "1 c", "12 oz", "2 MEDIUM"),
but central-tendency mass percentages are not computed — that was
the retired `rr-stats` job. To complete this section, one of:*

1. *Re-implement central-tendency math against the CSV (~50 lines,
   reusing `catalog_db.py`'s formulas — `numpy.mean`, `numpy.std`,
   1.96·σ/√n).*
2. *Land the F1 bridge (scrape_merged.py → recipes.db) so that
   `render_drop.py` works on these variants directly.*

*The shape this section will take is shown in
[`placeholder-brown-sugar-pb-ccc.md`](placeholder-brown-sugar-pb-ccc.md)
(rendered from a different variant that lives in the existing
`recipes.db`).*

## Instructions

Per RationalRecipes-r8hx option 1: pick the median source recipe
(lowest outlier score against the cluster's central tendency) and use
its instructions verbatim.

For this variant, the lowest-outlier source is:

> [www.cookbooks.com/Recipe-Details.aspx?id=883123](https://www.cookbooks.com/Recipe-Details.aspx?id=883123)
> (outlier_score=8.85)

The next four most-central sources, in case the top one is unusable:

| outlier_score | URL |
|---:|---|
| 9.15 | https://www.food.com/recipe/chocolate-chip-cookies-6482 |
| 9.81 | https://www.cookbooks.com/Recipe-Details.aspx?id=698527 |
| 10.03 | https://www.cookbooks.com/Recipe-Details.aspx?id=626959 |
| 10.51 | https://www.cookbooks.com/Recipe-Details.aspx?id=473872 |

*Manual step: fetch instructions text from the chosen source URL,
paste here. Per friction F5, instructions text isn't stored in
recipes.db — this is a cache-source-instructions opportunity.*

## Source recipes (provenance)

This drop's quantities (when computed) will be the central tendency
across these 98 source recipes. The 5 most central are listed above
under Instructions; the full 98 are in the manifest at
`output/merged/ehe7-ccc/manifest.json` under variant `b34c2dce79e2`.

Highest-outlier sources (least central, candidates for filtering in
the review tool per RationalRecipes-sj18):

| outlier_score | URL |
|---:|---|
| 64.89 | https://www.food.com/recipe/chocolate-chip-cookies-522705 |
| 55.38 | https://www.cookbooks.com/Recipe-Details.aspx?id=964168 |
| 45.39 | https://www.food.com/recipe/chocolate-chip-cookies-73698 |

## Methodology

Quantities will be averaged across N independent source recipes from
RecipeNLG and WDC, mass-normalized to per-100 g of batch.
Confidence intervals are 95% (1.96·σ/√n). Outliers are scored against
the cluster's central tendency. The chosen instruction set is taken
verbatim from the median source recipe (lowest outlier) and credited
explicitly. See
[RationalRecipes](https://github.com/ChrisSteinbach/RationalRecipes)
for the methodology source and
[`docs/design/recipe-drops.md`](../../design/recipe-drops.md) for the
design.

## Notes for the user

The hand-cycle's central question — *"does the pivot feel viable
based on this cycle?"* — is your call. The friction journal
quantifies what would help; this drop captures what we have so far.
A reasonable acceptance criterion would be: "after F1+F9 are bridged,
this drop is an `npm run render` away from being shippable."
