# Chocolate Chip Cookies — Hand-Cycle Drop

> Variant identified by RationalRecipes-ehe7 hand-cycle, 2026-05-05/06.
> Quantities filled in 2026-05-06 by RationalRecipes-v61w
> (`scripts/import_merged_artifacts.py` + `scripts/render_drop.py`
> against `output/catalog/recipes.db`).

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

Mass percentages averaged across **98 independent source recipes**
(reconstructed from `output/merged/ehe7-ccc/` via
`scripts/import_merged_artifacts.py`). 95% CIs are ±1.96·σ/√n. ``n`` is
the smallest subset for which an ingredient appears.

| Ingredient | Mass % | ± stddev | 95% CI | n | per 1 kg |
|---|---:|---:|---:|---:|---:|
| baking soda | 0.5% | 0.3% | 0.4–0.5% | 93 | 5 g |
| brown sugar | 13.9% | 11.9% | 11.5–16.3% | 66 | 139 g |
| chocolate chips | 21.3% | 15.7% | 18.2–24.4% | 71 | 213 g |
| egg | 8.9% | 4.4% | 8.0–9.8% | 96 | 89 g |
| flour | 32.4% | 15.5% | 29.3–35.4% | 94 | 324 g |
| margarine | 12.5% | 13.7% | 9.8–15.2% | 59 | 125 g |
| salt | 0.5% | 0.4% | 0.4–0.6% | 82 | 5 g |
| sugar | 9.5% | 9.5% | 7.6–11.3% | 60 | 95 g |
| vanilla | 0.6% | 0.4% | 0.5–0.7% | 86 | 6 g |
| granulated sugar | 0.0% | 0.0% | 0.0–0.0% | 0\* | 0 g\* |
| nuts | 0.0% | 0.0% | 0.0–0.0% | 0\* | 0 g\* |
| shortening | 0.0% | 0.0% | 0.0–0.0% | 0\* | 0 g\* |

\* *Granulated sugar / nuts / shortening are in the variant's
canonical ingredient set but not the CSV header (they appeared in <50%
of source recipes and were dropped from the per-row CSV at
extraction time). The reimport reconstructs zero usage for these
across all rows. Re-extracting against an updated `scrape_merged.py`
that writes directly to recipes.db (post-v61w) would recover real
percentages for them.*

**High-variance ingredients** (CV > 50% — bakers disagree on the
right amount, expected for a 98-recipe mix of tradition styles):
brown sugar (CV=86%), chocolate chips (CV=74%), margarine (CV=110%),
sugar (CV=100%), salt (CV=90%), vanilla (CV=71%), baking soda
(CV=56%). The variance reflects the genuine spread between
butter-forward, margarine-forward, and shortening-forward American
CCC traditions co-existing in the cluster — not a measurement
problem.

The shape this section was previewed in via
[`placeholder-brown-sugar-pb-ccc.md`](placeholder-brown-sugar-pb-ccc.md);
this is the real thing.

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
