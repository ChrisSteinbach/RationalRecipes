# Chocolate Chip Cookies — Hand-Cycle Drop

> Variant identified by RationalRecipes-ehe7 hand-cycle, 2026-05-05/06.
> Quantities filled in 2026-05-06 by RationalRecipes-v61w
> (`scripts/import_merged_artifacts.py` + `scripts/render_drop.py`
> against `output/catalog/recipes.db`). Instructions resolved against
> the local RecipeNLG corpus (no live web fetch required — F5 partial
> closure for RecipeNLG sources).

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
its instructions verbatim. The most-central source per
`recipes.db` is:

> [Chocolate Chip Cookies](https://www.cookbooks.com/Recipe-Details.aspx?id=473872)
> (outlier_score=6.32, the modal recipe of this 98-recipe cluster)

Verbatim instructions from that source (RecipeNLG copy, retrieved
2026-05-06 from `dataset/full_dataset.csv`):

> 1. Cream shortening, margarine and sugar.
> 2. Add eggs and vanilla. Add dry ingredients. Add chocolate chips last.
> 3. Bake at 350° for 10 to 12 minutes.

Bake-along ingredient list (the median source's exact quantities, for
context — these are not the averaged values, just what this single
recipe specifies):

- 2/3 c. margarine
- 2/3 c. shortening
- 1 c. brown sugar
- 1 c. sugar
- 2 eggs
- 1 tsp. salt
- 1 tsp. soda
- 2 tsp. vanilla
- 3 1/2 c. flour
- 1 (12 oz.) chocolate chips

The next four most-central sources, in case the top one reads as too
terse for publication (it does — see "Notes for the user" below):

| outlier_score | URL |
|---:|---|
| 7.35 | https://www.food.com/recipe/chocolate-chip-cookies-6482 |
| 7.42 | https://www.cookbooks.com/Recipe-Details.aspx?id=615517 |
| 7.42 | https://www.cookbooks.com/Recipe-Details.aspx?id=1017218 |
| 10.24 | https://www.cookbooks.com/Recipe-Details.aspx?id=883123 |

## Source recipes (provenance)

This drop's quantities are the central tendency across these 98
source recipes (full list in `recipes.db`'s `variant_members` for
`b34c2dce79e2`, also persisted in the manifest at
`output/merged/ehe7-ccc/manifest.json`). The 5 most central are
listed above under Instructions.

Highest-outlier sources (least central, candidates for filtering in
the review tool per RationalRecipes-sj18):

| outlier_score | URL |
|---:|---|
| 81.12 | https://www.food.com/recipe/chocolate-chip-cookies-522705 |
| 57.56 | https://www.cookstr.com/recipes/chocolate-chip-cookies-3 |
| 57.56 | https://cookpad.com/us/recipes/366891-chocolate-chip-cookies |
| 56.88 | https://www.chowhound.com/recipes/chocolate-chip-cookies-30464 |
| 53.71 | https://www.cookbooks.com/Recipe-Details.aspx?id=964168 |

Outlier scores re-computed post-v61w from `recipes.db`'s
`variant_members.outlier_score`. The original manifest's
`row_outlier_scores` had the same shape but slightly different absolute
values because the import zeroes the three non-header canonical
ingredients (granulated sugar, nuts, shortening), shifting the
distance metric. Same recipes are in the cluster either way — the
ranking just shuffles a little.

## Methodology

Quantities are averaged across 98 independent source recipes from
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

The hand-cycle's three acceptance criteria:

- ✓ **Drop artifact exists** — this file (`drop.md`) plus a Bluesky-
  shaped thread sketch at [`thread.md`](thread.md).
- ✓ **Friction log exists** — see [`friction-journal.md`](friction-journal.md);
  10 friction points with concrete recommendations and real timing
  data.
- ⚠ **Decision recorded** — *your call.* The friction journal's
  "Decision" section at the bottom is where to record the verdict.

**One known polish gap on this artifact:** the median-source's
instructions (id=473872) are terse — three steps, no hand-holding.
That's r8hx option 1 done literally: take the median's instructions
verbatim. Polished CCC instructions for a wider audience would
either pick a more detailed top-5 source (id=1017218 or id=883123
both work, see the Instructions section), expand the median lightly
("preheat oven to 350°F" before step 3), or — per r8hx option 2 —
synthesize instructions from the cluster instead of picking a single
source. This choice is the strongest editorial lever in the pivot;
the friction journal's F10 captures it as new data.

A reasonable shipping criterion: *"if I'd bake from this drop and
expect it to work, ship it."* The averaged ratios match common-sense
CCC (32% flour, 21% chocolate chips, 14% brown sugar, 12% margarine,
9% egg, 9% sugar, 0.5% each of salt / soda / vanilla) with expected
high variance on fat type and chip volume. The terse instructions
are the one thing a less-experienced baker would notice as thin.
