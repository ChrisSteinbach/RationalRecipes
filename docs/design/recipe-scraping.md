# Design: Automated Recipe Collection

**Status:** Draft
**Issue:** RationalRecipes-7ns (epic: RationalRecipes-b7t)
**Last updated:** 2026-04-24

## Problem

Recipe data collection is currently manual: open a search engine, read candidate
pages, transcribe ingredient proportions into CSV. This is the single biggest
bottleneck for expanding the dataset and for scaling the project's core thesis
(central-tendency averaging across many recipes yields a reliable recipe).

We need an automated pipeline that can, given a dish name, produce a set of
normalized ingredient-proportion rows suitable for the existing statistics
code in `src/rational_recipes/`.

## Goals

- **Corpus-first, not query-first**: start from large existing recipe
  archives (RecipeNLG, Web Data Commons) rather than live web search.
  Source diversity comes from the breadth of the corpus, not from
  per-dish query engineering.
- **Automatic grouping**: discover dish variants from the corpus rather
  than targeting them one at a time. The pipeline should answer "what
  coherent dish groups exist and which have enough data to average
  meaningfully?" — not just "find me 30 pannkakor recipes."
- **Structured-first**: rely on `schema.org/Recipe` JSON-LD where present (most
  modern recipe sites expose it for Google rich snippets).
- **LLM-assisted parsing**: use a local LLM to turn natural-language ingredient
  lines ("1 heaping cup flour, sifted") into structured fields. Local keeps
  iteration cheap and avoids API costs during exploration.
- **Language-neutral pipeline, en+sv maintained scope**: the pipeline
  itself handles any language present in the source corpora — a
  language-neutral LLM prompt with multilingual examples extracts
  cleanly across Swedish, German, Russian, and Japanese without
  per-language specialization. Maintained ingredient-DB coverage is
  English and Swedish only (the maintainer's working languages); adding
  another language requires the miss-tally → classification → DB-rebuild
  loop described in § Scope below, where step 3 (classification) needs
  native-speaker judgement.
- **Use the whole corpus**: the source corpora are fixed-size archives
  (RecipeNLG 2020, WDC 2023), so "sample size" per variant is whatever
  survives grouping and filtering — there is no crawl-until-enough loop.
  The minimum-group-size filters decide which variants have enough data
  to average, and those that don't are dropped rather than topped up.
- **Feeds the existing pipeline**: output should plug into the normalization
  and statistics code already in `src/rational_recipes/`.

## Non-goals (for now)

- Real-time / on-demand scraping from the PWA frontend. This is a batch
  research tool.
- Ingredient substitution reasoning (separate concern).
- Live web crawling. Archive-based data sources are sufficient for
  exploration. Targeted web search (Google Programmable Search API or
  similar) can supplement archives later if coverage gaps appear for
  specific dish variants.

## Scope

Maintained-language scope is **English and Swedish** — the languages the
project maintainer can personally validate for ingredient-name
classification. The extraction pipeline itself is language-neutral (see
§ Ingredient-line parsing) and was qualitatively validated on Swedish,
German, Russian, and Japanese during the extraction spike. The
constraint is not the extractor; it is the per-language ingredient-DB
coverage work, which runs in four steps:

1. LLM extraction of ingredient names from WDC hosts in the target
   language.
2. Frequency-rank names that don't resolve via
   `IngredientFactory.get_by_name`.
3. Classify each top-N miss per bead `RationalRecipes-3cu`'s policy:
   (a) synonym into an existing DB food, (b) new SUPPLEMENTARY entry
   with density, or (c) alias-only for a clear language variant.
4. Rebuild `ingredients.db` and re-tally to measure miss-rate drop.

Steps 1, 2, and 4 are mechanical and ship in
`scripts/tally_wdc_misses.py`. Step 3 requires native-speaker fluency
with cooking vocabulary, which the maintainer has for English and
Swedish only.

Other languages are out of maintained scope and may reopen later as a
contributor track. The `RationalRecipes-b7t.24` WDC volume survey
ranked candidate languages: German (53,892 recipes / 7 hosts), French
(37,922 / 8), Russian (31,882 / 8), Italian (31,224 / 5), Japanese
(19,160 / 3). A language-adding contribution is mostly DB entries
(~hundreds of synonym/supplementary rows) plus a re-tally measurement,
not code.

## Data sources

Two existing recipe archives provide large, ready-to-use corpora that
eliminate the need for live web search, HTTP fetching, and JSON-LD
extraction during exploration.

### RecipeNLG — the quick-start corpus

~2.2 million recipes in a single CSV. Fields: title, ingredients (as a
stringified Python list), directions, source, originating URL. ~1.6M
gathered by the RecipeNLG authors from web sources; ~600K from Recipe1M+.

Available at <https://recipenlg.cs.put.poznan.pl/> under non-commercial
research/educational terms. Also mirrored on Hugging Face and Kaggle.

**Strengths:** trivial to load (`pd.read_csv`), ingredients already
extracted as lists, large enough for statistical work.

**Weaknesses:** fixed 2020 snapshot, heavily English, biased toward large
US recipe aggregators (AllRecipes, Food.com, Cookbooks.com). Not a uniform
sample of the recipe web — it's a sample of sites convenient to crawl.

**Role in the pipeline:** fast iteration target. Use for developing and
validating the grouping and parsing stages. Ingredient lines are still
natural language ("1 cup flour") so LLM parsing is still needed.

### Web Data Commons — the broader corpus

WDC extracts `schema.org` data from Common Crawl snapshots annually. The
Recipe subset contains every `schema:Recipe` entity found in the crawl.
Most recent confirmed release: late 2023.

Two formats:

1. **N-Quads subsets** — raw RDF extractions, flexible but requires
   reassembling entities from triples.
2. **Schema.org Table Corpus 2023** — pre-grouped into per-host relational
   tables, one table per (class, website). Much friendlier for analysis.

Available at <http://webdatacommons.org/structureddata/schemaorgtables/2023/index.html>.

**Strengths:** processes the *entire* Common Crawl, not a hand-picked set
of sites. Cleaner corpus for reasoning about sampling bias. Structured
`recipeIngredient` and time fields (`cookTime`, `prepTime`, `totalTime` —
ISO 8601 durations) extracted from JSON-LD, no HTML parsing needed.
`recipeInstructions` arrives as a list of step objects rather than a
single prose blob. Top-100 hosts are surprisingly international (Swedish,
French, German, Japanese, Russian, Arabic, Indian sources alongside the
English aggregators).

**Weaknesses:** larger download (top100 = 315 MB, minimum3 = 1.7 GB,
rest = 9.5 MB), messier data (real-world markup has missing fields, bad
encodings, creative schema interpretations), more setup effort. Crucially,
`recipeIngredient` is raw natural-language strings (`"2 teaspoons curry
powder"`) not pre-extracted names — RecipeNLG's `NER` column gave us
clean names for free, WDC does not. Field coverage is **per-host bimodal**
(see § Per-host bimodality below): the global average for any given field
is misleading.

**Per-host bimodality.** Field coverage averages reported by WDC's
column statistics hide a strongly bimodal distribution. Schema-good hosts
publish a field on nearly every recipe; most hosts skip it entirely.
The headline example: `cookingMethod` is present in only 2.25% of tables
globally, but ICA.se publishes it on 69% of recipes — and on Swedish
pannkakor specifically it cleanly discriminates `Stekt` (pan-fried,
stekpannkaka) from `I ugn` (in oven, ugnspannkaka) with no LLM call. Time
fields are similarly per-host conventional: ICA publishes only `totalTime`
and never `cookTime`/`prepTime`; Food Network publishes `cookTime` and
`totalTime` but not `prepTime`. The loader must tolerate per-host
conventions and treat the time fields as a normalized duration set.
Per-dish coverage of "free" structured fields like `cookingMethod` depends
on which hosts serve that dish family — generalize from the average at
your peril.

Concrete pannkakor example from ICA.se (recon run 2026-04-10):

| Recipe                                      | `cookingMethod` |
|----------------------------------------------|-----------------|
| Fluffiga pannkakor med ricotta och citron    | `Stekt`         |
| Proteinpannkakor                             | `Stekt`         |
| Bananpannkakor med hasselnötskräm            | `Stekt`         |
| Ugnspannkaka med zucchini                    | `I ugn`         |
| Äppelpannkaka med vaniljyoghurt              | `I ugn`         |
| Saffranspannkaka                             | `I ugn`         |
| Dutch baby – ugnspannkaka med blåbär         | `I ugn`         |
| Fläskpannkaka                                | `Stekt,I ugn`   |

ICA's full `cookingMethod` vocabulary across the host also includes
`Kokt` (boiled) and others; the pannkakor slice above is a subset of
the tag space.

About 30% of ICA pannkakor rows have `cookingMethod` null, but where
present the field cleanly discriminates the variants we care about with
no LLM call. Multi-method recipes use comma-joined tags and must be
parsed as a set.

**Role in the pipeline:** the serious dataset, and a source of structured
signals RecipeNLG lacks. Use after the pipeline is validated on RecipeNLG.
The intent is to **merge** the two corpora, not run them in parallel
indefinitely — each repairs signals the other is missing. WDC donates
`totalTime` / `cookTime` / `prepTime`, `recipeYield`, `cookingMethod`, and
`keywords`; RecipeNLG donates clean NER names and sheer volume. Merging
happens *before* Level 3 variant-splitting: both corpora run L1/L2
independently on ingredient names that have already been canonicalized
to a shared English vocabulary at extraction time (see § Ingredient-line
parsing below — every name from either corpus is routed through
`IngredientFactory` before L2 Jaccard sees it, so cross-corpus merge
doesn't need its own language-normalization step). Level 3 operates on
the combined stream, using `cookingMethod` where WDC provides it;
RecipeNLG rows (no `cookingMethod`) fall into the unknown-method bucket
and merge back into the largest sub-group per the L3 partition rule.
Dish-identity mismatches (American pancakes vs Swedish pannkakor) are
already handled by Level 2 ingredient-set grouping — Phase 1 proved this
on 114 "swedish pancakes" recipes, which split cleanly at L2 into 49
American-style and 42 genuine pannkakor. Within-variant disagreement
after merge is a signal that the variant definition is still too loose,
not a reason to keep the corpora apart.

### Archive coverage gaps

Archive coverage will have gaps for niche dish variants, recent recipes,
and non-English sources. Supplementary collection strategies can be
explored if specific gaps prove blocking, but no live-search pipeline is
currently planned.

## Approach: corpus-driven grouping pipeline

Start from the whole corpus, automatically discover dish groups, and
identify which groups have enough data to average meaningfully — rather
than targeting one dish variant at a time and scraping the web for it.

Pipeline stages:

```
per corpus (RecipeNLG, WDC — run independently):
  → Level 1: title-based grouping
  → minimum group size filter
  → Level 2: ingredient-set grouping on canonicalized names
             (IngredientFactory maps each extracted name — RecipeNLG NER
              or WDC LLM output — to an English canonical before Jaccard)
  → minimum group size filter
  → ingredient-line parsing (local LLM, quantities and units)
  → unit normalization (existing pipeline)

then across corpora:
  → cross-corpus merge (URL-level join + ingredient-set near-dup @ 0.5)
  → Level 3: method + proportion grouping (split within L2 groups)
  → minimum group size filter
  → proportion-bucket hash deduplication (within-variant)
  → outlier flagging
  → human review (terminal shell over manifest.json)
  → CSV rows (compatible with existing statistics code)
                   + manifest.json (variant id, counts, provenance)
```

Each grouping level is more expensive than the last but operates on a
smaller set. The minimum-size filters shed noise early and ensure
downstream stages only process groups that can yield statistically
meaningful averages.

### Level 1: title-based grouping

Normalize recipe titles (lowercase, strip "recipe", strip possessives,
collapse whitespace) and group by similarity. This is the cheapest pass
and handles the common case: recipes titled "Swedish Pancakes",
"pannkakor", "Pannkakor Recipe" should land in the same bucket.

**Technique:** normalized exact-match (Q1 RESOLVED) — shipped in
`scrape/grouping.py`. Fuzzy matching (edit distance, token overlap) and
LLM-based canonicalization are documented fallbacks, not currently used;
revisit only if normalized exact-match proves insufficient at scale.

**What it can't do:** creative titles ("Grandma's Sunday Delight"),
cross-language synonyms without explicit mapping, or structural variants
hiding behind the same name ("pancake" catches American, Swedish, Dutch,
Japanese).

**Minimum group size filter:** drop groups below a threshold (TBD — likely
in the range of 5–20 recipes). Groups too small to average meaningfully are
noise at this stage.

**CLI: `rr-discover`.** Interactive L1 slicer over a RecipeNLG-format
corpus. Streams every row, normalizes titles through the Level 1
normalizer, and reports the top-K dish names above a minimum count
(text/CSV/JSON). Answers "what coherent L1 buckets does this corpus
actually contain?" without committing to a query up front. With
`--variants` it makes a second pass that collects full recipes for each
surviving title and runs `group_by_ingredients` (Level 2) in the same
invocation — so e.g. the American-pancake vs pannkakor split of a
"swedish pancakes" bucket, or the rice+cream-of-mushroom vs mayo+egg
families of "broccoli casserole", surface without a separate step.
Source: `src/rational_recipes/discover.py` and `discover_cli.py`,
entry point declared in `pyproject.toml`.

### Level 2: ingredient-set grouping

Within each Level 1 title group, represent each recipe as a set of
ingredient names (no quantities needed — just "flour", "milk", "eggs",
"butter", "salt"). Compute pairwise Jaccard similarity on ingredient sets
and cluster.

This catches the "pancake" problem: American buttermilk pancakes (flour +
buttermilk + baking powder + egg) have a different ingredient fingerprint
from Swedish pannkakor (flour + milk + egg + butter) and will split into
separate sub-groups even if they share a title group.

**What it can't do:** distinguish variants that use the same ingredients
in different proportions or with different techniques (ugnsmannkaka vs
stekpannkaka — same batter, oven-baked vs pan-fried).

**Minimum group size filter:** drop sub-groups below threshold after
splitting.

### Ingredient-line parsing (local LLM)

Each ingredient entry is a natural-language string. The LLM produces
structured fields:

```
"1 heaping cup flour, sifted"
→ { quantity: 1, quantity_modifier: "heaping", unit: "cup",
    ingredient: "flour", preparation: "sifted" }
```

**Model:** production default is `qwen3.6:35b-a3b` via Ollama on a remote
host, swapped in 2026-04-24 after the v2 benchmark sweep
(`RationalRecipes-jpp`); see § Phase 4 Results below. Phases 0–1 used
Gemma 4 e4b, which handled English at 100% accuracy on the Phase 0
sample. A remote Ollama host is required either way — e4b OOMs on a
16 GB local machine, and qwen3.6:35b-a3b is larger still. Both models
extract cleanly across Swedish, German, Russian, and Japanese when
paired with the language-neutral prompt described below.

**Prompt strategy:** a **language-neutral prompt** that instructs the model
to keep ingredient names in the original language. Reference implementation
in `src/rational_recipes/scrape/wdc.py`. Multilingual examples plus an
explicit "keep the original language" instruction eliminate translation
artifacts across all tested languages; an English-only prompt, by contrast,
causes the model to translate 20-90% of non-English ingredient names to
English and break downstream Jaccard clustering. This is a **Shape D
(host-specific policy)** approach:
LLM-neutral is the primary extraction strategy for all hosts; regex is an
optional fast-path for known schema-good Latin-script hosts (ica.se,
chefkoch.de). Regex breaks on Russian (unit words like `стакан` not in
the vocabulary, catastrophic truncation on comma-preps) and Japanese
(multi-char unit tokens `大さじ`/`小さじ` glue to ingredient names).

Coverage of the prompt's few-shot examples:

- Numeric + fractional quantities ("1 1/2", "½")
- Ranges ("1-2 cups") — emit `quantity_min`/`quantity_max`
- Implicit quantities ("a pinch", "to taste") — distinct sentinel
- Compound items ("salt and pepper to taste") — split or flag
- Whole-unit items ("2 large eggs")
- Ambiguous abbreviations ("T" vs "t" vs "tbsp")
- **Multilingual lines** (Swedish, German, Japanese, Russian examples)

**Validation evidence.** Against a 20-recipe ica.se gold standard (204
ingredient lines), the LLM with an English-only prompt scored P=0.835,
R=0.848, F1=0.841 — dominant failure mode: Swedish→English translation of
common ingredients (ägg→egg, smör→butter). The neutral prompt eliminates
that failure class. Regex on the same gold standard scored P=0.972,
R=0.982, F1=0.977, which is why regex remains as a fast-path for
Latin-script hosts even though it fails on non-Latin scripts. Known regex
failure modes (all fixable): Swedish plurals (`morötter`→`morot`),
comma-separated prep adjectives (`frysta, halvtinade blåbär`→`frysta`),
and packaging/container leakage (`1 förp kokta vita bönor`).

### Normalization

Feed parsed fields into the existing `normalize.py` / `units.py` /
`ingredient.py` pipeline. Ingredients not present in the ingredients DB
need a fallback:

- Short-term: skip the recipe, log the unknown ingredient.
- Medium-term: accumulate unknown-ingredient frequencies; hand-add the most
  common ones to the supplementary data feeding `build_db.py`.

### Level 3: cookingMethod-first variant partition

Within each Level 2 ingredient-set group, partition by `cookingMethod`
tags, then drop sub-groups below the minimum-size threshold.
Ugnsmannkaka (oven-baked, 200°C,
30 min) and stekpannkaka (pan-fried, stovetop, 2 min) share the same
batter but are different dishes; method catches this, proportions alone
cannot. This is the finest-grained grouping and targets variants that
share ingredients but differ in technique. Note: on RecipeNLG this is a
no-op until a second signal exists, because RecipeNLG carries no
`cookingMethod` field — see § Data availability below.

**Partition rule.** Within each L2 group, split by the distinct
`cookingMethod` tag sets. Rows with empty `cookingMethod` form an
"unknown-method" bucket that merges back into the largest sub-group
when it would otherwise be singleton, preserving stats rather than
splintering them. Sub-groups below `min_variant_size` are dropped by
the minimum group size filter below — the partition itself is
unconditional.

**Proportion signal as a follow-on.** When L3 cookingMethod-partition
lands two method-identical clusters that nonetheless average to very
different ratios (crêpes vs pannkakor — both pan-fried thin batters with
different flour:milk), proportion clustering on the normalized ingredient
vectors becomes the second split signal. This is a follow-on refinement,
not part of the initial L3 pass.

**Data availability.** WDC provides structured time fields (`cookTime`,
`prepTime`, `totalTime` as ISO 8601 durations) on most rows, and a
structured `cookingMethod` field on a minority of hosts — with strong
per-host bimodality. Globally `cookingMethod` is present in only ~2.25%
of tables, but on schema-good hosts (e.g. ICA.se: 69%) it is exactly the
"bake vs fry" enum the design wants. RecipeNLG, in contrast, has no time
or method fields at all — only free-text directions, which makes a
pure-method L3 a **no-op on the RecipeNLG side** until a second signal
exists (LLM method extraction from `recipeInstructions`, or proportion
clustering on parsed quantities). The loader framing is "use
`cookingMethod` where present, fall back to LLM extraction from
instructions prose where not", and the mix is expected to vary per
dish family depending on which hosts serve it.

**Minimum group size filter:** drop sub-groups below threshold. Since
the source corpora are fixed-size, a variant that falls below threshold
is simply dropped — there is no back-channel to collect more recipes for
it.

### Deduplication

Same recipe reposted across hosts inflates its weight in the average.

**Heuristic:** canonicalize each recipe as a sorted tuple of (ingredient,
rough-proportion-bucket). Hash. Near-duplicates collapse to a single
representative — any row in the collision group works, since the hash
already asserts they're interchangeable for ratio averaging.

Runs *within* a variant after normalization and after cross-corpus merge
(different problem than the URL + ingredient-set near-dup step, which
runs at merge time and is corpus-agnostic). Tracked in the same bead as
the merge (`RationalRecipes-toj`) because it's small and shares the
merged-stream context; may split later if scope grows.

Needs tuning once we have real data to see false-positive/negative rates.

## Taxonomic ambiguity & outlier handling

Two distinct failure modes to guard against. They look superficially similar
(a recipe that "doesn't fit") but need different treatment.

### Failure mode A: category contamination

**Problem:** different dishes sharing a name. "Pancake" catches American
buttermilk pancakes *and* Swedish pannkakor *and* Dutch babies. Averaging
across them is meaningless — their ingredient distributions don't belong to
the same underlying population.

**Defenses, in layers:**

- **Level 1 title grouping** narrows by name, but can't prevent ambiguous
  names from collecting multiple dish types.
- **Level 2 ingredient-set grouping** splits groups whose ingredient
  fingerprints diverge (e.g. baking-powder presence separates American
  pancakes from pannkakor).
- **Level 3 method + proportion grouping** catches technique variants
  hiding behind the same ingredient set.
- **Human review (Phase 2 / toj)** is the ultimate backstop when L1/L2/L3
  still leave a heterogeneous group — the reviewer can split variants or
  drop the group.

**Distance metric** is an open question — Jaccard on ingredient sets is
cheap; weighted by proportion is closer to what we actually care about.
Measure on real data before committing.

### Failure mode B: in-category outliers

**Problem:** recipes that legitimately belong to the target variant but
make unusual choices — grandmother's pannkakor with an unusual ratio, a
swap of cream for milk, an omitted "essential" ingredient.

These are legitimate data. With large N the averaging already absorbs
them; with small N one oddball meaningfully shifts the mean. The decision
to keep/drop is a judgment call, not an automated one.

**Approach:** flag, don't auto-drop. Compute per-recipe distance from the
group median (after dedup and variant-fit filtering) and surface outliers
for human decision. Keep the reason with the recipe.

### Human review as a first-class stage

Review is a pipeline stage, not an afterthought. The leverage point is
review *efficiency* — presentation determines whether this is a bottleneck
or a quick pass.

**Review view requirements:**

- **Per-dish table** of all collected recipes, sortable by fingerprint
  distance from the group center.
- **At-a-glance columns:** source URL, cook method, cook time, ingredient
  list, ratio deltas vs the current mean, novelty score.
- **Keyboard-driven decisions:** include / exclude / defer, one keystroke
  per row.
- **Live recomputation:** statistics update as rows are toggled, so the
  reviewer sees the effect of each decision immediately.
- **Persistent decisions:** each recipe carries a review status + reason;
  a re-crawl doesn't re-present rejects.
- **Split suggestion:** if the reviewer repeatedly rejects a coherent
  cluster, suggest splitting into two variant targets.

**Shell:** terminal UI (Python, stdlib + `rich`). Iteration shape for the
initial build is intentionally narrow — variant-level only, no per-row
interaction:

- **Input:** a `manifest.json` emitted by the merged pipeline alongside
  the per-variant CSVs. Each manifest entry carries
  `{variant_id, title, canonical_ingredients, cooking_methods,
  n_recipes, csv_path, source_urls}`.
- **Variant id:** `sha1(normalized_l1_title || "|" ||
  sorted(canonical_ingredient_set) || "|" ||
  sorted(cookingMethod_tag_set))` truncated to 12 hex chars. Stable
  across re-runs because canonicalization (3cu) maps names to a shared
  English vocabulary before the hash sees them. Method set is empty on
  the RecipeNLG side and on WDC hosts without `cookingMethod`; stays
  stable either way.
- **List view:** title | short ingredient list | N | status
  (pending / accept / drop / annotated).
- **Drill-in:** full ratio table (ingredient × recipe), source URLs,
  mean + stddev per ingredient.
- **Actions:** `a` accept, `d` drop, `n` annotate (free-text note),
  `?` defer. Decisions persisted to a JSON sidecar keyed by variant id;
  re-runs skip already-decided variants.
- **Deferred:** per-row keep/drop, live recomputation as rows toggle,
  fingerprint-distance sort, ratio deltas vs group center, split-action
  on L3-suggested clusters (all require signals not yet produced at this
  stage in the pipeline).

**Data labeling byproduct:** review decisions are labeled data, usable
for training or calibrating any downstream classifier built on top.

## Sample size

The source corpora are fixed-size archives, so sample size per variant is
whatever the grouping + filtering pipeline yields — not a target to hit.
For each variant, the pipeline averages across every surviving row; the
minimum-group-size filters decide which variants are viable and which get
dropped. `statistics.py` continues to report mean + confidence interval
on the final aggregate, so downstream consumers can see how tight the
estimate is, but those CIs describe the result — they do not gate
collection.

## Compliance & ethics

Archive-based data (RecipeNLG, WDC) shifts most compliance concerns
upstream — those projects handled crawl ethics when collecting the data.
Our obligations:

- Respect archive license terms (RecipeNLG: non-commercial research/
  educational use; WDC: inherits fair-use status of underlying pages).
- Live web search is not currently used (see § Archive coverage gaps);
  if that changes, the obligations are: respect `robots.txt` and
  `Crawl-delay`, identify the scraper in User-Agent, cache aggressively,
  rate-limit per host, no circumvention of paywalls or anti-bot measures.
- Store only what's needed (ingredient lists + provenance metadata, not
  full page archives long-term).
- Treat collected data as research-use; if/when the app surfaces averaged
  recipes publicly, credit the diversity of sources (not any single one).

## Exploration plan

Phased so we fail fast on the expensive uncertainties (LLM quality,
grouping effectiveness) before building infrastructure around them.

**Phase documentation convention:** each closed phase gets a short
`Results` subsection inline below (distilled measurements, key findings,
code pointers) and a one-paragraph summary in its bead's close note. No
standalone `docs/recipe-scraping-phase*.md` files. Live phase status
lives in the bead graph — run `bd show <bead-id>` for current state.

**Phase 0 — end-to-end manual dry run (no code)**
Manually searched for pannkakor recipes, checked pages for JSON-LD
presence, ran ingredient lines through Gemma 4 e4b by hand, compared
scraped ratios against the existing hand-entered CSV. No code written —
this phase was a gated go/no-go on the LLM + JSON-LD approach.

*Results (2026-04-07):*

| Metric                       | Result       |
|------------------------------|--------------|
| Page fetch success           | 5/7 (71%)    |
| JSON-LD presence (of fetched)| 4/5 (80%)    |
| Gemma 4 e4b parse accuracy   | 10/10 (100%) |

All 4 scraped pannkakor produced plausible ratios consistent with the
existing hand-entered CSV (flour:milk around 1:2). Edge cases handled:
unicode fractions, mixed fractions, implicit quantities (pinch, dash),
parenthetical prep notes. Decision: proceed to Phase 1. Full bead:
`RationalRecipes-4lm`.

**Phase 1 — RecipeNLG load + Level 1/2 grouping + LLM parsing**
Load RecipeNLG, implement title-based grouping (Level 1) and ingredient-set
grouping (Level 2) with minimum group size filters. Wire up LLM ingredient
parsing and normalization for a known test case (pannkakor). Hand-verify
output rows against the source data.

*Code:* `src/rational_recipes/scrape/` (loader, grouping, LLM parse,
pipeline orchestration). Exploration entry points:
`scripts/explore_groups.py` (fast, no LLM),
`scripts/scrape_to_csv.py` (full pipeline), and the `rr-discover` CLI
(title discovery over the whole corpus, with `--variants` for a Level 2
breakdown of each surviving title — see § "Level 1" above).

*Results (on 10 pannkakor title groups, 71 ingredient lines total):*

| Metric                   | Result       |
|--------------------------|--------------|
| LLM parse accuracy       | 71/71 (100%) |
| Ingredient-DB miss rate  | ~18%         |

Level 2 clustering vindicated the category-contamination concern: the
114-recipe "swedish pancakes" title group split cleanly into 49 American-
style recipes (`buttermilk + baking soda + flour + eggs + salt + sugar`),
42 genuine pannkakor (`butter + eggs + flour + milk + salt + sugar +
water`), and 4 lingonberry-sauce variants. Averaging the full 114 would
have been meaningless. Core baking ingredients (flour, milk, egg, butter,
salt, sugar, cream, water) resolved correctly; misses concentrated in
specialty items. Bead `RationalRecipes-3cu` subsequently folded the
pannkakor-scope misses (lingonberry, saffron, margarine, plus Swedish
vocabulary) into the DB. Full Phase 1 bead: `RationalRecipes-09f`.

**Phase 2 — WDC corpus + Level 3 grouping + dedup + review shell**
Load WDC Schema.org Table Corpus. Add method + proportion grouping
(Level 3), using WDC's structured `cookingMethod`/`cookTime` fields. Add
dedup heuristic and a minimal terminal-based review shell. Validate against the existing
hand-curated CSVs as correctness oracle. Measure: how well does Level 3
separate known variants (e.g. ugnsmannkaka vs stekpannkaka)?

*Results (2026-04-24):*

WDC Schema.org Table Corpus loader + per-corpus L1/L2 grouping shipped
in `scrape/wdc.py` and `scrape/grouping.py` (`RationalRecipes-ayw`).
Cross-corpus merge (URL join + Jaccard near-dup) and within-variant
proportion-bucket dedup shipped in `scrape/merge.py`
(`RationalRecipes-toj`). The merged pipeline (`scrape/pipeline_merged.py`,
driver `scripts/scrape_merged.py`) emits per-variant rr-stats-compatible
CSVs alongside a `manifest.json` keyed by a stable `variant_id =
sha1(normalized_l1_title | sorted(canonical_ingredient_set) |
sorted(cookingMethod_tag_set))[:12]`. Level 3 cookingMethod partition
shipped in `group_by_cooking_method()` and is wired into the merged
pipeline (`RationalRecipes-7eo`). Terminal review shell — variant-level
accept/drop/annotate/defer plus L3 split action — shipped in
`scripts/review_variants.py` (`RationalRecipes-eco`,
`RationalRecipes-4lf`).

Two settings landed via end-to-end validation on the pannkak / WDC
ica.se slice (`RationalRecipes-toj`):

- **Near-dup Jaccard threshold 0.3** (down from 0.5). A threshold
  sweep on the 10 RecipeNLG + 33 WDC ica.se cross-corpus stream
  (deterministic LLM extraction) found the documented saffranspannkaka
  pair (RecipeNLG food52.com × WDC ica.se) sits at Jaccard ~0.3-0.4
  — the two recipes list different optional accompaniments
  (blueberry jam vs sylt, vispgrädde vs whipping cream, ground
  almonds vs almond flour) so the intersection-over-union stays
  modest. 0.3 catches the pair with no false positives in the 43-row
  stream; 0.4 misses it under deterministic extraction. Matches the
  bottom of the 0.3-0.5 range documented in `RationalRecipes-3cu`.
- **Deterministic LLM calls** (`temperature=0`, `seed=42` in
  `scrape/parse.py::_ollama_generate`). The first round of validation
  reruns produced 31/43 differing `variant_id`s because LLM sampling
  noise was shifting the canonical ingredient set ("sugar" vs "flour"
  on the same recipe between runs; "chocolate 70 percent" vs "choklad"
  pre-canonicalization). With determinism on, two reruns produce
  byte-identical manifests.

Phase 2 acceptance (run on 10 RecipeNLG + 33 WDC ica.se pannkak rows,
the slice that contains the 3cu-documented cross-corpus pairs):
(a) URL + near-dup merge fires on the saffranspannkaka pair at the
new 0.3 default; the fläskpannkaka pair stays missed because
RecipeNLG title `Fläsk Pannkaka - Pork Pancake` doesn't normalize to
WDC's `fläskpannkaka` (the title-gating step never compares them —
tracked as `RationalRecipes-cw1`);
(b) within-variant proportion-bucket dedup did not fire on this slice
because each variant is singleton — the dedup mechanism stays unit-
tested in `test_dedup_in_place_collapses_identical_proportions`,
real-world dedup hits will land once Phase 4's broader title queries
build multi-recipe variants;
(c) `manifest.json` round-trips cleanly through `Manifest.read →
to_json_dict` and through the eco shell's
`summarize_variant()`/`pending_variants()` consumers across all 43
variants;
(d) `variant_id`s are byte-identical across two reruns of the full
pipeline once the LLM is deterministic.

**Phase 3 — outlier flagging**
Add outlier flagging on top of the review view: compute per-recipe
distance from the group median and surface outliers for the reviewer
to decide keep/drop, with the reason recorded alongside the recipe
(see § Failure mode B).

*Results (2026-04-24):*

Per-recipe outlier scoring shipped in `scrape/outlier.py`
(`compute_outlier_scores()`) and is wired into `MergedVariantResult`
in `pipeline_merged.py` — each row carries a score the review shell
can sort and highlight by. Score is Euclidean distance from the
per-ingredient median of the variant's proportion matrix (g-per-100g);
median chosen over mean so the outlier doesn't pull its own center,
missing ingredients treated as 0.0. Flagging is surfaced, not
auto-dropped (per § Failure mode B). Bead: `RationalRecipes-0g3`.

**Phase 4 — scale**
Broaden beyond the test case to many dish families. Swap in a larger LLM
if parse-accuracy measurements show it's worth it.
Productionize the review UI if it's getting heavy use.

*Results (2026-04-24):*

- **Model swap executed.** Production default swapped from Gemma 4 e2b
  to `qwen3.6:35b-a3b` following the v2 benchmark sweep across 14
  candidates (`RationalRecipes-jpp`). Eyeball validation on en/sv/de
  gold passes; ja/ru regressions remain unvalidated — native-speaker
  reviewers are unavailable (`RationalRecipes-2qk` closed 2026-04-24
  on that basis).
- **Systematic ingredient-DB coverage.** English (RecipeNLG corpus,
  `RationalRecipes-b7t.1`, miss rate 63.9% → 26.0% on 2.2M rows,
  clean-recipe fraction 1.3% → 14.6%) and Swedish (WDC ica.se +
  tasteline.com, `RationalRecipes-b7t.20`) shipped. These are the
  maintained-language scope (see § Scope).
- **Review UI** already built (see Phase 2 Results); "productionize
  if heavy use" remains deferred until heavy use emerges.

Phase 4 closed at en+sv coverage. Other languages (DE/FR/RU/IT/JA)
moved out of maintained scope — tooling is in place for a future
contributor track. Bead: `RationalRecipes-1z2`.

## Open questions (to resolve during exploration)

1. ~~**Level 1 grouping technique**~~ **RESOLVED** — normalized exact-match
   is sufficient. Shipped in `scrape/grouping.py`; `rr-discover` uses the
   same normalizer for interactive corpus exploration.
2. ~~**Level 2 clustering method**~~ **RESOLVED** — greedy single-pass
   Jaccard at threshold 0.6. Shipped in `group_by_ingredients()`. Phase 1
   validated on 114 "swedish pancakes" recipes: clean split into 49
   American-style, 42 genuine pannkakor, 4 lingonberry-sauce variants.
3. **Level 3 method extraction** — **RESOLVED for WDC, OPEN for
   RecipeNLG.** `group_by_cooking_method()` shipped in
   `scrape/grouping.py` and wired through `build_variants()` in
   `scrape/pipeline_merged.py`: partition by distinct `cookingMethod`
   tag set, singleton "unknown-method" bucket merges into the largest
   non-empty bucket, sub-groups below `min_variant_size` dropped.
   Proportion clustering as the design's follow-on signal stays open
   pending evidence that pure-method partition leaves real ratio-
   distinct clusters un-split. RecipeNLG-only input degenerates to a
   single unknown bucket (no cookingMethod field) — the LLM-method-
   extraction-from-`recipeInstructions` path is still unbuilt and is
   the load-bearing piece for closing the RecipeNLG side.
4. **Minimum group size thresholds** — **RESOLVED (defaults).** Default
   `min_group_size=3` at L1 and L2, `min_variant_size=3` at L3 (matching
   constant `DEFAULT_L3_MIN_VARIANT_SIZE` in `scrape/grouping.py`).
   Defaults match across levels because the constraint is the same:
   "enough recipes that mean ± stddev means something" — and `3` is the
   smallest N where two outliers don't completely dominate. Override
   per-level via the `--l1-min` / `--l2-min` / `--l3-min` flags on
   `scripts/scrape_merged.py`. Sensitivity sweep on 3+ dish families is
   gated on a real corpus run with the production model — captured in
   `RationalRecipes-7eo`'s acceptance criteria, deferred to live runs.
   Variants whose surviving group falls below threshold are dropped
   rather than topped up (the source corpora are fixed).
5. **Review UI shell** — **RESOLVED (form + minimum scope) / OPEN (build).**
   Terminal-based (Python, stdlib + `rich`). Minimum scope specified in
   § Human review as a first-class stage (variant-level only, no per-row
   interaction, JSON-sidecar decision persistence keyed by variant id).
   Build tracked in a dedicated bead that depends on `RationalRecipes-toj`
   (the merge must emit the `manifest.json` the shell consumes).
   Productionize only if heavy use emerges.
6. **Dedup sensitivity** — **PARTIALLY RESOLVED.** Cross-corpus near-dup
   Jaccard threshold picked at **0.5** for the initial merge (midpoint of
   the 0.4–0.6 range measured by bead 3cu on real saffranspannkaka /
   fläskpannkaka pairs). Set as a source-level default on the merge
   function, not yet a CLI flag — tune empirically once the merged stream
   produces false-positive/negative evidence. Proportion-bucket hash
   dedup (§ Deduplication) runs within-variant after normalization; its
   fingerprint coarseness is still open and tuned in the same bead
   (`RationalRecipes-toj`).
7. ~~**Gemma 4 e4b accuracy ceiling**~~ **RESOLVED** — e4b OOMs on 16 GB;
   e2b is the de facto local ceiling (F1≈0.84 Swedish, spike
   `RationalRecipes-a1k`). English A/B measurement closed in
   `RationalRecipes-5i1`. The accuracy-ceiling question itself was
   superseded by the v2 head-to-head sweep (`RationalRecipes-jpp`) across
   14 remote-hosted candidates (1–35B); local-ceiling constraints no
   longer bind the production choice because the scrape pipeline runs
   against a remote Ollama host.
8. **Non-English recipes** — **PARTIALLY RESOLVED.** The original
   question ("can the LLM handle non-English lines?") split into two
   subproblems, solved separately:

   (a) *Extraction keeping source language* — **RESOLVED.** A
       language-neutral prompt (multilingual examples + "keep the
       original language" instruction) handles Swedish, German, Russian,
       and Japanese with zero translation artifacts. No per-language
       prompt variants needed. Reference implementation in
       `src/rational_recipes/scrape/wdc.py`.

   (b) *Cross-corpus ingredient-name collision* — **RESOLVED for
       maintained scope (en+sv).** The neutral prompt deliberately
       keeps `vetemjöl` / `mjölk` / `ägg` in Swedish, so cross-corpus
       Jaccard sees 0 matches against RecipeNLG's English NER
       (`flour` / `milk` / `eggs`) without a further canonicalization
       step. `src/rational_recipes/scrape/canonical.py` (bead `3cu`)
       routes every extracted name through `IngredientFactory` at
       extraction time so downstream L2 Jaccard, cross-corpus dedup,
       and within-variant comparison all see a shared English
       vocabulary. Swedish pannkakor-family coverage ships in the
       synonym table; broader Swedish follow-on landed in
       `RationalRecipes-b7t.20` (WDC ica.se + tasteline.com). The
       `RationalRecipes-b7t.24` volume-ranked survey identified
       follow-on candidates in German, French, Russian, Italian, and
       Japanese; those languages are out of maintained scope and are
       retained as a pointer for future contributors (see § Scope).
9. ~~**Ingredient-DB coverage**~~ **RESOLVED** — Phase 1 measured ~18%
   miss rate on 10 pannkakor recipes (71 ingredient lines). Core baking
   ingredients resolve correctly; misses concentrate in specialty items.
   Bead `3cu` folded the pannkakor-scope misses (saffron, lingonberry,
   margarine, vanilla sugar, almond flour, Swedish syrup, plus Swedish
   core vocabulary) into the DB. Broader frequency-ranked additions
   landed in `RationalRecipes-b7t.1`: streaming the full RecipeNLG
   corpus via `scripts/tally_recipenlg_misses.py` (NER column as a
   pre-extracted name source, so no LLM calls needed) drove the
   English miss rate from 63.9% → 26.0% on 2.2M rows / 18.9M ingredient
   mentions over four rounds of synonym additions. The clean-recipe
   fraction (recipes where every NER name resolves) rose from 1.3% →
   14.6% — a 11× improvement that's load-bearing for the pipeline's
   "skip or keep" decision on each RecipeNLG row. Swedish equivalent
   landed in `RationalRecipes-b7t.20` (WDC ica.se + tasteline.com).
   Other languages (DE/FR/RU/IT/JA) are out of maintained scope; the
   per-language WDC LLM extraction infrastructure
   (`scripts/tally_wdc_misses.py`) is ready for a future contributor
   track — see § Scope.

## Dependencies on existing code

The pipeline should produce CSV rows in the format consumed by `read.py` so
that `rr-stats` and `rr-diff` work unchanged on the collected data. This
means respecting:

- Header row as ingredient names (looked up via `ingredient.Factory`)
- Data cells as `value unit` pairs parsed by `read.py`'s regex
- Units registered in `units.py`

If the pipeline wants richer provenance (source URL, scrape date, quality
score), that's additional columns or a sidecar file — don't break the
existing format.
