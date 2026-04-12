# Design: Automated Recipe Collection

**Status:** Draft
**Issue:** RationalRecipes-7ns (epic: RationalRecipes-b7t)
**Last updated:** 2026-04-08

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
- **Statistically framed output**: sample size driven by confidence-interval
  width, not a fixed per-dish target.
- **Feeds the existing pipeline**: output should plug into the normalization
  and statistics code already in `src/rational_recipes/`.

## Non-goals (for now)

- Real-time / on-demand scraping from the PWA frontend. This is a batch
  research tool.
- Ingredient substitution reasoning (separate concern).
- Multilingual expansion beyond the existing Swedish/French/English samples.
  The pipeline should not *prevent* this later, but we don't optimize for it.
- Live web crawling. Archive-based data sources are sufficient for
  exploration. Targeted web search (Google Programmable Search API or
  similar) can supplement archives later if coverage gaps appear for
  specific dish variants.

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
your peril. See [`docs/wdc_recon.md`](../wdc_recon.md) for the field-by-
field comparison and the pannkakor case study.

**Role in the pipeline:** the serious dataset, and a source of structured
signals RecipeNLG lacks. Use after the pipeline is validated on RecipeNLG.
The intent is to **merge** the two corpora, not run them in parallel
indefinitely — each repairs signals the other is missing. WDC donates
`totalTime` / `cookTime` / `prepTime`, `recipeYield`, `cookingMethod`, and
`keywords`; RecipeNLG donates clean NER names and sheer volume. Merging
happens *after* Level 3 variant-splitting, so that dish-identity mismatches
between corpora (the pannkakor case: American pancakes in RecipeNLG vs
Swedish pannkakor on ica.se) get routed to the right variant before their
ratios are averaged in. Within-variant disagreement after that routing is a
signal that the variant definition is still too loose, not a reason to keep
the corpora apart.

### When live search is still useful

Archive coverage will have gaps — niche dish variants, recent recipes,
non-English sources. If a variant has too few archive hits to compute
meaningful statistics, targeted web search (Google Programmable Search API
free tier, 100 queries/day) can supplement. But this is a gap-filling
measure, not the primary data path.

## Approach: corpus-driven grouping pipeline

The original design targeted one dish variant at a time: pick a dish,
search for it, filter results. The archive-based approach inverts this:
start from the whole corpus, automatically discover dish groups, and
identify which groups have enough data to average meaningfully.

Pipeline stages:

```
recipe archive (RecipeNLG / WDC)
  → Level 1: title-based grouping
  → minimum group size filter
  → Level 2: ingredient-set grouping (split/merge within title groups)
  → minimum group size filter
  → ingredient-line parsing (local LLM)
  → unit normalization (existing pipeline)
  → Level 3: method + proportion grouping (split within L2 groups)
  → minimum group size filter
  → deduplication
  → outlier flagging
  → human review
  → CSV rows (compatible with existing statistics code)
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

**Techniques:** exact match on normalized title is the baseline. Fuzzy
matching (edit distance, token overlap) catches minor variations. LLM-based
title canonicalization is an option if fuzzy matching proves insufficient,
but adds cost.

**What it can't do:** creative titles ("Grandma's Sunday Delight"),
cross-language synonyms without explicit mapping, or structural variants
hiding behind the same name ("pancake" catches American, Swedish, Dutch,
Japanese).

**Minimum group size filter:** drop groups below a threshold (TBD — likely
in the range of 5–20 recipes). Groups too small to average meaningfully are
noise at this stage.

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

**Scaling with a vector database:** pairwise Jaccard is fine within small
title groups, but at corpus scale (millions of recipes, or Level 2 without
a prior title filter) it becomes expensive. A vector DB (e.g. ChromaDB,
Qdrant, or LanceDB) indexes recipe embeddings for approximate
nearest-neighbor search. For Level 2, embed ingredient sets as sparse
vectors (one dimension per known ingredient, binary or TF-IDF weighted) —
this is essentially what Jaccard measures, in a form that supports
sub-linear lookup. Cluster discovery becomes: pick a seed recipe, query
for its neighborhood, check coherence. New recipes (e.g. from WDC after
validating on RecipeNLG) can be added incrementally without recomputing
the full index.

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

**Model for exploration:** Gemma 4 e4b via Ollama locally. Phase 0 dry run
showed 10/10 accuracy on straightforward English ingredient lines (unicode
fractions, mixed fractions, implicit quantities, parenthetical prep notes
all handled correctly). Speed: ~12s/line effective with thinking enabled;
batching and thinking-mode tuning needed for scale.

**Prompt strategy:** few-shot, with examples covering:

- Numeric + fractional quantities ("1 1/2", "½")
- Ranges ("1-2 cups") — emit `quantity_min`/`quantity_max`
- Implicit quantities ("a pinch", "to taste") — distinct sentinel
- Compound items ("salt and pepper to taste") — split or flag
- Whole-unit items ("2 large eggs")
- Ambiguous abbreviations ("T" vs "t" vs "tbsp")

**Validation:** hand-label ~50 ingredient lines from real recipes, run them
through the LLM, eyeball accuracy. Iterate prompt until the failure modes
are well-understood.

### Normalization

Feed parsed fields into the existing `normalize.py` / `units.py` /
`ingredient.py` pipeline. Ingredients not present in the ingredients DB
need a fallback:

- Short-term: skip the recipe, log the unknown ingredient.
- Medium-term: accumulate unknown-ingredient frequencies; hand-add the most
  common ones to the supplementary data feeding `build_db.py`.

### Level 3: method + proportion grouping

Within each Level 2 ingredient-set group, split by cooking method and/or
ingredient proportions. This is the finest-grained grouping and targets
variants that share ingredients but differ in technique or ratios.

**Method signal:** cooking technique (bake vs fry vs steam), temperature,
and time. Ugnsmannkaka (oven-baked, 200°C, 30 min) and stekpannkaka
(pan-fried, stovetop, 2 min) have the same batter but are different dishes.
Method catches this; proportions alone cannot.

**Proportion signal:** ingredient ratios after normalization. Crêpes and
pannkakor are both pan-fried thin batters, but their flour:milk ratios
differ. Proportions catch this; method alone cannot.

**Combined** gives the tightest clusters — "same stuff, made the same way"
is what "meaningfully averageable" actually means.

**Scaling with a vector database:** Level 3 is where a vector DB becomes
most valuable. The embedding space is richer — ingredient proportions
(continuous values after normalization) combined with method features
(cook time, technique category) — and the clusters live in a
high-dimensional space that's hard to reason about with simple thresholds.
A vector DB makes "find recipes that are made the same way with similar
proportions" a single nearest-neighbor query. The same index serves both
grouping (cluster discovery) and the variant-fit check (is this new recipe
close to an existing group?).

**Data availability:** WDC provides structured time fields (`cookTime`,
`prepTime`, `totalTime` as ISO 8601 durations) on most rows, and a
structured `cookingMethod` field on a minority of hosts — but with strong
per-host bimodality. Globally `cookingMethod` is present in only ~2.25%
of tables, but on schema-good hosts (e.g. ICA.se: 69%) it is exactly the
"bake vs fry" enum the design wants. RecipeNLG, in contrast, has no time
or method fields at all — only free-text directions. The right framing
for the loader is **"use `cookingMethod` where present, fall back to LLM
extraction from `recipeInstructions` prose where not"**, and to expect
the mix to vary per dish family depending on which hosts serve it. This
is still an argument for WDC as the primary corpus, but a weaker one than
"WDC gives us cookingMethod for free everywhere" — see
[`docs/wdc_recon.md`](../wdc_recon.md).

**Minimum group size filter:** drop sub-groups below threshold. The
threshold connects to the CI-width criterion already in `statistics.py` —
"enough recipes to compute a confidence interval below a target width"
is the principled version of a minimum group size.

### Deduplication

Same recipe reposted across hosts inflates its weight in the average.

**Heuristic:** canonicalize each recipe as a sorted tuple of (ingredient,
rough-proportion-bucket). Hash. Near-duplicates collapse. Keep the earliest
`datePublished` as the canonical.

Needs tuning once we have real data to see false-positive/negative rates.

### Quality filtering — open question

Content-farm and LLM-generated recipe pages are a real problem in 2026.
Averaging smooths noise, but if derivative/slop content dominates the
corpus we risk an echo chamber. This is distinct from taxonomic fit
(§ Taxonomic ambiguity below) — a recipe can be correctly the target
variant and still be derivative slop.

Candidate signals:

- Domain diversity within a group (many sources > one source repeated)
- Recipe metadata completeness (review count, cook time, photos)
- LLM-as-judge: have the local model rate plausibility of the recipe

**Defer this decision** until we have a hand-verified sample and can measure
how badly unfiltered averages drift.

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
- **Bimodality sanity check:** if a group shows a clearly bimodal
  distribution on key ratios after Level 2/3, something upstream misfired.
  Surface the clusters and stop — don't silently average.

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

**Shell:** CLI, notebook, or small web UI — open question. A terminal UI
fits the project's CLI-first DNA; a notebook is easier to prototype; the
PWA frontend is the eventual home. Start wherever iteration is fastest;
decide later.

**Data labeling byproduct:** review decisions are labeled data. If we
later add an LLM-as-judge, it can be calibrated against reviewer calls.

Rather than a fixed target ("30 recipes per dish"), collect until the
confidence interval on each ingredient proportion is below a target width.
Concretely: reuse the CI machinery already in `statistics.py`, add a
termination check, stop when CI width is acceptable for the dominant
ingredients.

This is honest about what we can realistically achieve — some dishes will
have abundant online sources, others won't — and it integrates naturally
with the project's existing statistical output.

## Compliance & ethics

Archive-based data (RecipeNLG, WDC) shifts most compliance concerns
upstream — those projects handled crawl ethics when collecting the data.
Our obligations:

- Respect archive license terms (RecipeNLG: non-commercial research/
  educational use; WDC: inherits fair-use status of underlying pages).
- If supplementing with live web search later: respect `robots.txt` and
  `Crawl-delay`, identify the scraper in User-Agent, cache aggressively,
  rate-limit per host, no circumvention of paywalls or anti-bot measures.
- Store only what's needed (ingredient lists + provenance metadata, not
  full page archives long-term).
- Treat collected data as research-use; if/when the app surfaces averaged
  recipes publicly, credit the diversity of sources (not any single one).

## Exploration plan

Phased so we fail fast on the expensive uncertainties (LLM quality,
grouping effectiveness) before building infrastructure around them.

**Phase 0 — end-to-end manual dry run (no code)** ✅ DONE
Manually searched for pannkakor recipes, checked 5 pages for JSON-LD
presence (4/5 = 80% hit rate), ran 10 ingredient lines through Gemma 4 e4b
(10/10 correct), compared to existing hand-entered CSV. Decision: proceed.
Full results in bead RationalRecipes-4lm.

**Phase 1 — RecipeNLG load + Level 1/2 grouping + LLM parsing**
Load RecipeNLG, implement title-based grouping (Level 1) and ingredient-set
grouping (Level 2) with minimum group size filters. Wire up LLM ingredient
parsing and normalization for a known test case (pannkakor). Hand-verify
output rows against the source data. Measure: grouping quality (how clean
are the pannkakor groups?), LLM parse accuracy at scale, ingredient-DB
miss rate.

**Phase 2 — WDC corpus + Level 3 grouping + vector DB + dedup + review shell**
Load WDC Schema.org Table Corpus. Introduce a vector database to index
recipe embeddings (ingredient proportions + method features) for Level 2/3
grouping at corpus scale. Add method + proportion grouping (Level 3),
using WDC's structured `cookingMethod`/`cookTime` fields. Add dedup
heuristic and a minimal review shell. Validate against the existing
hand-curated CSVs as correctness oracle. Measure: how well does Level 3
separate known variants (e.g. ugnsmannkaka vs stekpannkaka)?

**Phase 3 — outlier flagging + quality signals**
Add outlier flagging on top of the review view. Decide whether an
automated quality (slop) filter is needed based on what review catches
"for free" in phase 2. If needed, pick the minimal set of signals that
shows measurable improvement.

**Phase 4 — scale**
Broaden beyond the test case to many dish families. Swap in a larger LLM
if parse-accuracy measurements show it's worth it. Supplement with
targeted web search for dish variants underrepresented in the archives.
Productionize the review UI if it's getting heavy use.

## Open questions (to resolve during exploration)

1. **Level 1 grouping technique** — is normalized exact-match sufficient,
   or do we need fuzzy matching / LLM title canonicalization? Measure on
   RecipeNLG.
2. **Level 2 clustering method** — Jaccard threshold? Hierarchical
   clustering? DBSCAN? Measure on real title groups.
3. **Level 3 method extraction** — WDC gives structured fields; RecipeNLG
   needs LLM extraction from directions text. How reliable is each?
4. **Minimum group size thresholds** — likely different at each level.
   Connect to CI-width requirements from `statistics.py`.
5. **Review UI shell** — CLI / TUI / notebook / web? Pick by iteration
   speed first; productionize later.
6. **Quality (slop) filter necessity** — does human review catch content-
   farm content "for free", or do we need an automated pass before review?
7. **Dedup sensitivity** — how fuzzy should the ingredient-proportion
   fingerprint be before two entries are considered the same recipe?
8. **Gemma 4 e4b accuracy ceiling** — where does the small model plateau?
   What fraction of lines need the bigger model?
9. **Non-English recipes** — how does the parsing prompt generalize to
   Swedish/French? Do we need per-language examples?
10. **Ingredient-DB coverage** — threshold at which we batch-update the DB
    vs skip recipes with unknown ingredients?
11. **RecipeNLG vs WDC reconciliation** — at what stage of the pipeline
    (Level 1/2 grouping, Level 3 variant-splitting, post-variant averaging)
    does the cross-corpus merge happen cleanly? Within-variant ratio
    disagreement after merge is a signal the variant definition is too
    loose, not a reason to keep the corpora apart.

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
