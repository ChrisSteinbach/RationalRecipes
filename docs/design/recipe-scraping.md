# Design: Automated Recipe Collection

**Status:** Draft
**Issue:** RationalRecipes-7ns (epic: RationalRecipes-b7t)
**Last updated:** 2026-04-05

## Problem

Recipe data collection is currently manual: open a search engine, read candidate
pages, transcribe ingredient proportions into CSV. This is the single biggest
bottleneck for expanding the dataset and for scaling the project's core thesis
(central-tendency averaging across many recipes yields a reliable recipe).

We need an automated pipeline that can, given a dish name, produce a set of
normalized ingredient-proportion rows suitable for the existing statistics
code in `src/rational_recipes/`.

## Goals

- **Query-driven, not site-targeted**: given a dish name, discover recipes
  across arbitrary hosts. Source diversity matters for meaningful averages.
- **Polite**: respect robots.txt, rate limits, and per-host crawl delays. Low
  per-site load.
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
- Headless-browser rendering of JS-heavy recipe sites. Start with plain HTTP
  fetching; revisit if hit rate is too low.

## Approach: query-driven crawling

Pipeline stages:

```
dish-variant target (name + reference recipe)
  → search query generation
  → web search (Google Programmable Search API)
  → URL candidates
  → per-host compliance check (robots.txt, rate limit)
  → HTTP fetch
  → structured extraction (JSON-LD) | LLM fallback
  → ingredient-line parsing (local LLM)
  → unit normalization (existing pipeline)
  → deduplication
  → variant-fit filter (category contamination)
  → outlier flagging (in-category statistics)
  → quality filter (content-farm / slop signals)
  → human review (include / exclude / defer)
  → CSV row (compatible with existing statistics code)
```

### 1. Query generation

**Unit of collection: the dish variant, not the dish name.** "Pancake" is a
family — American buttermilk, Swedish pannkakor, French crêpe, Dutch baby —
averaging across them is meaningless. The existing `sample_input/` layout
already reflects this (separate `swedish_recipe_pannkisar.csv`,
`french_recipe_crepes.csv`, `english_recipe_crepes.csv`). We keep that model:
one target = one variant = one CSV.

For each variant, generate a handful of query variants to broaden the result
set (e.g. `"swedish pannkakor recipe"`, `"pannkakor traditional"`,
`"authentic pannkakor"`, native-language variants). Start simple — a fixed
list of suffixes per variant — revisit if result diversity is poor.

**Reference-recipe anchoring.** Each variant target carries a canonical
example (initially the existing hand-curated CSV). This anchor is used
downstream by the variant-fit filter to reject category contamination.

### 2. Web search — Google Programmable Search API

**Why:** free tier (100 queries/day) is sufficient for exploration. At ~10
results per query, that's a ceiling of ~1000 candidate URLs/day, well above
what we can process and hand-verify during exploration.

**Alternatives considered:**

- **Brave Search API** — paid but permissive; revisit if/when we outgrow the
  Google free tier.
- **SearXNG (self-hosted)** — no quota, but a maintenance burden we don't
  want during exploration.
- **Scraping Google directly** — rejected (ToS violation, CAPTCHAs).

**Open:** query-per-dish budget. If one dish needs 3–5 query variants × 10
results to get a good candidate pool, 100/day supports ~5–10 dishes/day.

### 3. Compliance layer

Before fetching *any* URL:

- Fetch and cache `robots.txt` per host (TTL ~24h).
- Respect `Disallow` rules for our User-Agent.
- Honor `Crawl-delay` if specified, otherwise default to a conservative
  minimum delay per host (≥1s).
- Send a descriptive User-Agent identifying the scraper and a contact URL.
- Cap concurrent fetches per host at 1.

Per-host load is naturally low because the candidate set is spread across
many hosts by search-engine ranking.

**Library candidates:** `reppy` or `python-robotexclusionrulesparser` for
robots.txt; `httpx` for fetching.

### 4. Structured extraction (JSON-LD primary)

Parse the fetched HTML and look for `schema.org/Recipe` JSON-LD blocks.
Recipe schema is nearly universal on mainstream recipe sites because Google
rewards it with rich snippets. Extract:

- `name` (for dedup / sanity check)
- `recipeIngredient` (array of strings — still natural language)
- `recipeYield` / `recipeServings` (for per-serving normalization)
- `author`, `datePublished` (for provenance / quality signals)

**Library:** `extruct` (handles JSON-LD, microdata, RDFa).

**Fallback for pages without JSON-LD:** either skip (simplest) or use the
LLM to extract ingredients from the raw HTML text. Start with **skip**;
measure hit rate; revisit.

### 5. Ingredient-line parsing (local LLM)

Each `recipeIngredient` entry is a natural-language string. The LLM's job is
to produce structured fields:

```
"1 heaping cup flour, sifted"
→ { quantity: 1, quantity_modifier: "heaping", unit: "cup",
    ingredient: "flour", preparation: "sifted" }
```

**Model for exploration:** Gemma 4 e4b via Ollama locally. This task is
bounded and template-able — a good fit for a small model. We expect a
larger model (run on a more powerful machine later) to mostly improve
edge-case coverage.

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

### 6. Normalization

Feed parsed fields into the existing `normalize.py` / `units.py` /
`ingredient.py` pipeline. Ingredients not present in the ingredients DB
need a fallback:

- Short-term: skip the recipe, log the unknown ingredient.
- Medium-term: accumulate unknown-ingredient frequencies; hand-add the most
  common ones to the supplementary data feeding `build_db.py`.

### 7. Deduplication

Same recipe reposted across hosts inflates its weight in the average.

**Heuristic:** canonicalize each recipe as a sorted tuple of (ingredient,
rough-proportion-bucket). Hash. Near-duplicates collapse. Keep the earliest
`datePublished` as the canonical.

Needs tuning once we have real data to see false-positive/negative rates.

### 8. Quality filtering — open question

Content-farm and LLM-generated recipe pages are a real problem in 2026.
Averaging smooths noise, but if derivative/slop content dominates the top
search results we risk an echo chamber. This is distinct from taxonomic
fit (§ Taxonomic ambiguity below) — a page can be correctly the target
variant and still be derivative slop.

Candidate signals:

- Domain age / domain reputation
- Presence of author byline + about page
- Recipe metadata completeness (does it have review count? cook time? photos?)
- LLM-as-judge: have the local model rate plausibility of the recipe

**Defer this decision** until we have a hand-verified sample and can measure
how badly unfiltered averages drift. The answer may be "a light
LLM-as-judge pass is enough"; the answer may also be "no automated filter
needed — human review catches the slop." "Slop" itself is a lazy category
and needs tightening before we can filter for it.

## Taxonomic ambiguity & outlier handling

Two distinct failure modes to guard against. They look superficially similar
(a recipe that "doesn't fit") but need different treatment.

### Failure mode A: category contamination

**Problem:** different dishes sharing a name. "Pancake" catches American
buttermilk pancakes *and* Swedish pannkakor *and* Dutch babies. Averaging
across them is meaningless — their ingredient distributions don't belong to
the same underlying population.

**Defenses, in layers:**

- **Upstream (queries):** variant-specific terms, handled in § Query
  generation.
- **Reference-recipe anchoring:** each variant target has a canonical
  example. Compute a fingerprint (ingredient set + rough proportion bucket)
  and reject candidates whose distance from the reference exceeds a
  threshold.
- **Structural signals from schema.org:** cook time, cook method, yield,
  equipment mentioned in instructions. A 25-minute oven-baked recipe
  shouldn't enter a pannkakor pool even if titled "pancake."
- **Bimodality sanity check:** if the collected set shows a clearly
  bimodal distribution on key ratios, something upstream misfired. Surface
  the clusters and stop — don't silently average.

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

- Respect `robots.txt` and `Crawl-delay`.
- Identify the scraper in User-Agent with a contact URL.
- Cache aggressively to avoid re-fetching.
- Per-host rate limiting (effectively low load since work is spread across
  hosts by search ranking).
- No circumvention of paywalls, login walls, or anti-bot measures.
- Store only what's needed (ingredient lists + provenance metadata, not
  full page archives long-term).
- Treat collected data as research-use; if/when the app surfaces averaged
  recipes publicly, credit the diversity of sources (not any single one).

## Exploration plan

Phased so we fail fast on the expensive uncertainties (LLM quality, JSON-LD
coverage) before building infrastructure around them.

**Phase 0 — end-to-end manual dry run (no code)**
Pick one dish with an existing CSV (e.g. `pannkakor`). Manually: do a Google
search, open 5 pages, check JSON-LD presence, hand-run 5–10 ingredient lines
through Gemma via the CLI, compare to the existing hand-entered CSV.
**Decision point:** is the LLM output trustworthy enough to keep going?

**Phase 1 — minimal automated pipeline**
Wire stages 2–6 end-to-end for a single dish, no quality filtering, no dedup.
Hand-verify every output row against the source page. Measure: JSON-LD hit
rate, LLM parse accuracy, ingredient-DB miss rate.

**Phase 2 — dedup + variant-fit filter + review shell**
Add the dedup heuristic, reference-recipe anchoring, and a minimal review
shell (probably a notebook or TUI first). Run the full pipeline on a dish
with a known variant (pannkakor), deliberately include some contamination
candidates (American pancake URLs) to verify the variant-fit filter catches
them, and measure how much time review actually takes per dish. Compare
averaged output against the existing hand-curated CSVs — this is the
correctness oracle.

**Phase 3 — outlier flagging + quality signals**
Add outlier flagging on top of the review view. Decide whether an
automated quality (slop) filter is needed based on what review catches
"for free" in phase 2. If needed, pick the minimal set of signals that
shows measurable improvement.

**Phase 4 — scale / model swap**
Broaden to more dishes. Swap in a larger LLM on a more powerful machine if
parse-accuracy measurements from earlier phases show it's worth it.
Revisit the JSON-LD fallback question (LLM on raw HTML) if hit rate is
low. Productionize the review UI if it's getting heavy use.

## Open questions (to resolve during exploration)

1. **Variant-fit distance metric** — Jaccard on ingredient sets?
   Proportion-weighted? Something else? Measure on real data.
2. **Variant-fit rejection threshold** — too tight rejects legitimate
   diversity; too loose lets contamination through. Calibrate against
   hand-labeled examples.
3. **Review UI shell** — CLI / TUI / notebook / web? Pick by iteration
   speed first; productionize later.
4. **Quality (slop) filter necessity** — does human review catch content-
   farm content "for free", or do we need an automated pass before review?
5. **Dedup sensitivity** — how fuzzy should the ingredient-proportion
   fingerprint be before two entries are considered the same recipe?
6. **JSON-LD hit rate in practice** — do we need an LLM fallback for
   raw-HTML extraction, or is skip-and-move-on fine?
7. **Gemma 4 e4b accuracy ceiling** — where does the small model plateau?
   What fraction of lines need the bigger model?
8. **Query-variant strategy** — fixed suffix list vs LLM-generated
   per-dish queries?
9. **Non-English recipes** — how does the parsing prompt generalize to
   Swedish/French? Do we need per-language examples?
10. **Ingredient-DB coverage** — threshold at which we batch-update the DB
    vs skip recipes with unknown ingredients?

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
