# Design: Full Catalog from Pipeline Output

**Status:** ✅ Live design — Phase 5 active work. Supersedes the JSON
catalog and per-query extraction choices in `docs/design/recipe-scraping.md`.
**Parent epic:** RationalRecipes-vwt (Phase 5: Populate the catalog at scale)
**Last updated:** 2026-04-28

## Problem

Phases 1–4 (`docs/design/recipe-scraping.md`) shipped the extraction
pipeline: per-query title grouping, cross-corpus merge, LLM ingredient-
line parsing, normalization, dedup, outlier scoring, per-variant CSVs,
a `manifest.json`, and a terminal review shell. Separately, the PWA
epic (`RationalRecipes-f85`) shipped a search- and category-filterable
frontend that reads a `CuratedRecipeCatalog` JSON.

The two halves are not wired together. The PWA ships today with four
hand-curated crêpe variants (`artifacts/curated_recipes.json`), and
the pipeline has never been run at a scale large enough to populate a
useful catalog. Three concrete gaps make this a one-off demo rather
than a product:

1. **No auto-discovery of dish families.** `scripts/scrape_merged.py`
   takes one title substring per invocation — it was a dev-loop
   affordance from Phase 1. At catalog-production time the corpora
   are fixed; L1 title grouping on the whole corpus is a superset
   of every possible query and does the discovery for free.
2. **JSON catalog over flat filesystem doesn't scale.** Bead `ntm`
   (closed 2026-04-04) picked JSON over SQLite explicitly because
   *"the dataset is small (~dozens of curated recipes)"*. Phase 5's
   goal is hundreds to low-thousands of auto-extracted variants; the
   scale premise inverts. Filters, review decisions, incremental
   updates, and per-recipe provenance all want a query layer the
   JSON shape cannot provide.
3. **Catalog source is the hand-curated artifact.** Even scaling
   extraction wouldn't change what users see: `sync-catalog.mjs`
   copies `curated_recipes.json` by name.

Result: `output/merged/run4/` contains 43 extracted variants (from one
`pannkak` query run four times during determinism validation) that the
PWA cannot display, and a backing store that would choke on a real
catalog size. The branch is not merge-ready.

## Goals

- **Auto-discovery, no maintainer typing.** L1 title grouping over the
  whole corpus decides which dish families get extracted. No human
  picks queries.
- **Substantial catalog.** Hundreds to low-thousands of averaged
  variants, ceiling set by corpus content × threshold, not by how
  many commands the maintainer ran.
- **SQLite as the backing store** for the full pipeline → PWA path.
  Same file-based, sql.js-served pattern already used by
  `ingredients.db`.
- **PWA over the whole extraction output.** Every variant the pipeline
  emits with `N ≥ min_variant_size` is browseable, with search and
  filters strong enough to navigate a large set — implemented as SQL
  queries, not in-memory JSON filtering.
- **Incremental.** Re-running is idempotent (`INSERT OR REPLACE` on
  deterministic `variant_id`). Resumable at the L1-group boundary.
- **Review optional, not a gate.** Decisions decorate variants via
  `variants.review_status`; unreviewed variants still ship.

## Non-goals

- Abandoning the per-query entry point for dev. `run_merged_pipeline`
  keeps its `title_query` parameter for fast iteration on one dish
  family; the batch driver just doesn't use it.
- Live web scraping. Archive-based corpora remain the source.
- Rebuilding the PWA. The existing catalog / detail views stay; their
  data source switches from a JSON payload to a `CatalogRepo`
  backed by sql.js.
- Guaranteeing quality per variant. Statistics (mean, CI, stddev,
  outlier score) let the user judge; the catalog does not promise
  every variant is "good."

## Approach

Three tracks. Track 0 is foundational and blocks the other two.

```
Track 0 — SQLite backing store  (vwt.6)
  recipes.db schema + CatalogDB writer + CatalogRepo reader
       │
Track 1 — whole-corpus extraction  (vwt.2)
  scripts/scrape_catalog.py
    stream corpora → L1-group → threshold → LLM + L2 + L3 → upsert DB
       │
Track 2 — PWA over the DB  (vwt.3, vwt.4)
  web/src/db.ts loads recipes.db alongside ingredients.db
  catalog_view.ts queries via CatalogRepo
  filter UI compiles to WHERE clauses

Track 1' (diagnostic, not required) — corpus title-frequency survey  (vwt.1)
  scripts/corpus_title_survey.py — informs --l1-min choice
```

### Track 0: SQLite backing store (supersedes `ntm`)

The PWA already loads a SQLite DB in the browser via
`web/src/db.ts` → `ingredients.db`. Adding a `recipes.db` reuses
the same pattern (WASM build of SQLite via `sql.js`, fetched once,
opened in-memory). Zero marginal runtime cost.

**Schema** (authoritative location: `src/rational_recipes/catalog_db.py`,
single source of truth — vwt.6 shipped 2026-04-24, PR #18). The shipped
schema deviates from the original design in three ways, each documented
inline in `catalog_db.py`:

- `variants.base_ingredient` + `variants.confidence_level` added so the
  PWA's `CuratedRecipe` shape round-trips.
- `variant_ingredient_stats.n_nonzero` renamed to `min_sample_size`
  (statistical concept the PWA consumes; pipeline-produced variants fill
  via `calculate_minimum_sample_sizes`).
- `recipes.source_type` and `corpus='curated'` added to support the
  4 hand-curated seed rows alongside extracted recipes.

Stats columns are **fraction 0..1** (matching CuratedRecipe JSON), not
percent — `upsert_variant` divides the pipeline's percent-form
`MergedNormalizedRow.proportions` by 100 at write time.

Schema below matches the live DDL in `catalog_db.py`. Authoritative
source is always the `_SCHEMA` tuple in that file.

```sql
-- One row per extracted source recipe. Corpus-tagged.
CREATE TABLE recipes (
  recipe_id      TEXT PRIMARY KEY,
  url            TEXT,
  title          TEXT,
  corpus         TEXT NOT NULL
                 CHECK(corpus IN ('recipenlg', 'wdc', 'curated')),
  language       TEXT,
  source_type    TEXT DEFAULT 'url'
                 CHECK(source_type IN ('url', 'book', 'text')),
  cooking_method TEXT,            -- comma-joined tag set, nullable
  cook_time_min  INTEGER,
  total_time_min INTEGER,
  extracted_at   TEXT             -- ISO timestamp
);

-- Raw natural-language lines (pre-LLM), kept for provenance/debug.
CREATE TABLE raw_ingredients (
  recipe_id  TEXT NOT NULL REFERENCES recipes(recipe_id),
  line_index INTEGER NOT NULL,
  raw_line   TEXT NOT NULL,
  PRIMARY KEY (recipe_id, line_index)
);

-- LLM-parsed + normalized-to-grams.
CREATE TABLE parsed_ingredients (
  recipe_id       TEXT NOT NULL REFERENCES recipes(recipe_id),
  canonical_name  TEXT NOT NULL,
  quantity        REAL,
  quantity_min    REAL,
  quantity_max    REAL,
  unit            TEXT,
  grams           REAL,
  preparation     TEXT,
  PRIMARY KEY (recipe_id, canonical_name)
);

-- One row per L1/L2/L3 surviving variant.
CREATE TABLE variants (
  variant_id                 TEXT PRIMARY KEY,
  normalized_title           TEXT NOT NULL,
  display_title              TEXT,          -- Pass 3 distinctive name
  category                   TEXT,
  description                TEXT,
  base_ingredient            TEXT,          -- denominator for ratios
  cooking_methods            TEXT,          -- sorted csv
  canonical_ingredient_set   TEXT NOT NULL, -- sorted csv (all ingredients,
                                           -- including those below the
                                           -- frequency filter — provenance)
  n_recipes                  INTEGER NOT NULL,
  confidence_level           REAL,
  review_status              TEXT,          -- accept|drop|annotate
  review_note                TEXT,
  reviewed_at                TEXT
);
CREATE INDEX idx_variants_nrecipes ON variants(n_recipes);
CREATE INDEX idx_variants_category ON variants(category);
CREATE INDEX idx_variants_title    ON variants(normalized_title);

-- Which recipes back which variant; outlier_score per member.
CREATE TABLE variant_members (
  variant_id    TEXT NOT NULL REFERENCES variants(variant_id),
  recipe_id     TEXT NOT NULL REFERENCES recipes(recipe_id),
  outlier_score REAL,
  PRIMARY KEY (variant_id, recipe_id)
);

-- Materialized per-(variant × ingredient) statistics.
-- Only ingredients passing the frequency filter (vwt.26) get a row.
CREATE TABLE variant_ingredient_stats (
  variant_id       TEXT NOT NULL REFERENCES variants(variant_id),
  canonical_name   TEXT NOT NULL,
  ordinal          INTEGER NOT NULL,  -- display order
  mean_proportion  REAL NOT NULL,     -- fraction 0..1 (g / total g)
  stddev           REAL,
  ci_lower         REAL,
  ci_upper         REAL,
  ratio            REAL,              -- proportion / base_ingredient
  min_sample_size  INTEGER NOT NULL,  -- count of source recipes
                                      -- containing this ingredient
  density_g_per_ml REAL,
  whole_unit_name  TEXT,
  whole_unit_grams REAL,
  PRIMARY KEY (variant_id, canonical_name)
);

-- Attribution / provenance links per variant.
CREATE TABLE variant_sources (
  variant_id  TEXT NOT NULL REFERENCES variants(variant_id),
  ordinal     INTEGER NOT NULL,
  source_type TEXT NOT NULL
              CHECK(source_type IN ('url', 'book', 'text')),
  title       TEXT,
  ref         TEXT NOT NULL,
  PRIMARY KEY (variant_id, ordinal)
);

-- Incremental-build log: which L1 groups have been processed.
CREATE TABLE query_runs (
  l1_group_key      TEXT PRIMARY KEY,
  run_at            TEXT NOT NULL,
  corpus_revisions  TEXT,
  variants_produced INTEGER NOT NULL,
  dry               INTEGER NOT NULL CHECK(dry IN (0, 1))
);

-- Pass 1 cache: one row per parsed ingredient line. Pipeline-only;
-- not shipped to the PWA (see DB size note below).
CREATE TABLE parsed_ingredient_lines (
  corpus      TEXT NOT NULL,
  recipe_id   TEXT NOT NULL,
  line_index  INTEGER NOT NULL,
  raw_line    TEXT NOT NULL,
  parsed_json TEXT,              -- NULL = cached failure
  model       TEXT NOT NULL,
  seed        INTEGER NOT NULL,
  PRIMARY KEY (corpus, recipe_id, line_index)
);
CREATE INDEX idx_parsed_lines_text
  ON parsed_ingredient_lines(raw_line, model, seed);
```

**Writer (Python):** `src/rational_recipes/catalog_db.py` exposes
`CatalogDB`:

- `CatalogDB.open(path)` — opens or creates, runs schema migrations.
- `upsert_variant(variant: MergedVariantResult, l1_key: str)` —
  one transaction writes all referenced rows. Idempotent on
  `variant_id`. Existing rows are replaced (deterministic content by
  construction: same source recipes + same deterministic LLM =
  byte-identical writes).
- `record_l1_run(l1_key, variants_produced, dry)` — marks an L1
  group as processed for resumability.
- `is_l1_fresh(l1_key, corpus_revisions) -> bool` — returns True
  when the group has a run row matching current corpus fingerprints.

**Reader (TypeScript):** `web/src/db.ts` extended to load
`recipes.db` in parallel with `ingredients.db`. New `CatalogRepo`
type in `web/src/catalog.ts` exposes:

- `listVariants(filters): CatalogVariant[]` — compiles
  `{ minSampleSize?, category?, titleSearch?, orderBy? }` to a single
  `SELECT ... WHERE ... ORDER BY` over the `variants` table.
- `getVariant(id): CatalogVariant`
- `getVariantMembers(id): VariantMember[]` — joined with `recipes`
  for URL / corpus display.
- `getIngredientStats(id): IngredientStats[]`

**Migration of existing data:** a one-shot script reads
`artifacts/curated_recipes.json` and writes the 4 hand-curated
recipes into the same schema. The JSON file stays on disk as a
historical seed but is no longer the production source.

**`rr-stats` compatibility:** the CSV-CLI pipeline (`rr-stats`,
`rr-diff`, etc.) was removed in vwt.8 + the orphan-math cleanup.
Central-tendency math now lives inline in `catalog_db.py` (Python)
and in TypeScript in `web/src/` (PWA). `rr-discover` stays as the
diagnostic for threshold-picking.

### Track 1: whole-corpus extraction (three-pass architecture)

**One command, three passes.** `scripts/scrape_catalog.py` streams both
corpora, discovers dish families via L1 title grouping in memory, then
processes them through three passes that can be run independently via
`--pass1-only`, `--pass2-only`, `--pass3-only`.

All three passes share a common startup: both corpora are streamed into
L1 groups in memory, filtered by `--l1-min` and `--language-filter`.

**Pass 1 (LLM-bound):** parse each recipe's ingredient lines via Ollama
and persist results into the `parsed_ingredient_lines` cache table.
One row per `(corpus, recipe_id, line_index)`, keyed so re-runs are
idempotent. Line-text dedup means the same ingredient line in different
recipes reuses the cached parse. Batch-bisection fallback (vwt.21)
handles LLM failures without falling back to N×1 single-line calls.
Parallelizable via `--pass1-workers`.

**Pass 2 (no LLM):** cluster + write variants from the cache. For each
L1 group: hydrate parsed ingredients from cache, merge corpora
(cross-corpus near-dup collapse), run L2 Jaccard clustering + L3
cookingMethod partitioning, normalize to grams + proportions, compute
per-ingredient statistics, and write via `upsert_variant`. Re-runnable
for threshold sweeps (`--l2-threshold`, `--near-dup-threshold`, etc.)
without LLM cost.

```
stream RecipeNLG + WDC top-100
  ↓
bucket by normalize_title(title) → {l1_key: [recipe, ...]}
  ↓
drop l1 groups where len < --l1-min (default 5)
  ↓
Pass 1 (--pass1-only):
  for each l1 group:
    for each recipe:
      LLM-parse ingredient lines → parsed_ingredient_lines table
  ↓
Pass 2 (--pass2-only):
  for each l1 group:
    if CatalogDB.is_l1_fresh(l1_key, corpus_revisions):
        continue                           ← resumability
    hydrate parses from cache
    cross-corpus merge + near-dup collapse
    L2 Jaccard-cluster on canonicalized names, drop <--l2-min
    L3 partition by cookingMethod, drop <--l3-min
    normalize_merged_row → grams + proportion dict
    compute variant_ingredient_stats (with frequency filter, vwt.26)
    CatalogDB.upsert_variant(variant, l1_key) (atomic transaction)
    CatalogDB.record_l1_run(...)
  ↓
Pass 3 (--pass3-only):
  for each L1 group with >1 variant:
    LLM generates distinctive display_title per variant
    ('Maple Pecan Pie' vs 'Bourbon Pecan Pie')
    singletons keep normalized_title as display_title
  ↓
emit summary: L1 groups processed / skipped / dry,
              variants produced, LLM call count, wallclock
```

**Typical workflow:** warm the cache once with Pass 1 (hours, Ollama-
bound), then iterate on clustering thresholds with Pass 2 (minutes,
CPU-only), and generate titles with Pass 3 (minutes, Ollama-bound).
To force Pass 2 to reprocess all groups (e.g. after code changes to
canonicalization or stats), clear the `query_runs` table first.

**Key efficiency property.** LLM extraction only runs on recipes in
surviving L1 groups — not the full corpus. At --l1-min=5 on a 2.2M-row
RecipeNLG + 100k-row WDC input, survivors are typically ~20k L1 groups
× a few dozen recipes each. Pass 1 is the expensive step (hours);
Passes 2 and 3 are fast.

**Resumability.** Per-L1-group commit boundary in Pass 2. Kill mid-run
→ next run checks `query_runs` and skips any group already processed
with matching corpus fingerprints. Pass 1 is resumable at the recipe-
line level (existing rows are skipped via `INSERT OR REPLACE`). Pass 3
skips variants that already have a distinctive `display_title` (use
`--pass3-force` to override).

**Failure handling.**
- Ollama unreachable → bail cleanly; in-progress transaction rolls back
  (DB-native); next run resumes.
- Zero-variant groups → recorded as `dry=1` so re-runs don't retry.
- Variant_id collision across runs → deterministic content, upsert is
  a no-op semantically.

**Data quality filters applied during extraction:**

- *Prose-line filtering* (vwt.22): `loaders.py::filter_ingredient_lines`
  drops lines >150 chars, embedded URLs, or 3+ sentence-end marks at
  corpus load time, before they reach the LLM.
- *WDC page-URL dedup* (vwt.23): multiple JSON-LD Recipe entities
  sharing a `page_url` are collapsed to the one with the longest
  ingredient list, preventing variant_members PK collisions.
- *Swedish→English ingredient forcing* (vwt.25): a static dictionary
  in `canonical.py::SWEDISH_TO_ENGLISH` rewrites common Swedish
  ingredient nouns to English during canonicalization. Runs both before
  and after the synonym-DB lookup so it catches DB misses and Swedish
  canonical hits. Prevents split counting (e.g. `pekannötter` + `pecans`
  treated as two ingredients).
- *Ingredient frequency filter* (vwt.26): `_compute_ingredient_stats`
  drops ingredients appearing in fewer than 10% of a variant's source
  recipes (threshold: `INGREDIENT_FREQ_THRESHOLD`). Only fires when the
  variant has ≥5 recipes (`_INGREDIENT_FREQ_MIN_N`). Removes noise like
  ketchup-in-pecan-pie without affecting salt-in-everything. Filter
  applies at stats-write time; raw `parsed_ingredients` and
  `canonical_ingredient_set` are preserved for provenance.

### Track 1': corpus title-frequency survey (diagnostic)

**Not a prerequisite.** `scripts/corpus_title_survey.py` is an
inspection tool — stream both corpora, count normalized titles,
rank descending, write `artifacts/corpus_title_survey.json`. Useful
for picking a sensible `--l1-min` before committing a multi-hour
LLM run. Never feeds the batch driver — 5B discovers groups itself.

### Track 2: PWA over the DB

**Step 1 — load `recipes.db`.** `web/src/db.ts` fetches and opens
both DBs in parallel. `CatalogRepo` wraps the `recipes.db` handle.

**Step 2 — rewrite `catalog.ts`.** Drop `CuratedRecipeCatalog` JSON
parsing. The existing `CuratedRecipe` / `CatalogIngredient` types
stay (the UI components already consume them), but they're hydrated
from SQL rows via `CatalogRepo.getVariant(id)` rather than a JSON
payload. Type-compatible swap; existing `catalog_view.ts` and
`detail_view.ts` mostly unchanged.

**Step 3 — filters compile to SQL.** `catalog_view.ts` gets
sample-size filter (dropdown: All / ≥3 / ≥10 / ≥30) and sort control.
Both wire into `CatalogRepo.listVariants({ minSampleSize, orderBy })`
which emits one `SELECT ... FROM variants WHERE n_recipes >= ? ORDER
BY ...`. Existing search + category dropdown keep working; they
become extra SQL predicates.

Optional v2: confidence filter based on CI width of the largest-mass
ingredient per variant. Skip for now unless UX needs it.

**Step 4 — sync.** `web/scripts/sync-catalog.mjs` copies
`output/catalog/recipes.db` → `web/public/recipes.db`.
`artifacts/curated_recipes.json` kept as a `--source=curated` fallback
for offline dev without a real DB.

**Catalog size budget.** The full `recipes.db` with ~35k variants is
~345 MB, but the bulk is the pipeline-only `parsed_ingredient_lines`
cache (~100 MB) and its indexes (~57 MB). The PWA-facing tables
(`variants`, `variant_ingredient_stats`, `variant_members`, `recipes`)
plus indexes total ~80 MB. The shipped DB should strip the
`parsed_ingredient_lines` table (and potentially `query_runs`) to
stay within a reasonable PWA cold-start budget. Target: <20 MB shipped
after stripping pipeline-only tables and running `VACUUM`.

## Minimum viable variant for shipping

`--l1-min=5`, `--l2-min=3`, `--l3-min=3`. Every shipped variant has
N ≥ 3 source recipes backing its averages. Conservative releases can
filter tighter at catalog-export or PWA-filter time.

## Review integration

**Review is CLI-only.** The PWA is read-only for end users; it
consumes whatever `recipes.db` ships after the maintainer's review
pass. There is no in-PWA review mode (scope decision 2026-04-24,
recorded as the close note on bead `vwt.7`).

`scripts/review_variants.py` (existing CLI from beads `eco`/`4lf`)
gets ported from `manifest.json` + JSON-sidecar to `recipes.db` —
reads variants from the `variants` table, persists decisions via
`UPDATE variants SET review_status = ?, review_note = ?,
reviewed_at = ?`. Tracked in bead `vwt.9`.

Default PWA query filter:
`WHERE review_status IS NULL OR review_status != 'drop'` — variants
without a decision still ship; only explicit drops are hidden.
Review is a progressive cleanup, not a gate.

## Resolved questions

1. **L1-min default** — 5 confirmed. At scale: ~38k L1 groups processed,
   ~35k variants produced. The catalog is large enough that the PWA
   needs a `min_sample_size` floor or other filtering to present a
   manageable set.
2. **Language heuristic scope** — pre-extraction filter implemented as
   `--language-filter en+sv` (default). Titles outside English + Swedish
   are dropped before Pass 1. Project scope reduced to en+sv
   (2026-04-24); DE/FR/RU/IT/JA beads closed.
3. **recipes.db size at real scale** — full DB is ~345 MB, but
   ~200 MB is the `parsed_ingredient_lines` cache + indexes (pipeline-
   only). PWA-facing tables total ~80 MB pre-VACUUM. The sync step
   should strip pipeline-only tables before shipping.
4. **Review workflow** — CLI shell (`scripts/review_variants.py`,
   bead vwt.9, shipped). At 35k variants, automated filtering
   (frequency filter, `n_recipes` floor) does the heavy lifting;
   review handles stragglers. No in-PWA review mode (scope decision
   2026-04-24).
5. **Deterministic variant_id** — working as designed. `temperature=0,
   seed=42` enforced in `parse.py::_ollama_generate`. Corpus updates
   tracked via `corpus_revisions` in `query_runs`.

## Open questions

1. **PWA catalog size** — 35k variants is too many to present
   unfiltered. The PWA needs either a default `n_recipes` floor
   (e.g. ≥10) or a UI control for it. `ListFilters.min_sample_size`
   exists in `catalog_db.py`; unclear whether the PWA exposes it.
2. **Shipped DB stripping** — `sync-catalog.mjs` copies `recipes.db`
   verbatim. It should drop `parsed_ingredient_lines` and `query_runs`
   (and `VACUUM`) before copying to `web/public/`. Not yet implemented.
3. **Pass 2 performance at scale** — a full reprocess (after clearing
   `query_runs`) takes ~3 hours over ~38k groups. Profiling needed to
   identify whether the bottleneck is per-line `lookup_cached_parse`
   calls, L2 Jaccard clustering on large groups, or DB write volume.

## Plan

- **vwt.6** — SQLite foundation. Schema + `CatalogDB` writer +
  `CatalogRepo` reader + migration of the 4 hand-curated recipes.
  Blocks vwt.2, vwt.3, vwt.4.
- **vwt.1** — Corpus title-frequency survey (diagnostic). Independent
  of everything else; useful before vwt.2's first real run.
- **vwt.2** — Whole-corpus extraction (`scripts/scrape_catalog.py`).
  Writes directly to `recipes.db`. Resumable at L1-group granularity.
- **vwt.3** — PWA loads `recipes.db` via sql.js; `CatalogRepo`
  replaces JSON parsing.
- **vwt.4** — PWA filters as SQL queries.
- **vwt.5** — First real run at scale. Measure catalog size,
  variants-per-L1-group, LLM wallclock, DB misses. Merge gate:
  PWA built from real `recipes.db` shows hundreds+ of variants
  with working filters and plausible ratios.

vwt.6 is the critical path. Once it lands, vwt.2 and vwt.3 can
progress in parallel.
