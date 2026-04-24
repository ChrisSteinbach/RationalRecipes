# Design: Full Catalog from Pipeline Output

**Status:** ✅ Live design — Phase 5 active work. Supersedes the JSON
catalog and per-query extraction choices in `docs/design/recipe-scraping.md`.
**Parent epic:** RationalRecipes-vwt (Phase 5: Populate the catalog at scale)
**Last updated:** 2026-04-24

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

Sketch below reflects the original design intent; consult `catalog_db.py`
for the live DDL.

```sql
-- One row per extracted source recipe. Corpus-tagged.
CREATE TABLE recipes (
  recipe_id      TEXT PRIMARY KEY,
  url            TEXT,
  title          TEXT,
  corpus         TEXT CHECK(corpus IN ('recipenlg', 'wdc')),
  language       TEXT,
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
  variant_id                 TEXT PRIMARY KEY,       -- 12-hex sha1
  normalized_title           TEXT NOT NULL,
  category                   TEXT,                   -- PWA bucket
  description                TEXT,
  cooking_methods            TEXT,                   -- sorted csv
  canonical_ingredient_set   TEXT NOT NULL,          -- sorted csv
  n_recipes                  INTEGER NOT NULL,
  review_status              TEXT,                   -- accept|drop|annotate
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
CREATE TABLE variant_ingredient_stats (
  variant_id      TEXT NOT NULL REFERENCES variants(variant_id),
  canonical_name  TEXT NOT NULL,
  mean_proportion REAL NOT NULL,  -- g / 100g total
  stddev          REAL,
  ci_lower        REAL,
  ci_upper        REAL,
  n_nonzero       INTEGER NOT NULL,
  PRIMARY KEY (variant_id, canonical_name)
);

-- Incremental-build log: which L1 groups have been processed.
CREATE TABLE query_runs (
  l1_group_key        TEXT PRIMARY KEY,  -- normalized_title
  run_at              TEXT NOT NULL,
  corpus_revisions    TEXT,              -- JSON blob
  variants_produced   INTEGER NOT NULL,
  dry                 INTEGER NOT NULL   -- 0/1
);
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

**`rr-stats` compatibility:** `merged_to_catalog.py` is retired or
rewritten as `catalog_export.py` with a `--format=csv` mode that
pulls a variant from the DB and emits an `rr-stats`-compatible CSV.
Existing CLI users keep working unchanged.

### Track 1: whole-corpus extraction

**One command.** `scripts/scrape_catalog.py` streams both corpora,
discovers dish families via L1 title grouping in memory, and runs
the rest of the pipeline on survivors.

```
stream RecipeNLG + WDC top-100
  ↓
bucket by normalize_title(title) → {l1_key: [recipe, ...]}
  ↓
drop l1 groups where len < --l1-min (default 5)
  ↓
for each surviving l1 group:
    if CatalogDB.is_l1_fresh(l1_key, corpus_revisions):
        continue                           ← resumability
    LLM-extract WDC ingredient names (bounded: group members only)
    L2 Jaccard-cluster on canonicalized names, drop <--l2-min
    L3 partition by cookingMethod, drop <--l3-min
    LLM-parse each surviving recipe's ingredient lines
    normalize_merged_row → grams + proportion dict
    compute variant_ingredient_stats per canonical ingredient
    CatalogDB.upsert_variant(variant, l1_key) (atomic transaction)
    CatalogDB.record_l1_run(...)
  ↓
emit summary: L1 groups processed / skipped / dry,
              variants produced, LLM call count, wallclock
```

**Key efficiency property.** LLM extraction only runs on recipes in
surviving L1 groups — not the full corpus. At --l1-min=5 on a 2.2M-row
RecipeNLG + 100k-row WDC input, survivors are typically a few thousand
L1 groups × a few dozen recipes each ≈ tens of thousands of recipes,
not millions. An afternoon of unattended LLM time, not weeks.

**Resumability.** Per-L1-group commit boundary. Kill mid-run → next
run checks `query_runs` and skips any group already processed with
matching corpus fingerprints. Shared LLM name-extraction cache
across groups (ingredient names overlap heavily).

**Failure handling.**
- Ollama unreachable → bail cleanly; in-progress transaction rolls back
  (DB-native); next run resumes.
- Zero-variant groups → recorded as `dry=1` so re-runs don't retry.
- Variant_id collision across runs → deterministic content, upsert is
  a no-op semantically.

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

**Catalog size budget.** 5 000 variants × avg 10 ingredients ×
numeric stats ≈ 3-5 MB SQLite binary, gzips to ~1 MB. Comparable to
`ingredients.db` (~3 MB shipped). Fine for first load. If we ever
exceed 50 MB, split by category or stream on demand — not a concern
now.

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

## Open questions

1. **L1-min default** — 5 is a guess; 3 would emit more variants with
   lower confidence. Resolve by running the pipeline at 5 and
   measuring (how many L1 groups survive, how many variants emerge
   after L2+L3, catalog size). Single real run answers this.
2. **Language heuristic scope** — whole-corpus L1 grouping doesn't
   care about language natively; a Cyrillic title group at N=20 still
   emerges. Do we filter them out pre-extraction (saves LLM time,
   drops out-of-scope titles) or at catalog-export time (slower,
   but the raw extraction stays complete)? Likely pre-extraction
   filter with an `--include-language` flag defaulting to en+sv.
3. **recipes.db size at real scale** — needs measurement on the first
   real run. Budget <20 MB shipped for reasonable PWA cold-start.
4. **Review workflow** — does review stay a CLI shell against
   `recipes.db`, or does it move in-PWA (accept/drop buttons)?
   CLI first; revisit if review becomes a bottleneck.
5. **Deterministic variant_id** — already deterministic by schema
   (sha1 of normalized title + sorted ingredient set + sorted method
   set), but the canonicalization layer uses the LLM, which must stay
   deterministic (temperature=0, seed=42 — already enforced per
   Phase 2). Corpus updates could shift the extraction; a
   `corpus_revisions` fingerprint in `query_runs` handles this.

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
