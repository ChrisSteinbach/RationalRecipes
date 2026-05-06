# RationalRecipes — Recipe Drops

## Status

Active design doc as of 2026-05-05. Supersedes `full-catalog.md`
(catalog-shipping framing) and the closed `vwt` epic. The earlier
`recipe-scraping.md` (Phase 1–4) and `phase-5e-investigation.md`
(merge-gate investigation) remain as historical record.

## Vision

Publish averaged recipes individually as a **regular drop**: one
researched, central-tendency recipe at a time, distributed via
Bluesky/Twitter, anchored on a permanent canonical home.

The methodology — averaging quantities across many independent source
recipes from RecipeNLG and WDC, with confidence intervals — is
preserved. What changes is the unit of work: per-recipe instead of
whole-corpus. The human is in the loop on every drop, which is where
domain expertise has the highest leverage and the model's failures are
cheapest to absorb.

The pivot retires the catalog-shipping framing (vwt epic, merge gate
vwt.5, Pass 3 LLM titling, ingredient-fold backfills, category routing).
Most of the *infrastructure* survives — loaders, grouping, central-
tendency math, the review CLI — repurposed as a per-recipe research
workbench.

## Per-drop workflow

1. **Pick** a candidate from the queue (`docs/queue.md` — see
   RationalRecipes-kx9a).
2. **Probe**: run `scripts/scrape_merged.py` (or its successor) on the
   candidate title. Pulls source recipes from RecipeNLG + WDC, groups
   at L1/L2/L3, computes central tendencies.
3. **Refine**: use `scripts/review_variants.py` (extended per
   RationalRecipes-sj18) to drop outlier source recipes, fold
   equivalent ingredients, split sub-variants, approve the final
   variant.
4. **Synthesize instructions**: per RationalRecipes-r8hx — approach
   TBD (median-source / cluster-then-LLM / full-LLM-synthesis /
   skeleton+human).
5. **Render**: produce a publication-shape artifact — markdown for the
   canonical home plus threadable text for social.
6. **Publish**: post to social with a link to the canonical home;
   update the queue's "Done" section.

## What survives

Per-recipe research workbench (live):

- `src/rational_recipes/scrape/` — loaders (RecipeNLG, WDC), grouping,
  canonicalization, parsing, outlier detection, ingredient fold
  (incl. the salvaged `_fold_one_variant` for per-variant DB ops),
  merging.
- `src/rational_recipes/catalog_db.py` — SQLite read/write. The DB
  schema is preserved; the `category` column remains even though no
  one populates it (PWA filter still references it pending z9cz).
- `src/rational_recipes/ingredient.py`, `units.py` — primitives.
- `src/rational_recipes/discover.py`, `discover_cli.py` —
  `rr-discover` threshold diagnostic.
- `src/rational_recipes/corpus_title_survey.py` — title-frequency
  survey (feeds the recipe queue).
- `src/rational_recipes/data/ingredients.db` — USDA/FAO ingredient DB.
- `scripts/scrape_merged.py` — per-recipe extractor.
- `scripts/review_variants.py` — review CLI (will be extended per
  sj18).
- `scripts/explore_groups.py` — L1/L2 grouping exploration.
- `scripts/build_db.py`, `scripts/download_data.sh` — ingredients DB
  rebuild + corpus download.

## What was retired

Removed in the 2026-05-05 cleanup commits:

- 3-pass orchestrator (`scrape_catalog.py`, `scrape_progress.py`,
  `profile_pass3.py`, `catalog_pipeline.py`, `pass3_titles.py`).
- Catalog-scale backfills (`backfill_categories`,
  `backfill_ingredient_fold`, `discover_fold_candidates`,
  `invalidate_non_english_parses`).
- Catalog export and seed (`export_catalog_json`,
  `migrate_curated_to_db`).
- `categories.py` (only consumers were the doomed catalog_pipeline +
  backfill_categories).
- Driver script `rebuild-catalog.sh`.
- Historical benchmarks, shadow-compares, tally scripts.
- Tests for all of the above.

See commits `faaf44a` (Phase 1: catalog pipeline) and `90e55a2`
(Phase 2: historical scripts) for the full deletion list.

## Open decisions

The pivot has substantive decisions that the rest of the design
hangs on. Each has its own bead; this section captures them so the
rest of the doc has stable references. As of 2026-05-06: canonical
home resolved (z9cz), cadence deferred (5z8w), instruction approach
resolved (r8hx — full LLM synthesis with human review), LLM model
choice still open (2n09).

### Canonical home (RationalRecipes-z9cz) — RESOLVED 2026-05-06

Each drop's permanent public record is markdown on a **static site**
(option 1 in the original framing). Default host: GitHub Pages
(revisit if specific platform features warrant it). The static site
is the canonical URL that social posts link to; it survives platform
pivots and is SEO-indexable, which preserves the "How much salt in
chocolate chip cookies?" → indexed-page advantage that the catalog
framing originally bought.

The existing PWA is **repurposed as a maintainer-only editor**
(RationalRecipes-bl4y) for the per-drop research workflow — drop
source recipes from a cluster, reassign canonical mappings for
source ingredients, combine ingredients with equivalence ratios. It
is no longer the public catalog browser; that role moves to the
static site.

This is a hybrid of option 1 + a custom PWA-as-editor framing not
contemplated in the original three options. The category-routing
concerns that motivated `zx14` and `j54p` close with this resolution
since they were public-browse problems.

### Cadence policy (RationalRecipes-5z8w) — DEFERRED 2026-05-06

Frequency, target-vs-commitment, skip handling. Deferred until the
per-drop workflow is stable enough to ground the decision. Revisit
once several drops have shipped, per-drop time has stabilized, and
the editorial workflow (substitution review, instruction synthesis,
render) is no longer in flux.

### Instruction-derivation approach (RationalRecipes-r8hx) — RESOLVED 2026-05-06

**Full LLM synthesis (option 3) with human review.** For each
variant, the N source instruction sequences are sent to an LLM with
the variant's averaged ingredient profile; the LLM produces one
canonical instruction set, which the maintainer reviews and edits
before publication.

The Pilot step (comparing options 1 and 2 side-by-side on 5–10
already-shipped variants) was skipped — the user opted directly for
the highest-quality-ceiling approach on the assumption that
human-in-the-loop review compensates for hallucination risk and
per-drop volume keeps per-variant LLM cost trivial.

The output is treated as **generative consensus**, not measurement.
It is labeled distinctly from the central-tendency mass profile when
published, preserving the catalog's honesty about what is and isn't
empirically averaged.

Implementation gap (not yet beaded — shape TBD): synthesis pipeline,
schema column for `canonical_instructions`, review surface (CLI via
sj18 or PWA via bl4y), `render_drop.py` integration, model choice (a
sub-question of 2n09 since synthesis is a different capability
profile from structured-extraction parsing).

### LLM model choice (filed separately during this session)

Under the catalog framing, `gemma4:e2b` (5.1B Q4_K_M) was chosen for
~10× throughput over `qwen3.6:35b-a3b` per closed bead `vwt.18` —
catalog-scale Pass 1 took hours instead of weeks. Under the pivot,
throughput matters less per-recipe and quality matters more. The
remote Ollama (`http://192.168.50.189:11434`) currently exposes:

- Small / fast: `gemma3:1b`, `qwen2.5:3b`, `llama3.2:3b`,
  `nemotron-3-nano:4b`, `gemma4:e2b`, `phi3.5:latest`
- Mid: `mistral-nemo:12b`
- Large: `mistral-small:24b`, `devstral:24b`, `qwen3.5:27b`,
  `gemma4:26b`, `qwen3-coder:30b`, `nemotron-3-nano:30b`,
  `gemma4:31b`, `qwen3.6:35b-a3b`

Two distinct model needs:

- **Ingredient-line parsing** (existing `scrape/parse.py` path).
  Quality matters more than speed per drop. Candidates: revert to
  `qwen3.6:35b-a3b` (was the original; swap was throughput-driven),
  or try `gemma4:31b` / `qwen3.5:27b`.
- **Instruction synthesis** (new, per r8hx options 2/3). Should be
  evaluated independently — coherent multi-paragraph generation is a
  different capability profile from structured-extraction parsing.

See `RationalRecipes-2n09` for the dedicated bead.

## Acceptance for the pivot

The pivot is "validated" when:

- One drop has been produced end-to-end by hand (ehe7).
- Timing is acceptable for the chosen cadence (5z8w).
- Strategic decisions resolved 2026-05-06: canonical home (z9cz: static site + PWA-as-editor per bl4y) and instruction approach (r8hx: full LLM synthesis with human review). Implementation work for the synthesis pipeline still pending.
- The first drop has been published.

If those hold, the pivot is no longer experimental and the second drop
can lean on tooling extensions (sj18, kx9a).

## Architecture

```
corpora (RecipeNLG CSV, WDC top-100 zip)
   ↓
scripts/scrape_merged.py  (single-dish-family, on demand)
   ↓
recipes.db  (SQLite — sink for finalized variants only)
   ↓
scripts/review_variants.py  (refine, fold, render)
   ↓
publication artifact
   ↓
canonical home + social
```

The earlier 3-pass catalog pipeline is gone; its Pass 1 caching idea
(`parsed_ingredient_lines` table) survives in `recipes.db`'s schema
and could be reused by per-recipe runs as an optimization.

## Historical context

- `docs/design/recipe-scraping.md` — Phase 1–4 rationale (hand-curated
  era through corpus-mining).
- `docs/design/full-catalog.md` — Phase 5 catalog-shipping direction
  (superseded; preserved for historical reasoning).
- `docs/design/phase-5e-investigation.md` — investigation record from
  the merge-gate work (closed with the gate).
- Closed beads: `RationalRecipes-vwt` (epic), `vwt.5` (merge gate),
  `1cx0`, `8b1p`, `pj4f`, `wq3v`, `eklh`, `k6b5`, `3pah`, `uowz`,
  `fsiu`, `1ysm`.
