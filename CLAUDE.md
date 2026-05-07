# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Orientation

**Read this first, then `docs/design/recipe-drops.md` for the current direction.**

RationalRecipes averages many independent recipes for the same dish into one "central-tendency" recipe with confidence intervals. As of the **2026-05-05 pivot**, the product is a series of **researched recipe drops** — one polished, averaged recipe at a time, distributed via Bluesky/Twitter and anchored on a permanent canonical home.

The methodology (averaging quantities across many independent source recipes from RecipeNLG and WDC, with CIs) is preserved. What changed is the unit of work: per-recipe instead of whole-corpus. The human is in the loop on every drop.

**Current state (2026-05-07):** Branch `recipe-drops`. Catalog-shipping pipeline retired (commits `faaf44a` + `90e55a2`); per-recipe research workbench survives. `scrape_merged.py` now writes directly to `recipes.db` so `render_drop.py` produces complete drops on fresh extractions (`RationalRecipes-v61w`, `RationalRecipes-ehe7`). Open work tracked under the recipe-drops direction — run `bd ready` to see unblocked items. Strategic decisions resolved 2026-05-06: canonical home (`z9cz`) — static site for public artifacts (default host GitHub Pages); instruction-derivation approach (`r8hx`) — full LLM synthesis with human review. Maintainer-editor decision revised 2026-05-07 (`bl4y`): the PWA-as-editor plan was abandoned after y43 retired sql.js + recipes.db shipping (commit `f3ad7ab`); the editor moved to a localhost Streamlit app (`1t8x`, `scripts/editor.py`) and the PWA itself was retired (`n1q3`). Cadence policy (`5z8w`) deferred until the workflow is stable.

**Active design doc:** `docs/design/recipe-drops.md`. The earlier `full-catalog.md` is superseded but preserved as historical context.

## Scope guidance

- **Primary deliverable is the next drop**, not a complete catalog. Each drop is a finished artifact with central-tendency masses, CIs, and a chosen instruction set.
- **Public canonical home is a static site** (z9cz resolved 2026-05-06). Each drop's permanent record is markdown; default host GitHub Pages. Social posts (Bluesky/Twitter) link to it.
- **Maintainer editor is a Streamlit app on localhost** (`scripts/editor.py`, RationalRecipes-1t8x — bl4y revised after the PWA's sql.js retirement in y43). It reads + writes `recipes.db` directly via `CatalogDB`. Planned operations: drop source recipes from a cluster (filter), combine ingredients with equivalence ratios (substitute), reassign canonical mappings for source ingredients (h6q1/xekj — pending). The previous "PWA-as-editor" plan was abandoned because the PWA's sql.js + recipes.db path was retired in y43; the PWA itself was retired in `RationalRecipes-n1q3`.
- **Maintainer review has two surfaces.** CLI track: `scripts/review_variants.py` (extended per `RationalRecipes-sj18`). Editor track: `scripts/editor.py` (Streamlit). Both call into the same `CatalogDB` helpers — `add_filter_override` / `add_substitute_override` / `clear_override` — and share `_recompute_stats_for_variant`, so an override applied via either surface produces the same `variant_ingredient_stats`.
- **Historical design docs**: `docs/design/full-catalog.md` (Phase 5 catalog framing, superseded), `docs/design/recipe-scraping.md` (Phase 1–4 rationale), `docs/design/phase-5e-investigation.md` (closed merge-gate investigation).

## Architecture

```
corpora (RecipeNLG CSV, WDC top-100 zip)
   ↓
scripts/scrape_merged.py  (single-dish-family, on demand)
   ↓
recipes.db  (SQLite — sink for finalized variants + variant_ingredient_stats)
   ↓
scripts/review_variants.py  +  scripts/render_drop.py  (refine, fold, render)
   ↓
publication artifact (markdown + threadable text)
   ↓
canonical home + social drop
```

The `scrape/` submodule (`src/rational_recipes/scrape/`) handles loaders (RecipeNLG, WDC), grouping (L1 title / L2 ingredient-set / L3 cookingMethod), canonicalization, merging, deduplication, outlier scoring, and LLM calls via Ollama. The catalog DB writer (`catalog_db.py`) is the sink — `scrape_merged.py` invokes it on every run (per `RationalRecipes-v61w`), so variants land in `recipes.db` with `variant_ingredient_stats` populated and are immediately renderable via `scripts/render_drop.py`. CSV+manifest output is preserved as a debugging affordance (pass `--no-csv` to skip). Pass-1-style ingredient-line caching in the `parsed_ingredient_lines` table survives as an optimization for per-recipe runs.

## Key directories

| Path | Purpose |
|---|---|
| `src/rational_recipes/scrape/` | Per-recipe research workbench — loaders, grouping, canonicalization, merging, parsing (LLM + regex), outlier detection, ingredient fold (incl. salvaged `_fold_one_variant`), per-recipe pipeline (`pipeline_merged.py`) |
| `src/rational_recipes/catalog_db.py` | SQLite writer + reader + schema. `category` column preserved as a no-op; public category filter retired with z9cz |
| `src/rational_recipes/ingredient.py`, `units.py` | Ingredient + unit primitives |
| `src/rational_recipes/discover.py`, `discover_cli.py` | `rr-discover` threshold diagnostic |
| `src/rational_recipes/corpus_title_survey.py` | Title-frequency survey (feeds the recipe queue) |
| `src/rational_recipes/data/ingredients.db` | USDA/FAO ingredient DB |
| `src/rational_recipes/editor/` | Maintainer-editor helper layer (`operations.py`) — testable wrappers around `CatalogDB` consumed by `scripts/editor.py` and any future editor surface |
| `scripts/scrape_merged.py` | Per-recipe extractor (the production path under the pivot). Writes `variants` + `variant_members` + `variant_ingredient_stats` directly to `recipes.db`; `--no-csv` skips the legacy CSV+manifest output. |
| `scripts/import_merged_artifacts.py` | One-shot importer: rebuild variants from a directory's `manifest.json` + per-variant CSVs into `recipes.db`. Built for `RationalRecipes-ehe7` to retro-fit pre-v61w extractions; not needed for fresh runs. |
| `scripts/render_drop.py` | Render one `recipes.db` variant as a publication-ready markdown drop. |
| `scripts/review_variants.py` | CLI review tool against `recipes.db` |
| `scripts/editor.py` | Streamlit maintainer editor (filter / substitute / clear-override) — `streamlit run scripts/editor.py -- --db output/catalog/recipes.db`. Optional dep: `pip install -e '.[editor]'` |
| `scripts/explore_groups.py` | Quick L1/L2 grouping exploration (no LLM) |
| `scripts/corpus_title_survey.py` | Title-frequency diagnostic CLI |
| `scripts/build_db.py` | Rebuild `ingredients.db` from FDC/FAO sources |
| `dataset/` | Raw corpora (gitignored) |
| `output/catalog/recipes.db` | Per-recipe pipeline output (gitignored) |
| `docs/design/recipe-drops.md` | **Live design doc** |
| `docs/design/full-catalog.md` | Superseded Phase 5 catalog design |
| `docs/design/recipe-scraping.md` | Historical Phase 1–4 design |

## Git workflow

The `main` branch is protected — direct pushes are blocked. All changes merge via PR. The current feature branch is `recipe-drops`; the prior `corpus-driven-design-update` branch is preserved as the historical record of the catalog-shipping work.

The pivot's "merge gate" replacement is **acceptance for the pivot** (per `docs/design/recipe-drops.md`): one hand-cycle drop produced end-to-end (`RationalRecipes-ehe7`), first drop published. Strategic decisions resolved 2026-05-06 (`z9cz`, `r8hx`); cadence (`5z8w`) deferred.

## Commands

```bash
# Python tests + linting
python3 -m pytest
python3 -m ruff check .
python3 -m mypy src

# Maintainer editor (Streamlit, localhost) — RationalRecipes-1t8x
pip install -e '.[editor]'
streamlit run scripts/editor.py -- --db output/catalog/recipes.db

# Rebuild the ingredients DB from raw sources (rare — committed file covers most work)
scripts/download_data.sh
python3 scripts/build_db.py

# Per-recipe extraction (single dish family) — writes to recipes.db by default
python3 scripts/scrape_merged.py <title-substring>

# Render a variant as a drop (after extraction)
python3 scripts/render_drop.py <variant_id>

# Import legacy CSV+manifest artifacts into recipes.db (one-shot, for ehe7)
python3 scripts/import_merged_artifacts.py output/merged/<dir>/

# Find ready work
bd ready
```

Default Ollama endpoints (per `ollama-tuning-report.md`, 2026-05-07):

- **Parsing → `http://192.168.50.189:11444`** (parse-fast, NP=4, KEEP_ALIVE=5m). Default for `scrape_merged.py` and `eval_models.py` via `parse.OLLAMA_BASE_URL`.
- **Synthesis → `http://192.168.50.189:11446`** (synth-deep, NP=1, KEEP_ALIVE=0). Default for `synthesize_instructions.py` via `SYNTHESIS_OLLAMA_BASE_URL`.
- **Balanced (`:11445`, NP=2)** is dominated on both throughput and ctx ceiling — not recommended; do not point production traffic at it.
- **Legacy `:11434`** is the auto-tuned NP=8 instance — wrong for both parsing (KV cache headroom) and synthesis (no long-ctx provisioning). Fallback / debug only.

parse-fast's NP=4 only beats the alternatives under concurrent dispatch — parsing clients must run with concurrency ≥4 to see the speedup (already wired in via `RationalRecipes-e6rl`, commit `cab5c32`, `--parse-concurrency` flag, default 4).

Model historically `gemma4:e2b` (chosen for catalog-scale throughput per closed bead `vwt.18`). Under the pivot, the model choice is being reconsidered — quality matters more per-recipe than throughput; the 2n09 eval flagged `mistral-small:24b` as the strongest 24 B candidate but the production parsing default is not yet picked. Override with `--ollama-url` / `--base-url` and `--model` as needed.

## Dependencies

Python 3.12+. Runtime: `numpy`, stdlib `sqlite3`. LLM extraction: Ollama (model under reconsideration — see above). Dev: `ruff`, `mypy`, `pytest`, `pytest-cov`, `pre-commit`. Maintainer editor (optional, `[editor]` extra): `streamlit`. Declared in `pyproject.toml`.

## Conventions

- **Beads for task tracking** — see `AGENTS.md`. Do not use markdown TODOs, TaskCreate, or any other tracker.
- **Commits per item**, pushed together, single PR per feature branch (see global multi-item-sessions preference).
- **No Python docstring padding** — short single-line docstrings only. Multi-paragraph prose belongs in `docs/design/`.
- **Tests colocated** under `tests/`, one file per module.
- **Deterministic LLM calls** — `temperature=0, seed=42` in `scrape/parse.py::_ollama_generate`. Never remove these; Phase 2 proved non-determinism shifts `variant_id`s between runs.
