# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Orientation

**Read this first, then `docs/design/recipe-drops.md` for the current direction.**

RationalRecipes averages many independent recipes for the same dish into one "central-tendency" recipe with confidence intervals. As of the **2026-05-05 pivot**, the product is a series of **researched recipe drops** — one polished, averaged recipe at a time, distributed via Bluesky/Twitter and anchored on a permanent canonical home.

The methodology (averaging quantities across many independent source recipes from RecipeNLG and WDC, with CIs) is preserved. What changed is the unit of work: per-recipe instead of whole-corpus. The human is in the loop on every drop.

**Current state (2026-05-06):** Branch `recipe-drops`. Catalog-shipping pipeline retired (commits `faaf44a` + `90e55a2`); per-recipe research workbench survives. `scrape_merged.py` now writes directly to `recipes.db` so `render_drop.py` produces complete drops on fresh extractions (`RationalRecipes-v61w`, `RationalRecipes-ehe7`). Open work tracked under the recipe-drops direction — run `bd ready` to see unblocked items. Three substantive decisions still open: canonical home (`RationalRecipes-z9cz`), cadence policy (`5z8w`), instruction-derivation approach (`r8hx`).

**Active design doc:** `docs/design/recipe-drops.md`. The earlier `full-catalog.md` is superseded but preserved as historical context.

## Scope guidance

- **Primary deliverable is the next drop**, not a complete catalog. Each drop is a finished artifact with central-tendency masses, CIs, and a chosen instruction set.
- **The PWA's fate is pending `RationalRecipes-z9cz`.** It still works against `recipes.db` but is no longer the primary product surface. Do not invest in PWA features without resolving z9cz first.
- **Maintainer review is CLI-only.** `scripts/review_variants.py` reads `recipes.db` and persists decisions via `UPDATE variants SET review_status = ?`. Will be extended (per `RationalRecipes-sj18`) with substitution / filter / render-for-publication operations.
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
| `src/rational_recipes/catalog_db.py` | SQLite writer + reader + schema. `category` column preserved (filter still referenced by PWA pending z9cz) |
| `src/rational_recipes/ingredient.py`, `units.py` | Ingredient + unit primitives |
| `src/rational_recipes/discover.py`, `discover_cli.py` | `rr-discover` threshold diagnostic |
| `src/rational_recipes/corpus_title_survey.py` | Title-frequency survey (feeds the recipe queue) |
| `src/rational_recipes/data/ingredients.db` | USDA/FAO ingredient DB |
| `web/` | PWA — fate pending `RationalRecipes-z9cz` |
| `scripts/scrape_merged.py` | Per-recipe extractor (the production path under the pivot). Writes `variants` + `variant_members` + `variant_ingredient_stats` directly to `recipes.db`; `--no-csv` skips the legacy CSV+manifest output. |
| `scripts/import_merged_artifacts.py` | One-shot importer: rebuild variants from a directory's `manifest.json` + per-variant CSVs into `recipes.db`. Built for `RationalRecipes-ehe7` to retro-fit pre-v61w extractions; not needed for fresh runs. |
| `scripts/render_drop.py` | Render one `recipes.db` variant as a publication-ready markdown drop. |
| `scripts/review_variants.py` | CLI review tool against `recipes.db` |
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

The pivot's "merge gate" replacement is **acceptance for the pivot** (per `docs/design/recipe-drops.md`): one hand-cycle drop produced end-to-end (`RationalRecipes-ehe7`), canonical home + instruction approach decided (`z9cz`, `r8hx`), first drop published.

## Commands

```bash
# Python tests + linting
python3 -m pytest
python3 -m ruff check .
python3 -m mypy src

# PWA dev loop (fate pending z9cz)
cd web && npm install && npm run dev
# or: npm test (Vitest), npm run build

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

Default Ollama: `http://192.168.50.189:11434`, model historically `gemma4:e2b` (chosen for catalog-scale throughput per closed bead `vwt.18`). Under the pivot, the model choice is being reconsidered — quality matters more per-recipe than throughput. Override with `--ollama-url` and `--model` as needed.

## Dependencies

Python 3.12+. Runtime: `numpy`, stdlib `sqlite3`. LLM extraction: Ollama (model under reconsideration — see above). Dev: `ruff`, `mypy`, `pytest`, `pytest-cov`, `pre-commit`. Frontend: Node 20+, `vite`, `sql.js`, `vitest`. Declared in `pyproject.toml` and `web/package.json`.

## Conventions

- **Beads for task tracking** — see `AGENTS.md`. Do not use markdown TODOs, TaskCreate, or any other tracker.
- **Commits per item**, pushed together, single PR per feature branch (see global multi-item-sessions preference).
- **No Python docstring padding** — short single-line docstrings only. Multi-paragraph prose belongs in `docs/design/`.
- **Tests colocated** under `tests/`, one file per module.
- **Deterministic LLM calls** — `temperature=0, seed=42` in `scrape/parse.py::_ollama_generate`. Never remove these; Phase 2 proved non-determinism shifts `variant_id`s between runs.
