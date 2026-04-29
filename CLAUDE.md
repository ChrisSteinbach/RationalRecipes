# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Orientation

**Read this first, then `docs/design/full-catalog.md` for the current direction.**

RationalRecipes averages many independent recipes for the same dish into one "central-tendency" recipe with confidence intervals. The product is a **browser-based recipe catalog** (PWA) populated by a **Python extraction pipeline** that mines public recipe corpora (RecipeNLG, Web Data Commons). The two halves meet at a **SQLite database** served client-side via `sql.js`.

**Current state (2026-04-28):** Phase 5 pipeline features complete on branch `corpus-driven-design-update`. The merge gate is `vwt.5` (full-corpus run, judge results, ship). Post-implementation polish beads (vwt.21–26) addressed batch-parse reliability, WDC dedup, prose-line filtering, Pass 3 distinctive titles, Swedish→English ingredient forcing, and ingredient frequency filtering. Shipped under Phase 5: SQLite catalog backing store (vwt.6), corpus title-frequency survey (vwt.1), three-pass whole-corpus extraction pipeline (vwt.2 + vwt.16), PWA over recipes.db (vwt.3), SQL filters (vwt.4), legacy CSV CLI removal (vwt.8 + orphan cleanup), CLI review tool ported to recipes.db (vwt.9).

**Active epic:** `RationalRecipes-vwt`. Run `bd ready` to see unblocked work. Design: `docs/design/full-catalog.md`.

## Scope guidance

- **Primary UI is the PWA** (`web/`). Everything user-facing happens there.
- **The CSV-CLI pipeline (`rr-stats`, `rr-diff`, `read.py`, `columns.py`, `merge.py`, `difference.py`, `output.py`, `utils.py`, `statistics.py`, `ratio.py`, `ratio_format.py`, `normalize.py`, `catalog.py`, `sample_input/`) was removed in vwt.8 + the orphan-math cleanup follow-up.** Central-tendency math now lives in TypeScript in `web/src/` (PWA) and inline in `catalog_db.py` (Python — `math.sqrt` for stddev, 1.96·σ/√n for CIs). `rr-discover` stays as the diagnostic for threshold-picking.
- **Maintainer review is CLI-only.** `scripts/review_variants.py` reads `recipes.db` and persists decisions via `UPDATE variants SET review_status = ?`. The PWA is read-only for end users — it consumes the post-review `recipes.db` and filters dropped variants out of the default catalog query.
- **Historical design doc `docs/design/recipe-scraping.md`** captures Phase 1-4 rationale. Read for context, but where it conflicts with `full-catalog.md` the newer doc wins.

## Architecture (target state, end of Phase 5)

```
corpora (RecipeNLG CSV, WDC top-100 zip)
   ↓
scripts/scrape_catalog.py  (three-pass, resumable)
   ├─ Pass 1 (--pass1-only): LLM-parse ingredient lines → cache table
   ├─ Pass 2 (--pass2-only): cluster + write variants from cache (no LLM)
   └─ Pass 3 (--pass3-only): generate distinctive display_titles via LLM
   ↓
recipes.db  (SQLite; schema in src/rational_recipes/catalog_db.py)
   ↓  (file copy via web/scripts/sync-catalog.mjs)
web/public/recipes.db
   ↓  (fetched + sql.js in-browser)
PWA catalog view + detail view
```

The `scrape/` submodule (`src/rational_recipes/scrape/`) handles loaders (RecipeNLG, WDC), grouping (L1 title / L2 ingredient-set / L3 cookingMethod), canonicalization, merging, deduplication, outlier scoring, and LLM calls via Ollama. The catalog DB writer (`catalog_db.py`) is the sink.

## Key directories

| Path | Purpose |
|---|---|
| `src/rational_recipes/scrape/` | Extraction pipeline (live) — loaders, grouping, canonicalization, LLM calls, `catalog_pipeline.py` (three-pass orchestrator), `pass3_titles.py` (distinctive title generation), `loaders.py` (shared loader helpers incl. prose-line filter) |
| `src/rational_recipes/catalog_db.py` | SQLite writer + reader + schema (live) — `CatalogDB.upsert_variant`, `list_variants`, `update_review_status`, ingredient frequency filter, etc. |
| `src/rational_recipes/ingredient.py`, `units.py` | Ingredient + unit primitives (live, used by scrape) |
| `src/rational_recipes/discover.py`, `discover_cli.py` | `rr-discover` diagnostic (live) |
| `src/rational_recipes/corpus_title_survey.py` | vwt.1 survey lib (live) |
| `src/rational_recipes/data/ingredients.db` | USDA/FAO ingredient DB (live, shipped to browser) |
| `web/` | Vite + TypeScript + sql.js PWA — loads `recipes.db` + `ingredients.db` client-side |
| `scripts/scrape_catalog.py` | Whole-corpus batch driver (live, vwt.2) |
| `scripts/corpus_title_survey.py` | Title-frequency diagnostic CLI (vwt.1) |
| `scripts/migrate_curated_to_db.py` | One-shot seed of 4 hand-curated recipes into recipes.db (vwt.6) |
| `scripts/review_variants.py` | CLI review tool against recipes.db (vwt.9) |
| `scripts/scrape_progress.py` | Read-only progress reporter for a running/dead `scrape_catalog` (vwt.27) |
| `scripts/build_db.py` | Rebuild `ingredients.db` from FDC/FAO sources |
| `dataset/` | Raw corpora (gitignored) |
| `output/catalog/recipes.db` | Pipeline output (gitignored) |
| `docs/design/full-catalog.md` | **Live design doc** |
| `docs/design/recipe-scraping.md` | Historical Phase 1-4 design |

## Git workflow

The `main` branch is protected — direct pushes are blocked. All changes merge via PR. Create a feature branch for any work. The current feature branch (`corpus-driven-design-update`) is explicitly **not merge-ready** — the merge gate is `vwt.5` (first real `scrape_catalog` run over full corpus, PWA built from the resulting DB, working filters, plausible ratios).

## Commands

```bash
# Python tests + linting
python3 -m pytest
python3 -m ruff check .
python3 -m mypy src

# PWA dev loop
cd web && npm install && npm run dev
# or: npm test (Vitest), npm run build

# Rebuild the ingredients DB from raw sources (rare — committed file covers most work)
scripts/download_data.sh
python3 scripts/build_db.py

# Find ready work
bd ready
```

Canonical extraction: `python3 scripts/scrape_catalog.py` (defaults: `--model gemma4:e2b`, `--ollama-url http://192.168.50.189:11434`; override either if your setup differs). The old per-query `scripts/scrape_merged.py` path stays for dev iteration on a single dish family but is no longer the catalog-production path.

## Dependencies

Python 3.12+. Runtime: `numpy`, stdlib `sqlite3`. LLM extraction: Ollama with `gemma4:e2b` (production default since 2026-04-25, per bead `vwt.18` — swapped from `qwen3.6:35b-a3b` for ~10× throughput; merge gate `vwt.5` is the validation hop, re-scrape is cheap if the catalog reads wrong). Remote Ollama host still preferred. Dev: `ruff`, `mypy`, `pytest`, `pytest-cov`, `pre-commit`. Frontend: Node 20+, `vite`, `sql.js`, `vitest`. Declared in `pyproject.toml` and `web/package.json`.

## Conventions

- **Beads for task tracking** — see `AGENTS.md`. Do not use markdown TODOs, TaskCreate, or any other tracker.
- **Commits per item**, pushed together, single PR per feature branch (see global multi-item-sessions preference).
- **No Python docstring padding** — short single-line docstrings only. Multi-paragraph prose belongs in `docs/design/`.
- **Tests colocated** under `tests/`, one file per module.
- **Deterministic LLM calls** — `temperature=0, seed=42` in `scrape/parse.py::_ollama_generate`. Never remove these; Phase 2 proved non-determinism shifts `variant_id`s between runs.
