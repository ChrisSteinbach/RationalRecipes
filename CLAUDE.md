# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Orientation

**Read this first, then `docs/design/full-catalog.md` for the current direction.**

RationalRecipes averages many independent recipes for the same dish into one "central-tendency" recipe with confidence intervals. The product is a **browser-based recipe catalog** (PWA) populated by a **Python extraction pipeline** that mines public recipe corpora (RecipeNLG, Web Data Commons). The two halves meet at a **SQLite database** served client-side via `sql.js`.

**Current state (2026-04-24):** mid-Phase 5 on branch `corpus-driven-design-update`. Phases 1-4 shipped the extraction pipeline (per-query LLM-driven, writes per-variant CSVs + `manifest.json`). Phase 5 is the rework that auto-discovers dish families over the whole corpus and replaces the JSON catalog artifact with a SQLite DB that the PWA queries directly.

**Active epic:** `RationalRecipes-vwt`. Run `bd ready` to see unblocked work. Design: `docs/design/full-catalog.md`.

## Scope guidance

- **Primary UI is the PWA** (`web/`). Everything user-facing happens there.
- **The CSV-CLI pipeline (`rr-stats`, `rr-diff`, `read.py`, `merge.py`, `sample_input/`) was removed in vwt.8.** `rr-discover` stays — it's the diagnostic for threshold-picking under bead `vwt.1`. The math modules (`statistics.py`, `ratio.py`, `ratio_format.py`, `normalize.py`) remain as the reference implementation the TS port in `web/src/` was derived from.
- **Maintainer review is CLI-only.** `scripts/review_variants.py` is the review tool. The PWA is read-only for end users — it consumes the post-review `recipes.db`. Bead `RationalRecipes-vwt.9` tracks porting the review tool from its current `manifest.json` sidecar to `recipes.db`.
- **Historical design doc `docs/design/recipe-scraping.md`** captures Phase 1-4 rationale. Read for context, but where it conflicts with `full-catalog.md` the newer doc wins.

## Architecture (target state, end of Phase 5)

```
corpora (RecipeNLG CSV, WDC top-100 zip)
   ↓
scripts/scrape_catalog.py  (whole-corpus, LLM-driven, resumable)
   ↓
recipes.db  (SQLite; schema in src/rational_recipes/catalog_db.py)
   ↓  (file copy via web/scripts/sync-catalog.mjs)
web/public/recipes.db
   ↓  (fetched + sql.js in-browser)
PWA catalog view + detail view + review UI
```

The `scrape/` submodule (`src/rational_recipes/scrape/`) handles loaders (RecipeNLG, WDC), grouping (L1 title / L2 ingredient-set / L3 cookingMethod), canonicalization, merging, deduplication, outlier scoring, and LLM calls via Ollama. The catalog DB writer (`catalog_db.py`, in-progress under bead `vwt.6`) is the sink.

## Key directories

| Path | Purpose |
|---|---|
| `src/rational_recipes/scrape/` | Extraction pipeline (**live**) |
| `src/rational_recipes/catalog_db.py` | SQLite writer + schema (**to be built** — vwt.6) |
| `src/rational_recipes/ingredient.py`, `units.py`, `normalize.py` | Normalization primitives (live, used by scrape) |
| `src/rational_recipes/data/ingredients.db` | USDA/FAO ingredient DB (live, shipped to browser) |
| `web/` | Vite + TypeScript + sql.js PWA |
| `scripts/scrape_catalog.py` | Whole-corpus batch driver (**to be built** — vwt.2) |
| `scripts/build_db.py` | Rebuild `ingredients.db` from FDC/FAO sources (live) |
| `dataset/` | Raw corpora (gitignored) |
| `output/catalog/` | Pipeline output, including `recipes.db` (gitignored — planned) |
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

Once `vwt.2` lands, the canonical extraction command becomes `python3 scripts/scrape_catalog.py --ollama-url ...`; the old per-query `scrape_merged.py` path stays until the cleanup bead.

## Dependencies

Python 3.12+. Runtime: `numpy`, stdlib `sqlite3`. LLM extraction: Ollama with `qwen3.6:35b-a3b` (production default since 2026-04-24, per bead `jpp`). Remote Ollama host required — the model is too large for a 16 GB local. Dev: `ruff`, `mypy`, `pytest`, `pytest-cov`, `pre-commit`. Frontend: Node 20+, `vite`, `sql.js`, `vitest`. Declared in `pyproject.toml` and `web/package.json`.

## Conventions

- **Beads for task tracking** — see `AGENTS.md`. Do not use markdown TODOs, TaskCreate, or any other tracker.
- **Commits per item**, pushed together, single PR per feature branch (see global multi-item-sessions preference).
- **No Python docstring padding** — short single-line docstrings only. Multi-paragraph prose belongs in `docs/design/`.
- **Tests colocated** under `tests/`, one file per module.
- **Deterministic LLM calls** — `temperature=0, seed=42` in `scrape/parse.py::_ollama_generate`. Never remove these; Phase 2 proved non-determinism shifts `variant_id`s between runs.
