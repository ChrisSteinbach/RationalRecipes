# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

RationalRecipes is a Python CLI tool for statistical analysis and comparison of recipe ratios. It reads recipe data from CSV files (with mixed weight/volume units), normalizes everything to grams, and computes mean ratios with confidence intervals.

The codebase targets **Python 3.12+**. Source lives in `src/rational_recipes/` (standard src layout).

## Git Workflow

The `main` branch is protected — direct pushes are blocked. All changes must be merged via pull request. Always create a feature branch and open a PR.

## Commands

```bash
# Run all tests
python3 -m pytest

# Run a single test file
python3 -m pytest tests/test_ratio.py

# Run a single test method
python3 -m pytest tests/test_ratio.py::TestRatio::test_precision

# Run the stats tool (after pip install -e .)
rr-stats sample_input/crepes/swedish_recipe_pannkisar.csv -w 1000 -m milk+water

# Run the diff tool (after pip install -e .)
rr-diff sample_input/crepes/french_recipe_crepes.csv sample_input/crepes/english_recipe_crepes.csv

# Explore title/ingredient-set groups in RecipeNLG (fast, no LLM)
python3 scripts/explore_groups.py pannkak --l1-min=1 --l2-min=1

# Full scrape pipeline → CSV (slow — one LLM call per ingredient line)
python3 scripts/scrape_to_csv.py pannkak --l1-min=1 --l2-min=1 \
    --ollama-url http://localhost:11434 --model gemma4:e2b -v
```

Scrape scripts need RecipeNLG at `dataset/full_dataset.csv` (2.2 GB, gitignored) and a running Ollama instance.

Dependencies are declared in `pyproject.toml`. Runtime: `numpy`. Dev: `ruff`, `mypy`, `pytest`, `pytest-cov`, `pre-commit`.

## Architecture

Two CLI entry points (`rr-stats` and `rr-diff`, defined in `pyproject.toml`) share a common pipeline:

1. **CSV parsing** (`read.py`) — header row defines ingredients (looked up via `ingredient.Factory`), data rows are `value unit` pairs parsed via regex
2. **Unit normalization** (`normalize.py`) — all measurements converted to grams using the unit system (`units.py`). Units self-register with `units.Factory`; ingredients self-register with `ingredient.Factory`
3. **Column merging** (`merge.py`) — optional combining of ingredient columns (e.g., merge water into milk) with support for partial-percentage merges
4. **Statistics** (`statistics.py`) — normalizes to 100g proportions, computes mean/stddev/confidence intervals using numpy. Supports zero-value filtering for sparse ingredients
5. **Ratio formatting** (`ratio.py`) — `Ratio` wraps the baker's percentage values; `RatioElement` formats individual ingredients as grams/ml/whole-units. Supports per-ingredient weight restrictions
6. **Diff** (`difference.py`) — percentage difference and percentage change between two ratios

The `utils.py` module wires the pipeline together (`get_ratio_and_stats`) and handles CLI option parsing helpers. `columns.py` translates ingredient names to column indexes. `output.py` is a simple line-buffer formatter.

### Unit/Ingredient registries

`units.py` uses a module-level Factory pattern: instances register themselves at import time via class-level `_UNITS` dict. Lookup is case-insensitive with synonym support. Adding a new unit only requires defining the instance at module scope.

`ingredient.py` uses a SQLite-backed lazy lookup via `Factory.get_by_name()`. The database (`src/rational_recipes/data/ingredients.db`) is built from USDA FoodData Central SR Legacy (~8K foods with portion weights) and FAO/INFOODS Density Database v2.0 (~600 density values), plus supplementary data for ingredients not in either source. To rebuild the database:

```bash
scripts/download_data.sh   # fetch raw data to data/fdc/ and data/fao/
python3 scripts/build_db.py  # build ingredients.db (requires openpyxl)
```
