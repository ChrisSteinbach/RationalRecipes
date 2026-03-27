# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

RationalRecipes is a Python 2 CLI tool for statistical analysis and comparison of recipe ratios. It reads recipe data from CSV files (with mixed weight/volume units), normalizes everything to grams, and computes mean ratios with confidence intervals.

The codebase targets **Python 3.12+**.

## Commands

```bash
# Run all tests
./test

# Run a single test file
PYTHONPATH=. python -m unittest tests.test_ratio

# Run a single test method
PYTHONPATH=. python -m unittest tests.test_ratio.TestRatio.test_precision

# Run the stats tool
./stats sample_input/crepes/swedish_recipe_pannkisar.csv -w 1000 -m milk+water

# Run the diff tool
./diff sample_input/crepes/french_recipe_crepes.csv sample_input/crepes/english_recipe_crepes.csv
```

Dependency: `numpy`

## Architecture

Two CLI entry points (`stats` and `diff` scripts at repo root) share a common pipeline:

1. **CSV parsing** (`read.py`) — header row defines ingredients (looked up via `ingredient.Factory`), data rows are `value unit` pairs parsed via regex
2. **Unit normalization** (`normalize.py`) — all measurements converted to grams using the unit system (`units.py`). Units self-register with `units.Factory`; ingredients self-register with `ingredient.Factory`
3. **Column merging** (`merge.py`) — optional combining of ingredient columns (e.g., merge water into milk) with support for partial-percentage merges
4. **Statistics** (`statistics.py`) — normalizes to 100g proportions, computes mean/stddev/confidence intervals using numpy. Supports zero-value filtering for sparse ingredients
5. **Ratio formatting** (`ratio.py`) — `Ratio` wraps the baker's percentage values; `RatioElement` formats individual ingredients as grams/ml/whole-units. Supports per-ingredient weight restrictions
6. **Diff** (`difference.py`) — percentage difference and percentage change between two ratios

The `utils.py` module wires the pipeline together (`get_ratio_and_stats`) and handles CLI option parsing helpers. `columns.py` translates ingredient names to column indexes. `output.py` is a simple line-buffer formatter.

### Unit/Ingredient registries

`units.py` and `ingredient.py` use a module-level Factory pattern: instances register themselves at import time via class-level `_UNITS`/`_INGREDIENTS` dicts. Lookup is case-insensitive with synonym support. Adding a new ingredient or unit only requires defining the instance at module scope.
