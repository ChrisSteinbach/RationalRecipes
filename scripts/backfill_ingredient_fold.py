#!/usr/bin/env python3
"""Thin shim — entry point for the ingredient-fold backfill (RationalRecipes-2p6).

Logic lives in ``rational_recipes.cli.backfill_ingredient_fold``.
"""

from __future__ import annotations

from rational_recipes.cli.backfill_ingredient_fold import run

if __name__ == "__main__":
    raise SystemExit(run())
