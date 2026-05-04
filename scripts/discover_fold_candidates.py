#!/usr/bin/env python3
"""Thin shim — entry point for fold-candidate discovery (RationalRecipes-2p6).

Logic lives in ``rational_recipes.cli.discover_fold_candidates``.
"""

from __future__ import annotations

from rational_recipes.cli.discover_fold_candidates import run

if __name__ == "__main__":
    raise SystemExit(run())
