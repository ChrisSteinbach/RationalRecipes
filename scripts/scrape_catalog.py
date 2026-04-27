#!/usr/bin/env python3
"""Thin shim — entry point for the whole-corpus extraction pipeline.

Logic lives in ``rational_recipes.cli.scrape_catalog``. Tests import
that module directly; this file exists so ``python3 scripts/scrape_catalog.py``
keeps working.
"""

from __future__ import annotations

from rational_recipes.cli.scrape_catalog import run

if __name__ == "__main__":
    raise SystemExit(run())
