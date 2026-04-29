#!/usr/bin/env python3
"""Thin shim — entry point for the scrape_catalog progress reporter.

Logic lives in ``rational_recipes.cli.scrape_progress``. Tests import
that module directly; this file exists so
``python3 scripts/scrape_progress.py`` keeps working.
"""

from __future__ import annotations

from rational_recipes.cli.scrape_progress import main

if __name__ == "__main__":
    raise SystemExit(main())
