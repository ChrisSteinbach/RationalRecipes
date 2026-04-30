#!/usr/bin/env python3
"""Thin shim — entry point for the non-English parse-cache invalidation.

Logic lives in ``rational_recipes.cli.invalidate_non_english_parses``.
Tests import that module directly; this file exists so
``python3 scripts/invalidate_non_english_parses.py`` keeps working.
"""

from __future__ import annotations

from rational_recipes.cli.invalidate_non_english_parses import main

if __name__ == "__main__":
    raise SystemExit(main())
