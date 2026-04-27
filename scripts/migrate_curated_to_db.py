#!/usr/bin/env python3
"""Thin shim — entry point for the curated-recipes → SQLite migration.

Logic lives in ``rational_recipes.cli.migrate_curated_to_db``. Tests
import that module directly; this file exists so
``python3 scripts/migrate_curated_to_db.py`` keeps working.
"""

from __future__ import annotations

from rational_recipes.cli.migrate_curated_to_db import main

if __name__ == "__main__":
    raise SystemExit(main())
