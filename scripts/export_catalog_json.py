#!/usr/bin/env python3
"""Thin shim — entry point for the catalog → JSON exporter (vwt.y43).

Logic lives in ``rational_recipes.cli.export_catalog_json``. Tests
import that module directly; this file exists so
``python3 scripts/export_catalog_json.py`` keeps working.
"""

from __future__ import annotations

from rational_recipes.cli.export_catalog_json import main

if __name__ == "__main__":
    raise SystemExit(main())
