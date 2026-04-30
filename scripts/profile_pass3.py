#!/usr/bin/env python3
"""Thin shim — entry point for the Pass 3 profiler (vwt.29).

Logic lives in ``rational_recipes.cli.profile_pass3``. Tests import
that module directly; this file exists so
``python3 scripts/profile_pass3.py`` keeps working.
"""

from __future__ import annotations

from rational_recipes.cli.profile_pass3 import main

if __name__ == "__main__":
    raise SystemExit(main())
