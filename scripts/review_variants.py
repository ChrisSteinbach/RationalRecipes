#!/usr/bin/env python3
"""Thin shim — entry point for the variant-review CLI.

Logic lives in ``rational_recipes.cli.review_variants``. Tests import
that module directly; this file exists so ``python3 scripts/review_variants.py``
keeps working.
"""

from __future__ import annotations

import sys

from rational_recipes.cli.review_variants import main

if __name__ == "__main__":
    sys.exit(main())
