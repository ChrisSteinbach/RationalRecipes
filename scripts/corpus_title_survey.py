#!/usr/bin/env python3
"""Thin shim — entry point for the cross-corpus title-frequency survey.

Logic lives in ``rational_recipes.cli.corpus_title_survey``. Tests import
that module directly; this file exists so ``python3 scripts/corpus_title_survey.py``
keeps working.
"""

from __future__ import annotations

from rational_recipes.cli.corpus_title_survey import run

if __name__ == "__main__":
    raise SystemExit(run())
