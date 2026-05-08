# Dataset releases

> **2026-04-24 (vwt.8):** the JSON-catalog release process described
> here is retired. The PWA now reads `recipes.db` (SQLite) directly via
> sql.js instead of `curated_recipes.json`, and the JSON-building scripts
> (`scripts/export_curated_recipes.py`, `scripts/merged_to_catalog.py`)
> were removed. The new release process — cutting, versioning, and
> shipping `recipes.db` — is deferred to bead `RationalRecipes-vwt.5`
> (first real `scrape_catalog` run over the full corpus). This file
> will be rewritten against the SQLite flow once that ships.
>
> The sections below are preserved as a historical reference for what
> the JSON-catalog release convention looked like. None of the commands
> in them still work.

## Versioning

Date-based: `YYYY.MM.DD`, optionally with a `.N` suffix for same-day
re-releases (e.g. `2026.04.24.1` if a fix went out later the same day).
Not semver — the schema version (`version: 1` in the catalog) is the
only thing that bumps semantically.

## Release notes convention

Append to the `## History` section below, newest on top. One entry per
release: version, one-line summary, and any schema or methodology
changes that downstream consumers should know about.

## History

- **2026.04.24** — Initial release convention. Four hand-curated crepe
  variants (Swedish pancakes, English pannkakor, French crêpes, English
  crepes), baseline for the pipeline-driven catalogs that follow.
  Snapshot preserved at `artifacts/curated_recipes.json`.
