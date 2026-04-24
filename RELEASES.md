# Dataset releases

A *dataset release* is a tagged, provenance-stamped build of the
`CuratedRecipeCatalog` JSON that ships in the PWA at
`web/public/curated_recipes.json`. Releases are how the project goes
from "ran the pipeline yesterday" to "here's the 2026.04.24 dataset,
here's what changed."

## Versioning

Date-based: `YYYY.MM.DD`, optionally with a `.N` suffix for same-day
re-releases (e.g. `2026.04.24.1` if a fix went out later the same day).
Not semver — the schema version (`version: 1` in the catalog) is the
only thing that bumps semantically, and it lives separately in
`schema/curated_recipes.schema.json`.

## Cutting a release

From the repo root, with a clean working tree:

```bash
# Hand-curated catalog (currently the live source of the PWA's data)
python3 scripts/export_curated_recipes.py \
    --dataset-version 2026.04.24 \
    --notes "Brief summary of what's new or changed since last release." \
    -o web/public/curated_recipes.json

# Or, from a merged-pipeline run
python3 scripts/merged_to_catalog.py output/merged/manifest.json \
    --dataset-version 2026.04.24 \
    --notes "..." \
    --default-category crepes \
    -o web/public/curated_recipes.json
```

Both scripts auto-populate `pipeline_revision` from `git rev-parse --short HEAD`
and `released` from today's date. Override with `--pipeline-revision` /
`--released` if you need to re-stamp an older build.

Commit and tag:

```bash
git add web/public/curated_recipes.json RELEASES.md
git commit -m "Release dataset 2026.04.24"
git tag dataset-2026.04.24
git push && git push --tags
```

## Release notes convention

Append to the `## History` section below, newest on top. One entry per
release: version, one-line summary, and any schema or methodology
changes that downstream consumers should know about.

The catalog's `metadata.notes` field carries the same summary in-band
so the PWA can surface it without a network fetch.

## Artifact storage

The catalog is checked into the repo at `web/public/curated_recipes.json`
— the PWA ships as static files, so the dataset must travel with it.
Keep releases under ~5 MB or revisit (split into shards, move to a
separate CDN, etc.).

## History

- **2026.04.24** — Initial release convention. Four hand-curated crepe
  variants (Swedish pancakes, English pannkakor, French crêpes, English
  crepes), baseline for the pipeline-driven catalogs that follow.
