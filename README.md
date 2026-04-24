# RationalRecipes

Recipes by the central-tendency of many recipes.

Gather a corpus of recipes for a dish, normalize every ingredient to grams,
compute mean proportions with confidence intervals — produce a single
"average" recipe that reflects what most cooks actually do. The premise:
across enough independent recipes, shared structure reveals itself and the
noise averages out.

The project has two halves that meet at a SQLite database:

- **PWA** (`web/`) — a Vite + vanilla TypeScript + sql.js browser app that
  serves the averaged catalog as a fully client-side browsable recipe
  book. No backend, no API; just a static bundle + a prebuilt SQLite
  file. This is the primary user-facing surface.
- **Python extraction pipeline** (`src/rational_recipes/`) — discovers
  dishes in existing recipe corpora (RecipeNLG, Web Data Commons), parses
  ingredient lines via a local LLM, normalizes to grams, and writes
  per-variant statistics into `recipes.db`. The PWA reads that database.

The CSV-oriented command-line tools (`rr-stats`, `rr-diff`) that led the
Phase 0 UX were retired under bead `RationalRecipes-vwt.8` — the PWA now
covers what they used to do. The `rr-discover` diagnostic survives as a
threshold-picking aid for the extraction pipeline.

## Layout

| Path | Contents |
| --- | --- |
| `web/` | Client-side PWA (Vite + TS + sql.js) — primary UI |
| `src/rational_recipes/scrape/` | RecipeNLG + WDC loaders, dish grouping, LLM parse, extraction pipeline |
| `src/rational_recipes/catalog_db.py` | SQLite schema + reader/writer (the `recipes.db` contract) |
| `src/rational_recipes/ingredient.py`, `units.py` | Ingredient + unit primitives used by the pipeline |
| `src/rational_recipes/discover_cli.py` | `rr-discover` — extraction-pipeline threshold diagnostic |
| `scripts/scrape_catalog.py` | Whole-corpus batch extraction driver (LLM, resumable) |
| `scripts/review_variants.py` | Maintainer CLI review tool (variant accept/drop/annotate) |
| `scripts/migrate_curated_to_db.py` | Seed a fresh `recipes.db` from the historical curated JSON |
| `scripts/build_db.py` | Rebuild `ingredients.db` from USDA / FAO sources |
| `docs/design/full-catalog.md` | Active design doc (Phase 5) |
| `docs/design/recipe-scraping.md` | Historical Phase 1-4 design |

## Quick start

```bash
# Python package (extraction pipeline + diagnostics) in editable mode
python3 -m pip install -e .

# PWA dev loop
cd web
npm install
npm run dev
```

Run any CLI with `--help` for the full option set.

## Extraction pipeline

`scripts/scrape_catalog.py` is the canonical extraction driver. It streams
both corpora end to end, auto-discovers dish families, groups candidates
by title (L1) and ingredient set (L2), parses ingredient lines through a
local Ollama model, normalizes each line to grams via the ingredient
database, and writes per-variant statistics directly into `recipes.db`.

```bash
python3 scripts/scrape_catalog.py \
    --ollama-url http://remote-ollama:11434 \
    --output output/catalog/recipes.db
```

The run is resumable — re-invoking with the same `--output` picks up
where the previous invocation left off. Expect a full run to take hours;
see `docs/design/full-catalog.md` for the cost model and the corpus-first
rationale.

### `rr-discover` — title-frequency diagnostic

`rr-discover` streams a RecipeNLG CSV, counts normalized title forms,
and ranks the most common dishes. With `--variants` it makes a second
pass and splits each dish into L2 buckets by ingredient set — useful
for spotting polyglot groups (e.g. 'pancakes' splits into American /
Swedish / other variants) and for picking grouping thresholds before a
full extraction run.

Scrape scripts need RecipeNLG at `dataset/full_dataset.csv` (2.2 GB,
gitignored) and a running Ollama instance. See
[`docs/design/recipe-scraping.md`](docs/design/recipe-scraping.md) for
the full Phase 1-4 design rationale and
[`docs/design/full-catalog.md`](docs/design/full-catalog.md) for the
current Phase 5 direction.

## PWA

`web/` is a fully client-side recipe browser built with Vite, vanilla
TypeScript, and sql.js. It fetches the prebuilt `recipes.db` over
static hosting, loads it into the browser via sql.js, and renders a
catalog view with filters (sample size, variant count, ingredient set)
plus a detail view that produces a scaled recipe at any target weight.
No backend, no API.

```bash
cd web
npm install
npm run dev
# or: npm test (Vitest), npm run build
```

See epic **RationalRecipes-vwt** (tracked in beads) for the catalog MVP
scope and progress.

## Ingredient database

`src/rational_recipes/data/ingredients.db` is built from:

- **USDA FoodData Central SR Legacy** — ~8K foods with portion weights.
- **FAO/INFOODS Density Database v2.0** — ~600 density values for
  volume-to-weight conversion.
- Supplementary entries for ingredients not covered by either source.

To rebuild:

```bash
scripts/download_data.sh     # fetch raw data to data/fdc/ and data/fao/
python3 scripts/build_db.py  # build ingredients.db (requires openpyxl)
```

To measure coverage against a corpus:

```bash
# whole-RecipeNLG tally (~70s, no LLM — uses the NER column)
python3 scripts/tally_recipenlg_misses.py --top 30

# per-language WDC tally (slow — runs the LLM extractor, cached by URL)
python3 scripts/tally_wdc_misses.py --label swedish \
    --hosts ica.se,tasteline.com --limit 200 --top 40
```

On the full 2.2M-row RecipeNLG corpus the English miss rate sits at ~26%
after bead `RationalRecipes-b7t.1` (was 64% before the frequency-ranked
synonym additions). Swedish equivalent landed in `b7t.20` (WDC ica.se
+ tasteline.com). Other languages are out of maintained scope — see
`docs/design/recipe-scraping.md` § Scope for the contribution channel.

## Development

```bash
python3 -m pytest            # run the full test suite
python3 -m ruff check .      # lint
python3 -m mypy src          # type check
```

Python 3.12+. Runtime deps: `numpy`. Dev deps: `ruff`, `mypy`, `pytest`,
`pytest-cov`, `pre-commit`. Declared in `pyproject.toml`.
