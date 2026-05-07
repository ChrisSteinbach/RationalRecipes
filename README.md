# RationalRecipes

Recipes by the central-tendency of many recipes.

Gather a corpus of recipes for a dish, normalize every ingredient to grams,
compute mean proportions with confidence intervals — produce a single
"average" recipe that reflects what most cooks actually do. The premise:
across enough independent recipes, shared structure reveals itself and the
noise averages out.

The project's current shape (as of the **2026-05-05 recipe-drops pivot**) — see `CLAUDE.md` for the live overview:

- **Python extraction pipeline** (`src/rational_recipes/`) — discovers
  dishes in existing recipe corpora (RecipeNLG, Web Data Commons), parses
  ingredient lines via a local LLM, normalizes to grams, and writes
  per-variant statistics into `recipes.db`.
- **Streamlit maintainer editor** (`scripts/editor.py`) — local-only UI
  over `recipes.db` for filter / substitute / canonical-reassign
  operations on a variant's source recipes.
- **Per-drop publication artifacts** (`docs/drops/`) — central-tendency
  recipes rendered as markdown drops (CIs + sources) for distribution
  via Bluesky/Twitter and a static canonical home.

The earlier PWA (Vite + sql.js browser app) was retired in
`RationalRecipes-n1q3` (2026-05-07); the static-site canonical home is
tracked under `RationalRecipes-z9cz`. The CSV-oriented command-line
tools (`rr-stats`, `rr-diff`) that led the Phase 0 UX were retired
earlier under `vwt.8`. The `rr-discover` diagnostic survives as a
threshold-picking aid for the extraction pipeline.

## Layout

| Path | Contents |
| --- | --- |
| `src/rational_recipes/scrape/` | RecipeNLG + WDC loaders, dish grouping, LLM parse, extraction pipeline |
| `src/rational_recipes/catalog_db.py` | SQLite schema + reader/writer (the `recipes.db` contract) |
| `src/rational_recipes/editor/` | Maintainer-editor helper layer — testable wrappers around `CatalogDB` |
| `src/rational_recipes/ingredient.py`, `units.py` | Ingredient + unit primitives used by the pipeline |
| `src/rational_recipes/discover_cli.py` | `rr-discover` — extraction-pipeline threshold diagnostic |
| `scripts/scrape_merged.py` | Per-recipe extraction driver (per the recipe-drops pivot — single dish family on demand) |
| `scripts/review_variants.py` | Maintainer CLI review tool (variant accept/drop/annotate; substitute/filter/canonical-reassign overrides) |
| `scripts/editor.py` | Streamlit maintainer editor (localhost) |
| `scripts/render_drop.py` | Render a `recipes.db` variant as a publication-ready markdown drop |
| `scripts/build_db.py` | Rebuild `ingredients.db` from USDA / FAO sources |
| `docs/design/recipe-drops.md` | Active design doc (recipe-drops pivot, 2026-05-05) |
| `docs/design/full-catalog.md` | Superseded Phase 5 catalog design |
| `docs/design/recipe-scraping.md` | Historical Phase 1-4 design |

## Quick start

```bash
# Python package (extraction pipeline + diagnostics) in editable mode
python3 -m pip install -e .

# Maintainer editor (Streamlit, localhost) — optional dep group
python3 -m pip install -e '.[editor]'
streamlit run scripts/editor.py -- --db output/catalog/recipes.db
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
