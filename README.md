# RationalRecipes

Recipes by the central-tendency of many recipes.

Gather a corpus of recipes for a dish, normalize every ingredient to grams,
compute mean proportions with confidence intervals — produce a single
"average" recipe that reflects what most cooks actually do. The premise:
across enough independent recipes, shared structure reveals itself and the
noise averages out.

The project has two parts that meet at a SQLite database:

- **Python pipeline** (`src/rational_recipes/`) — discovers dishes in
  existing recipe corpora (RecipeNLG, Web Data Commons), parses ingredient
  lines via a local LLM, normalizes to grams, and produces per-dish CSVs
  and stats.
- **PWA** (`web/`) — a Vite + vanilla TypeScript + sql.js browser app that
  serves the resulting recipes as a fully client-side browsable catalog.

## Layout

| Path | Contents |
| --- | --- |
| `src/rational_recipes/` | Stats library and CLIs (`rr-stats`, `rr-diff`, `rr-discover`) |
| `src/rational_recipes/scrape/` | RecipeNLG + WDC loaders, dish grouping, LLM parse, pipeline |
| `web/` | Client-side PWA (Vite + TS + sql.js) |
| `scripts/` | Database build, benchmark, and pipeline utilities |
| `sample_input/` | Worked CSV examples, one subdirectory per dish |
| `docs/design/` | Architecture decision documents |

## Quick start

```bash
# install the Python package (and CLI entry points) in editable mode
python3 -m pip install -e .

# average a set of recipes at 1000g total, merging 'water' into 'milk'
rr-stats sample_input/crepes/swedish_recipe_pannkisar.csv -w 1000 -m milk+water

# compare two recipe sets and show per-ingredient percentage differences
rr-diff sample_input/crepes/french_recipe_crepes.csv sample_input/crepes/english_recipe_crepes.csv

# discover common dish names in a RecipeNLG corpus (see 'Scrape pipeline' below)
python3 scripts/explore_groups.py pannkak --l1-min=1 --l2-min=1
```

Run any CLI with `--help` for the full option set.

## The three CLIs

### `rr-stats` — central-tendency recipe from a CSV

```
 $ rr-stats sample_input/crepes/swedish_recipe_pannkisar.csv -w 1000 -m milk+water

Recipe ratio in units of weight is 1.00:3.56:1.02:0.17:0.02 (all purpose flour:milk:egg:butter:salt)

1000g Recipe
------------
173g or 329ml all purpose flour
618g or 618ml milk
177g, 150ml or 3 egg(s) where each egg is 53g
29g or 29ml butter
3g or 2ml salt

Note: these calculations are based on 200 distinct recipe proportions. Duplicates have been removed.
```

`-v` additionally prints 95% confidence intervals for each proportion and the
minimum sample size needed to reach a given interval width — useful when
deciding whether a dataset is "enough."

### `rr-diff` — per-ingredient comparison of two recipe sets

```
 $ rr-diff sample_input/crepes/french_recipe_crepes.csv sample_input/crepes/english_recipe_crepes.csv

Ratio for data set 1 in units of weight is 1.00:1.86:0.12:0.75:0.18:0.01 (all purpose flour:milk:water:egg:butter:salt)
Ratio for data set 2 in units of weight is 1.00:2.17:0.22:1.17:0.23:0.01 (all purpose flour:milk:water:egg:butter:salt)

Percentage difference between salt proportions 58%
Percentage difference between water proportions 40%
...
Overall percentage difference = 25%
```

### `rr-discover` — find common dishes in a RecipeNLG corpus

Streams a RecipeNLG CSV, counts normalized title forms, and ranks the most
common dishes. With `--variants` it makes a second pass and splits each dish
into L2 buckets by ingredient set — useful for spotting polyglot groups
(e.g. 'pancakes' splits into American / Swedish / other variants).

## CSV input format

`rr-stats` and `rr-diff` accept CSVs where the header row is ingredient
names and each data row is `value unit` pairs. Weight and volume units may
be mixed freely.

```
Flour, Egg, Milk, Butter, Salt
1c, 1 large, 3 cups, 2 tbsp, 0.5 tsp
200g, 55gram, 0.7l, 0, 1 pinch
16oz, 2.5 medium, 2.5c, 1 stick, 0
```

Missing ingredients are written as `0`. Unit synonyms (`c`, `cup`, `cups`)
resolve through the unit registry in `src/rational_recipes/units.py`.

## Scrape pipeline

CSVs for individual dishes can be produced by hand, or by running the scrape
pipeline against one of the bundled corpus loaders. The pipeline:

1. Streams a recipe corpus (RecipeNLG CSV or Web Data Commons Schema.org
   archive).
2. Groups candidate recipes by normalized title (L1) and by ingredient set
   (L2), so that genuinely different dishes sharing a name end up in
   separate groups.
3. Parses each ingredient line via a local Ollama model (`parse.py`),
   extracting `quantity`, `unit`, `ingredient`, `preparation`.
4. Normalizes everything to grams using the ingredient database (USDA
   FoodData Central + FAO/INFOODS densities + supplementary entries in
   `src/rational_recipes/data/ingredients.db`).
5. Emits a per-group CSV in the same format as the hand-curated inputs.

```bash
# explore what dish groups exist for a prefix (fast, no LLM)
python3 scripts/explore_groups.py pannkak --l1-min=1 --l2-min=1

# full scrape → CSV (slow — one LLM call per ingredient line)
python3 scripts/scrape_to_csv.py pannkak --l1-min=1 --l2-min=1 \
    --ollama-url http://localhost:11434 -v
```

Run `scripts/scrape_to_csv.py --help` to see the default Ollama model and
override flags.

Scrape scripts need RecipeNLG at `dataset/full_dataset.csv` (2.2 GB,
gitignored) and a running Ollama instance. See
[`docs/design/recipe-scraping.md`](docs/design/recipe-scraping.md) for the
full design rationale and the corpus-first, structured-first approach.

## PWA

`web/` is a fully client-side recipe browser built with Vite, vanilla
TypeScript, and sql.js. It loads a prebuilt SQLite database into the
browser and serves a curated catalog of averaged recipes — no backend, no
API, just static hosting. See epic **RationalRecipes-f85** (tracked in
beads) for the MVP scope and progress.

```bash
cd web
npm install
npm run dev
```

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
synonym additions). Per-language misses on WDC are tracked in beads
`b7t.20` (Swedish) and siblings for DE/FR/RU/IT/JA.

## Development

```bash
python3 -m pytest            # run the full test suite
python3 -m ruff check .      # lint
python3 -m mypy src          # type check
```

Python 3.12+. Runtime deps: `numpy`. Dev deps: `ruff`, `mypy`, `pytest`,
`pytest-cov`, `pre-commit`. Declared in `pyproject.toml`.
