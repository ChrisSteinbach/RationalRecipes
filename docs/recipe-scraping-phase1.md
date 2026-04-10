# Phase 1: RecipeNLG Scraping Pipeline

**Status:** Implemented
**Design:** [docs/design/recipe-scraping.md](design/recipe-scraping.md)
**Bead:** RationalRecipes-09f (closed)

This document describes what Phase 1 of the automated recipe collection
pipeline does, how it is structured, and how to run it. For the broader
design and open questions, see the design doc.

## What it does

Given a title query (e.g. `pannkak`, `pancake`), the pipeline:

1. Loads candidate recipes from the RecipeNLG corpus (2.2M rows)
2. Groups them by normalized title (**Level 1**)
3. Within each title group, clusters recipes by ingredient-set Jaccard
   similarity (**Level 2**) to split visually-similar dishes with
   different ingredient fingerprints
4. Parses each ingredient line through a local or remote LLM (Ollama)
   into `{quantity, unit, ingredient, preparation}` fields
5. Normalizes against the existing `units`/`ingredient` registries
6. Emits a CSV per L2 cluster compatible with `rr-stats`

The output plugs directly into the existing statistics code — no changes
to `read.py`, `normalize.py`, or `statistics.py` were required.

## Module layout

```
src/rational_recipes/scrape/
├── __init__.py
├── recipenlg.py    # CSV loader → Recipe objects
├── grouping.py     # Level 1 (title) and Level 2 (Jaccard) grouping
├── parse.py        # LLM ingredient parsing via Ollama REST API
└── pipeline.py     # End-to-end orchestration, CSV output

scripts/
├── explore_groups.py   # Fast grouping exploration (no LLM)
└── scrape_to_csv.py    # Full pipeline → CSV files

tests/
├── test_scrape_recipenlg.py
└── test_scrape_grouping.py
```

## Data flow

```
RecipeNLG full_dataset.csv
        │
        ▼
  RecipeNLGLoader.search_title(query)
        │  (streaming read, lazy filter)
        ▼
  Recipe objects
        │
        ▼
  group_by_title(recipes, min_group_size)     ← Level 1
        │
        ▼
  group_by_ingredients(group, threshold, min) ← Level 2
        │  (Jaccard on the NER column — no LLM needed for grouping)
        ▼
  parse_ingredient_lines(recipe.ingredients)  ← LLM (Ollama)
        │
        ▼
  normalize_recipe(...)                       ← units.py, ingredient.py
        │
        ▼
  PipelineResult.to_csv()                     ← rr-stats-compatible
```

## Grouping

### Level 1: title normalization

`normalize_title` applies the following steps in order:

- lowercase
- remove parenthesized/bracketed text (e.g. `"Fraspannkakor(Swedish Crisp Pancakes)"` → `"fraspannkakor"`)
- strip possessives (`'s`, `\u2019s`)
- strip trailing `recipe`/`recipes`
- collapse whitespace

Groups are then formed by exact match on the normalized form. Groups
smaller than `min_group_size` (default 5) are dropped.

This is the cheapest pass and catches the common case where the same
dish has many title spellings. It intentionally does not do fuzzy
matching or LLM canonicalization — those are reserved for later phases
if the exact-match baseline proves insufficient.

### Level 2: ingredient-set Jaccard clustering

Within each Level 1 group, each recipe is represented as a set of
ingredient names extracted from the RecipeNLG **NER column** (a
pre-extracted list of ingredient tokens that ships with the dataset).
No LLM is needed for this stage because RecipeNLG already did the NER
extraction.

Clusters are formed by a single-pass greedy assignment: iterate recipes,
assign each to the first existing cluster whose centroid has Jaccard
similarity above a threshold (default 0.6), or start a new cluster.
Clusters smaller than `min_group_size` (default 3) are dropped.

**Why this matters.** Consider the 114 recipes in the RecipeNLG "swedish
pancakes" title group. Level 2 splits them into:

- **L2[0]** — 49 recipes with `buttermilk + baking soda + flour + eggs +
  salt + sugar` — actually American-style pancakes mislabeled as
  "Swedish"
- **L2[1]** — 42 recipes with `butter + eggs + flour + milk + salt +
  sugar + water` — genuine Swedish pannkakor
- **L2[2]** — 4 recipes with `eggs + flour + lingonberries + maple syrup
  + milk + salt + vanilla` — pannkakor served with lingonberries

This is exactly the category-contamination failure mode the design doc
warns about. Averaging across all 114 "swedish pancakes" recipes would
produce meaningless ratios. Level 2 catches it cleanly.

## LLM parsing

`parse.py` calls the Ollama REST API (`/api/generate`) with `format: json`
and a few-shot system prompt containing ~12 examples covering:

- Numeric and fractional quantities (`"1 1/2"` → 1.5)
- Implicit units (`"3 eggs, separated"` → unit `MEDIUM`)
- Whole-unit sizes (`"2 large eggs"` → unit `LARGE`)
- Prepared forms (`"heavy cream, whipped"` → `ingredient: cream`,
  `preparation: whipped`)
- Abbreviation normalization (`"c."` → `cup`, `"Tbsp."` → `tbsp`)

The prompt targets **Gemma 4 e4b** as the default model, but both the
model and the Ollama base URL are parameters — any Ollama-hosted model
can be swapped in via `--model` and `--ollama-url`.

**Phase 1 measurements** on the 10 `pannkak*` recipes (71 ingredient
lines total):

| Metric                   | Result     |
|--------------------------|------------|
| LLM parse accuracy       | 71/71 (100%) |
| Ingredient-DB miss rate  | ~18%       |

All core baking ingredients (flour, milk, egg, butter, salt, sugar,
cream, water) resolved correctly against the existing database. Misses
were concentrated in specialty items: `margarine`, `saffron`,
`lingonberry sauce`, `pork fat`, `almond meal`, `blueberry jam`. These
feed directly into open question Q10 (RationalRecipes-9td): at what
threshold do we batch-update the DB vs skip recipes with unknown
ingredients?

## Output format

Each Level 2 group produces one CSV file with:

- **Header row**: ingredient names that appear in at least half the
  group's recipes (the Level 2 centroid, effectively)
- **Data rows**: one per recipe, with `value unit` strings in each cell
  (e.g. `1.5 cup`, `4 MEDIUM`, `0.5 tsp`), or `0` if the ingredient is
  absent from that recipe

The format is the exact shape consumed by `read.py`, so the existing
pipeline can process it without modification:

```bash
rr-stats output/tjack_pannkaka_l2_0.csv
```

Hand-verification of one recipe (`Tjack Pannkaka`, row 592596 from
RecipeNLG) confirmed that the pipeline output round-trips through
`read.py` → `normalize.py` to sensible gram values:

| Source line         | Parsed cell | Normalized (g) |
|---------------------|-------------|----------------|
| `4 eggs`            | `4 MEDIUM`  | 176.0          |
| `1 1/2 c. flour`    | `1.5 cup`   | 188.9          |
| `1/2 tsp. salt`     | `0.5 tsp`   | 3.0            |
| `1 1/2 c. milk`     | `1.5 cup`   | 368.7          |
| `1/4 c. butter`     | `0.25 cup`  | 57.2           |

## Running it

The RecipeNLG dataset must be present at `dataset/full_dataset.csv`
(2.2 GB download, not checked into the repo — see `.gitignore`).

### Exploring groups (fast, no LLM)

```bash
# All "pancake" title groups with at least 20 recipes, L2 clusters of 3+
python3 scripts/explore_groups.py pancake

# All pannkakor recipes regardless of group size
python3 scripts/explore_groups.py pannkak --l1-min=1 --l2-min=1

# Custom thresholds
python3 scripts/explore_groups.py "chocolate cake" --l1-min=10 --l2-threshold=0.7
```

### Full pipeline → CSV (slow — LLM calls)

```bash
# Default: local Ollama (http://localhost:11434), gemma4:e4b
python3 scripts/scrape_to_csv.py pannkak --l1-min=1 --l2-min=1 -v

# Remote Ollama with a different model
python3 scripts/scrape_to_csv.py pannkak \
    --ollama-url http://192.168.50.189:11434 \
    --model gemma4:e2b \
    --l1-min=1 --l2-min=1 -v

# Then feed the output into rr-stats
rr-stats output/tjack_pannkaka_l2_0.csv
```

### Key script flags

| Flag              | Default              | Purpose |
|-------------------|----------------------|---------|
| `--dataset`       | `dataset/full_dataset.csv` | Path to RecipeNLG CSV |
| `--l1-min`        | 3 (scrape) / 5 (explore) | Min recipes per Level 1 title group |
| `--l2-threshold`  | 0.6 (scrape) / 0.5 (explore) | Jaccard similarity to join a L2 cluster |
| `--l2-min`        | 3                    | Min recipes per Level 2 cluster |
| `--model`         | `gemma4:e4b`         | Ollama model for ingredient parsing |
| `--ollama-url`    | `http://localhost:11434` | Ollama API base URL |

Set `--l1-min=1 --l2-min=1` when working with a small corpus like the
10 pannkakor recipes — the defaults are tuned for broader queries with
hundreds of matches.

## Known limitations

- **Sequential LLM calls.** Every ingredient line is a separate Ollama
  request. ~10s per line on local `gemma4:e4b`, noticeably faster on
  the remote `gemma4:e2b`. Parsing 40+ recipes is slow.
- **Ingredient-DB coverage.** Specialty ingredients (lingonberry,
  saffron, margarine) miss the database and get dropped silently from
  output rows. This is tracked as Q10 (RationalRecipes-9td).
- **Level 1 exact match.** Recipes with creative or typo'd titles fall
  out of their true group. A fuzzy-matching step is a design-doc open
  question.
- **Level 2 greedy clustering.** Order-dependent and no centroid
  recomputation. Works well enough on the NER sets observed so far, but
  may need revisiting at scale (design doc proposes a vector DB for
  larger corpora).
- **English-only prompt.** The few-shot examples are all English.
  Non-English ingredient parsing is tracked as Q9
  (RationalRecipes-kdm).

## What's next

Phase 2 (RationalRecipes-toj) adds dedup, variant-fit filtering, and a
review shell. In parallel, a bead exists for loading the Web Data
Commons Schema.org Recipe corpus (RationalRecipes-1o6) as the "serious"
dataset with structured `cookingMethod`/`cookTime` fields required for
Level 3 grouping.
