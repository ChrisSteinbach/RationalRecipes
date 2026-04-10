## WDC Schema.org Recipe Corpus — Recon Notes

**Status:** Recon snapshot, 2026-04-10. Lifespan: until the WDC loader (`RationalRecipes-ayw`) lands and these notes get folded into orientation docs.
**Related:** [`docs/design/recipe-scraping.md`](design/recipe-scraping.md), beads `RationalRecipes-1o6` (recon), `RationalRecipes-ayw` (loader + comparison).

These are field notes from inspecting the WDC Schema.org Table Corpus 2023 Recipe subset before writing a loader. They exist to keep the loader design grounded in real data rather than the design doc's prior assumptions, several of which turned out to be wrong.

### Source

Schema.org Table Corpus 2023, Recipe subset, from <http://webdatacommons.org/structureddata/schemaorgtables/2023/index.html>. Downloads served from `https://data.dws.informatik.uni-mannheim.de/structureddata/schemaorgtables/2023/Recipe/`.

### Archive layout

Three archives, mutually exclusive partitions of the same corpus by host size:

| Archive | Size | Hosts | Rows | Notes |
|---|---|---|---|---|
| `Recipe_top100.zip` | 315 MB | 100 | 619K | The 100 largest hosts (median 4,617 rows/host) |
| `Recipe_minimum3.zip` | 1.7 GB | 33,174 | 3.36M | Hosts with ≥3 rows (median 29 rows/host) |
| `Recipe_rest.zip` | 9.5 MB | 7,727 | ~12K | Tail of 1–2-row hosts |

Total: 41,001 hosts, ~3.99M recipes.

Each archive expands to one `.json.gz` file per host, named `Recipe_<host>_October2023.json.gz`. Each `.json.gz` is JSON-Lines (one row object per line) — there is no per-table envelope; the table identity is the filename. Pandas can load a single host directly with `pd.read_json(path, lines=True, compression='gzip')`.

Currently downloaded locally: `Recipe_top100.zip` only, in `dataset/wdc/`. The other two archives are deferred until top100 proves insufficient for the dish under test. `dataset/wdc/` also contains `Recipe_jsonsample.json`, `Recipe_csvsample.csv`, and `Recipe_statistics.zip` (extracted to `stats/`).

### Per-row schema

Real example row (Food Network, "Pigs-in-a-Blanket"), abbreviated:

```json
{
  "row_id": 0,
  "name": "Everything-Spiced Pigs-in-a-Blanket ...",
  "recipeingredient": ["3 tablespoons honey", "1/4 cup sesame seeds", ...],
  "recipeinstructions": [
    {"text": "Cut each piece of puff pastry in half ..."},
    {"text": "Serve the pigs-in-a-blanket with the cherry pepper relish ..."}
  ],
  "author": {"name": "Amanda Freitag"},
  "publisher": {"name": "Food Network"},
  "datepublished": "2016-12-09T23:20:02.975-05:00",
  "recipeyield": "4 servings",
  "cooktime": "P0Y0M0DT0H35M0.000S",
  "totaltime": "P0Y0M0DT0H55M0.000S",
  "recipecategory": "appetizer",
  "keywords": "Sausage Recipes,Appetizer,High Fiber",
  "page_url": "https://www.foodnetwork.com/recipes/..."
}
```

Notable shape decisions made by WDC:

- **`recipeingredient` is a list of raw natural-language strings**, *not* pre-parsed names. (RecipeNLG's `NER` column gave us free clean names; WDC does not.)
- **`recipeinstructions` is a list of `{text: ...}` dicts**, one per step. Pre-split into steps, but the text itself is prose.
- **Time fields are ISO 8601 durations** with mixed verbosity: `PT20M` and `P0Y0M0DT0H35M0.000S` both occur. Loader needs to handle both.
- **`page_url` is per-row.** Useful for cross-corpus dedup against RecipeNLG's `link`.
- **Nested dicts** for `author`, `publisher`, `aggregaterating`, `nutrition`, `review`. Some are single dicts, some are lists, depending on host.

### Field coverage — global vs per-host

The most important finding from this recon: **field coverage is per-host bimodal**, not uniform across the corpus. Global percentages (column statistics across all 41K tables) hide that some hosts publish a field on nearly every recipe while most hosts skip it entirely.

Compare global stats to ICA.se (one of the top100 hosts, 3,258 recipes, Sweden's largest supermarket recipe site):

| Field | Global (% of tables) | ICA.se (% of rows) |
|---|---|---|
| `name` | 99% | 100% |
| `recipeingredient` | 80% | 100% |
| `recipeinstructions` | 87% | 100% |
| `recipeyield` | 72% | 99% |
| `recipecategory` | 62% | 89% |
| `preptime` | 69% | **0%** |
| `cooktime` | 66% | **0%** |
| `totaltime` | 60% | **100%** |
| **`cookingmethod`** | **2.25%** | **69.2%** |

Two implications:

1. **`cookingmethod` is not "functionally absent."** It is rare on average but heavily concentrated on schema-good hosts. ICA.se publishes it on 69% of recipes; the global average makes it look like a 2.25% curiosity. The right loader behavior is "use it where present, fall back to LLM extraction from `recipeinstructions` where not." Per-host or per-dish-family coverage matters more than the global rate.

2. **Time-field conventions vary per host.** ICA.se publishes only `totaltime`; Food Network publishes `cooktime` and `totaltime` but not `preptime`; other hosts will have other conventions. The loader should treat the three time fields as a normalized "duration set" rather than expecting any specific one.

We have no reason to believe ICA is uniquely well-behaved; it just happens to be the first non-English host I inspected. The bimodality is the headline. Per-host field profiles (a small table of which fields each top-100 host populates) would be a useful sanity-check artefact for the loader.

### Pannkakor in `top100`

`Recipe_top100.zip` is far less English-biased than I initially assumed. The 100 hosts include `ica.se`, `tasteline.com` (Swedish), `valio.fi` (Finnish), `cuisineaz.com`, `notrefamille.com` (French), `daskochrezept.de` (German), `lezizyemeklerim.com` (Turkish), `delishkitchen.tv` (Japanese), `the-challenger.ru` (Russian), `sayidaty.net` (Arabic), `bawarchi.com` (Indian), and others, alongside English aggregators like `bettycrocker.com` and `bakeitwithlove.com`.

Pannkakor counts from just two Swedish hosts:

- **ica.se**: 33 pannkakor (in 3,258 recipes)
- **tasteline.com**: 43 pannkakor (in 7,795 recipes)

That is 76 candidates *before* Level 2 splits ugnspannkaka from stekpannkaka, and *before* looking at the other 98 hosts.

`cookingmethod` on the ICA pannkakor is exactly the Level 3 split signal the design doc was hoping for, in Swedish:

| Recipe | `cookingmethod` |
|---|---|
| Fluffiga pannkakor med ricotta och citron | `Stekt` (pan-fried) |
| Proteinpannkakor | `Stekt` |
| Bananpannkakor med hasselnötskräm | `Stekt` |
| Ugnspannkaka med zucchini... | `I ugn` (in oven) |
| Äppelpannkaka med vaniljyoghurt | `I ugn` |
| Saffranspannkaka | `I ugn` |
| Dutch baby – ugnspannkaka med blåbär | `I ugn` |
| Fläskpannkaka | `Stekt,I ugn` |

About 30% of pannkakor rows have `cookingmethod` null on ICA, but where present it cleanly discriminates the variants we care about with no LLM call.

Multi-method recipes use comma-joined tags: `Stekt,I ugn`, `I ugn,Kokt`, `I ugn,Gratinerad`. The loader should parse these as a set, not a single label.

### Comparison vs RecipeNLG (relevant differences)

| Aspect | RecipeNLG | WDC |
|---|---|---|
| Total size | 2.2M recipes | ~4M recipes |
| Format | Single CSV | One JSON.gz per host |
| Ingredient names | Pre-extracted (`NER` column) | Raw lines only — needs extraction |
| Method signal | Free-text `directions` | `cookingmethod` for some hosts; otherwise `recipeinstructions` prose |
| Time fields | None | `cooktime`/`preptime`/`totaltime` (per-host conventions) |
| Yield | None | `recipeyield` (free-text) |
| Provenance | `link` per recipe | `page_url` per row + host-as-table |
| Language bias | Heavy English, US aggregators | Much more international even in top100 |
| Snapshot date | 2020 | October 2023 |

For pannkakor specifically: the comparison is **not** "same English aggregators, different parser." It is a real coverage difference — RecipeNLG points at AllRecipes-style American pancakes, top100 WDC points at ICA.se Swedish pannkakor. Disagreements between the two corpora will reveal corpus bias, not parser bias.

### Implications for the loader (`RationalRecipes-ayw`)

The loader in `src/rational_recipes/scrape/wdc.py` should:

1. **Yield `Recipe`-shaped objects compatible with `scrape/grouping.py`**, mirroring the dataclass shape from `scrape/recipenlg.py` (`title`, `ingredients`, `ner`, `source`, `link`). For WDC, `ner` cannot be filled from the source data — either leave it empty and have grouping fall back to a heuristic / LLM-derived ingredient-name extractor, or run extraction at load time. This is an active decision the loader bead needs to resolve, not a thing the recon can pre-decide.
2. **Stream rows from one `.json.gz` per host**, with the host as `source`. Don't materialize the full corpus in memory.
3. **Tolerate per-host field-conventions.** Treat `cooktime`/`preptime`/`totaltime` as a normalized duration set; treat `cookingmethod` as an optional comma-split set; treat absence of any of these as normal.
4. **Parse ISO 8601 durations** in both verbose (`P0Y0M0DT0H35M0.000S`) and short (`PT20M`) forms.
5. **Carry the WDC-only structured fields** (`cookingmethod` set, duration set, `recipecategory`, `keywords`, `recipeyield`) on optional fields, reserved for Level 3.
6. **Preserve `page_url`** so cross-corpus dedup against RecipeNLG's `link` is possible later.

### Open questions for the loader bead

- **Ingredient-name extraction at load time vs. at grouping time?** Either we run a lightweight extractor (heuristic or LLM) in the loader so WDC `Recipe.ner` looks like RecipeNLG's, or `grouping.py` learns to handle a "no NER, raw lines only" path. Decide before writing the loader.
- **`top100` vs. `minimum3`?** Top100 alone has plenty of pannkakor and the ICA-style schema-good hosts. `minimum3` adds another 33K hosts (1.7 GB) and reaches the long tail of small international sites. Defer until top100 proves insufficient.
- **Cross-corpus dedup against RecipeNLG?** RecipeNLG snapshot is 2020, WDC is October 2023; site overlap (food.com, foodnetwork.com, allrecipes) is significant but the 3-year gap means many URLs won't match. URL canonicalization (strip query strings, normalize trailing slashes) is the minimum cost; may also need ingredient-set fingerprint dedup.
- **How representative is ICA?** ICA is the first non-English schema-good host I looked at. A loader sanity check should profile field coverage across all 100 top hosts and surface the per-host distribution, not just the global average.
