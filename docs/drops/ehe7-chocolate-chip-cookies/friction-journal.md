# Hand-cycle friction journal — RationalRecipes-ehe7

Captured 2026-05-05 during the autonomous run that produced the
chocolate chip cookies hand-cycle artifact. The point of this log is
to surface what tooling extensions would actually pay off in practice
(per RationalRecipes-sj18, RationalRecipes-r8hx, RationalRecipes-2n09).

## Workflow as actually executed

1. Mark RationalRecipes-ehe7 in_progress.
2. Survey the corpus title-frequency data → chocolate chip cookies has
   1,414 source recipes (RecipeNLG 1,367 + WDC 47), enough headroom.
3. Discover existing recipes.db has 337 variants but **no plain
   chocolate chip cookies** — prior catalog runs filtered by other
   substrings. Decision: kick off a fresh `scrape_merged.py` run on
   the remote Ollama (`192.168.50.189:11434`, model `gemma4:e2b`) in
   the background, work on the rest of the cycle in parallel.
4. While extraction runs, query existing variants to draft a
   render-for-publication script (sj18 prototype).
5. Render an existing variant (Brown Sugar Peanut Butter Chocolate
   Chip Cookies, n=17 — closest available approximation) to validate
   the rendering shape end-to-end.
6. When CCC extraction completes, rerender against the fresh data.
7. Capture friction throughout.

## Friction points

### F1: scrape_merged.py outputs CSVs+manifest, not recipes.db

The per-recipe extractor `scripts/scrape_merged.py` (calling
`scrape/pipeline_merged.py`) writes one CSV per variant plus a
`manifest.json`, into a directory. The review tool
`scripts/review_variants.py` reads `recipes.db`. They're not on the
same data path.

The retired `scrape_catalog.py` wrote directly to `recipes.db`. Under
the pivot, scrape_merged.py needs to either:

- Be extended to write to `recipes.db` after the CSVs land (a small
  loader pass), or
- Be replaced/wrapped by a per-recipe pipeline that writes directly
  to the DB.

**Implication for RationalRecipes-sj18**: the review-tool extension
should assume `recipes.db` is the canonical store. The bridge from
`scrape_merged.py` is a separate small piece of work.

### F2: scrape_merged.py does fresh LLM parsing — no Pass 1 cache reuse

`output/catalog/recipes.db` already has 274,030 cached parsed
ingredient lines from prior catalog runs. `pipeline_merged.py`
doesn't query the `parsed_ingredient_lines` cache; it re-parses
everything via Ollama.

Under the pivot, the cache is a free speedup for any per-recipe run
whose ingredients overlap with prior runs. Worth wiring this up
before more drops happen — would take many minutes off each run for
common ingredients.

### F3: variant_sources table is empty across the entire DB

The schema has a `variant_sources` table designed for explicit URL /
book / text references, but it has zero rows. Source attribution is
recoverable via `variant_members → recipes.url`, but it's an extra
join.

Per-drop publication wants a clean source list. Either populate
`variant_sources` during extraction or always render via the
`variant_members + recipes` join.

### F4: density_g_per_ml and whole_unit_* fields are mostly empty

The schema supports per-ingredient density and whole-unit conversions
(e.g., 1 egg = 50g), but on the inspected variant (Brown Sugar PB
CCC) every density/unit field was NULL.

For publication, "1¼ cups flour" or "3 large eggs" reads naturally;
"19% mass fraction" reads clinically. The conversion data is
available from `ingredients.db` (USDA/FAO) but not propagated into
`variant_ingredient_stats`.

**Implication**: a render extension that pulls densities from
`ingredients.db` at render time would make the output much friendlier.
Or, populate `variant_ingredient_stats.density_g_per_ml` during
extraction.

### F5: no canonical instructions stored — must fetch from source URL

Per RationalRecipes-r8hx, the hand-cycle uses option 1: take the
median (lowest-outlier-score) source recipe's instructions verbatim.
But `recipes.db` doesn't store instructions text — only ingredient
parses. Recovering instructions means re-fetching from the source
URL or going back to RecipeNLG/WDC raw data.

For one drop a week this is a manual paste. For ten drops it would
hurt. Two cheap options:

- Cache the full source recipe (instructions + everything) in
  `recipes.db` during extraction.
- Lookup-on-demand via the corpus loaders.

### F6: review_variants.py is interactive only — no scriptable render

The review tool is a keystroke loop (a/d/n/?/q). For the hand-cycle,
I wrote a separate `scripts/render_drop.py` that queries the DB
directly. Per sj18, this rendering should land *as a subcommand of
review_variants.py* so the review and render flows share a CLI.

**Implication for sj18**: review_variants.py should grow subcommands
(or a sub-mode), not just keystrokes.

### F7: with n=17, every ingredient was high-CV

The placeholder variant (Brown Sugar PB CCC) had n=17 and the
auto-flagged "high-variance" section caught nearly every ingredient
(CV > 50%). This is the small-sample noise floor.

**Implication for RationalRecipes-5z8w (cadence)**: drops should have
a minimum cluster size to be publishable. n=20 is probably the floor;
n=50+ gives meaningful CIs. The recipe queue should bias toward
high-frequency dishes (top-50 by combined count) for early drops.

### F8: scrape_merged.py is silent during the run

`-v` is set but `pipeline_merged.run_merged_pipeline` doesn't emit
incremental progress to stdout. There's no way to estimate ETA from
a running process other than `ps` for CPU time. Mid-run failure mode
is invisible — you discover it when the process exits.

Worth noting since per-drop timing data feeds 5z8w. A simple
"recipes parsed / total" counter would help.

### F9: scrape_merged.py CSVs are display strings, not normalized proportions

Discovered post-extraction: the per-variant CSVs that
`pipeline_merged.py` writes contain raw display strings ("12 tbsp",
"2 MEDIUM", "1 c", "0"), not numeric mass proportions. The LLM did
parse those during the run (log: "Parsed: 1058 rows → 1058
normalized") but the *normalized form* is in-memory only; the CSV
serialization keeps cells.

Central-tendency mass-percent stats — the actual product of a drop —
are not computed by `pipeline_merged.py`. That was the retired
`rr-stats`'s job (CSV-CLI pipeline removed in vwt.8). Under the
catalog flow, Pass 2 of `catalog_pipeline.py` (also now retired)
computed `variant_ingredient_stats` directly into `recipes.db`.
With both gone, `scrape_merged.py` is an upstream-only tool.

**Consequence**: `render_drop.py` reads `variant_ingredient_stats`,
which is empty for these new variants. To render an actual averaged
CCC drop, one of:

1. **Bridge** the manifest+CSVs into `recipes.db`, computing stats
   on the way (~50 lines, reuses the math in `catalog_db.py`).
2. **Reimplement** central-tendency stats over the CSVs at render
   time (also ~50 lines but lives in render_drop.py).
3. **Promote** `pipeline_merged.py` to write
   `variant_ingredient_stats` directly (the cleanest fix, supersedes
   the CSV format).

This compounds with F1 (CSVs vs recipes.db). A single fix that
addresses both — `pipeline_merged.py` writes structured normalized
output to recipes.db — is probably the right move.

## Timing

Background extraction of `chocolate chip cookies` against the remote
Ollama (`192.168.50.189:11434`, `gemma4:e2b`):

- **Wall-clock**: 111m00s (kicked off 22:57, finished 00:48).
- **CPU**: 9m14s user + 0m03s sys = 9m17s total.
- **~92% wait time** (Ollama network round-trips), ~8% local CPU.

Earlier guess that the local-clustering phase had quadratic behavior
was **wrong** — the 100% CPU readings I saw mid-run were the local
phases (corpus loading + L1/L2/L3 grouping). Once parsing started,
the process spent most of its time waiting on Ollama. Final stats
from the run:

- 5,636 RecipeNLG rows + 584 WDC rows loaded for the substring.
- 5,865 merged rows after url-dup + near-dup removal.
- 24 L1 groups → 10 L2 variants emitted (after `--min-variant-size=20`,
  `--max-variants-per-l1=8`).
- 1,058 ingredient-line parses (only the surviving variants got
  parsed).

**Throughput**: ~570 source recipes / hour wall-clock, against a
single-host Ollama. With local Pass-1-cache reuse (per F2) or more
Ollama parallelism, this could collapse.

**Implications for `RationalRecipes-5z8w` (cadence)**:

- One drop's research (extraction only) costs ~2 hours of wall-clock
  today. Cookie-shaped clusters are at the upper end (1,400+ source
  recipes); narrower clusters would be faster.
- Weekly cadence is plausible without optimization. Daily cadence
  wants F2 first.

## What survived the friction

The render shape (`scripts/render_drop.py`) feels right:

- Mass percentages with stddev and 95% CI tell the story honestly.
- The "low-confidence" and "high-variance" auto-flagging surfaces
  exactly the per-cluster caveats a baker should know.
- "Per 1 kg of batch" is concrete; mass percentages alone aren't
  bakable.
- Source list with outlier scores is the trust-establishing
  provenance.

What's missing for shipping:

- Volume / whole-unit conversions (F4).
- Instructions text (F5).
- A friendlier title (we'd want "Chocolate Chip Cookies" not the
  Pass-3-LLM-derived display_title; the pivot's human-written titles
  are better).

## Recommendations for next round

If the pivot proceeds, the right order of next steps is:

1. **Bridge `pipeline_merged.py` → `recipes.db` with computed stats**
   (combines F1 + F9). The cleanest fix: have `pipeline_merged.py`
   write `variants` + `variant_members` + `variant_ingredient_stats`
   directly, computing means/stddev/CIs inline (the math already
   lives in `catalog_db.py`). Supersedes the CSV+manifest format
   (which was for the retired `rr-stats`). After this, `render_drop.py`
   works on fresh extractions and the hand-cycle drop completes.
2. **Wire `parsed_ingredient_lines` cache reuse into `pipeline_merged.py`**
   (F2). Free speedup proportional to corpus overlap with prior
   drops. Without this, every drop re-parses everything.
3. **Populate `variant_ingredient_stats.density_g_per_ml` from
   `ingredients.db`** during extraction (F4). Makes rendering produce
   "cups" / "tbsp" not just grams.
4. **Cache source instructions in `recipes.db`** (F5). Removes the
   manual paste step. Simplest implementation: store full source
   instruction text on the `recipes` table during extraction.
5. **Promote `render_drop.py` into a `review_variants.py render` subcommand**
   (sj18). Add `--substitute` / `--filter` operations there too.
6. **Add `--progress` to `scrape_merged.py`** (F8). Not blocking but
   makes 5z8w easier to inform.

These are roughly ordered by friction-per-drop. (1) is the gating
item — without it, ehe7 cannot fully complete. (2) and (3) are
quality-of-life. (4) eliminates a recurring manual step. (5)
unifies the CLI. (6) is cosmetic.
