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

## Timing

(To be filled in once the CCC extraction completes.)

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

1. **Bridge scrape_merged.py → recipes.db** (a dozen lines).
   Unblocks F1, lets review_variants.py see fresh extractions.
2. **Wire parsed_ingredient_lines cache reuse into pipeline_merged.py**
   (F2). Free 10× speedup on subsequent runs.
3. **Populate variant_ingredient_stats.density_g_per_ml from
   ingredients.db** during extraction (F4). Makes rendering produce
   "cups" / "tbsp" not just grams.
4. **Cache source instructions in recipes.db** (F5). Removes the
   manual paste step.
5. **Promote `render_drop.py` into a `review_variants.py render` subcommand**
   (sj18). Add `--substitute` / `--filter` operations there too.
6. **Add `--progress` to scrape_merged.py** (F8). Not blocking but
   makes 5z8w easier to inform.

These are roughly ordered by friction-per-drop.
