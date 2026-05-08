# Phase 5E Investigation: Findings & Speed-Up Options

**Status:** Investigation memo — vwt.5 merge gate paused 2026-04-25. The pipeline is correct but not tractable at corpus scale. This document captures what was measured, what was learned, and what to do next.

**Parent bead:** `RationalRecipes-vwt.5` (blocked on `vwt.13`).
**Related design doc:** `docs/design/full-catalog.md`.

## Summary

vwt.5 was scoped as "run the pipeline at scale, measure, decide whether to merge." We didn't reach that decision because two issues surfaced before any production-scale run could complete:

1. **Correctness issue (resolved at the CLI layer)** — the default cross-corpus near-dup threshold (`merge_corpora`'s `near_dup_threshold=0.3`) catastrophically collapses RecipeNLG rows in WDC-present L1 groups. Pancakes lost 108/114 rnlg rows to 5 wdc rows. Fix: raise to 0.8 (`--near-dup-threshold` flag shipped; default-flip in `merge.py` deferred to a follow-up). With the fix, chocolate chip cookies produces 35 healthy L2 clusters instead of zero.
2. **Throughput issue (blocker, no fix yet)** — `parse_ingredient_lines` makes one LLM call per ingredient line, not per recipe. A single L1 group like chocolate chip cookies needs ~8,680 calls (~12h at qwen3.6:35b-a3b's ~5s/call). The top 30 L1 keys would need ~17 days. The full corpus is years.

The merge gate cannot pass until throughput drops by ≥10×.

## What we measured (2026-04-24 / 2026-04-25)

### Corpus title survey (`output/catalog/corpus_title_survey.json`)

Streamed RecipeNLG (2,231,142 rows) + WDC top-100 (618,615 rows) under the en+sv language predicate. 45,882 normalized titles meet `combined ≥ 5`.

| `l1_min` | L1 keys | combined recipes |
|---|---|---|
| 5 | 45,882 | — |
| 100 | 1,283 | — |
| 500 | 168 | 164,432 |
| 1000 | 55 | 86,364 |
| 1500 | 20 | 42,698 |
| 2000 | 7 | 20,535 |
| 4000 | 2 | 8,279 |

### Top-30 threshold sweep (`logs/threshold_sweep.log`)

LLM-free analysis (used existing structured `ingredient_names` from RecipeNLG; skipped WDC where uncached). 30 top L1 keys, default `l2_threshold=0.6`, `l2_min=3`:

- **Total L2 clusters projected: 2,682** across the top 30 keys alone
- Per-key range: 27 (zucchini bread) to 219 (chicken casserole)
- The pipeline produces a substantial catalog at default L2 settings, given enough wallclock

### WDC-triggered near-dup collapse

Two slices end-to-end with LLM:

- **Swedish pancakes** (1 group, 114 rnlg + 5 wdc): 108 rnlg rows eaten as near-dups of 5 minimal-ingredient WDC rows. 11 survivors fail L2 clustering. **0 variants.**
- **Apple banana bread** (during the aborted broad banana-bread slice): 11 rnlg + 1 wdc → 6 surviving recipes → 0 L2 clusters. Same pattern at smaller scale.

Root cause: 30% Jaccard is the floor between any two recipes that share `{flour, egg, milk, salt, sugar}` (~5 of ~8 ingredients ≈ 60%). A WDC recipe with only basic ingredients becomes a "universal duplicate" that swallows almost everything in its L1 group.

### Confirmation of the fix

`chocolate chip cookies` at `--near-dup-threshold 0.8`:

- 1414 input → 868 merged (39% dedup, vs ~91% at 0.3)
- L2 @ 0.6 → **35 clusters** (sweep predicted 30 — close)
- First cluster has 317 recipes — healthy variant emerging

This proves both that the fix works and that the broader pipeline is correct on a WDC-rich realistic key.

### Throughput data point

The CCK@0.8 run was killed at ~3h wallclock, 0 variants written, still on the first L3 sub-group's parse loop (317 recipes × ~10 lines × ~5s/call ≈ 4.4h to finish that one sub-group alone). The full L1 group would have taken ~12h. The full top-30 run would have taken ~17 days at this rate.

## What's correct in the architecture

Worth acknowledging before we change things:

- **Whole-corpus L1 grouping** is right. Streams once, discovers every dish family, no maintainer typing. Resumable at L1-group boundary via `query_runs`. Designed for this exact merge-gate use case.
- **L2 Jaccard / L3 cookingMethod** clustering is correct and produces healthy variant counts on real data (sweep proves it).
- **SQLite as the backing store** is right. No CSV/manifest churn; `recipes.db` is the single source. PWA reads it via sql.js, review CLI writes it.
- **Determinism** (`temperature=0`, `seed=42`) is mandatory and currently works. Variant IDs are stable across reruns. Any speed-up must preserve this.
- **The 4 hand-curated baseline** (Swedish Pancakes, French Crêpes, etc.) is a load-bearing reference for plausibility checks. Future runs should be spot-checked against it.

## Speed-up options

Ordered roughly tweak → rewrite. Numbers are rough (rule-of-thumb estimates, not benchmarked unless noted).

### Tier 1 — direct attacks on the parse hot path (low effort, ship the merge gate)

#### A. Batch ingredient lines per recipe (filed as `vwt.13`, **shipped 2026-04-25**)
Change `parse_ingredient_lines` to send all N lines as a single prompt and parse a `{"results": [...]}` array out. Falls back to per-line on malformed responses, length mismatches, JSON failures.
- **Measured speedup: 1.5-1.6×** on qwen3.6:35b-a3b (live test: 20-line batch 77s vs ~124s projected per-line). Smaller than the 5-10× projected — output-token generation dominates wallclock, so the savings are bounded by amortized prefill (system prompt) only, not per-call latency.
- **Effort:** ~3h shipped (10 new unit tests in `tests/test_scrape_parse.py` + live smoke).
- **Determinism:** preserved per-call (same seed + temperature). Per-line and batched outputs differ slightly (different prompt prefixes → different sampling paths even at temp=0). Variant ID stable (depends on canonical names, not quantities). Variant statistics may shift slightly between modes — acceptable trade-off.
- **Implication: A alone does not unblock vwt.5.** Combine with B for the real win.

#### B. Sample recipes per cluster (filed as `vwt.14`)
Cap parsing at, say, 50 recipes per L3 sub-group (random sample with fixed seed). Variance estimates need ~30 samples to be statistically meaningful; CIs widen by `√(N/50)` but stay tight.
- **Estimated speedup: 5-15×** on big clusters (the 317-recipe sub-group becomes a 50-recipe sub-group; many smaller clusters unchanged).
- **Effort: trivial** (~30 min). Add a `--max-recipes-per-cluster` knob, plus a sampling step in `_process_group` or `build_variants`.
- **Tradeoff:** statistical fidelity for the dominant sub-types essentially unchanged; minor sub-types get noisier CIs. Acceptable.

**Combined A+B: ~10-25× speedup**, brings the top-30 run from ~17 days to ~1-2 days, or l1_min=2000 (7 keys) to ~13h overnight. **This combination unblocks vwt.5** but the merge gate still requires a long unattended run. For sub-day wallclock, push into Tier 2 or 3.

#### C. Cache parsed lines across recipes
Same line `"1 cup all-purpose flour"` appears in many recipes. Hash the line → cache the `ParsedIngredient`. Currently only WDC ingredient-name extraction is cached.
- **Estimated speedup: 2-3×** (depends on dedup rate of input lines across the corpus; expect high)
- **Effort: small** (~1h). One JSON file or SQLite table keyed by line text + model + seed.
- **Determinism:** preserved by including model + seed in the cache key.

#### D. Concurrency on the HTTP client
Currently sequential `urllib.request.urlopen`. Ollama can serve 4-8 concurrent generates without quality loss (it queues internally). Switch to `httpx` async or a thread pool.
- **Estimated speedup: 2-4×**
- **Effort: small-medium** (~1-2h, plus careful error handling under concurrency).
- **Determinism:** preserved (each request still has its own seed; ordering matters only for the per-recipe output list).

### Tier 2 — change the parse model or replace LLM where possible (medium effort)

#### E. Smaller/faster model for parse (**measured 2026-04-25**)
`parse_ingredient_lines` is mechanical — line → structured fields. Doesn't need 35B parameters. Live benchmark via `scripts/benchmark_batched_parse.py` against 20 ica.se recipes (221 lines, batched parse only):

| model | wall | l/s | qty | unit (raw) | ing (raw) | speedup |
|---|---|---|---|---|---|---|
| **gemma4:e2b** | 81.7s | 2.70 | 0.94 | 0.57 | 0.68 | **12.8×** |
| nemotron-3-nano:4b | 98.6s | 2.24 | 0.96 | 0.66 | 0.67 | 10.7× |
| qwen2.5:3b | 233.5s | 0.95 | 0.82 | 0.63 | 0.73 | 4.5× |
| qwen3.6:35b-a3b | 1046.2s | 0.21 | 0.94 | 0.80 | 0.91 | 1.0× |
| mistral-nemo:12b | (5-recipe slice) | 0.07 | 0.94 | 0.33 | 0.53 | 0.35× — slower |
| gemma3:1b | (5-recipe slice) | 1.18 | 0.85 | 0.40 | 0.55 | 5.9× |

**Critical reframe (2026-04-25)** — per project policy, the PWA shows ingredients and units in English regardless of source language (see memory `project_english_display.md`). That makes the small-model "errors" actually *desired behavior*: when `gemma4:e2b` translates `tsk → tsp`, it's doing exactly what the app needs. Hand-inspection of 31 flagged unit-mismatches showed 29 were correct cross-language translations (`tsk → tsp`) or prompt-mandated default fills (`smör` no qty → `g`). Only ~2 lines were real misparses (e.g. `lönnsirap → linserap`).

The Swedish benchmark gold preserves raw Swedish unit strings, so it scores against an outdated criterion. Real unit accuracy under the English-display policy is ~0.95+ across the small models.

**Reframed action plan**:
1. The pipeline storage is already English-only by construction: `variant_ingredient_stats.canonical_name` comes from USDA matching (English), quantities are stored as grams. None of the source-language unit strings reach the PWA.
2. The only language leak is `display_title` (picked from a representative cluster member). Decide a policy (always English title; use L1 key) before `vwt.10` (ica.se loader) lands — Swedish-only L1 clusters will need explicit handling.
3. **Shadow comparison criterion** (vwt.18): compare `qwen3.6:35b-a3b` vs `gemma4:e2b` parse outputs on ~200 real WDC recipes after running each through `normalize_merged_row`. Match on `(canonical_name, grams ±5%)`. The literal unit string doesn't matter — both `tsp` and `tsk` convert to ~5g downstream.
4. If ≥95% post-normalization agreement, swap the default and combine with batching + sampling.

- **Effort: small** (~1h shadow run + analysis script).

#### F. Rule-based parser, LLM fallback
80%+ of ingredient lines fit `QTY [UNIT] NAME [, prep]`. Regex/heuristic those (instant, free), only send the messy 20% to the LLM.
- **Estimated speedup: ~5×** (cheap regex handles most calls; LLM handles fewer)
- **Effort: medium** (~1 day). Need a robust enough regex + a "did I confidently parse this?" check.
- **Risk:** parsing errors that match the regex but extract wrong fields. Mitigation: shadow the regex result against the LLM result on a sample to measure agreement.

#### G. Skip parse for RecipeNLG `ner` column
RecipeNLG already has a `ner` column with canonicalized ingredient names per row. The current pipeline ignores it and re-parses from raw lines. We could use `ner` for the `canonical_name` and only use the LLM for quantity/unit extraction (much simpler — narrower task, smaller prompt).
- **Estimated speedup: 2-3×** (smaller per-call work)
- **Effort: small-medium** (~3h). Need to verify `ner` matches our USDA canonicalization.
- **Caveat:** WDC doesn't have `ner`. Pipeline still needs full parse for WDC.

### Tier 3 — restructure where the LLM lives (bigger changes)

#### H. Two-pass design: extract once, statistic many times
Pass 1 (one-time): Parse every ingredient line in the corpus once into a `parsed_ingredient_lines` table keyed by `(corpus, recipe_id, line_index)`. Resumable.
Pass 2 (cheap, repeatable): Cluster + statistics + write `variants` table from the parsed-line cache. No LLM.
- **Estimated speedup:** Pass 1 is still big (the same parse work, just amortized). But threshold tuning, l1_min sweeps, anything iterative becomes seconds instead of hours. Pass 2 enables re-running the merge gate freely.
- **Effort: medium** (~1-2 days). Schema change, refactor `_process_group`.
- **Synergy:** combine with (A)+(B)+(C) and Pass 1 itself becomes feasible.
- **Bonus:** the cache table is shippable as an artifact, so reproducing variants doesn't require rerunning LLM at all.

#### I. Pre-index RecipeNLG offline
Convert the 2.2 GB CSV into a SQLite index once (~10 min one-time). Every dev run currently pays a 3.5 min full-stream cost just to find the matching rows. Indexed lookup makes title-filter slices instant.
- **Estimated speedup:** dev iteration goes from 3.5 min/run to seconds. Doesn't help full-corpus runs much (still need every row).
- **Effort: small** (~2h). Sibling to current `RecipeNLGLoader`.
- **Use:** big quality-of-life improvement for testing, less critical for the merge-gate run itself.

#### J. Hosted LLM for the parse step
Anthropic Haiku 4.5 is fast at structured-output JSON parsing and supports prompt caching. Switching parse calls from local Ollama → Anthropic API would solve throughput at the cost of $.
- **Estimated speedup:** 10-50× (Haiku 4.5 is ~10× faster per call, plus prompt caching amortizes the system prompt across all parse calls in a batch).
- **Effort: small-medium** (~3-5h to wire the API client + caching). Need to handle API errors / rate limits.
- **Cost:** estimating ~$5-30 for a top-30 run depending on cache hit rate. Cheap relative to engineer time.
- **Pros:** also reliable JSON via tool-use. **Cons:** introduces a paid external dependency for what was a self-hostable system.

### Tier 4 — total rewrites (high effort, decide if needed)

#### K. Replace LLM parsing entirely with regex + USDA matching
The parse step does (a) quantity/unit extraction (well-defined regex) and (b) name canonicalization (fuzzy match against `ingredients.db` USDA table — already shipped). Both are doable deterministically without LLM. Keep the LLM only for canonicalization at the L2-cluster level (`pipeline_merged.canonicalize_*`), not per line.
- **Estimated speedup:** ~100×+. The system becomes IO-bound.
- **Effort: high** (~3-5 days). Major surface area: write the regex, build the fuzzy-match harness, test against the curated baseline, decide error policy.
- **Risk:** quality regression on quirky inputs. Mitigation: shadow-mode against current LLM output on the full curated set to measure delta before flipping.
- **Long-term value:** removes LLM from the hot path. Makes the project self-hosting and cost-free in production.

#### L. Distill the corpus offline once, ship a derived dataset
Run a one-time offline batch (could be days of LLM time) that processes RecipeNLG into a "structured RecipeNLG" parquet/SQLite with every recipe's lines pre-parsed. Ship that derived dataset alongside the raw corpus. The pipeline operates on structured data; no LLM in the hot path.
- **Estimated speedup:** post-distillation, the pipeline runs in minutes (no LLM).
- **Effort: high** (~1 week including the batch design + execution).
- **Long-term value:** makes the pipeline reproducible by anyone with the derived dataset; removes the LLM dependency from the inner loop.
- **Caveat:** WDC still needs LLM extraction (page text → ingredient names) since WDC doesn't ship structured. But WDC is small (~600k rows vs 2.2M for RecipeNLG), tractable.

#### M. Switch architectures: embeddings + nearest-neighbor instead of clustering
Get sentence embeddings of every ingredient line, cluster by cosine similarity, pick canonical reps per cluster. Replaces both the per-line parse and the L2 Jaccard step. Different system entirely.
- **Estimated speedup:** unclear — depends on embedding model speed and quality
- **Effort: very high** (~weeks). Whole-system change.
- **Risk:** embedding similarity ≠ ingredient identity. Hard to predict quality without a prototype.
- **Verdict:** interesting research direction but not a tactical merge-gate fix.

## Recommended path

Two tiers of action depending on how much investment makes sense:

### Pragmatic (close vwt.5 within a week)
1. Ship **A** (batch lines per recipe). One commit, ~2-3h.
2. Ship **B** (sample 50 recipes per cluster, configurable). One commit, ~30 min.
3. Combined ~25-50× speedup makes a top-30 overnight run viable.
4. Optional: **C** (parse cache) for re-runs.
5. Run vwt.12 (real merge-gate run) at `l1_min=1500`, `near-dup=0.8`. 6-8h overnight.
6. Spot-check against curated baseline. Close vwt.5.

### Strategic (close vwt.5 within a month, set up for long-term)
Do the pragmatic path **plus**:

7. Ship **H** (two-pass parsed-line cache table). Makes future iteration cheap.
8. Re-evaluate: is the parse step accurate enough that we could replace it with **K** (regex + USDA) for ~100× speedup and zero LLM in production?
9. Consider **J** (Anthropic Haiku for parse) if Ollama remains the bottleneck — cheap to try.

### Don't do (yet)
- **F** (rule-based parser) and **G** (RecipeNLG `ner` reuse) sound nice but interact awkwardly with the existing prompt design and add complexity. Defer until A+B prove insufficient.
- **L** (offline distillation) is overkill if A+B work. Revisit only if production-time LLM cost becomes a problem.
- **M** (embeddings rewrite) is a research project, not a fix.

## Open questions for the maintainer

1. Comfortable with a ~6-8h overnight run, or do you want sub-hour? (Determines whether to stop at A+B or push into Tier 2/3.)
2. Is paid API access (J) acceptable for the parse path, or is Ollama-only a hard requirement?
3. Does sampling (B) violate any statistical-rigor requirement you care about? The CI math holds, but maintainers sometimes feel "more samples = better catalog."
4. Should the near_dup default in `merge.py` flip from 0.3 → 0.8 now (closes vwt.11), or wait for a real run to confirm?
5. Is `vwt.10` (ica.se loader) in scope for closing this branch, or a follow-up after merge?

## Artifacts kept from the investigation

- `output/catalog/corpus_title_survey.json` — full title-frequency survey
- `logs/threshold_sweep.log` — top-30 LLM-free clustering analysis
- `logs/devslice_swedishpancakes.log`, `logs/devslice_bananabread.log`, `logs/cck_08.log` — per-slice diagnostics
- `output/catalog/dev_slice/wdc_names.json`, `output/catalog/cck_threshold_test/` — partial WDC ingredient caches (gitignored, but useful as cache seeds for re-runs)
- This document.

## Decision log

| date | decision |
|---|---|
| 2026-04-24 | Survey shows 45,882 L1 keys at l1_min=5 → infeasible. Plan to use higher l1_min for first real run. |
| 2026-04-24 | Pannkak slice → 0 groups (Swedish pannkakor near-absent from loaded corpora). ica.se dump not wired in. Filed `vwt.10`. |
| 2026-04-25 | Swedish pancakes slice → 0 variants from 119 input recipes. Investigated near_dup, found 108/114 collapse at default 0.3. Filed `vwt.11`. |
| 2026-04-25 | Top-30 LLM-free sweep → pipeline healthy at default thresholds when WDC-free. Confirmed near_dup is the issue, not L2 threshold. |
| 2026-04-25 | Shipped `--near-dup-threshold` and `--title-exact` CLI flags (uncommitted at time of writing). |
| 2026-04-25 | CCK@0.8 confirms fix at L2 stage (35 clusters). Parse phase ran for 3h with 0 variants written; investigated and discovered per-line LLM call structure. Filed `vwt.13` (P1). |
| 2026-04-25 | vwt.5 marked blocked on vwt.13. This memo written. |
