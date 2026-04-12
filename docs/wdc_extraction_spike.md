# WDC Ingredient-Name Extraction Spike

**Bead**: RationalRecipes-a1k  
**Date**: 2026-04-12  
**Dataset**: ica.se pannkakor (33 recipes from WDC Recipe_top100)

## Setup

Three extractors compared on 33 Swedish pannkakor recipes from ica.se,
measured against a hand-labelled gold standard (20 recipes, 204 ingredient
lines).

| Extractor | Method |
|-----------|--------|
| **(a) LLM** | `parse.py` via Ollama — gemma4:e4b (OOM), fell back to gemma3:1b |
| **(b) Regex** | Strip leading qty+unit, Swedish adjectives, trailing comma-clauses |
| **(c) Raw** | Baseline — raw ingredient string as-is |

## Results

| Extractor | Precision | Recall | F1    | Failure% | Clusters | Wall time |
|-----------|-----------|--------|-------|----------|----------|-----------|
| Raw       | 0.940     | 0.987  | 0.961 | 0.0%     | 33       | —         |
| **Regex** | **0.967** | **0.977** | **0.972** | **0.0%** | **33** | **instant** |
| LLM (gemma4:e4b) | —  | —      | —     | 100%     | 0        | OOM       |
| LLM (gemma3:1b) | ~0.35* | ~0.40* | ~0.37* | 0% | —  | 10.7s/line |

\* LLM P/R estimated from 5-recipe sample (see below).

## Key Findings

### 1. gemma4:e4b cannot load on this hardware

The target model needs 9.9 GiB system RAM. With a typical desktop workload
(IDE + browser) on a 16 GB machine, only ~8.4 GiB is available. Every call
returned HTTP 500: `"model requires more system memory (9.9 GiB) than is
available (8.4 GiB)"`. No GPU available for offloading (Intel Iris Xe only).

**Implication**: An LLM-at-title-match strategy (Shape A) for WDC requires
either a machine with 32+ GB RAM or a remote Ollama host. This is a
deployment constraint, not a code problem.

### 2. gemma3:1b produces garbled/mixed-language output on Swedish

5-recipe sample (50 ingredient lines) revealed three fatal failure modes:

**Inconsistent translation**: The English-trained system prompt causes the
model to randomly translate some Swedish words to English while keeping
others Swedish. Examples:
- "mjölk" → "milk", "ägg" → "egg", "socker" → "sugar" (translated)
- "vetemjöl", "strösocker", "bakpulver" (kept Swedish)

This is fatal for Jaccard clustering — "mjölk" and "milk" are different
strings.

**Hallucinations/garbled text** (~30% of lines):
- "lönnsirap" → "sypap"
- "räkor med skal" → "krab" (shrimp ≠ crab)
- "purjolök" → "porphol"
- "rädisor" → "rærso"
- "basilika" → "basilia" / "kruska"
- "smör" → "margarine" (wrong ingredient)

**Extreme latency**: 10.7s per line on CPU. At ~10 lines/recipe, 33 recipes
would take ~58 minutes. Even the 5-recipe sample took 520s total.

### 3. Regex is fast, reliable, and ~97% accurate

The regex stripper handles Swedish ingredient lines surprisingly well:
- Zero failures (every line produces output)
- P=0.967, R=0.977, F1=0.972 across 20 gold-standard recipes
- Instant execution

**Known regex failure modes** (all fixable):
- **Plurals**: "morötter" not matched to gold "morot", "äpplen" → "äpple"
  (needs a small Swedish lemmatizer or hardcoded plural map)
- **Comma-separated prep adjectives**: "250 g frysta, halvtinade blåbär" →
  "frysta" (comma-tail strip removes the real noun)
- **Packaging/container leakage**: "1 förp kokta vita bönor" → "förp kokta
  vita bönor" ("förp"/"burk"/"paket" should be stripped like units)
- **Equipment in ingredient list**: "spritspåse" (piping bag) is not an
  ingredient

### 4. Jaccard clustering fails at threshold 0.6 regardless of extractor

All three extractors produce 33 clusters (one per recipe). This is NOT an
extraction quality problem — these recipes genuinely have different
ingredient sets. "Fluffiga pannkakor med ricotta" shares only ~4 of 12
ingredients with "Äppelpannkaka" (vetemjöl, ägg, mjölk, salt), giving
Jaccard ~0.25.

**This means**: For WDC pannkakor on ica.se, L2 clustering won't create
meaningful groups. The signal for variant-splitting (Stekt vs I ugn) must
come from the `cookingMethod` field, not ingredient-set overlap. This
confirms the recon prediction (cookingMethod is 69% populated on ica.se
and cleanly discriminates "Stekt" from "I ugn").

## Recommendation: Shape B (regex-only for L2)

**Winner**: Regex. Clear margin on every dimension.

| Dimension         | Regex           | LLM              |
|-------------------|-----------------|-------------------|
| Accuracy (F1)     | 0.972           | ~0.37 (1b) / OOM (e4b) |
| Latency           | instant         | 10.7s/line        |
| Failure rate      | 0%              | 100% (e4b) / 0% (1b, but garbled) |
| RAM required      | 0               | 10+ GB            |
| Maintenance       | one regex file  | model + Ollama server |

**For the WDC loader (RationalRecipes-ayw)**:
1. Use regex extraction at L2 (fix the ~3% failure modes: plurals, package
   units, comma-prep patterns).
2. Use `cookingMethod` field for variant discrimination (Stekt vs I ugn),
   not Jaccard clustering alone.
3. Reserve LLM for a future fallback on hosts with truly messy
   ingredient formatting — not ica.se.

## Open Q9 (non-English parsing): partially resolved

Swedish parsing works via regex for structured `recipeingredient` data from
schema-good hosts like ica.se. The LLM approach fails due to
English-prompt bias and model limitations on non-Latin-heavy text. If
gemma4:e4b could load, results might improve — but the prompt would still
need Swedish examples and explicit "keep the original language" instructions.

**Status**: Resolved for ica.se/regex. Escalate only if a messy non-English
host appears where regex can't cope and LLM is the only option.
