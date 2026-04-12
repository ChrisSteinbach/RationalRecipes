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
| **(a) LLM** | `parse.py` via Ollama — gemma4:e2b (remote), gemma3:1b (local fallback) |
| **(b) Regex** | Strip leading qty+unit, Swedish adjectives, trailing comma-clauses |
| **(c) Raw** | Baseline — raw ingredient string as-is |

## Results

| Extractor | Precision | Recall | F1    | Failure% | Clusters | Wall time |
|-----------|-----------|--------|-------|----------|----------|-----------|
| Raw       | 0.944     | 0.991  | 0.966 | 0.0%     | 33       | —         |
| **Regex** | **0.972** | **0.982** | **0.977** | **0.0%** | **33** | **instant** |
| LLM (gemma4:e2b) | 0.835 | 0.848 | 0.841 | 0.0% | 32 | 1.05s/line |
| LLM (gemma3:1b) | ~0.35 | ~0.40 | ~0.37 | 0% | — | 10.7s/line |

gemma4:e4b could not be tested: OOM on the 16 GB local machine (needs
9.9 GiB), and only e2b was available on the remote host. The e2b results
are a reasonable lower-bound proxy for e4b capability since the
architecture is identical — only quantization differs.

## Key Findings

### 1. Gemma 4 (e2b) systematically translates Swedish to English

The dominant LLM failure mode is **not** garbled text (that was gemma3:1b's
problem) but rather **consistent translation of common Swedish ingredients
to English**. The English system prompt and English-only examples in
`parse.py` bias the model:

- "ägg" → "egg" (in 18 of 20 gold recipes — nearly universal)
- "smör" → "butter" or "margarine"
- "äpple" → "apple", "banan" → "banana"
- "blåbär" → "blueberry", "peppar" → "pepper"
- "spiskummin" → "cumin"

This is fatal for Jaccard clustering because "ägg" ≠ "egg" as strings.
Fixing the prompt (Swedish examples, explicit "keep original language"
instruction) would likely resolve this, but that's prompt engineering
work that the regex doesn't need.

One positive: the LLM produced 32 clusters vs 33 for regex — it merged
two ugnspannkaka recipes by normalizing spenat/spinat/spinach to the same
concept. So the LLM does slightly better at canonicalization, but the
English translation problem overwhelms this advantage.

### 2. gemma3:1b is unusable for Swedish

5-recipe local test (gemma3:1b, CPU) showed three failure modes beyond
translation: hallucinated names ("lönnsirap"→"sypap", "purjolök"→"porphol",
"rädisor"→"rærso"), wrong ingredients ("räkor"→"krab", "smör"→"margarine"),
and 10.7s/line latency. This model is too small for multilingual NER.

### 3. gemma4:e4b cannot load locally

Needs 9.9 GiB on a 16 GB machine with typical desktop load (8.4 GiB free).
An LLM strategy requires either a dedicated machine or remote Ollama host.

### 4. Regex is fast, reliable, and ~97% accurate

The regex stripper handles Swedish ingredient lines surprisingly well:
- Zero failures (every line produces output)
- P=0.972, R=0.982, F1=0.977 across 20 gold-standard recipes
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

### 5. Jaccard clustering fails at threshold 0.6 regardless of extractor

All extractors produce 32-33 clusters (nearly one per recipe). This is NOT
an extraction quality problem — these recipes genuinely have different
ingredient sets. "Fluffiga pannkakor med ricotta" shares only ~4 of 12
ingredients with "Äppelpannkaka" (vetemjöl, ägg, mjölk, salt), giving
Jaccard ~0.25.

**This means**: For WDC pannkakor on ica.se, L2 clustering won't create
meaningful groups. The signal for variant-splitting (Stekt vs I ugn) must
come from the `cookingMethod` field, not ingredient-set overlap. This
confirms the recon prediction (cookingMethod is 69% populated on ica.se
and cleanly discriminates "Stekt" from "I ugn").

## Recommendation: Shape B (regex-only for L2)

**Winner**: Regex. Clear margin on every dimension that matters.

| Dimension         | Regex           | LLM (e2b)         |
|-------------------|-----------------|---------------------|
| Accuracy (F1)     | 0.977           | 0.841               |
| Latency           | instant         | 1.05s/line          |
| Failure rate      | 0%              | 0% (but wrong lang) |
| Infrastructure    | none            | remote Ollama host  |
| Maintenance       | one regex file  | model + server + prompt tuning |

**For the WDC loader (RationalRecipes-ayw)**:
1. Use regex extraction at L2 (fix the ~3% failure modes: plurals, package
   units, comma-prep patterns).
2. Use `cookingMethod` field for variant discrimination (Stekt vs I ugn),
   not Jaccard clustering alone.
3. Reserve LLM for a future fallback on hosts with truly messy ingredient
   formatting — not ica.se. If revisited, the prompt must include Swedish
   examples and an explicit "keep the original language" instruction.

## Open Q9 (non-English parsing): partially resolved

Swedish parsing works via regex for structured `recipeingredient` data from
schema-good hosts like ica.se. The LLM approach fails due to English-prompt
bias — even Gemma 4 translates ~20% of Swedish ingredient names to English,
which breaks downstream Jaccard clustering. A Swedish-tuned prompt might
fix this, but it's unnecessary given regex performance.

**Status**: Resolved for ica.se/regex. Escalate only if a messy non-English
host appears where regex can't cope and LLM is the only option.
