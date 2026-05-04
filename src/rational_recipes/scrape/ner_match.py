"""RecipeNLG NER-column → per-line ingredient name resolution (am5).

The RecipeNLG corpus ships an ``NER`` column with a list of human/auto-
extracted ingredient nouns per recipe (``["brown sugar", "milk", …]``).
These are clean ingredient names without quantities, units, or
preparation notes — the very thing the LLM hot path is paid to extract.

When we have NER values for a recipe, we can usually skip the LLM for
ingredient-name extraction by mapping each raw ingredient line back to
the NER value that names its ingredient. The mapping is substring-
based: the longest NER value that appears as a substring of the line
is its name. The bias is conservative — when 0 or multiple NER values
match at the longest length, we return ``None`` so the caller falls
through to the regex+USDA path or the LLM.

Examples (from a 5-row sample of dataset/full_dataset.csv):

  line: "1 c. firmly packed brown sugar"
  ner:  ["brown sugar", "milk", "vanilla"]
  → "brown sugar"

  line: "1 (10 3/4 oz.) can cream of chicken soup"
  ner:  ["chicken", "cream of chicken soup"]
  → "cream of chicken soup"     (longest substring wins)

  line: "1 c. semi sweet chocolate chips"
  ner:  ["semi sweet chocolate chips"]
  → "semi sweet chocolate chips"

  line: "1 large container Cool Whip"
  ner:  ["pineapple", "condensed milk", "lemons"]
  → None                          (no substring match — LLM handles it)

The match is case-insensitive on a substring basis. We deliberately
don't word-boundary-anchor: NER plurals like "tomatoes" must match
"crushed tomatoes" without an exact-word constraint, and "egg" should
match the "egg" in "2 eggs". Substring matching across word boundaries
risks false positives (e.g. "fish" inside "selfish") but real
ingredient lines don't mix the two vocabularies.
"""

from __future__ import annotations

from collections.abc import Sequence


def resolve_ner_for_line(
    raw_line: str,
    ner_list: Sequence[str],
) -> str | None:
    """Pick the best NER candidate for a raw ingredient line.

    Returns the longest NER value (case-insensitive) that appears as a
    substring of ``raw_line``. When multiple NER values tie for longest,
    or when none match, returns ``None`` so the caller can fall back to
    the regex+USDA / LLM path.

    Empty / whitespace-only NER values are skipped — RecipeNLG's NER
    column occasionally carries blanks.
    """
    if not raw_line or not ner_list:
        return None

    lower_line = raw_line.lower()
    longest_length = 0
    # Track distinct candidates by their lowercased form so the same NER
    # noun appearing twice in a recipe (e.g. ``vanilla`` for both an
    # extract and a sugar entry) doesn't read as a tie. Distinct-spelling
    # ties still defeat the matcher and route the line to the LLM.
    distinct_at_longest: dict[str, str] = {}

    for ner in ner_list:
        if not ner:
            continue
        ner_normalized = ner.strip()
        if not ner_normalized:
            continue
        ner_lower = ner_normalized.lower()
        if ner_lower not in lower_line:
            continue
        length = len(ner_lower)
        if length > longest_length:
            longest_length = length
            distinct_at_longest = {ner_lower: ner_normalized}
        elif length == longest_length:
            distinct_at_longest.setdefault(ner_lower, ner_normalized)

    # Tie at longest length on DISTINCT spellings is ambiguous — return
    # None so the LLM gets a chance. Same-spelling repeats collapse and
    # resolve as a single candidate.
    if len(distinct_at_longest) == 1:
        return next(iter(distinct_at_longest.values()))
    return None
