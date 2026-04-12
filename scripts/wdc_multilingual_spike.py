#!/usr/bin/env python3
"""Spike: multilingual WDC extraction + language-neutral LLM prompt.

Extends the ica.se bakeoff (a1k) to Japanese, Russian, and German hosts,
and tests whether a language-neutral prompt fixes the translation problem.

Usage:
    python3 scripts/wdc_multilingual_spike.py --ollama-url http://host:11434
"""

from __future__ import annotations

import argparse
import gzip
import json
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path

WDC_DIR = Path("dataset/wdc")

# Host configs: (file_stem, language, sample_query, expected_unit_re)
HOSTS = [
    ("ica.se", "Swedish", "pannkak", None),
    ("chefkoch.de", "German", "Pfannkuchen", None),
    ("edimdoma.ru", "Russian", "блин", None),
    ("macaro-ni.jp", "Japanese", "ケーキ", None),
]

# ---------------------------------------------------------------------------
# Language-neutral prompt (the fix under test)
# ---------------------------------------------------------------------------

NEUTRAL_PROMPT = """\
You are an ingredient parser. Given a recipe ingredient line in ANY language,
extract structured fields.

Return ONLY a JSON object with these fields:
- "ingredient": the base ingredient name in the ORIGINAL LANGUAGE, lowercase,
  no preparation notes, no quantities, no units. Keep the original language —
  do NOT translate to English.
- "quantity": number (float). For fractions, convert to decimal. If no
  quantity, use 1.
- "unit": the unit of measurement in the original language, lowercase.
  If no unit, use "" (empty string).
- "preparation": any preparation notes. Empty string if none.

Examples in different languages:

Input: "3 dl vetemjöl"
Output: {"ingredient": "vetemjöl", "quantity": 3.0, "unit": "dl", \
"preparation": ""}

Input: "2 große Eier"
Output: {"ingredient": "eier", "quantity": 2.0, "unit": "große", \
"preparation": ""}

Input: "卵 3個"
Output: {"ingredient": "卵", "quantity": 3.0, "unit": "個", \
"preparation": ""}

Input: "молоко - 500 мл"
Output: {"ingredient": "молоко", "quantity": 500.0, "unit": "мл", \
"preparation": ""}

Input: "250 g frysta, halvtinade blåbär"
Output: {"ingredient": "blåbär", "quantity": 250.0, "unit": "g", \
"preparation": "frysta, halvtinade"}
"""


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


@dataclass
class SampleRecipe:
    host: str
    lang: str
    row_id: int
    name: str
    ingredients_raw: list[str]
    cooking_method: str | None


def load_sample(
    host: str, lang: str, query: str, max_recipes: int = 10
) -> list[SampleRecipe]:
    """Load up to max_recipes matching query from a WDC host file."""
    gz = WDC_DIR / f"Recipe_{host}_October2023.json.gz"
    if not gz.exists():
        print(f"  WARNING: {gz} not found, skipping", file=sys.stderr)
        return []

    recipes: list[SampleRecipe] = []
    with gzip.open(gz, "rt", encoding="utf-8") as f:
        for line in f:
            rec = json.loads(line)
            name = rec.get("name", "")
            ings = rec.get("recipeingredient")
            if not name or not ings or len(ings) < 3:
                continue
            if query.lower() not in name.lower():
                continue
            recipes.append(
                SampleRecipe(
                    host=host,
                    lang=lang,
                    row_id=rec["row_id"],
                    name=name,
                    ingredients_raw=ings,
                    cooking_method=rec.get("cookingmethod"),
                )
            )
            if len(recipes) >= max_recipes:
                break
    return recipes


# ---------------------------------------------------------------------------
# LLM extractor with prompt variants
# ---------------------------------------------------------------------------

OLLAMA_URL = "http://localhost:11434"


def llm_extract_line(
    line: str,
    model: str,
    system_prompt: str,
    base_url: str,
) -> tuple[str | None, float]:
    """Extract ingredient name via LLM. Returns (name, wall_seconds)."""
    import urllib.error
    import urllib.request

    payload = json.dumps(
        {
            "model": model,
            "system": system_prompt,
            "prompt": f'Parse this ingredient line:\nInput: "{line}"\nOutput:',
            "format": "json",
            "stream": False,
        }
    ).encode()

    req = urllib.request.Request(
        f"{base_url}/api/generate",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    t0 = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            body = json.loads(resp.read())
            raw = body.get("response", "")
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
        return None, time.monotonic() - t0

    elapsed = time.monotonic() - t0
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        start = raw.find("{")
        end = raw.rfind("}") + 1
        if start >= 0 and end > start:
            try:
                data = json.loads(raw[start:end])
            except json.JSONDecodeError:
                return None, elapsed
        else:
            return None, elapsed

    name = data.get("ingredient", "")
    if isinstance(name, str) and name.strip():
        return name.lower().strip(), elapsed
    return None, elapsed


# ---------------------------------------------------------------------------
# Regex extractor (generalized attempt)
# ---------------------------------------------------------------------------

# Western pattern: qty unit name
_WESTERN_QTY_UNIT = re.compile(
    r"^\s*[\d½¼¾⅓⅔\s/.,\-]+\s*"
    r"(?:dl|msk|tsk|krm|st|g|kg|ml|l|cl|cup|cups|tbsp|tsp|oz|lb|"
    r"EL|TL|Stück|Prise|Zehe|Bund|Packung|Becher|"
    r"ст\.?\s*л\.?|ч\.?\s*л\.?|шт\.?|мл|г|кг|л|щепот\w*|"
    r"個|本|枚|杯|大さじ|小さじ|カップ|合|丁|片|束|袋"
    r")\b\.?\s*",
    re.IGNORECASE,
)

# Japanese: name comes first, qty+unit at end
_JP_TAIL_QTY = re.compile(
    r"\s*[\d½¼¾０-９]+\s*(?:個|本|枚|杯|大さじ|小さじ|カップ|合|丁|片|束|袋|g|ml|cc|cm)\s*$"
)

# Russian: "ingredient - qty unit" pattern
_RU_DASH_SEP = re.compile(r"\s*[-–—]\s*[\d½¼¾\s/.,]+\s*.*$")

_PAREN_RE = re.compile(r"\s*[\(（].*?[\)）]\s*")
_TAIL_COMMA = re.compile(r"\s*,\s*.*$")


def regex_extract_line(line: str, lang: str) -> str | None:
    """Best-effort regex extraction, language-aware."""
    s = line.strip()
    if not s:
        return None

    # Remove parentheticals
    s = _PAREN_RE.sub(" ", s).strip()

    if lang == "Japanese":
        # Japanese: strip trailing qty+unit, take what's left
        s = _JP_TAIL_QTY.sub("", s).strip()
        # Strip leading quantities if present (some JP sites do qty first)
        s = re.sub(r"^[\d０-９\s/.,]+\s*", "", s).strip()
        # Remove common JP qualifiers
        s = re.sub(r"少々|適量|適宜|お好みで", "", s).strip()
    elif lang == "Russian":
        # Russian: strip " - qty unit" suffix
        s = _RU_DASH_SEP.sub("", s).strip()
        # Strip leading qty+unit if Western-ordered
        s = _WESTERN_QTY_UNIT.sub("", s).strip()
    else:
        # Western (Swedish, German, English): qty unit name
        s = _WESTERN_QTY_UNIT.sub("", s).strip()
        # Strip bare leading quantity
        s = re.sub(r"^[\d½¼¾\s/.,\-]+\s+", "", s).strip()

    # Strip trailing comma clause
    s = _TAIL_COMMA.sub("", s).strip()

    return s.lower().strip() if s else None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ollama-url", default="http://192.168.50.189:11434")
    parser.add_argument("--model", default="gemma4:e2b")
    parser.add_argument("--max-recipes", type=int, default=10, help="Recipes per host")
    args = parser.parse_args()

    # Load the original English prompt for comparison
    sys.path.insert(0, "src")
    from rational_recipes.scrape.parse import _SYSTEM_PROMPT as ENGLISH_PROMPT

    print("=" * 70)
    print("  WDC Multilingual Extraction Spike")
    print("=" * 70)

    for host, lang, query, _ in HOSTS:
        print(f"\n{'─' * 70}")
        print(f"  {host} ({lang}) — query: {query!r}")
        print(f"{'─' * 70}")

        recipes = load_sample(host, lang, query, args.max_recipes)
        if not recipes:
            print("  No recipes found, skipping.")
            continue

        print(f"  Found {len(recipes)} recipes\n")

        for r in recipes[:5]:  # detailed view of first 5
            print(f"  [{r.row_id}] {r.name[:65]}")
            for ing in r.ingredients_raw:
                # Run all three: regex, LLM-english, LLM-neutral
                regex_name = regex_extract_line(ing, lang) or "FAIL"

                llm_en_name, llm_en_t = llm_extract_line(
                    ing, args.model, ENGLISH_PROMPT, args.ollama_url
                )
                llm_en_name = llm_en_name or "FAIL"

                llm_neu_name, llm_neu_t = llm_extract_line(
                    ing, args.model, NEUTRAL_PROMPT, args.ollama_url
                )
                llm_neu_name = llm_neu_name or "FAIL"

                # Compact display
                ing_display = ing[:35].ljust(35)
                print(
                    f"    {ing_display}"
                    f"  regex={regex_name:20s}"
                    f"  en={llm_en_name:20s}"
                    f"  neu={llm_neu_name}"
                )
            print()

        # Summary stats over all recipes
        total = 0
        regex_ok = 0
        en_ok = 0
        neu_ok = 0
        en_translated = 0
        neu_translated = 0
        en_time = 0.0
        neu_time = 0.0

        for r in recipes:
            for ing in r.ingredients_raw:
                total += 1
                rn = regex_extract_line(ing, lang)
                if rn and rn != "FAIL":
                    regex_ok += 1

                en_name, et = llm_extract_line(
                    ing, args.model, ENGLISH_PROMPT, args.ollama_url
                )
                en_time += et
                if en_name:
                    en_ok += 1
                    # Check if it looks translated (Latin chars in non-Latin lang)
                    if lang in ("Japanese", "Russian") and re.search(
                        r"[a-zA-Z]{3,}", en_name
                    ):
                        en_translated += 1

                neu_name, nt = llm_extract_line(
                    ing, args.model, NEUTRAL_PROMPT, args.ollama_url
                )
                neu_time += nt
                if neu_name:
                    neu_ok += 1
                    if lang in ("Japanese", "Russian") and re.search(
                        r"[a-zA-Z]{3,}", neu_name
                    ):
                        neu_translated += 1

        print(f"  Summary ({total} lines across {len(recipes)} recipes):")
        print(f"    Regex:       {regex_ok}/{total} extracted")
        print(
            f"    LLM-english: {en_ok}/{total} extracted"
            f"  ({en_translated} translated to English)"
            f"  {en_time:.0f}s"
        )
        print(
            f"    LLM-neutral: {neu_ok}/{total} extracted"
            f"  ({neu_translated} translated to English)"
            f"  {neu_time:.0f}s"
        )


if __name__ == "__main__":
    main()
