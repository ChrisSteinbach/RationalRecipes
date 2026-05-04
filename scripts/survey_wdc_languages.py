"""Survey WDC top-100 hosts by language and recipe volume.

Reads the WDC pre-computed per-host stats and classifies each host's
language. For the b7t.24 bead: produces a language-ranked recipe-volume
table to drive per-language ingredient DB coverage decisions.

Classification strategy:
1. Country-code TLD (.de, .fr, .ru, ...) — high confidence.
2. Hand-curated map of generic .com/.net/.org hosts with well-known
   language affiliation (e.g. donnamoderna.com → Italian).
3. Content peek into the per-host .json.gz for anything else: read the
   first N recipe `name` fields and run a script-and-keyword classifier.
4. "multi" / "unknown" buckets for brand sites and CDNs that do not map
   to a single language community.

Usage:
    python3 scripts/survey_wdc_languages.py
    python3 scripts/survey_wdc_languages.py --verify  # peek content for non-TLD hosts
"""

from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import sys
import unicodedata
import zipfile
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
STATS_CSV = (
    REPO_ROOT
    / "dataset"
    / "wdc"
    / "stats"
    / "table_statistics"
    / "Recipe_October2023_statistics_top100.csv"
)
TOP100_ZIP = REPO_ROOT / "dataset" / "wdc" / "Recipe_top100.zip"

# ISO 639-1 codes used as canonical language labels.
LANG_NAMES = {
    "en": "English",
    "de": "German",
    "fr": "French",
    "it": "Italian",
    "es": "Spanish",
    "pt": "Portuguese",
    "ru": "Russian",
    "sv": "Swedish",
    "no": "Norwegian",
    "da": "Danish",
    "fi": "Finnish",
    "nl": "Dutch",
    "pl": "Polish",
    "cs": "Czech",
    "sk": "Slovak",
    "ro": "Romanian",
    "sr": "Serbian",
    "hr": "Croatian",
    "tr": "Turkish",
    "ar": "Arabic",
    "ja": "Japanese",
    "zh": "Chinese",
    "ko": "Korean",
    "id": "Indonesian",
    "hi": "Hindi",
    "multi": "Multi/brand",
    "unknown": "Unknown",
}

# ccTLD → language. Only includes ccTLDs that map cleanly to a single
# dominant recipe-content language. Multilingual ccTLDs (.ch, .be, .ca)
# are routed through the generic map below for per-host disambiguation.
TLD_LANG = {
    "de": "de",
    "at": "de",
    "fr": "fr",
    "it": "it",
    "es": "es",
    "ru": "ru",
    "ua": "ru",  # Russian is dominant in legacy .ua recipe sites
    "se": "sv",
    "no": "no",
    "dk": "da",
    "fi": "fi",
    "nl": "nl",
    "pl": "pl",
    "cz": "cs",
    "sk": "sk",
    "ro": "ro",
    "rs": "sr",
    "hr": "hr",
    "tr": "tr",
    "jp": "ja",
    "kr": "ko",
    "tw": "zh",  # Traditional Chinese
    "cn": "zh",
    "id": "id",
    "in": "hi",  # though many .in food sites are English; treat as hi for surveys
    "br": "pt",
    "pt": "pt",
}

# Generic-TLD hosts with known language affiliation. Curated by host
# name + project context. "multi" means a brand or aggregator that does
# not map to a single language audience.
GENERIC_HOST_LANG = {
    # English
    "yummly.com": "en",
    "relish.com": "en",
    "food.com": "en",
    "bigoven.com": "en",
    "brewersfriend.com": "en",
    "justapinch.com": "en",
    "tasteofhome.com": "en",
    "ckbk.com": "en",
    "foodnetwork.com": "en",
    "food52.com": "en",
    "stevehacks.com": "en",
    "bbcgoodfood.com": "en",
    "waitrose.com": "en",
    "americastestkitchen.com": "en",
    "bakespace.com": "en",
    "bettycrocker.com": "en",
    "vegetariantimes.com": "en",
    "blueapron.com": "en",
    "ifood.tv": "en",
    "recipes.net": "en",
    "keyingredient.com": "en",
    "bawarchi.com": "en",  # Indian English
    "cookmonkeys.com": "en",
    "completerecipes.com": "en",
    "insanelygoodrecipes.com": "en",
    "archanaskitchen.com": "en",  # Indian English
    "cooksmarts.com": "en",
    # Italian
    "donnamoderna.com": "it",
    # French
    "notrefamille.com": "fr",
    "cuisineaz.com": "fr",
    "chefsimon.com": "fr",
    "larecette.net": "fr",
    # Spanish
    "kiwilimon.com": "es",
    "comoquiero.net": "es",
    "recetasgratis.net": "es",
    "hazteveg.com": "es",
    # Portuguese
    "tudoreceitas.com": "pt",
    "globo.com": "pt",  # Brazilian Portuguese
    # Arabic
    "sayidaty.net": "ar",
    "newsabah.com": "ar",
    # Turkish
    "lezizyemeklerim.com": "tr",
    "nefisyemektarifleri.com": "tr",
    # Russian
    "1001eda.com": "ru",
    # Swedish
    "tasteline.com": "sv",
    # Japanese
    "delishkitchen.tv": "ja",
    "macaro-ni.jp": "ja",  # ccTLD already covers, listed for clarity
    # Multi-language brand / aggregator / CDN
    "azureedge.net": "multi",  # CDN
    "pinterest.com": "multi",
    "weightwatchers.com": "multi",  # localized per region
    "thermomix.com": "multi",
    "knorr.com": "multi",
    "weber.com": "multi",
    "mccormick.com": "multi",
    "worldrecipes.eu": "multi",
    "inspiced.co": "multi",  # multi-language
    "marmiton.org": "fr",
    # CH ambiguous → Migros publishes de/fr/it; treat as multi
    "migros.ch": "multi",
    # BE ambiguous: VTM is Flemish (Dutch); Carrefour BE is bilingual fr/nl
    "vtm.be": "nl",
    "carrefour.be": "multi",
    # CA: Reader's Digest CA is English
    "readersdigest.ca": "en",
    # Cookpad — origin Japan; .com is the international/Japanese-language service
    "cookpad.com": "ja",
    # NL ccTLD covered already, listed for completeness
    "kookjij.nl": "nl",
    "ah.nl": "nl",
    # ID
    "yummy.co.id": "id",
    # vkys.info — Slovak/Czech word for "taste"; .info — leave for content peek
    "vkys.info": "unknown",
    # Croatian — content peek shows Latin-script Balkan cuisine
    # (musaka, kolač, ljutenica, kifle).
    "recepti.com": "hr",
    # bakeitwithlove.com — site is US English, but WDC sample has translated
    # versions in 5+ languages (Croatian, Turkish, Japanese, Spanish, English).
    # Treat as multi for ingredient-coverage purposes.
    "bakeitwithlove.com": "multi",
}


def tld_of(host: str) -> str:
    """Return the bottom-level country/generic TLD piece (.de, .com, .co.id → id)."""
    parts = host.lower().split(".")
    if len(parts) >= 2 and parts[-2] == "co" and len(parts[-1]) == 2:
        return parts[-1]  # e.g. co.id → id, co.uk → uk
    return parts[-1]


def classify_host(host: str) -> tuple[str, str]:
    """Return (lang_code, source) where source ∈ {tld, generic, content, unknown}."""
    h = host.lower()
    if h in GENERIC_HOST_LANG:
        return GENERIC_HOST_LANG[h], "generic"
    tld = tld_of(h)
    if tld in TLD_LANG:
        return TLD_LANG[tld], "tld"
    return "unknown", "unknown"


# Fast script-based fallback for a content peek. Very cheap, only used
# if --verify is passed or a host is unknown.
SCRIPT_RANGES = {
    "Cyrillic": "ru",
    "Arabic": "ar",
    "Hiragana": "ja",
    "Katakana": "ja",
    "Han": "zh",  # could be ja/ko too, weakest signal
    "Hangul": "ko",
    "Thai": "th",
}


def script_lang(text: str) -> str | None:
    """Best-effort: dominant non-Latin script in text → ISO code, or None."""
    counts: dict[str, int] = defaultdict(int)
    for ch in text:
        if ch.isalpha():
            try:
                name = unicodedata.name(ch)
            except ValueError:
                continue
            for prefix, lang in SCRIPT_RANGES.items():
                if name.startswith(prefix.upper()):
                    counts[lang] += 1
                    break
        if sum(counts.values()) > 200:
            break
    if not counts:
        return None
    return max(counts, key=counts.get)  # type: ignore[arg-type]


def peek_host_content(host: str, n_names: int = 20) -> str:
    """Concatenate up to N recipe names from this host's gzipped JSON
    inside Recipe_top100.zip. Returns "" if not present."""
    member = f"Recipe_{host}_October2023.json.gz"
    if not TOP100_ZIP.exists():
        return ""
    with zipfile.ZipFile(TOP100_ZIP) as zf:
        try:
            raw = zf.read(member)
        except KeyError:
            return ""
    names: list[str] = []
    with gzip.open(io.BytesIO(raw), "rt", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            name = obj.get("name") or ""
            if isinstance(name, str) and name:
                names.append(name)
            if len(names) >= n_names:
                break
    return " ".join(names)


def verify_via_content(host: str) -> str | None:
    """Return language code inferred from content peek, or None.

    Only uses the script-based detector (Cyrillic, Arabic, CJK). Latin-script
    languages cannot be reliably distinguished from a 30-name sample without
    a real language-ID library, so we leave them to the hand-curated map.
    Han characters get classified as Chinese here, which produces a known
    false positive on Japanese hosts that mostly use kanji — content-peek
    treat ja/zh disagreements with that limitation in mind.
    """
    text = peek_host_content(host, n_names=30)
    if not text:
        return None
    return script_lang(text)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Content-peek every non-TLD host to confirm classification.",
    )
    parser.add_argument(
        "--show-hosts",
        action="store_true",
        help="Print per-host classification, not just per-language aggregate.",
    )
    args = parser.parse_args()

    if not STATS_CSV.exists():
        print(f"Stats CSV not found: {STATS_CSV}", file=sys.stderr)
        return 1

    with STATS_CSV.open() as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    # Each classified row: (host, n_recipes, lang_code, source).
    classified: list[tuple[str, int, str, str]] = []
    # Each disagreement row: (host, map_lang, content_lang, map_source).
    disagreements: list[tuple[str, str, str, str]] = []
    for row in rows:
        host = row["host"]
        n = int(row["number_of_rows"])
        lang, source = classify_host(host)
        if lang == "unknown":
            inferred = verify_via_content(host)
            if inferred:
                lang, source = inferred, "content"
        elif args.verify and lang not in ("multi",):
            inferred = verify_via_content(host)
            if inferred and inferred != lang:
                disagreements.append((host, lang, inferred, source))
        classified.append((host, n, lang, source))

    # Aggregate per language.
    lang_totals: dict[str, int] = defaultdict(int)
    lang_hosts: dict[str, list[tuple[str, int]]] = defaultdict(list)
    for host, n, lang, _src in classified:
        lang_totals[lang] += n
        lang_hosts[lang].append((host, n))

    print("=" * 70)
    print("WDC top-100 hosts by language (recipe count)")
    print("=" * 70)
    print(f"{'Language':<22} {'Hosts':>6} {'Recipes':>10}  {'Cumulative':>10}")
    print("-" * 70)
    total = sum(lang_totals.values())
    cum = 0
    for lang, n in sorted(lang_totals.items(), key=lambda x: -x[1]):
        cum += n
        n_hosts = len(lang_hosts[lang])
        pct_cum = 100.0 * cum / total
        print(
            f"{LANG_NAMES.get(lang, lang):<22} {n_hosts:>6} {n:>10}  {pct_cum:>9.1f}%"
        )
    print("-" * 70)
    print(f"{'TOTAL':<22} {len(classified):>6} {total:>10}")

    if args.show_hosts:
        print()
        print("=" * 70)
        print("Per-host detail (grouped by language)")
        print("=" * 70)
        for lang, _ in sorted(lang_totals.items(), key=lambda x: -x[1]):
            print(f"\n[{LANG_NAMES.get(lang, lang)}]")
            for host, n in sorted(lang_hosts[lang], key=lambda x: -x[1]):
                src = next(s for h, _, _l, s in classified if h == host)
                print(f"  {host:<40} {n:>8}  ({src})")

    if args.verify and disagreements:
        print()
        print("=" * 70)
        print(
            f"Content-verify disagreements ({len(disagreements)}) — hand-curated kept"
        )
        print("=" * 70)
        for host, orig, inferred, source in disagreements:
            print(f"  {host:<40} map={orig}/{source}  content→{inferred}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
