"""Cross-language canonicalization of ingredient names via IngredientFactory.

Extraction paths (RecipeNLG NER, WDC LLM) produce raw ingredient names in
whatever language the source recipe used. This module maps each raw name
to its English canonical form by looking it up through the ingredient
synonym table. Names that don't resolve are kept in their
lowercased-stripped original form so partial DB coverage doesn't silently
drop ingredients from the set.

As of bead e4s, Pass 1 (the WDC ingredient-line LLM parse — see
``scrape/wdc.py::NEUTRAL_PROMPT``) is the primary translation point: the
LLM is instructed to emit English ingredient names directly. The
``SWEDISH_TO_ENGLISH`` dictionary and ``_translate_swedish`` stay as
defense-in-depth — they catch LLM misses and rewrite the handful of foods
whose ingredients-DB canonical is itself Swedish (e.g. ``olja`` → oil,
``tomat`` → tomato). Translation runs both before and after the synonym
lookup so it catches DB misses and Swedish-canonical hits alike.
"""

from __future__ import annotations

from collections.abc import Iterable

from rational_recipes.ingredient import Factory as IngredientFactory

# Static Swedish→English translations for ingredient nouns the synonym
# DB does not cover (or whose DB canonical is itself Swedish). Keys are
# lowercased exact matches; the post-DB pass means Swedish DB canonicals
# like "olja" still get rewritten.
SWEDISH_TO_ENGLISH: dict[str, str] = {
    # Flours and grains
    "mjöl": "flour",
    "rågmjöl": "rye flour",
    "havremjöl": "oat flour",
    "majsmjöl": "cornmeal",
    "maizena": "cornstarch",
    "ströbröd": "breadcrumbs",
    # Sugars and sweeteners
    "rörsocker": "sugar",
    "vaniljsocker": "vanilla sugar",
    "lönnsirap": "maple syrup",
    "sirap": "syrup",
    # Fats and oils
    "margarin": "margarine",
    "olja": "oil",
    "rapsolja": "canola oil",
    "olivolja": "olive oil",
    "kokosolja": "coconut oil",
    "solrosolja": "sunflower oil",
    # Dairy
    "vispgrädde": "whipping cream",
    "gräddfil": "sour cream",
    "crème fraiche": "creme fraiche",
    "färskost": "cream cheese",
    "kvarg": "quark",
    "ost": "cheese",
    "yoghurt": "yogurt",
    "filmjölk": "buttermilk",
    "mjölkpulver": "milk powder",
    # Eggs
    "äggula": "egg yolk",
    "äggvita": "egg white",
    # Nuts
    "nötter": "nuts",
    "pekannötter": "pecans",
    "valnötter": "walnuts",
    "hasselnötter": "hazelnuts",
    "jordnötter": "peanuts",
    "mandlar": "almonds",
    "sötmandel": "sweet almond",
    "rivna nötter": "almonds",
    "kokos": "coconut",
    # Spices and herbs
    "vanilj": "vanilla",
    "ingefära": "ginger",
    "muskot": "nutmeg",
    "nejlika": "clove",
    "peppar": "pepper",
    "svartpeppar": "black pepper",
    "vitpeppar": "white pepper",
    "persilja": "parsley",
    "basilika": "basil",
    "rosmarin": "rosemary",
    "timjan": "thyme",
    "lagerblad": "bay leaf",
    "koriander": "coriander",
    "kummin": "cumin",
    # Acids
    "vinäger": "vinegar",
    "citron": "lemon",
    "citronsaft": "lemon juice",
    "limesaft": "lime juice",
    # Other baking
    "kakao": "cocoa",
    "kakaopulver": "cocoa powder",
    "choklad": "chocolate",
    "russin": "raisins",
    # Vegetables
    "lök": "onion",
    "gul lök": "onion",
    "rödlök": "red onion",
    "vitlök": "garlic",
    "morot": "carrot",
    "tomat": "tomato",
    "krossade tomater": "crushed tomatoes",
    "tomatpuré": "tomato paste",
    "potatis": "potato",
    "paprika": "bell pepper",
    "gurka": "cucumber",
    # Fruits and berries
    "äpple": "apple",
    "banan": "banana",
    "apelsin": "orange",
    "päron": "pear",
    "jordgubbar": "strawberries",
    "hallon": "raspberries",
    # Meats and seafood
    "nötkött": "ground beef",
    "korv": "sausage",
    "skinka": "ham",
    "räkor": "shrimp",
    # Cheese
    "mozzarellaost": "mozzarella cheese",
    # Corn and beans
    "majs": "corn",
    "bönor": "beans",
    "vita bönor": "white beans",
    # Fruit
    "ananas": "pineapple",
    # Juice
    "apelsinjuice": "orange juice",
    # Sauces and condiments
    "soja": "soy sauce",
    "sojasås": "soy sauce",
    "soyasås": "soy sauce",
    "majonnäs": "mayonnaise",
    "senap": "mustard",
    "tomatsås": "tomato sauce",
    # Spices (additional)
    "cayennepeppar": "cayenne pepper",
    # Herbs (additional)
    "salladslök": "scallion",
    "gräslök": "chives",
    # Baking (additional)
    "chokladhackor": "chocolate chips",
    "citronskal": "lemon zest",
    "sesamfrön": "sesame seeds",
    # Acids (additional)
    "ättika": "vinegar",
    "balsamvinäger": "balsamic vinegar",
    "rödvinsvinäger": "red wine vinegar",
    # Broths
    "nötbuljong": "beef broth",
    # Liquids
    "kaffe": "coffee",
    # Additional Swedish DB-canonical ingredients caught during r6w
    # post-translation audit (see usda_match._to_english). The bias is
    # the same as the rest of the dict: minimum-noise, exact-match
    # entries that flip Swedish to English. Each one is a real DB
    # canonical_name returned for an English-input query — leaving it
    # unmapped means a regex hit would produce e.g. "spenat" while the
    # LLM hot path emits "spinach", splitting variants downstream.
    "spenat": "spinach",
    "blomkål": "cauliflower",
    "bröd": "bread",
    "cashewnötter": "cashews",
    "kikärtor": "chickpeas",
    "kokosmjölk": "coconut milk",
    "paprikapulver": "paprika",
    "pinjenötter": "pine nuts",
    "purjolök": "leek",
    "röda linser": "red lentils",
    "sesamolja": "sesame oil",
    "solrosfrön": "sunflower seeds",
    "svarta bönor": "black beans",
    "vallmofrön": "poppy seeds",
    "ärtor": "peas",
    # Caught by the cache shadow comparison — DB synonyms whose canonical
    # field is Swedish even though the synonym name is English.
    "champinjoner": "mushrooms",
    "mynta": "mint",
    "parmesanost": "parmesan cheese",
    # Pannkakor + recipe-staple Swedish words that previously routed
    # through ``food.canonical_name``. Now that the DB returns per-synonym
    # canonicals (dfm), bare Swedish words need an explicit translation
    # to land on the English umbrella for variant clustering. Auto-extend
    # below adds ASCII-folded siblings (``socker`` already ASCII) so OCR
    # pipelines that strip diacritics still hit the same target.
    "vetemjöl": "flour",
    "mjölk": "milk",
    "ägg": "egg",
    "socker": "sugar",
    "smör": "butter",
    "grädde": "cream",
    "bakpulver": "baking powder",
    "kanel": "cinnamon",
    "kardemumma": "cardamom",
    "fläsk": "bacon",
    "saffran": "saffron",
    "havregryn": "oats",
    "jäst": "yeast",
    "vatten": "water",
    "honung": "honey",
    # Spice/herb Swedish staples that didn't have explicit dict entries
    "ris": "rice",
    "mandel": "almond",
    # Additional words found in the DB whose food canonicals are themselves
    # Swedish — translating the synonym directly avoids the food-umbrella
    # round-trip.
    "kidneybönor": "kidney beans",
    "mörk choklad": "dark chocolate",
    "morötter": "carrots",
}


# ASCII-folded variants extend the static dict so a corpus that strips
# diacritics (OCR pipelines, some HTML normalizers) still hits the
# Swedish→English path. Each diacritic key auto-generates the å→a /
# ä→a / ö→o folded sibling at module load — explicit dict entries
# override (so adding e.g. ``"agg": "egg"`` directly stays canonical).
_ASCII_FOLD = str.maketrans(
    {"å": "a", "ä": "a", "ö": "o", "Å": "A", "Ä": "A", "Ö": "O"}
)

_EXTENDED_SWEDISH_TO_ENGLISH: dict[str, str] = dict(SWEDISH_TO_ENGLISH)
for _sv, _en in SWEDISH_TO_ENGLISH.items():
    _ascii_form = _sv.translate(_ASCII_FOLD)
    if _ascii_form != _sv:
        _EXTENDED_SWEDISH_TO_ENGLISH.setdefault(_ascii_form, _en)


def _translate_swedish(name: str) -> str:
    """Apply the Swedish→English dictionary (incl. ASCII-folded forms)."""
    return _EXTENDED_SWEDISH_TO_ENGLISH.get(name, name)


_SWEDISH_DIACRITICS = frozenset("åäöÅÄÖ")

# Common English plural↔singular endings used to collapse morphological
# variants (``eggs`` → ``egg``, ``tomatoes`` → ``tomato``) onto their
# food's English umbrella canonical. Listed longest-first so ``"ies"`` is
# tried before ``"es"`` and ``"s"``.
_PLURAL_ENDINGS: tuple[tuple[str, str], ...] = (
    ("ies", "y"),
    ("es", ""),
    ("s", ""),
)


def _collapse_plural(lookup: str, canonical: str) -> str | None:
    """Return ``canonical`` if ``lookup`` is a plural/singular variant.

    Used to merge ``eggs`` / ``egg`` and ``tomatoes`` / ``tomato`` onto a
    single canonical even though per-synonym semantics would otherwise
    keep them distinct. Returns ``None`` when the two strings aren't a
    morphological pair (so callers fall through to the per-synonym form).
    """
    if not lookup or not canonical:
        return None
    if lookup == canonical:
        return canonical
    for ending, replacement in _PLURAL_ENDINGS:
        if (
            lookup.endswith(ending)
            and lookup[: -len(ending)] + replacement == canonical
        ):
            return canonical
        if (
            canonical.endswith(ending)
            and canonical[: -len(ending)] + replacement == lookup
        ):
            return canonical
    return None


def canonicalize_name(name: str) -> str:
    """Map a raw ingredient name to a canonical English form.

    Per-synonym resolution (dfm) with three rules:

    1. **Specific synonyms preserve identity.** Pre-translate via
       ``SWEDISH_TO_ENGLISH``; if the result resolves as a DB synonym,
       return it. ``cheddar`` and ``cheese`` stay distinct even though
       both alias to the same FDC food.
    2. **Plural/singular morphological pairs collapse.** ``eggs`` and
       ``egg`` (sharing food canonical ``egg``) both return ``egg`` so
       variant clustering treats them as one.
    3. **Foreign-language synonyms fall to the food umbrella.** A
       Swedish word with diacritics that the dict can't translate
       (``körsbärstomater``) routes through the food's English
       umbrella ``food_canonical_name`` (``tomato``).

    Returns the lowercased-stripped original on miss (empty input yields
    empty string).
    """
    normalized = name.lower().strip()
    if not normalized:
        return ""
    pre_translated = _translate_swedish(normalized)

    # Tier 1: pre-translated form is a known synonym.
    try:
        ingredient = IngredientFactory.get_by_name(pre_translated)
    except KeyError:
        ingredient = None
    if ingredient is not None:
        # Foreign-not-translated guard: a Swedish diacritic word that the
        # dict couldn't map needs the food umbrella, not the per-synonym.
        looks_foreign = (
            pre_translated == normalized
            and any(c in _SWEDISH_DIACRITICS for c in normalized)
        )
        food_canon_raw = ingredient.food_canonical_name()
        food_canon_en = (
            _translate_swedish(food_canon_raw) if food_canon_raw else None
        )
        if looks_foreign:
            return food_canon_en or pre_translated
        # Plural/singular collapse against the food's English umbrella.
        if food_canon_en:
            collapsed = _collapse_plural(pre_translated, food_canon_en)
            if collapsed is not None:
                return collapsed
        return pre_translated

    # Tier 2: pre-translated isn't a DB synonym. The original might be —
    # useful for Swedish words whose dict translation produces a longer
    # phrase that's not itself a synonym (e.g. ``krossade tomater`` →
    # ``crushed tomatoes``: the LHS is in the synonym table, the RHS is
    # not).
    try:
        ingredient = IngredientFactory.get_by_name(normalized)
    except KeyError:
        return pre_translated
    # Prefer the dict translation when present — it's an intentional,
    # often-more-specific mapping than the food umbrella.
    if pre_translated != normalized:
        return pre_translated
    food_canon_raw = ingredient.food_canonical_name()
    if food_canon_raw:
        return _translate_swedish(food_canon_raw)
    return pre_translated


def canonicalize_names(names: Iterable[str]) -> frozenset[str]:
    """Canonicalize a batch of raw ingredient names."""
    return frozenset(c for c in (canonicalize_name(n) for n in names) if c)
