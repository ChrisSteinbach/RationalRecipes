"""Cross-language canonicalization of ingredient names via IngredientFactory.

Extraction paths (RecipeNLG NER, WDC LLM) produce raw ingredient names in
whatever language the source recipe used. This module maps each raw name
to its English canonical form by looking it up through the ingredient
synonym table. Names that don't resolve are kept in their
lowercased-stripped original form so partial DB coverage doesn't silently
drop ingredients from the set.

A static Swedish→English dictionary (``SWEDISH_TO_ENGLISH``) covers
common baking/cooking nouns the synonym table misses, and also rewrites
the handful of foods whose ingredients-DB canonical is itself Swedish
(e.g. ``olja``, ``tomat``). Translation runs both before and after the
synonym lookup so it catches DB misses and Swedish-canonical hits alike.
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
}


def _translate_swedish(name: str) -> str:
    """Apply the Swedish→English dictionary; passthrough on miss."""
    return SWEDISH_TO_ENGLISH.get(name, name)


def canonicalize_name(name: str) -> str:
    """Map a raw ingredient name to a canonical English form.

    Pre-translates Swedish nouns via ``SWEDISH_TO_ENGLISH``, looks the
    result up in the ingredient synonym table, then post-translates the
    DB result for the few foods whose stored canonical is Swedish.
    Returns the lowercased-stripped original on miss (empty input yields
    empty string).
    """
    normalized = name.lower().strip()
    if not normalized:
        return ""
    pre_translated = _translate_swedish(normalized)
    try:
        result = IngredientFactory.get_by_name(pre_translated).canonical_name()
    except KeyError:
        result = pre_translated
    return _translate_swedish(result)


def canonicalize_names(names: Iterable[str]) -> frozenset[str]:
    """Canonicalize a batch of raw ingredient names."""
    return frozenset(c for c in (canonicalize_name(n) for n in names) if c)
