"""LLM-based ingredient line parsing via Ollama.

Turns natural-language ingredient strings ("1 1/2 cups flour, sifted") into
structured fields (quantity, unit, ingredient, preparation).

Uses the Ollama REST API (http://localhost:11434) for reliability.
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from dataclasses import dataclass

logger = logging.getLogger(__name__)

OLLAMA_BASE_URL = "http://localhost:11434"

_SYSTEM_PROMPT = """\
You are an ingredient parser. Given a recipe ingredient line, extract structured fields.

Return ONLY a JSON object with these fields:
- "quantity": number (float). For fractions like "1/2", convert to decimal (0.5). \
For "1 1/2", convert to 1.5. For ranges like "1-2", use the midpoint (1.5). \
For "a pinch" or "to taste", use 0. If no quantity, use 1.
- "unit": the unit of measurement (e.g. "cup", "tbsp", "tsp", "oz", "lb", "g", "ml"). \
Normalize abbreviations: "c." → "cup", "Tbsp." → "tbsp", "tsp." → "tsp". \
For whole items like "2 eggs" or "2 large eggs", use "MEDIUM" (or "LARGE"/"SMALL" if specified). \
If no unit, use "g" for weight items or "MEDIUM" for countable items.
- "ingredient": the base ingredient name, lowercase, no preparation notes. \
E.g. "all-purpose flour" → "flour", "unsalted butter" → "butter", \
"large eggs" → "egg". Remove plurals for countable items.
- "preparation": any preparation notes (e.g. "sifted", "melted", "chopped", "separated"). \
Empty string if none.

Examples:
Input: "1 c. all-purpose flour"
Output: {"quantity": 1.0, "unit": "cup", "ingredient": "flour", "preparation": ""}

Input: "1/2 c. evaporated milk"
Output: {"quantity": 0.5, "unit": "cup", "ingredient": "milk", "preparation": ""}

Input: "2 Tbsp. butter or margarine"
Output: {"quantity": 2.0, "unit": "tbsp", "ingredient": "butter", "preparation": ""}

Input: "3 eggs, separated"
Output: {"quantity": 3.0, "unit": "MEDIUM", "ingredient": "egg", "preparation": "separated"}

Input: "2 large eggs"
Output: {"quantity": 2.0, "unit": "LARGE", "ingredient": "egg", "preparation": ""}

Input: "1 1/2 c. water"
Output: {"quantity": 1.5, "unit": "cup", "ingredient": "water", "preparation": ""}

Input: "1 pinch salt"
Output: {"quantity": 1.0, "unit": "pinch", "ingredient": "salt", "preparation": ""}

Input: "1/4 tsp. salt"
Output: {"quantity": 0.25, "unit": "tsp", "ingredient": "salt", "preparation": ""}

Input: "1 cup milk"
Output: {"quantity": 1.0, "unit": "cup", "ingredient": "milk", "preparation": ""}

Input: "1/2 cup heavy cream, whipped"
Output: {"quantity": 0.5, "unit": "cup", "ingredient": "cream", "preparation": "whipped"}

Input: "1 tablespoon sugar"
Output: {"quantity": 1.0, "unit": "tbsp", "ingredient": "sugar", "preparation": ""}

Input: "1 Tbsp. margarine"
Output: {"quantity": 1.0, "unit": "tbsp", "ingredient": "margarine", "preparation": ""}
"""


@dataclass(frozen=True, slots=True)
class ParsedIngredient:
    """Structured representation of a parsed ingredient line."""

    quantity: float
    unit: str
    ingredient: str
    preparation: str
    raw: str


def _ollama_generate(
    prompt: str,
    model: str,
    system: str = _SYSTEM_PROMPT,
    base_url: str = OLLAMA_BASE_URL,
    timeout: float = 120.0,
    num_predict: int = 256,
) -> str | None:
    """Call Ollama REST API /api/generate, return the response text.

    ``num_predict`` caps the generated-token count. Ingredient JSONs are
    well under 100 tokens, so a small cap turns degenerate token-loop
    responses (seen on larger models like gemma4:26b) into bounded-length
    failures rather than 300s timeouts.

    ``temperature=0`` + ``seed=42`` make the response deterministic so
    that downstream ``variant_id`` hashes are stable across reruns of
    the merged pipeline (RationalRecipes-toj). Without them, sampling
    noise was occasionally swapping ingredients ("sugar" vs "flour")
    between runs and producing different ``canonical_ingredient_set``s
    for the same recipe.
    """
    payload = json.dumps(
        {
            "model": model,
            "system": system,
            "prompt": prompt,
            "format": "json",
            "stream": False,
            "options": {
                "num_predict": num_predict,
                "temperature": 0.0,
                "seed": 42,
            },
        }
    ).encode()

    req = urllib.request.Request(
        f"{base_url}/api/generate",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = json.loads(resp.read())
            # "response" is the visible output for non-thinking models; some
            # thinking models (e.g. qwen3.5) place the JSON in "thinking"
            # and leave "response" empty. Prefer response when non-empty.
            visible = body.get("response") or ""
            if visible.strip():
                return visible
            thinking = body.get("thinking") or ""
            return thinking or visible
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
        logger.warning("Ollama API call failed: %s", e)
        return None


def parse_ingredient_line(
    line: str,
    model: str = "qwen3.6:35b-a3b",
    base_url: str = OLLAMA_BASE_URL,
    system_prompt: str | None = None,
    timeout: float = 120.0,
    num_predict: int = 256,
) -> ParsedIngredient | None:
    """Parse a single ingredient line using Ollama.

    Returns None if parsing fails.
    """
    prompt = f'Parse this ingredient line:\nInput: "{line}"\nOutput:'

    raw_output = _ollama_generate(
        prompt,
        model=model,
        system=system_prompt or _SYSTEM_PROMPT,
        base_url=base_url,
        timeout=timeout,
        num_predict=num_predict,
    )
    if raw_output is None:
        return None

    try:
        data = json.loads(raw_output)
    except json.JSONDecodeError:
        # Try extracting JSON from the output
        start = raw_output.find("{")
        end = raw_output.rfind("}") + 1
        if start >= 0 and end > start:
            try:
                data = json.loads(raw_output[start:end])
            except json.JSONDecodeError:
                logger.warning(
                    "Could not parse JSON from Ollama for %r: %s",
                    line,
                    raw_output[:200],
                )
                return None
        else:
            logger.warning(
                "No JSON in Ollama output for %r: %s",
                line,
                raw_output[:200],
            )
            return None

    # Smaller Ollama models (notably gemma3n:e2b) occasionally misspell the
    # "ingredient" key as "ingruedient"/"ingrredient"/etc.; accept any key
    # whose name starts with "ingr" as the ingredient field so we don't
    # discard an otherwise-usable parse.
    ingredient_key = next(
        (k for k in data if isinstance(k, str) and k.lower().startswith("ingr")),
        None,
    )
    if ingredient_key is None:
        logger.warning("No ingredient key in parsed data for %r: %s", line, data)
        return None

    try:
        return ParsedIngredient(
            quantity=float(data["quantity"]),
            unit=str(data["unit"]),
            ingredient=str(data[ingredient_key]).lower().strip(),
            preparation=str(data.get("preparation", "")),
            raw=line,
        )
    except (KeyError, ValueError, TypeError) as e:
        logger.warning("Invalid parsed data for %r: %s — %s", line, data, e)
        return None


def parse_ingredient_lines(
    lines: list[str],
    model: str = "qwen3.6:35b-a3b",
    base_url: str = OLLAMA_BASE_URL,
    system_prompt: str | None = None,
    timeout: float = 120.0,
    num_predict: int = 256,
) -> list[ParsedIngredient | None]:
    """Parse multiple ingredient lines. Returns a list parallel to the input."""
    return [
        parse_ingredient_line(
            line,
            model=model,
            base_url=base_url,
            system_prompt=system_prompt,
            timeout=timeout,
            num_predict=num_predict,
        )
        for line in lines
    ]
