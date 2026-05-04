"""Shared loader-level helpers for corpus ingestion."""

from __future__ import annotations

_MAX_INGREDIENT_LINE_LEN = 150
_SENTENCE_ENDS = (". ", "! ", "? ")
_PROSE_SENTENCE_THRESHOLD = 3


def looks_like_prose(line: str) -> bool:
    """Return True if *line* looks like narrative text, not an ingredient.

    Trips on length > 150 chars, embedded URLs, or 3+ sentence-end
    punctuation marks. Used to drop notes, paragraphs, and stray URLs
    that slipped into ``recipeingredient`` arrays before they reach the
    LLM parser, which would otherwise burn a call and return ``{}``.
    """
    if len(line) > _MAX_INGREDIENT_LINE_LEN:
        return True
    if "http://" in line or "https://" in line:
        return True
    sentence_count = sum(line.count(end) for end in _SENTENCE_ENDS)
    if sentence_count >= _PROSE_SENTENCE_THRESHOLD:
        return True
    return False


def filter_ingredient_lines(lines: tuple[str, ...]) -> tuple[str, ...]:
    """Drop prose-looking entries from a raw ingredient-line tuple."""
    return tuple(line for line in lines if not looks_like_prose(line))
