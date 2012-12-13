"""Normalize input data."""

def to_grams(ingredients, rows):
    """Normalize input ingredient measures to grams (from volume based and
       non-gram based weight units."""
    rows = rows
    line_nr = 2
    for row in rows:
        yield tuple(unit.norm(value, ingredient, line_nr) for (value, unit),
                    ingredient in zip(row, ingredients))
        line_nr += 1

def normalize_to_100g(rows):
    """Normalize to 100g recipe"""
    for row in rows:
        multiplier = 100 / float(sum(row))
        yield tuple(value * multiplier for value in row)
