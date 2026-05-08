"""Variant raw-form provenance reader (extracted from RationalRecipes-4rgy).

Joins ``recipes.db`` variant members back to the source corpus
(RecipeNLG ``full_dataset.csv``), buckets raw ingredient lines under the
variant's canonical ingredients, and returns structured observations.

The CLI in ``scripts/inspect_variant_provenance.py`` is a thin shell that
imports this module and prints a text breakdown. The maintainer editor
(``scripts/editor.py`` via ``rational_recipes.editor.operations``) imports
the same module to surface a per-canonical breakdown table inline.

Read-only; no LLM calls. Cached LLM parses + regex fallbacks resolve
each raw line, the longest whole-word substring match in the variant's
canonical map decides which bucket a line lands in.
"""

from __future__ import annotations

import ast
import csv
import json
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path

from rational_recipes.catalog_db import CatalogDB
from rational_recipes.ingredient import Factory as IngredientFactory
from rational_recipes.scrape.canonical import canonicalize_name
from rational_recipes.scrape.ingredient_fold import FOLD_MAP, _build_form_index
from rational_recipes.scrape.parse import ParsedIngredient
from rational_recipes.scrape.regex_parse import (
    _consume_unit,
    _strip_approx_prefix,
    _strip_bullet,
    _strip_parentheticals,
    parse_quantity,
    regex_parse_line,
)
from rational_recipes.units import BadUnitException
from rational_recipes.units import Factory as UnitFactory


@dataclass(frozen=True, slots=True)
class RawObservation:
    """One raw-line observation tied to a variant canonical."""

    recipe_id: str
    raw_line: str
    form_key: str  # normalized descriptive form for grouping
    grams: float | None  # mass in grams when convertible


@dataclass(frozen=True, slots=True)
class FormSummary:
    """Aggregated raw-form row in a canonical breakdown table."""

    form_key: str
    count: int
    mean_grams: float | None
    n_with_grams: int  # observations with a mass conversion
    example_raw_line: str
    recipe_ids: tuple[str, ...]  # source recipes contributing this form


@dataclass(frozen=True, slots=True)
class CanonicalProvenance:
    """All observations for one canonical ingredient in a variant."""

    canonical: str
    forms: list[FormSummary]
    total_observations: int


@dataclass(frozen=True, slots=True)
class VariantProvenance:
    """Full provenance bundle for a variant — canonicals + corpus coverage."""

    variant_id: str
    canonicals: list[CanonicalProvenance]
    n_recipenlg_members: int
    n_recipenlg_hit: int
    n_other_corpora: int
    unmatched_count: int
    unmatched_per_recipe: dict[str, list[str]]
    n_corpus_members: int  # number of source recipes whose raw lines were found


def build_variant_canonical_map(
    variant_canonicals: Iterable[str],
    fold_map: Mapping[str, frozenset[str]] = FOLD_MAP,
) -> dict[str, str]:
    """Map any in-family form -> the variant's keeper canonical."""
    form_to_family = _build_form_index(fold_map)
    variant_set = set(variant_canonicals)

    family_keeper: dict[str, str] = {}
    for family, forms in fold_map.items():
        keepers = [f for f in forms if f in variant_set]
        if len(keepers) == 1:
            family_keeper[family] = keepers[0]

    out: dict[str, str] = {c: c for c in variant_set}
    for form, family in form_to_family.items():
        keeper = family_keeper.get(family)
        if keeper is not None:
            out[form] = keeper
    return out


def extract_form_key(line: str) -> tuple[float | None, str | None, str]:
    """Strip leading qty + unit; return (qty, unit_canonical, descriptive_form_key)."""
    text = line.strip()
    text = _strip_bullet(text).strip()
    text = _strip_approx_prefix(text).strip()
    text = _strip_parentheticals(text)
    if not text:
        return None, None, ""

    qty_parse = parse_quantity(text)
    if qty_parse is None:
        return None, None, " ".join(text.lower().split())

    qty = qty_parse.quantity
    after_qty = text[qty_parse.consumed:].lstrip()
    unit, after_unit = _consume_unit(after_qty)

    name_part = after_unit.split(",", 1)[0].strip()
    form_key = " ".join(name_part.lower().split())
    return qty, unit, form_key


def _grams_for_parse(parsed: ParsedIngredient) -> float | None:
    """Convert a parsed ingredient to grams via the unit + ingredient registry."""
    canonical = canonicalize_name(parsed.ingredient)
    if not canonical:
        return None
    try:
        ingredient = IngredientFactory.get_by_name(canonical)
    except KeyError:
        return None
    unit = UnitFactory.get_by_name(parsed.unit)
    if unit is None:
        return None
    if parsed.quantity == 0:
        return 0.0
    try:
        g = unit.norm(parsed.quantity, ingredient)
    except BadUnitException:
        return None
    return float(g)


def _resolve_canonical_for_line(
    raw_line: str,
    db: CatalogDB,
    *,
    model: str,
    seed: int,
) -> ParsedIngredient | None:
    """Return a ParsedIngredient for raw_line: cached LLM first, regex second."""
    found, payload = db.lookup_cached_parse(raw_line, model=model, seed=seed)
    if found and payload is not None:
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            data = None
        if isinstance(data, dict):
            try:
                return ParsedIngredient(
                    quantity=float(data["quantity"]),
                    unit=str(data["unit"]),
                    ingredient=str(data["ingredient"]).lower().strip(),
                    preparation=str(data.get("preparation", "")),
                    raw=raw_line,
                )
            except (KeyError, ValueError, TypeError):
                pass
    rp = regex_parse_line(raw_line)
    if rp is not None:
        return rp.parsed
    return None


def _substring_fallback(
    form_key: str, canonical_map: Mapping[str, str]
) -> str | None:
    """Map a form_key to a variant canonical via whole-word substring match."""
    if not form_key:
        return None
    padded = f" {form_key} "
    best: tuple[int, str] | None = None
    for form, target in canonical_map.items():
        if not form:
            continue
        needle = f" {form} "
        starts = padded.startswith(form + " ")
        ends = padded.endswith(" " + form)
        if needle in padded or starts or ends:
            if best is None or len(form) > best[0]:
                best = (len(form), target)
    return best[1] if best else None


def aggregate_raw_observations(
    member_raw_lines: Mapping[str, list[str]],
    canonical_map: Mapping[str, str],
    *,
    db: CatalogDB,
    model: str = "gemma4:e2b",
    seed: int = 42,
) -> tuple[
    dict[str, list[RawObservation]],
    dict[str, list[str]],
]:
    """Bucket raw ingredient lines under the variant's canonical ingredients.

    Returns ``(observations_per_canonical, unmatched_per_recipe)``. The
    second dict captures lines that parsed but didn't map to any of the
    variant's canonicals.
    """
    observations: dict[str, list[RawObservation]] = defaultdict(list)
    unmatched: dict[str, list[str]] = defaultdict(list)

    for recipe_id, lines in member_raw_lines.items():
        for line in lines:
            parsed = _resolve_canonical_for_line(line, db, model=model, seed=seed)
            _, _, form_key = extract_form_key(line)
            target: str | None = None
            grams: float | None = None
            if parsed is not None:
                canon = canonicalize_name(parsed.ingredient)
                if canon:
                    target = canonical_map.get(canon)
                grams = _grams_for_parse(parsed)
            if target is None:
                target = _substring_fallback(form_key, canonical_map)
            if target is None:
                unmatched[recipe_id].append(line)
                continue
            if not form_key:
                form_key = target
            observations[target].append(
                RawObservation(
                    recipe_id=recipe_id,
                    raw_line=line,
                    form_key=form_key,
                    grams=grams,
                )
            )

    return dict(observations), dict(unmatched)


def load_recipenlg_raw_lines(
    recipenlg_path: Path, target_urls: set[str]
) -> dict[str, list[str]]:
    """Stream RecipeNLG once and return ``{url: raw_ingredient_lines}`` for hits."""
    out: dict[str, list[str]] = {}
    if not target_urls:
        return out
    with open(recipenlg_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            link = row.get("link", "")
            if link not in target_urls:
                continue
            try:
                parsed = ast.literal_eval(row.get("ingredients", "[]"))
            except (ValueError, SyntaxError):
                parsed = []
            if isinstance(parsed, list):
                out[link] = [str(s) for s in parsed]
            if len(out) == len(target_urls):
                break
    return out


def summarize_observations(
    observations: Mapping[str, list[RawObservation]],
    canonical_order: Iterable[str],
) -> list[CanonicalProvenance]:
    """Group observations by form_key and emit one CanonicalProvenance per canonical."""
    out: list[CanonicalProvenance] = []
    for canon in canonical_order:
        rows = observations.get(canon, [])
        forms: dict[str, list[RawObservation]] = defaultdict(list)
        for obs in rows:
            forms[obs.form_key].append(obs)
        summaries: list[FormSummary] = []
        for form_key, obs_list in forms.items():
            with_grams = [o.grams for o in obs_list if o.grams is not None]
            mean_g = (
                sum(with_grams) / len(with_grams) if with_grams else None
            )
            summaries.append(
                FormSummary(
                    form_key=form_key or "(unspecified)",
                    count=len(obs_list),
                    mean_grams=mean_g,
                    n_with_grams=len(with_grams),
                    example_raw_line=obs_list[0].raw_line,
                    recipe_ids=tuple(o.recipe_id for o in obs_list),
                )
            )
        summaries.sort(key=lambda s: (-s.count, s.form_key))
        out.append(
            CanonicalProvenance(
                canonical=canon,
                forms=summaries,
                total_observations=len(rows),
            )
        )
    return out


def load_variant_provenance(
    db: CatalogDB,
    variant_id: str,
    recipenlg_path: Path,
    *,
    model: str = "gemma4:e2b",
    seed: int = 42,
) -> VariantProvenance | None:
    """Top-level reader: join corpus, aggregate, summarize.

    Returns ``None`` if the variant doesn't exist. Returns a
    ``VariantProvenance`` with empty canonicals + ``n_recipenlg_hit=0``
    when the corpus CSV is missing or has no overlap with the variant —
    the editor surface uses the coverage counters to render an empty
    state without crashing.
    """
    variant = db.get_variant(variant_id)
    if variant is None:
        return None

    stats = db.get_ingredient_stats(variant_id)
    canonical_order = [s.canonical_name for s in stats]
    members = db.get_variant_members(variant_id)

    recipenlg_members = [
        m for m in members if m.corpus == "recipenlg" and m.url
    ]
    skipped_corpora = [m for m in members if m.corpus != "recipenlg"]
    target_urls = {m.url for m in recipenlg_members if m.url is not None}

    if recipenlg_path.exists() and target_urls:
        url_to_lines = load_recipenlg_raw_lines(recipenlg_path, target_urls)
    else:
        url_to_lines = {}

    member_raw_lines: dict[str, list[str]] = {}
    url_to_recipe_id = {m.url: m.recipe_id for m in recipenlg_members}
    for url, lines in url_to_lines.items():
        recipe_id = url_to_recipe_id.get(url)
        if recipe_id is not None:
            member_raw_lines[recipe_id] = lines

    canonical_map = build_variant_canonical_map(canonical_order)
    observations, unmatched = aggregate_raw_observations(
        member_raw_lines, canonical_map, db=db, model=model, seed=seed
    )
    canonicals = summarize_observations(observations, canonical_order)
    unmatched_lines = sum(len(v) for v in unmatched.values())

    return VariantProvenance(
        variant_id=variant_id,
        canonicals=canonicals,
        n_recipenlg_members=len(recipenlg_members),
        n_recipenlg_hit=len(member_raw_lines),
        n_other_corpora=len(skipped_corpora),
        unmatched_count=unmatched_lines,
        unmatched_per_recipe=dict(unmatched),
        n_corpus_members=len(member_raw_lines),
    )
