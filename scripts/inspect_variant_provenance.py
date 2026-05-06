#!/usr/bin/env python3
"""Spike: surface raw forms behind each variant canonical (RationalRecipes-4rgy).

Reads ``recipes.db`` for a variant's members, joins back to the source corpus
(RecipeNLG ``full_dataset.csv``), and groups raw ingredient lines under the
variant's canonical ingredients. Read-only; no LLM calls.

Usage:
    python3 scripts/inspect_variant_provenance.py b34c2dce79e2
"""

from __future__ import annotations

import argparse
import ast
import csv
import json
import sys
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
class _RawObservation:
    """One raw-line observation tied to a variant canonical."""

    raw_line: str
    form_key: str  # normalized descriptive form for grouping
    grams: float | None  # mass in grams when convertible


def build_variant_canonical_map(
    variant_canonicals: Iterable[str],
    fold_map: Mapping[str, frozenset[str]] = FOLD_MAP,
) -> dict[str, str]:
    """Map any in-family form -> the variant's keeper canonical.

    Handles fold families: if the variant has 'margarine' as keeper of the
    butter family, every form in {butter, margarine, ...} maps to 'margarine'.
    Single-form canonicals map to themselves.
    """
    form_to_family = _build_form_index(fold_map)
    variant_set = set(variant_canonicals)

    family_keeper: dict[str, str] = {}
    for family, forms in fold_map.items():
        keepers = [f for f in forms if f in variant_set]
        if len(keepers) == 1:
            family_keeper[family] = keepers[0]
        # If 0 or 2+ keepers in the variant, the fold either didn't apply
        # to this family or the variant is post-fold-with-multiple-survivors;
        # fall through to direct-name mapping below.

    out: dict[str, str] = {c: c for c in variant_set}
    for form, family in form_to_family.items():
        keeper = family_keeper.get(family)
        if keeper is not None:
            out[form] = keeper
    return out


def extract_form_key(line: str) -> tuple[float | None, str | None, str]:
    """Strip leading qty + unit; return (qty, unit_canonical, descriptive_form_key).

    The form_key preserves descriptive specifiers like '70% cacao' or 'crisco'
    that aggressive USDA canonicalization throws away. Lowercased and
    whitespace-collapsed for grouping.
    """
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

    # Drop trailing prep after first comma; keep parentheticals already
    # stripped above.
    name_part = after_unit.split(",", 1)[0].strip()
    form_key = " ".join(name_part.lower().split())
    return qty, unit, form_key


def _grams_for_parse(parsed: ParsedIngredient) -> float | None:
    """Convert a parsed ingredient to grams via the unit + ingredient registry.

    Returns None if either lookup fails or the unit is incompatible with the
    ingredient (e.g. asking for cup-of-egg when egg only carries WholeUnit).
    """
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
    # Regex fallback (deterministic, offline).
    rp = regex_parse_line(raw_line)
    if rp is not None:
        return rp.parsed
    return None


def _substring_fallback(
    form_key: str, canonical_map: Mapping[str, str]
) -> str | None:
    """Map a form_key to a variant canonical via whole-word substring match.

    Picks the longest matching canonical-form key when several match (so
    ``70% cacao chocolate chips`` lands on ``chocolate chips`` rather than
    a single-word ``chips`` if both were in the map). Whole-word boundary
    is checked via space-padded comparison to avoid ``salt`` matching
    inside ``shallot``.
    """
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
    dict[str, list[_RawObservation]],
    dict[str, list[str]],
]:
    """Bucket raw ingredient lines under the variant's canonical ingredients.

    Returns ``(observations_per_canonical, unmatched_per_recipe)``. The
    second dict captures lines that parsed but didn't map to any of the
    variant's canonicals (e.g. a recipe-specific cinnamon line in a
    variant whose canonical set excludes cinnamon).
    """
    observations: dict[str, list[_RawObservation]] = defaultdict(list)
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
            # Fallback: regex/cache may have resolved to a too-specific
            # canonical (semisweet chocolate chips, crisco shortening) that
            # isn't itself in the variant's set. Substring-match the form
            # against canonical_map keys to recover the bucket.
            if target is None:
                target = _substring_fallback(form_key, canonical_map)
            if target is None:
                unmatched[recipe_id].append(line)
                continue
            if not form_key:
                form_key = target
            observations[target].append(
                _RawObservation(raw_line=line, form_key=form_key, grams=grams)
            )

    return dict(observations), dict(unmatched)


def _load_recipenlg_raw_lines(
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


def format_breakdown(
    observations: Mapping[str, list[_RawObservation]],
    canonical_order: list[str],
    *,
    n_members: int,
) -> str:
    """Render the per-canonical breakdown as the bead's example shape."""
    lines: list[str] = []
    for canon in canonical_order:
        rows = observations.get(canon, [])
        n_sources = len({o.raw_line for o in rows})  # rough: distinct lines
        # Better: count by unique recipe contributions (one obs per recipe).
        # Each member can contribute at most one line per canonical here, but
        # that's not enforced — the source-count stays informational.
        lines.append(f"{canon} — {len(rows)} observations across {n_members} members")
        if not rows:
            lines.append("    (no source recipe contributed this canonical)")
            continue
        forms: dict[str, list[float | None]] = defaultdict(list)
        sample_raw: dict[str, str] = {}
        for obs in rows:
            forms[obs.form_key].append(obs.grams)
            sample_raw.setdefault(obs.form_key, obs.raw_line)
        # Sort by descending count, then by form_key.
        for form_key in sorted(
            forms, key=lambda k: (-len(forms[k]), k)
        ):
            counts = forms[form_key]
            with_grams = [g for g in counts if g is not None]
            mean_g = sum(with_grams) / len(with_grams) if with_grams else None
            grams_str = (
                f"mean {mean_g:.1f} g (n={len(with_grams)}/{len(counts)})"
                if mean_g is not None
                else "mass n/a"
            )
            example = sample_raw[form_key]
            label = form_key if form_key else "(unspecified)"
            lines.append(
                f"    {len(counts):>3} sources · '{label}' · {grams_str}"
            )
            lines.append(f"          e.g. {example!r}")
        del n_sources  # informational; not used in output yet
    return "\n".join(lines)


def inspect_variant(
    variant_id: str,
    *,
    db_path: Path,
    recipenlg_path: Path,
    model: str = "gemma4:e2b",
    seed: int = 42,
) -> str:
    """Top-level orchestration: open db, join corpus, render breakdown."""
    db = CatalogDB.open(db_path)
    try:
        variant = db.get_variant(variant_id)
        if variant is None:
            return f"Variant {variant_id!r} not found in {db_path}."

        stats = db.get_ingredient_stats(variant_id)
        canonical_order = [s.canonical_name for s in stats]
        members = db.get_variant_members(variant_id)

        recipenlg_members = [
            m for m in members if m.corpus == "recipenlg" and m.url
        ]
        skipped_corpora = [m for m in members if m.corpus != "recipenlg"]

        target_urls = {m.url for m in recipenlg_members if m.url is not None}
        url_to_lines = _load_recipenlg_raw_lines(recipenlg_path, target_urls)

        # Re-key by recipe_id (preserve linkage to variant_members rows).
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

        unmatched_lines = sum(len(v) for v in unmatched.values())
        n_rnlg = len(recipenlg_members)
        n_other = len(skipped_corpora)
        n_hit = len(member_raw_lines)
        n_target = len(target_urls)
        header = [
            f"Variant: {variant.normalized_title!r} ({variant_id})",
            f"  members={variant.n_recipes}, recipenlg_members={n_rnlg},"
            f" wdc/skipped={n_other}",
            f"  recipenlg URLs hit in corpus: {n_hit}/{n_target}",
            f"  unmatched lines (no variant canonical): {unmatched_lines}",
            "",
        ]
        body = format_breakdown(
            observations, canonical_order, n_members=len(member_raw_lines)
        )
        return "\n".join(header) + body
    finally:
        db.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("variant_id", help="Variant id (e.g. b34c2dce79e2)")
    parser.add_argument(
        "--db",
        type=Path,
        default=Path("output/catalog/recipes.db"),
        help="Path to recipes.db (default: output/catalog/recipes.db)",
    )
    parser.add_argument(
        "--recipenlg",
        type=Path,
        default=Path("dataset/full_dataset.csv"),
        help="Path to RecipeNLG full_dataset.csv",
    )
    parser.add_argument("--model", default="gemma4:e2b", help="Cached-parse model key")
    parser.add_argument("--seed", type=int, default=42, help="Cached-parse seed key")
    args = parser.parse_args(argv)

    if not args.db.exists():
        print(f"recipes.db not found at {args.db}", file=sys.stderr)
        return 1
    if not args.recipenlg.exists():
        print(f"RecipeNLG CSV not found at {args.recipenlg}", file=sys.stderr)
        return 1

    print(
        inspect_variant(
            args.variant_id,
            db_path=args.db,
            recipenlg_path=args.recipenlg,
            model=args.model,
            seed=args.seed,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
