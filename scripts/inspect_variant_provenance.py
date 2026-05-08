#!/usr/bin/env python3
"""Spike: surface raw forms behind each variant canonical (RationalRecipes-4rgy).

Reads ``recipes.db`` for a variant's members, joins back to the source corpus
(RecipeNLG ``full_dataset.csv``), and groups raw ingredient lines under the
variant's canonical ingredients. Read-only; no LLM calls.

The domain logic lives in ``rational_recipes.provenance`` and is shared
with the maintainer editor (``scripts/editor.py``). This script is the
text-rendering CLI shell on top of that shared API.

Usage:
    python3 scripts/inspect_variant_provenance.py b34c2dce79e2
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Iterable
from pathlib import Path

from rational_recipes.catalog_db import CatalogDB
from rational_recipes.provenance import (
    CanonicalProvenance,
    VariantProvenance,
    aggregate_raw_observations,
    build_variant_canonical_map,
    extract_form_key,
    load_recipenlg_raw_lines,
    load_variant_provenance,
)

# Re-export internals the test module reaches in through this script's namespace
# so backward-compatible imports continue to work.
__all__ = [
    "aggregate_raw_observations",
    "build_variant_canonical_map",
    "extract_form_key",
    "format_breakdown",
    "inspect_variant",
    "load_recipenlg_raw_lines",
    "main",
]


def format_breakdown(
    provenance: Iterable[CanonicalProvenance], *, n_members: int
) -> str:
    """Render the per-canonical breakdown as the bead's example shape."""
    lines: list[str] = []
    for canon_prov in provenance:
        lines.append(
            f"{canon_prov.canonical} — "
            f"{canon_prov.total_observations} observations across "
            f"{n_members} members"
        )
        if not canon_prov.forms:
            lines.append("    (no source recipe contributed this canonical)")
            continue
        for form in canon_prov.forms:
            grams_str = (
                f"mean {form.mean_grams:.1f} g "
                f"(n={form.n_with_grams}/{form.count})"
                if form.mean_grams is not None
                else "mass n/a"
            )
            lines.append(
                f"    {form.count:>3} sources · '{form.form_key}' · {grams_str}"
            )
            lines.append(f"          e.g. {form.example_raw_line!r}")
    return "\n".join(lines)


def _format_header(prov: VariantProvenance, title: str) -> list[str]:
    return [
        f"Variant: {title!r} ({prov.variant_id})",
        f"  recipenlg_members={prov.n_recipenlg_members},"
        f" wdc/skipped={prov.n_other_corpora}",
        f"  recipenlg URLs hit in corpus: "
        f"{prov.n_recipenlg_hit}/{prov.n_recipenlg_members}",
        f"  unmatched lines (no variant canonical): {prov.unmatched_count}",
        "",
    ]


def inspect_variant(
    variant_id: str,
    *,
    db_path: Path,
    recipenlg_path: Path,
    model: str = "gemma4:e2b",
    seed: int = 42,
) -> str:
    """Top-level orchestration: open db, load provenance, render breakdown."""
    db = CatalogDB.open(db_path)
    try:
        variant = db.get_variant(variant_id)
        if variant is None:
            return f"Variant {variant_id!r} not found in {db_path}."
        prov = load_variant_provenance(
            db, variant_id, recipenlg_path, model=model, seed=seed
        )
        if prov is None:
            return f"Variant {variant_id!r} not found in {db_path}."
        header = _format_header(prov, variant.normalized_title)
        body = format_breakdown(
            prov.canonicals, n_members=prov.n_corpus_members
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
