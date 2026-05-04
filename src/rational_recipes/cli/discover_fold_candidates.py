"""Discover candidate generic/specific ingredient fold pairs from a recipes.db.

Scans ``variant_ingredient_stats`` for pairs of canonical names that
co-occur in the same variant and where one's whitespace-tokenized
form is a strict subset of the other's (e.g. ``salt`` ⊂ ``kosher salt``,
``butter`` ⊂ ``unsalted butter``). Ranks pairs by the count of variants
they co-occur in — the high-frequency pairs are the real-world
ambiguities the catalog actually exposes — and writes the ranked list
to JSON for human review (per RationalRecipes-2p6's discovery step).

The ranking is conservative: pairs without a strict-substring token
relationship are not surfaced (avoids over-suggesting unrelated names).
This is a diagnostic only; nothing in the pipeline reads the output.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import defaultdict
from collections.abc import Sequence
from pathlib import Path
from typing import Any


def _tokens(name: str) -> frozenset[str]:
    return frozenset(t for t in name.lower().split() if t)


def discover_pairs(
    conn: sqlite3.Connection,
    *,
    min_cooccurrence: int = 2,
) -> list[dict[str, Any]]:
    """Find substring-related ingredient pairs that co-occur in variants.

    Returns a list of dicts ranked by descending co-occurrence count. The
    ``generic`` field is the shorter (more general) form, ``specific``
    the longer; ``cooccurrence`` is the number of distinct variants both
    appear in. ``example_variants`` is up to 5 (variant_id, title) tuples.
    """
    rows = conn.execute(
        """
        SELECT v.variant_id, v.normalized_title, s.canonical_name
        FROM variants v
        JOIN variant_ingredient_stats s ON s.variant_id = v.variant_id
        ORDER BY v.variant_id
        """
    ).fetchall()

    by_variant: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for variant_id, title, canonical_name in rows:
        by_variant[variant_id].append((title, canonical_name))

    pair_counts: dict[tuple[str, str], int] = defaultdict(int)
    pair_examples: dict[tuple[str, str], list[tuple[str, str]]] = defaultdict(list)

    for variant_id, items in by_variant.items():
        names = [n for _, n in items]
        title = items[0][0]
        seen_pairs: set[tuple[str, str]] = set()
        for i, a in enumerate(names):
            tokens_a = _tokens(a)
            if not tokens_a:
                continue
            for b in names[i + 1 :]:
                if a == b:
                    continue
                tokens_b = _tokens(b)
                if not tokens_b:
                    continue
                # Strict subset → one is the generic form.
                if tokens_a < tokens_b:
                    generic, specific = a, b
                elif tokens_b < tokens_a:
                    generic, specific = b, a
                else:
                    continue
                key = (generic, specific)
                if key in seen_pairs:
                    continue
                seen_pairs.add(key)
                pair_counts[key] += 1
                if len(pair_examples[key]) < 5:
                    pair_examples[key].append((variant_id, title))

    out: list[dict[str, Any]] = []
    for (generic, specific), count in pair_counts.items():
        if count < min_cooccurrence:
            continue
        out.append(
            {
                "generic": generic,
                "specific": specific,
                "cooccurrence": count,
                "example_variants": [
                    {"variant_id": vid, "title": t}
                    for vid, t in pair_examples[(generic, specific)]
                ],
            }
        )
    out.sort(key=lambda d: (-d["cooccurrence"], d["generic"], d["specific"]))
    return out


def group_by_generic(pairs: Sequence[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Group pairs by their generic-form root.

    Returns ``{generic: {"forms": {form: count, ...}, "total": N}}`` so
    reviewers can see a whole family at a glance.
    """
    families: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"forms": {}, "total": 0}
    )
    for p in pairs:
        fam = families[p["generic"]]
        forms = fam["forms"]
        forms[p["specific"]] = p["cooccurrence"]
        fam["total"] += p["cooccurrence"]
    return dict(
        sorted(families.items(), key=lambda kv: (-kv[1]["total"], kv[0]))
    )


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        type=Path,
        default=Path("output/catalog/recipes.db"),
        help="Path to recipes.db (default: %(default)s).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("output/catalog/fold_candidates.json"),
        help="Path to write ranked candidate list (default: %(default)s).",
    )
    parser.add_argument(
        "--min-cooccurrence",
        type=int,
        default=2,
        help=(
            "Drop pairs that co-occur in fewer than this many variants "
            "(default: %(default)s). 1 surfaces single-variant ambiguities; "
            "higher cuts the long tail."
        ),
    )
    return parser.parse_args(argv)


def run(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    if not args.db.exists():
        print(f"recipes.db not found: {args.db}", file=sys.stderr)
        return 1

    conn = sqlite3.connect(str(args.db))
    try:
        pairs = discover_pairs(conn, min_cooccurrence=args.min_cooccurrence)
    finally:
        conn.close()

    families = group_by_generic(pairs)
    payload = {
        "db": str(args.db),
        "min_cooccurrence": args.min_cooccurrence,
        "n_pairs": len(pairs),
        "n_families": len(families),
        "pairs": pairs,
        "families": families,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(
        f"discovered {len(pairs)} pair(s) across {len(families)} family root(s) "
        f"→ {args.output}"
    )
    for generic, fam in list(families.items())[:15]:
        forms = ", ".join(
            f"{name}({count})"
            for name, count in sorted(
                fam["forms"].items(), key=lambda kv: -kv[1]
            )
        )
        print(f"  {generic:24s} total={fam['total']:4d}  {forms}")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
