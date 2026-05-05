#!/usr/bin/env python3
"""Render one variant from recipes.db as a publication-ready drop.

Hand-cycle prototype for RationalRecipes-ehe7. Demonstrates the shape
sj18 (review_variants.py extension) would land. Not production code —
expect rough edges, hardcoded formatting, no tests. The friction this
exposes is the data we want.

Usage:
    python3 scripts/render_drop.py <variant_id> [--db PATH] [--out PATH]
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path


def _format_pct(value: float | None, *, places: int = 1) -> str:
    if value is None:
        return "—"
    return f"{value * 100:.{places}f}%"


def _format_ci(low: float | None, high: float | None) -> str:
    if low is None or high is None:
        return "—"
    return f"{low * 100:.1f}–{high * 100:.1f}%"


def _scale_to_grams(mean_proportion: float, batch_grams: float) -> float:
    return mean_proportion * batch_grams


def render(
    db_path: Path, variant_id: str, batch_grams: float = 1000.0
) -> str:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    variant = conn.execute(
        """SELECT variant_id, display_title, normalized_title, n_recipes,
                  canonical_ingredient_set, description, base_ingredient,
                  cooking_methods
           FROM variants WHERE variant_id = ?""",
        (variant_id,),
    ).fetchone()
    if variant is None:
        raise SystemExit(f"variant_id {variant_id} not found in {db_path}")

    stats = conn.execute(
        """SELECT canonical_name, ordinal, mean_proportion, stddev,
                  ci_lower, ci_upper, ratio, min_sample_size,
                  density_g_per_ml, whole_unit_name, whole_unit_grams
           FROM variant_ingredient_stats
           WHERE variant_id = ?
           ORDER BY ordinal""",
        (variant_id,),
    ).fetchall()

    sources = conn.execute(
        """SELECT r.title, r.url, r.corpus, r.language, vm.outlier_score
           FROM variant_members vm
           JOIN recipes r ON r.recipe_id = vm.recipe_id
           WHERE vm.variant_id = ?
           ORDER BY vm.outlier_score""",
        (variant_id,),
    ).fetchall()

    title = variant["display_title"] or variant["normalized_title"]
    n = variant["n_recipes"]

    lines: list[str] = []
    lines.append(f"# {title}")
    lines.append("")
    lines.append(
        f"> Averaged from **{n} independent source recipes**. "
        f"Mass percentages are mean ± stddev across the cluster; "
        f"sample size n is the smallest subset for which an ingredient "
        f"appears."
    )
    lines.append("")
    if variant["description"]:
        lines.append(variant["description"])
        lines.append("")

    lines.append("## Ingredients (mass fractions)")
    lines.append("")
    lines.append("| Ingredient | Mass % | ± stddev | 95% CI | n | per 1 kg |")
    lines.append("|---|---:|---:|---:|---:|---:|")
    for s in stats:
        grams_for_batch = _scale_to_grams(s["mean_proportion"], batch_grams)
        lines.append(
            f"| {s['canonical_name']} "
            f"| {_format_pct(s['mean_proportion'])} "
            f"| {_format_pct(s['stddev'])} "
            f"| {_format_ci(s['ci_lower'], s['ci_upper'])} "
            f"| {s['min_sample_size']} "
            f"| {grams_for_batch:.0f} g |"
        )
    lines.append("")

    # Notes / caveats
    lines.append("## Notes")
    lines.append("")
    low_n = [s for s in stats if s["min_sample_size"] < n / 3]
    if low_n:
        lines.append(
            "**Low-confidence ingredients** (appear in fewer than a third "
            "of source recipes — read as 'sometimes used' rather than "
            "canonical):"
        )
        for s in low_n:
            lines.append(
                f"- *{s['canonical_name']}* — n={s['min_sample_size']}/{n}"
            )
        lines.append("")

    high_var = [
        s for s in stats
        if s["stddev"] is not None
        and s["mean_proportion"] is not None
        and s["mean_proportion"] > 0
        and s["stddev"] / s["mean_proportion"] > 0.5
    ]
    if high_var:
        lines.append(
            "**High-variance ingredients** (CV > 50% — bakers disagree on "
            "the right amount):"
        )
        for s in high_var:
            cv = s["stddev"] / s["mean_proportion"]
            lines.append(
                f"- *{s['canonical_name']}* — CV={cv:.0%} "
                f"({_format_pct(s['mean_proportion'])} ± "
                f"{_format_pct(s['stddev'])})"
            )
        lines.append("")

    # Instructions placeholder — per r8hx, hand-cycle uses option 1
    # (median source recipe's instructions, picked by lowest outlier
    # score). The actual instructions are not in recipes.db's variants
    # table; you'd need to fetch from the source URL. For the
    # hand-cycle, paste them in manually after picking the median.
    lines.append("## Instructions")
    lines.append("")
    if sources:
        median_source = sources[0]
        lines.append(
            f"*Per RationalRecipes-r8hx option 1: the source recipe "
            f"closest to the central tendency (lowest outlier score = "
            f"{median_source['outlier_score']:.2f}) is*"
        )
        lines.append("")
        lines.append(
            f"> [{median_source['title']}]"
            f"(https://{median_source['url']})"
        )
        lines.append("")
        lines.append(
            "*Fetch the instructions from that source and paste them "
            "here. For the hand-cycle drop, this is a manual step.*"
        )
        lines.append("")
    else:
        lines.append(
            "*No source recipes found in variant_members — extraction "
            "may have run without source linking. Investigate.*"
        )
        lines.append("")

    # Source attribution — list all source URLs.
    if sources:
        lines.append("## Source recipes")
        lines.append("")
        lines.append(
            f"Averaged across {len(sources)} sources from RecipeNLG and "
            f"WDC corpora:"
        )
        lines.append("")
        for src in sources:
            corpus_tag = src["corpus"]
            url = f"https://{src['url']}" if src["url"] else "(no URL)"
            outlier = src["outlier_score"]
            lines.append(
                f"- [{src['title']}]({url}) "
                f"— {corpus_tag}, outlier score {outlier:.1f}"
            )
        lines.append("")

    # Methodology footer.
    lines.append("---")
    lines.append("")
    lines.append(
        "*Methodology: ingredient quantities averaged across N "
        "independent source recipes, mass-normalized to per-100 g of "
        "batch. Confidence intervals are 95% (1.96·σ/√n). Outliers "
        "scored against the cluster's central tendency. See "
        "[RationalRecipes](https://github.com/ChrisSteinbach/RationalRecipes) "
        "for the methodology source.*"
    )

    conn.close()
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("variant_id")
    parser.add_argument(
        "--db",
        type=Path,
        default=Path("output/catalog/recipes.db"),
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Write to this path; default is stdout.",
    )
    parser.add_argument(
        "--batch-grams",
        type=float,
        default=1000.0,
        help="Reference batch size for the 'per N g' column.",
    )
    args = parser.parse_args(argv)

    md = render(args.db, args.variant_id, batch_grams=args.batch_grams)
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(md)
        print(f"Wrote {args.out}", file=sys.stderr)
    else:
        sys.stdout.write(md)
        sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
