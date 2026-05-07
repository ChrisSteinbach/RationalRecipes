#!/usr/bin/env python3
"""Render one variant from recipes.db as a publication-ready drop.

Hand-cycle prototype for RationalRecipes-ehe7. ia1x extends it: when
``variants.canonical_instructions`` is non-NULL the rendered drop
includes a section labeled "Canonical instructions (generative
consensus)" sourced from the LLM-synthesized + human-reviewed
instruction set; otherwise the original median-source placeholder
path runs unchanged.

Usage:
    python3 scripts/render_drop.py <variant_id> [--db PATH] [--out PATH]
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

from rational_recipes.catalog_db import CatalogDB
from rational_recipes.render.instruction_picker import (
    fetch_directions_text,
    pick_median_source,
)


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


# Natural-language quantity rendering (RationalRecipes-4ba4 / F4).
#
# When ``variant_ingredient_stats`` carries density or whole-unit data,
# render the per-batch mass as something readable ("1¼ cups flour", "3
# medium eggs") next to the gram count. Volume conversions use US legal
# cup (236.588 ml), Tbsp (14.7868 ml), tsp (4.92892 ml). Whole-unit
# rendering wins when both are populated (an egg has both a density of
# ~1.03 g/ml and a "1 medium = 44 g" entry; "3 eggs" beats "0.6 cups").
# Fractions round to nearest ¼ for readability — finer rounding is
# spurious precision against the CIs already printed in the table.

_CUP_ML = 236.588
_TBSP_ML = 14.7868
_TSP_ML = 4.92892
_FRACTION_GLYPHS = {0.25: "¼", 0.5: "½", 0.75: "¾"}


def _format_quarter_fraction(value: float) -> str:
    """Format ``value`` to nearest ¼ with unicode fractions.

    Sub-quarter values (under 0.125) fall back to two decimals so a
    "1 tbsp" rendering for a near-zero value doesn't round up to "¼".
    """
    if value < 0.125:
        return f"{value:.2f}"
    rounded = round(value * 4) / 4
    whole = int(rounded)
    frac = rounded - whole
    glyph = _FRACTION_GLYPHS.get(round(frac, 2), "")
    if whole == 0:
        return glyph or f"{rounded:.2f}"
    if glyph:
        return f"{whole}{glyph}"
    return str(whole)


def _format_natural_quantity(
    grams: float,
    density: float | None,
    whole_unit_name: str | None,
    whole_unit_grams: float | None,
) -> str:
    """Render ``grams`` as a human-friendly quantity (4ba4).

    Strategy:
    - Whole-unit form ("3 medium", "1 large") wins when populated —
      eggs/lemons/onions read better as a count than as a volume.
    - Density form: cups for ≥ ½ cup, tbsp for ≥ ½ tbsp, else tsp.
    - Empty fallback: "" (caller renders "— g" or just the gram count
      in the dedicated column).
    """
    if whole_unit_grams and whole_unit_grams > 0:
        n = round(grams / whole_unit_grams)
        if n < 1:
            n = 1
        unit = whole_unit_name or "unit"
        return f"{n} {unit}"
    if density and density > 0:
        ml = grams / density
        if ml >= _CUP_ML * 0.5:
            return f"{_format_quarter_fraction(ml / _CUP_ML)} cup"
        if ml >= _TBSP_ML * 0.5:
            return f"{_format_quarter_fraction(ml / _TBSP_ML)} tbsp"
        return f"{_format_quarter_fraction(ml / _TSP_ML)} tsp"
    return ""


def _format_url(url: str | None) -> str:
    """Prepend ``https://`` only when the URL doesn't already carry a scheme.

    Source URLs in ``recipes.db`` come from two corpora with mixed
    conventions: RecipeNLG often stores ``www.cookbooks.com/...``
    (no scheme) while WDC and some food.com rows arrive as
    ``https://...``. Blindly prepending produced ``https://https://...``
    in the early hand-cycle output.
    """
    if not url:
        return "(no URL)"
    if url.startswith(("http://", "https://")):
        return url
    return f"https://{url}"


def render(
    db_path: Path, variant_id: str, batch_grams: float = 1000.0
) -> str:
    # Open via CatalogDB first so any pending schema migrations
    # (e.g. ia1x's canonical_instructions columns) run before the
    # raw-sqlite SELECTs below see the table. Closes immediately —
    # the connection is then re-opened with row_factory tuned for
    # column-name access.
    CatalogDB.open(db_path).close()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    variant = conn.execute(
        """SELECT variant_id, display_title, normalized_title, n_recipes,
                  canonical_ingredient_set, description, base_ingredient,
                  cooking_methods, canonical_instructions,
                  canonical_instructions_reviewed_at
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
        """SELECT vm.recipe_id, r.title, r.url, r.corpus, r.language,
                  vm.outlier_score
           FROM variant_members vm
           JOIN recipes r ON r.recipe_id = vm.recipe_id
           WHERE vm.variant_id = ?
           ORDER BY vm.outlier_score""",
        (variant_id,),
    ).fetchall()

    # variant_members rows are deliberately preserved when an editor
    # filters a recipe (sj18: filter is reversible by clearing the
    # override). The rendered drop must NOT expose filtered-out recipes
    # — neither in the source list nor as the median-source for
    # instructions. Read filter overrides and exclude their recipe_ids
    # client-side. Substitute overrides need no handling here: the
    # ingredient table is already rendered from the recomputed
    # variant_ingredient_stats, which honored substitutions at write
    # time.
    excluded_recipe_ids = {
        row["recipe_id"]
        for row in conn.execute(
            """SELECT json_extract(payload, '$.recipe_id') AS recipe_id
               FROM variant_overrides
               WHERE variant_id = ? AND override_type = 'filter'""",
            (variant_id,),
        ).fetchall()
        if row["recipe_id"] is not None
    }
    active_sources = [
        s for s in sources if s["recipe_id"] not in excluded_recipe_ids
    ]

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
    # The "Quantity" column is the F4 addition (RationalRecipes-4ba4):
    # natural-language form ("1¼ cups", "3 medium") rendered from
    # density / whole-unit metadata in variant_ingredient_stats. When the
    # row is missing those columns the cell stays blank and the gram
    # column carries the load — that's the pre-F4 baseline.
    lines.append(
        "| Ingredient | Mass % | ± stddev | 95% CI | n | per 1 kg | Quantity |"
    )
    lines.append("|---|---:|---:|---:|---:|---:|---:|")
    for s in stats:
        grams_for_batch = _scale_to_grams(s["mean_proportion"], batch_grams)
        natural = _format_natural_quantity(
            grams_for_batch,
            s["density_g_per_ml"],
            s["whole_unit_name"],
            s["whole_unit_grams"],
        )
        natural_cell = natural if natural else "—"
        lines.append(
            f"| {s['canonical_name']} "
            f"| {_format_pct(s['mean_proportion'])} "
            f"| {_format_pct(s['stddev'])} "
            f"| {_format_ci(s['ci_lower'], s['ci_upper'])} "
            f"| {s['min_sample_size']} "
            f"| {grams_for_batch:.0f} g "
            f"| {natural_cell} |"
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

    # Instructions. Two paths:
    # - canonical_instructions populated → render the synthesized
    #   consensus (per r8hx, full LLM synthesis with human review;
    #   ia1x landed the persistence column). Label distinctly so a
    #   reader can tell empirical-mass-fractions apart from generative
    #   text.
    # - canonical_instructions NULL → fall back to the hand-cycle
    #   "median source" path (r8hx option 1, the ehe7 hand-cycle
    #   default).
    canonical_instructions = variant["canonical_instructions"]
    reviewed_at = variant["canonical_instructions_reviewed_at"]
    if canonical_instructions:
        lines.append("## Canonical instructions (generative consensus)")
        lines.append("")
        lines.append(
            "> Synthesized from the cluster's source instruction "
            "sequences via LLM and reviewed by the maintainer "
            "(RationalRecipes-r8hx, full LLM synthesis with human "
            "review). This is **generative consensus**, not "
            "empirically averaged — the mass fractions above are the "
            "measurement; these instructions are an editorial "
            "synthesis."
        )
        lines.append("")
        lines.append(canonical_instructions.rstrip())
        lines.append("")
        if reviewed_at:
            lines.append(f"*Reviewed at: {reviewed_at}*")
            lines.append("")
    else:
        lines.append("## Instructions")
        lines.append("")
        if active_sources:
            # ie1a: pick the most-complete instructions among the top-N
            # most-central sources, not the literal median. F10 in the
            # ehe7 friction journal observed that the lowest-outlier
            # recipe is sometimes terse and a near-central runner-up
            # has substantially better text. The picker falls back to
            # active_sources[0] when no candidate has directions_text
            # (pre-F5 behavior), preserving the literal-median outline.
            median_source = pick_median_source(active_sources, conn)
            lines.append(
                f"*Per RationalRecipes-r8hx option 1 (refined by "
                f"RationalRecipes-ie1a): the most-completely-instructed "
                f"source among the top-5 most-central recipes "
                f"(outlier score = "
                f"{median_source['outlier_score']:.2f}) is*"
            )
            lines.append("")
            lines.append(
                f"> [{median_source['title']}]"
                f"({_format_url(median_source['url'])})"
            )
            lines.append("")
            # F5 / 15g4: when ``recipes.directions_text`` is populated for
            # the picked source, inline the instructions directly so the
            # drop is a finished artifact rather than a placeholder. Falls
            # back to the manual-fetch guidance when the column is missing
            # or NULL (pre-F5 baseline, WDC sources, etc).
            picked_directions = fetch_directions_text(
                conn, median_source["recipe_id"]
            )
            if picked_directions:
                lines.append(picked_directions.rstrip())
                lines.append("")
            else:
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

    # Source attribution — list active source URLs (filter overrides
    # exclude recipes from the count and the list, but the underlying
    # variant_members rows are intentionally preserved).
    if active_sources:
        lines.append("## Source recipes")
        lines.append("")
        lines.append(
            f"Averaged across {len(active_sources)} sources from RecipeNLG "
            f"and WDC corpora:"
        )
        lines.append("")
        for src in active_sources:
            corpus_tag = src["corpus"]
            url = _format_url(src["url"])
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
