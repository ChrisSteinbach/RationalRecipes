#!/usr/bin/env python3
"""Variant-level terminal review shell for merged-pipeline output (bead eco).

Consumes a manifest.json + per-variant CSVs (toj's contract) and walks
the operator through each undecided variant. Persists decisions to a
JSON sidecar keyed by variant_id; re-runs skip already-decided
variants.

Minimum scope per docs/design/recipe-scraping.md § "Human review as a
first-class stage": variant-level only, no per-row interaction, no
live recomputation, no fingerprint-distance sort. Per-row toggle and
the L3 split action (bead 4lf) live in follow-on beads.

Usage:
    python3 scripts/review_variants.py output/merged/manifest.json \\
        --decisions output/merged/decisions.json
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt
from rich.table import Table

from rational_recipes.review import (
    ReviewAction,
    ReviewDecisions,
    format_variant_status,
    pending_variants,
    progress_summary,
    short_ingredient_list,
    summarize_variant,
)
from rational_recipes.scrape.manifest import Manifest, VariantManifestEntry

ACTION_PROMPT = "Action: (a)ccept / (d)rop / (?)defer / (q)uit and save"
ACTION_KEYS = {
    "a": ReviewAction.ACCEPT,
    "d": ReviewAction.DROP,
    "?": ReviewAction.DEFER,
}


def _list_view(
    console: Console, manifest: Manifest, decisions: ReviewDecisions
) -> None:
    """Render the all-variants list with status."""
    table = Table(title="Variants", show_lines=False)
    table.add_column("Title", overflow="fold")
    table.add_column("Ingredients", overflow="fold")
    table.add_column("N", justify="right")
    table.add_column("Methods", overflow="fold")
    table.add_column("Status")

    for entry in manifest.variants:
        table.add_row(
            entry.title,
            short_ingredient_list(entry),
            str(entry.n_recipes),
            ", ".join(entry.cooking_methods) or "—",
            format_variant_status(entry, decisions),
        )
    console.print(table)


def _drill_in(
    console: Console,
    entry: VariantManifestEntry,
    manifest_dir: Path,
) -> None:
    """Render one variant's detail block: stats, sources, outlier scores."""
    csv_path = manifest_dir / entry.csv_path
    summaries = summarize_variant(entry, csv_path)

    console.rule(f"[bold]{entry.title}[/bold]  (variant_id={entry.variant_id})")
    console.print(
        f"N recipes: [bold]{entry.n_recipes}[/bold]   "
        f"Methods: {', '.join(entry.cooking_methods) or '—'}   "
        f"Canonical ingredients: {len(entry.canonical_ingredients)}"
    )

    stats_table = Table(title="Per-ingredient stats (raw CSV cells)", show_lines=False)
    stats_table.add_column("Ingredient")
    stats_table.add_column("Mean", justify="right")
    stats_table.add_column("Stddev", justify="right")
    for s in summaries:
        stats_table.add_row(s.name, f"{s.mean:.2f}", f"{s.stddev:.2f}")
    console.print(stats_table)

    if entry.row_outlier_scores:
        scores = list(entry.row_outlier_scores)
        max_score = max(scores) if scores else 0.0
        console.print(
            f"Outlier scores (max {max_score:.2f}): "
            + ", ".join(f"{s:.2f}" for s in scores)
        )

    if entry.source_urls:
        console.print(
            Panel(
                "\n".join(entry.source_urls),
                title=f"Source URLs ({len(entry.source_urls)})",
                expand=False,
            )
        )


def _prompt_action(console: Console) -> ReviewAction | None:
    """Single-keystroke-style action prompt. Returns None on quit."""
    while True:
        raw = Prompt.ask(ACTION_PROMPT, console=console).strip().lower()
        if raw == "q":
            return None
        if raw in ACTION_KEYS:
            return ACTION_KEYS[raw]
        console.print(f"[yellow]Unknown action {raw!r}; pick one of a/d/?/q[/yellow]")


def _prompt_note(console: Console) -> str:
    """Optional free-text note attached to any action."""
    raw = Prompt.ask("Optional note (empty to skip)", default="", console=console)
    return raw.strip()


def review_loop(
    manifest_path: Path,
    decisions_path: Path,
    *,
    console: Console | None = None,
) -> int:
    """Interactive loop. Returns 0 on clean quit, 1 on input error."""
    console = console or Console()
    if not manifest_path.exists():
        console.print(f"[red]Manifest not found: {manifest_path}[/red]")
        return 1

    manifest = Manifest.read(manifest_path)
    decisions = ReviewDecisions.read(decisions_path)
    manifest_dir = manifest_path.parent

    pending = pending_variants(manifest, decisions)
    decided, total, breakdown = progress_summary(manifest, decisions)
    console.print(
        f"[bold]Manifest:[/bold] {manifest_path}   "
        f"[bold]Decisions:[/bold] {decisions_path}\n"
        f"Progress: {decided}/{total} decided ({breakdown})"
    )

    if not pending:
        console.print("[green]All variants already decided.[/green]")
        _list_view(console, manifest, decisions)
        return 0

    _list_view(console, manifest, decisions)
    console.print(f"\n[bold]Reviewing {len(pending)} pending variant(s)...[/bold]\n")

    for i, entry in enumerate(pending, start=1):
        console.rule(f"[{i}/{len(pending)}]")
        _drill_in(console, entry, manifest_dir)
        action = _prompt_action(console)
        if action is None:
            console.print("[yellow]Saving and quitting.[/yellow]")
            break
        note = _prompt_note(console)
        decisions.record(entry.variant_id, action, note)
        # Persist after each decision so a crash doesn't lose work.
        decisions.write(decisions_path)
        console.print(
            f"[green]Recorded {action.value}[/green]"
            + (f' with note: "{note}"' if note else "")
        )

    decided, total, breakdown = progress_summary(manifest, decisions)
    console.print(f"\nFinal progress: {decided}/{total} decided ({breakdown})")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "manifest",
        type=Path,
        help="Path to manifest.json emitted by the merged pipeline",
    )
    parser.add_argument(
        "--decisions",
        type=Path,
        help="Path to decisions JSON sidecar (default: <manifest dir>/decisions.json)",
    )
    args = parser.parse_args()
    decisions_path = args.decisions or (args.manifest.parent / "decisions.json")
    return review_loop(args.manifest, decisions_path)


if __name__ == "__main__":
    sys.exit(main())
