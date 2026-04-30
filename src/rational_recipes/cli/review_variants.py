"""Variant-level terminal review shell backed by ``recipes.db`` (bead vwt.9).

Supersedes the manifest.json + JSON-sidecar shell (beads eco / 4lf).
Reads unreviewed variants from the SQLite catalog DB produced by the
whole-corpus extraction pipeline and persists decisions in-place via
UPDATE on the ``variants`` table (``review_status``, ``review_note``,
``reviewed_at``).

Data flow to the PWA: the DB is copied verbatim into
``web/public/recipes.db`` by ``web/scripts/sync-catalog.mjs``; the
PWA's default catalog filter hides any variant with
``review_status = 'drop'``. Dropping in review = invisible in the PWA.

Keystrokes:
    a  accept                 → review_status = 'accept'
    d  drop                   → review_status = 'drop' (hidden in PWA)
    n  annotate (free text)   → review_status = 'annotate', note saved
    ?  defer                  → no write; variant stays pending
    q  quit and save

Usage:
    python3 scripts/review_variants.py
    python3 scripts/review_variants.py --db output/catalog/recipes.db
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt
from rich.table import Table

from rational_recipes.catalog_db import (
    CatalogDB,
    ListFilters,
    VariantRow,
)

DEFAULT_DB_PATH = Path("output/catalog/recipes.db")


class ReviewAction(StrEnum):
    ACCEPT = "accept"
    DROP = "drop"
    ANNOTATE = "annotate"
    DEFER = "defer"
    QUIT = "quit"


_ACTION_KEYS: dict[str, ReviewAction] = {
    "a": ReviewAction.ACCEPT,
    "d": ReviewAction.DROP,
    "n": ReviewAction.ANNOTATE,
    "?": ReviewAction.DEFER,
    "q": ReviewAction.QUIT,
}

_ACTION_PROMPT = "Action: (a)ccept / (d)rop / (n)annotate / (?)defer / (q)uit"


@dataclass(frozen=True, slots=True)
class ReviewInput:
    """One reviewer input: the chosen action + optional note text.

    Tests inject pre-built ReviewInput iterators; interactive use builds
    them from keystrokes via ``_read_action`` + ``_read_note``.
    """

    action: ReviewAction
    note: str = ""


InputSource = Callable[[VariantRow], ReviewInput]


def _read_action(console: Console) -> ReviewAction:
    """Prompt until the reviewer picks a valid action keystroke."""
    while True:
        raw = Prompt.ask(_ACTION_PROMPT, console=console).strip().lower()
        if raw in _ACTION_KEYS:
            return _ACTION_KEYS[raw]
        console.print(
            f"[yellow]Unknown action {raw!r}; pick one of "
            f"{'/'.join(sorted(_ACTION_KEYS))}[/yellow]"
        )


def _read_note(console: Console) -> str:
    """Free-text note prompt for the annotate action."""
    return Prompt.ask("Note", default="", console=console).strip()


def _default_input_source(console: Console) -> InputSource:
    """Interactive keystroke + note source used by the CLI entry point."""

    def source(_variant: VariantRow) -> ReviewInput:
        action = _read_action(console)
        if action is ReviewAction.ANNOTATE:
            return ReviewInput(action=action, note=_read_note(console))
        return ReviewInput(action=action)

    return source


def _short_ingredient_list(variant: VariantRow, max_items: int = 5) -> str:
    items = list(variant.canonical_ingredient_set[:max_items])
    overflow = len(variant.canonical_ingredient_set) - max_items
    if overflow > 0:
        items.append(f"+{overflow}")
    return ", ".join(items)


def _list_view(console: Console, pending: list[VariantRow]) -> None:
    """Render the list of pending variants at session start."""
    table = Table(title="Pending variants", show_lines=False)
    table.add_column("Title", overflow="fold")
    table.add_column("Ingredients", overflow="fold")
    table.add_column("N", justify="right")
    table.add_column("Methods", overflow="fold")
    for v in pending:
        table.add_row(
            v.display_title or v.normalized_title,
            _short_ingredient_list(v),
            str(v.n_recipes),
            ", ".join(v.cooking_methods) or "—",
        )
    console.print(table)


def _drill_in(console: Console, db: CatalogDB, variant: VariantRow) -> None:
    """Render one variant's detail block: stats, sources, outlier scores."""
    console.rule(
        f"[bold]{variant.display_title or variant.normalized_title}[/bold]  "
        f"(variant_id={variant.variant_id})"
    )
    console.print(
        f"N recipes: [bold]{variant.n_recipes}[/bold]   "
        f"Methods: {', '.join(variant.cooking_methods) or '—'}   "
        f"Canonical ingredients: {len(variant.canonical_ingredient_set)}"
    )

    stats = db.get_ingredient_stats(variant.variant_id)
    if stats:
        stats_table = Table(
            title="Per-ingredient stats (proportion 0..1)", show_lines=False
        )
        stats_table.add_column("Ingredient")
        stats_table.add_column("Mean", justify="right")
        stats_table.add_column("Stddev", justify="right")
        stats_table.add_column("Ratio", justify="right")
        for s in stats:
            stats_table.add_row(
                s.canonical_name,
                f"{s.mean_proportion:.3f}",
                f"{s.stddev:.3f}" if s.stddev is not None else "—",
                f"{s.ratio:.2f}" if s.ratio is not None else "—",
            )
        console.print(stats_table)

    members = db.get_variant_members(variant.variant_id)
    scored = [m for m in members if m.outlier_score is not None]
    if scored:
        max_score = max(m.outlier_score for m in scored if m.outlier_score is not None)
        console.print(
            f"Outlier scores (max {max_score:.2f}): "
            + ", ".join(
                f"{m.outlier_score:.2f}"
                for m in scored
                if m.outlier_score is not None
            )
        )

    urls = [m.url for m in members if m.url]
    if urls:
        console.print(
            Panel(
                "\n".join(urls),
                title=f"Source URLs ({len(urls)})",
                expand=False,
            )
        )


def _apply_input(
    db: CatalogDB, variant: VariantRow, decision: ReviewInput
) -> bool:
    """Persist one decision. Returns False on DEFER (no write)."""
    if decision.action is ReviewAction.DEFER:
        return False
    note = decision.note or None
    if decision.action is ReviewAction.ACCEPT:
        db.update_review_status(variant.variant_id, "accept", note=note)
    elif decision.action is ReviewAction.DROP:
        db.update_review_status(variant.variant_id, "drop", note=note)
    elif decision.action is ReviewAction.ANNOTATE:
        db.update_review_status(variant.variant_id, "annotate", note=note)
    else:  # QUIT — caller checks action first, so this path is a bug.
        raise AssertionError(f"apply called with terminal action {decision.action}")
    return True


def review_loop(
    db: CatalogDB,
    *,
    console: Console | None = None,
    input_source: InputSource | None = None,
) -> int:
    """Interactive variant-level review loop. Returns the count of writes.

    ``input_source`` is a factory that returns a ``ReviewInput`` for
    each variant — the CLI default prompts via rich; tests inject a
    canned iterator. A ReviewInput with action ``QUIT`` ends the loop
    without persisting anything for that variant.
    """
    console = console or Console()
    source = input_source or _default_input_source(console)

    pending = db.list_variants(ListFilters(pending_only=True))
    console.print(
        f"[bold]Pending variants:[/bold] {len(pending)}  "
        f"(DB default: unreviewed only)"
    )
    if not pending:
        console.print("[green]No variants pending review.[/green]")
        return 0

    _list_view(console, pending)
    console.print(f"\n[bold]Reviewing {len(pending)} variant(s)...[/bold]\n")

    writes = 0
    for i, variant in enumerate(pending, start=1):
        console.rule(f"[{i}/{len(pending)}]")
        _drill_in(console, db, variant)
        decision = source(variant)
        if decision.action is ReviewAction.QUIT:
            console.print("[yellow]Quitting; partial session persisted.[/yellow]")
            break
        if _apply_input(db, variant, decision):
            writes += 1
            detail = f' with note "{decision.note}"' if decision.note else ""
            console.print(
                f"[green]Recorded {decision.action.value}[/green]{detail}"
            )
        else:
            console.print("[dim]Deferred (no DB write)[/dim]")

    console.print(f"\n[bold]Session complete: {writes} decision(s) persisted.[/bold]")
    return writes


def iter_input_source(inputs: list[ReviewInput]) -> InputSource:
    """Build an ``InputSource`` that replays a fixed list (testing aid)."""
    it: Iterator[ReviewInput] = iter(inputs)

    def source(_variant: VariantRow) -> ReviewInput:
        try:
            return next(it)
        except StopIteration:
            return ReviewInput(action=ReviewAction.QUIT)

    return source


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB_PATH,
        help=f"Path to recipes.db (default: {DEFAULT_DB_PATH})",
    )
    args = parser.parse_args(argv)

    console = Console()
    if not args.db.exists():
        console.print(f"[red]Catalog DB not found: {args.db}[/red]")
        return 1

    db = CatalogDB.open(args.db)
    try:
        review_loop(db, console=console)
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
