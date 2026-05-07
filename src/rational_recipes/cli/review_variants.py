"""Variant-level terminal review shell backed by ``recipes.db`` (bead vwt.9).

Supersedes the manifest.json + JSON-sidecar shell (beads eco / 4lf).
Reads unreviewed variants from the SQLite catalog DB produced by the
whole-corpus extraction pipeline and persists decisions in-place via
UPDATE on the ``variants`` table (``review_status``, ``review_note``,
``reviewed_at``).

Reviewed status flows through the rest of the system: ``render_drop.py``
renders only the chosen variant regardless of status (the user picks the
variant_id explicitly), and the Streamlit maintainer editor
(``scripts/editor.py``) lists ``drop`` variants alongside others so the
maintainer can revisit a previous reject decision.

Interactive review keystrokes:
    a  accept                 → review_status = 'accept'
    d  drop                   → review_status = 'drop' (hidden by default)
    n  annotate (free text)   → review_status = 'annotate', note saved
    ?  defer                  → no write; variant stays pending
    q  quit and save

Subcommands (sj18 + h6q1 — editorial overrides on a specific variant):
    review                                       Interactive review loop (default).
    substitute VID FROM TO                       Fold canonical FROM into TO.
    filter VID RECIPE_ID [--reason]              Exclude one source recipe.
    canonical-reassign VID RECIPE_ID RAW NEW     Reassign one source's raw line.
    overrides VID                                List active overrides on a variant.
    clear-override OVERRIDE_ID                   Remove one override (reverses it).

Usage:
    python3 scripts/review_variants.py
    python3 scripts/review_variants.py --db output/catalog/recipes.db
    python3 scripts/review_variants.py substitute b34c2dce79e2 shortening butter
    python3 scripts/review_variants.py filter b34c2dce79e2 abc123 --reason "bad units"
    python3 scripts/review_variants.py canonical-reassign b34c2dce79e2 abc123 \\
        "70% cacao chocolate chips" 70-percent-cacao-chocolate-chips
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


def _run_substitute(
    db: CatalogDB,
    console: Console,
    variant_id: str,
    from_name: str,
    to_name: str,
) -> int:
    """Apply a substitution override and print the resulting stats."""
    try:
        override_id = db.add_substitute_override(variant_id, from_name, to_name)
    except ValueError as exc:
        console.print(f"[red]{exc}[/red]")
        return 2
    console.print(
        f"[green]Recorded substitute (override_id={override_id}):[/green]"
        f" {from_name} → {to_name}"
    )
    _print_post_recompute_summary(db, console, variant_id)
    return 0


def _run_filter(
    db: CatalogDB,
    console: Console,
    variant_id: str,
    recipe_id: str,
    reason: str,
) -> int:
    """Apply a filter override and print the resulting stats."""
    try:
        override_id = db.add_filter_override(variant_id, recipe_id, reason=reason)
    except ValueError as exc:
        console.print(f"[red]{exc}[/red]")
        return 2
    console.print(
        f"[green]Recorded filter (override_id={override_id}):[/green]"
        f" excluded recipe {recipe_id}"
        + (f" — {reason}" if reason else "")
    )
    _print_post_recompute_summary(db, console, variant_id)
    return 0


def _run_canonical_reassign(
    db: CatalogDB,
    console: Console,
    variant_id: str,
    recipe_id: str,
    raw_text: str,
    new_canonical: str,
) -> int:
    """Apply a per-source canonical_reassign override and print the result."""
    try:
        override_id = db.add_canonical_reassign_override(
            variant_id, recipe_id, raw_text, new_canonical
        )
    except ValueError as exc:
        console.print(f"[red]{exc}[/red]")
        return 2
    console.print(
        f"[green]Recorded canonical_reassign "
        f"(override_id={override_id}):[/green]"
        f" recipe={recipe_id}  {raw_text!r} → {new_canonical}"
    )
    _print_post_recompute_summary(db, console, variant_id)
    return 0


def _run_overrides(db: CatalogDB, console: Console, variant_id: str) -> int:
    """List active overrides for one variant."""
    overrides = db.list_overrides(variant_id)
    if not overrides:
        console.print(f"[dim]No overrides recorded for {variant_id}.[/dim]")
        return 0
    table = Table(title=f"Overrides for {variant_id}", show_lines=False)
    table.add_column("ID", justify="right")
    table.add_column("Type")
    table.add_column("Payload", overflow="fold")
    table.add_column("Created at")
    for ov in overrides:
        table.add_row(
            str(ov.override_id),
            ov.override_type,
            ", ".join(f"{k}={v}" for k, v in ov.payload.items()),
            ov.created_at,
        )
    console.print(table)
    return 0


def _run_clear_override(
    db: CatalogDB, console: Console, override_id: int
) -> int:
    """Delete one override row and recompute the affected variant."""
    if db.clear_override(override_id):
        console.print(
            f"[green]Cleared override {override_id}; stats recomputed.[/green]"
        )
        return 0
    console.print(f"[yellow]No override with id {override_id}.[/yellow]")
    return 1


def _print_post_recompute_summary(
    db: CatalogDB, console: Console, variant_id: str
) -> None:
    """Render a small post-recompute table so the editor sees the effect."""
    variant = db.get_variant(variant_id)
    if variant is None:
        return
    stats = db.get_ingredient_stats(variant_id)
    table = Table(
        title=f"Post-override stats — n={variant.n_recipes} sources",
        show_lines=False,
    )
    table.add_column("Ingredient")
    table.add_column("Mean", justify="right")
    table.add_column("Stddev", justify="right")
    table.add_column("n", justify="right")
    for s in stats:
        table.add_row(
            s.canonical_name,
            f"{s.mean_proportion:.3f}",
            f"{s.stddev:.3f}" if s.stddev is not None else "—",
            str(s.min_sample_size),
        )
    console.print(table)


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB_PATH,
        help=f"Path to recipes.db (default: {DEFAULT_DB_PATH})",
    )
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("review", help="Interactive review loop (default).")

    p_sub = sub.add_parser(
        "substitute",
        help="Fold canonical ingredient FROM_NAME into TO_NAME for a variant.",
    )
    p_sub.add_argument("variant_id")
    p_sub.add_argument("from_name")
    p_sub.add_argument("to_name")

    p_filter = sub.add_parser(
        "filter", help="Exclude one source recipe from a variant's average."
    )
    p_filter.add_argument("variant_id")
    p_filter.add_argument("recipe_id")
    p_filter.add_argument("--reason", default="")

    p_reassign = sub.add_parser(
        "canonical-reassign",
        help=(
            "Reassign a single source's raw ingredient line to a different "
            "canonical (per-source override; h6q1)."
        ),
    )
    p_reassign.add_argument("variant_id")
    p_reassign.add_argument("recipe_id")
    p_reassign.add_argument("raw_text")
    p_reassign.add_argument("new_canonical")

    p_overrides = sub.add_parser(
        "overrides", help="List active overrides on a variant."
    )
    p_overrides.add_argument("variant_id")

    p_clear = sub.add_parser(
        "clear-override",
        help="Remove one override row (reverses substitute or filter).",
    )
    p_clear.add_argument("override_id", type=int)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    console = Console()
    if not args.db.exists():
        console.print(f"[red]Catalog DB not found: {args.db}[/red]")
        return 1

    db = CatalogDB.open(args.db)
    try:
        command = args.command or "review"
        if command == "review":
            review_loop(db, console=console)
            return 0
        if command == "substitute":
            return _run_substitute(
                db, console, args.variant_id, args.from_name, args.to_name
            )
        if command == "filter":
            return _run_filter(
                db, console, args.variant_id, args.recipe_id, args.reason
            )
        if command == "canonical-reassign":
            return _run_canonical_reassign(
                db,
                console,
                args.variant_id,
                args.recipe_id,
                args.raw_text,
                args.new_canonical,
            )
        if command == "overrides":
            return _run_overrides(db, console, args.variant_id)
        if command == "clear-override":
            return _run_clear_override(db, console, args.override_id)
        console.print(f"[red]Unknown command: {command}[/red]")
        return 2
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
