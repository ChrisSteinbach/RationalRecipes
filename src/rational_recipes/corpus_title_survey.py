"""Cross-corpus normalized-title frequency survey (vwt.1 diagnostic)."""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from rational_recipes.scrape.grouping import normalize_title
from rational_recipes.scrape.recipenlg import RecipeNLGLoader
from rational_recipes.scrape.wdc import WDCLoader

SURVEY_VERSION = 1

LANGUAGE_FILTER_ALL = "all"
LANGUAGE_FILTER_EN_SV = "en+sv"
LANGUAGE_FILTERS = (LANGUAGE_FILTER_ALL, LANGUAGE_FILTER_EN_SV)

# Swedish-only diacritics; everything else non-ASCII is rejected by the en+sv
# heuristic. Lowercase and uppercase forms enumerated explicitly to avoid
# locale-dependent .lower() surprises.
_SWEDISH_DIACRITICS = frozenset("åäöÅÄÖ")


def accept_all(_title: str) -> bool:
    """Predicate: accept every title."""
    return True


def accept_en_sv(title: str) -> bool:
    """Accept titles whose non-ASCII chars are limited to Swedish diacritics.

    Rejects Cyrillic, CJK, Arabic, etc. — anything that would surface as a
    dish family the maintainer can't read or review. ASCII-only titles
    (English) and ASCII + å/ä/ö titles (Swedish) survive.
    """
    for ch in title:
        if ord(ch) < 128:
            continue
        if ch in _SWEDISH_DIACRITICS:
            continue
        return False
    return True


LANGUAGE_FILTER_PREDICATES: dict[str, Callable[[str], bool]] = {
    LANGUAGE_FILTER_ALL: accept_all,
    LANGUAGE_FILTER_EN_SV: accept_en_sv,
}


@dataclass(frozen=True, slots=True)
class TitleTally:
    """One row of the merged corpus survey."""

    title: str
    recipenlg: int
    wdc: int

    @property
    def combined(self) -> int:
        return self.recipenlg + self.wdc


def tally_titles(
    titles: Iterable[str],
    *,
    accept: Callable[[str], bool] = accept_all,
) -> tuple[Counter[str], int]:
    """Count normalized titles passing *accept*; return (counter, rows_seen).

    rows_seen counts every input row regardless of filter outcome so callers
    can record the corpus size in the survey output.
    """
    counter: Counter[str] = Counter()
    rows = 0
    for title in titles:
        rows += 1
        normalized = normalize_title(title)
        if not normalized:
            continue
        if not accept(normalized):
            continue
        counter[normalized] += 1
    return counter, rows


def merge_tallies(
    recipenlg: Counter[str],
    wdc: Counter[str],
    *,
    min_combined: int | None = None,
) -> list[TitleTally]:
    """Merge per-corpus counts into one ranked list (combined desc, title asc)."""
    keys = set(recipenlg) | set(wdc)
    rows = [
        TitleTally(title=k, recipenlg=recipenlg.get(k, 0), wdc=wdc.get(k, 0))
        for k in keys
    ]
    if min_combined is not None:
        rows = [r for r in rows if r.combined >= min_combined]
    rows.sort(key=lambda r: (-r.combined, r.title))
    return rows


def _input_mtime(*paths: Path) -> datetime:
    """Latest mtime across *paths*, as a UTC datetime.

    Used as a deterministic default for the survey's generated_at so reruns on
    unchanged inputs produce byte-identical JSON.
    """
    latest = max(p.stat().st_mtime for p in paths)
    return datetime.fromtimestamp(latest, tz=UTC)


def build_survey(
    *,
    recipenlg_path: Path,
    wdc_zip_path: Path,
    language_filter: str = LANGUAGE_FILTER_ALL,
    min_combined: int | None = None,
    hosts: Sequence[str] | None = None,
    generated_at: datetime | None = None,
    recipenlg_loader: RecipeNLGLoader | None = None,
    wdc_loader: WDCLoader | None = None,
) -> dict[str, Any]:
    """Stream both corpora, merge tallies, return the survey JSON dict.

    Loaders are injectable for testing; defaults open the real corpora.
    """
    if language_filter not in LANGUAGE_FILTER_PREDICATES:
        raise ValueError(
            f"Unknown language_filter {language_filter!r}; "
            f"expected one of {sorted(LANGUAGE_FILTER_PREDICATES)}"
        )
    accept = LANGUAGE_FILTER_PREDICATES[language_filter]
    if generated_at is None:
        generated_at = _input_mtime(recipenlg_path, wdc_zip_path)

    rnlg = recipenlg_loader or RecipeNLGLoader(path=recipenlg_path)
    wdc = wdc_loader or WDCLoader(zip_path=wdc_zip_path)

    rnlg_counts, rnlg_rows = tally_titles(
        (r.title for r in rnlg.iter_recipes()),
        accept=accept,
    )
    wdc_counts, wdc_rows = tally_titles(
        (r.title for r in wdc.iter_all(hosts=hosts)),
        accept=accept,
    )

    titles = merge_tallies(rnlg_counts, wdc_counts, min_combined=min_combined)

    return {
        "version": SURVEY_VERSION,
        "generated_at": generated_at.isoformat(),
        "min_combined_count": min_combined,
        "language_filter": language_filter,
        "corpus_revisions": {
            "recipenlg_path": str(recipenlg_path),
            "recipenlg_rows": rnlg_rows,
            "wdc_zip": str(wdc_zip_path),
            "wdc_rows": wdc_rows,
        },
        "titles": [
            {
                "title": t.title,
                "recipenlg": t.recipenlg,
                "wdc": t.wdc,
                "combined": t.combined,
            }
            for t in titles
        ],
    }
