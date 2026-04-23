#!/usr/bin/env python3
"""Pretty-print gold JSONL files for eyeball validation.

Three modes:

- ``--flagged``: only show the entries the labeler flagged as judgment
  calls (Swedish divergences from v1, multi-word ingredients,
  overrides). Start here — these are where mistakes are most likely.
- ``--all``: all entries, aligned tabular form. Use to skim the bulk.
- ``--category=<name>``: filter to one bucket (plural, comma_prep,
  packaging, mixed_adversarial for English; volume/weight/spoon/
  count/package for Swedish).

To accept the gold as-is, do nothing — it's committed.
To fix an entry, open the gold JSONL directly and edit the
``expected`` dict. Then re-run the sanity check:

    python3 -m pytest tests/test_benchmark_scoring.py

The jpp sweep consumes these files via load_english_corpus /
load_swedish_corpus / load_multilingual_corpus in
scripts/benchmark_models.py.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ENGLISH = Path("scripts/benchmark_data/english_messy_gold.jsonl")
SWEDISH = Path("scripts/benchmark_data/swedish_ica_se_structured_gold.jsonl")
MULTILINGUAL = Path("scripts/benchmark_data/multilingual_gold.jsonl")

# Entries the Swedish labeler flagged as diverging from v1 or applying a
# judgment call. See the commit message of fa81ebd.
SWEDISH_FLAGGED_ROWS = {13, 136, 280, 528, 607, 761, 1489, 1700}
SWEDISH_FLAGGED_LINES = {
    "salt och peppar",
    "finrivet skal och juice av 1/2 tvättad citron",
    "spritspåse",
}

# English entries where I had to pick between v1-style strip and keep-compound.
ENGLISH_FLAGGED_LINES = {
    "2 containers Cool Whip, thawed",
    "1 (3 oz.) pkg. raspberry flavor Jell-O",
    "4 small salmon steaks",
    "8 skinless chicken breasts",
    "6 boneless chicken breasts, cut into thin strips",
    "2 whole boneless, skinless chicken breasts, sliced thin",
    "2 bay leaves, torn very small",
    "2 cans diced Ro-Tel",
    "1/3 c. green onion tops, chopped",
    "2 sleeves graham crackers",
    "16 double graham crackers, rolled (1 1/4 c.)",
    "2 Tbsp. Parmesan cheese, freshly grated",
    "3/4 c. maraschino cherries, halved",
    "1 (7 oz.) jar Marshmallow Creme",
    "1 (6 oz.) can pork and beans",
}

# Multilingual lines worth a second look (unit choices, prep ambiguity)
MULTILINGUAL_FLAGGED_LINES = {
    "3 Forellenfilet(s) , geräucherte",
    "2 m.-große Ei(er)",
    "2 Scheibe/n Bergkäse oder eine andere kräftige Sorte",
    "Ваниль по вкусу",
    "Сыр (пармезан) ¼ стакана",
    "Огурец (крупный) 1 шт.",
    "小麦粉 適量",
    "糸唐辛子 少々",
    "カット野菜[炒め物用] 40g",
}


def _load(path: Path) -> list[dict]:
    items = []
    with path.open() as f:
        for raw in f:
            raw = raw.strip()
            if not raw:
                continue
            items.append(json.loads(raw))
    return items


def _format_row(item: dict, corpus: str) -> str:
    e = item["expected"]
    line = item["line"]
    label = (
        f"qty={e['quantity']:g} "
        f"unit={e.get('unit', ''):<10} "
        f"ing={e.get('ingredient', ''):<25} "
        f"prep={e.get('preparation', '')}"
    )
    # Terminal width-friendly
    tag = ""
    if corpus == "swedish":
        tag = f"r{item.get('row_id', '?'):<5}"
    elif corpus == "multilingual":
        tag = f"{item.get('language', '?'):<3}"
    elif corpus == "english":
        tag = f"{item.get('category', '?'):<17}"
    return f"{tag} {line:<70} → {label}"


def _is_flagged(item: dict, corpus: str) -> bool:
    line = item["line"].strip()
    if corpus == "swedish":
        return (
            item.get("row_id") in SWEDISH_FLAGGED_ROWS or line in SWEDISH_FLAGGED_LINES
        )
    if corpus == "english":
        return line in ENGLISH_FLAGGED_LINES
    if corpus == "multilingual":
        return line in MULTILINGUAL_FLAGGED_LINES
    return False


def _review(
    path: Path,
    corpus: str,
    *,
    flagged_only: bool,
    category: str | None,
    limit: int | None,
) -> None:
    items = _load(path)
    if category is not None:
        key = "category" if corpus == "english" else "language"
        items = [i for i in items if i.get(key) == category]
    if flagged_only:
        items = [i for i in items if _is_flagged(i, corpus)]
    if limit is not None:
        items = items[:limit]

    if not items:
        print(f"(no entries in {path.name} matching the filter)")
        return

    print(f"## {path.name} — showing {len(items)} entries\n")
    for item in items:
        print(_format_row(item, corpus))


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("corpus", choices=("english", "swedish", "multilingual", "all"))
    p.add_argument(
        "--flagged",
        action="store_true",
        help="Only show flagged judgment-call entries",
    )
    p.add_argument(
        "--category",
        default=None,
        help="Filter: category (English) or language (multilingual)",
    )
    p.add_argument("--limit", type=int, default=None)
    args = p.parse_args()

    paths = {
        "english": (ENGLISH, "english"),
        "swedish": (SWEDISH, "swedish"),
        "multilingual": (MULTILINGUAL, "multilingual"),
    }

    if args.corpus == "all":
        for _name, (path, label) in paths.items():
            if not path.exists():
                print(f"(missing: {path})", file=sys.stderr)
                continue
            _review(
                path,
                label,
                flagged_only=args.flagged,
                category=args.category,
                limit=args.limit,
            )
            print()
        return

    path, label = paths[args.corpus]
    if not path.exists():
        sys.exit(f"missing: {path}")
    _review(
        path,
        label,
        flagged_only=args.flagged,
        category=args.category,
        limit=args.limit,
    )


if __name__ == "__main__":
    main()
