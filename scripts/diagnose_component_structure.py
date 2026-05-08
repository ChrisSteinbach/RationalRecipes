#!/usr/bin/env python3
"""Diagnose how often the corpora carry detectable multi-component structure
in their directions / recipeinstructions text (RationalRecipes-0x1z, Small).

Read-only, no LLM calls. Pure regex + heuristics. The answer feeds the
decision on whether to invest in Medium/Large component-extraction work.

Usage:
    python3 scripts/diagnose_component_structure.py
    python3 scripts/diagnose_component_structure.py --sample-size 100000 --json out.json
    python3 scripts/diagnose_component_structure.py --full
"""

from __future__ import annotations

import argparse
import ast
import csv
import gzip
import io
import json
import random
import re
import sys
import zipfile
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from pathlib import Path

# RecipeNLG cells (full directions blobs) routinely exceed Python's default
# CSV field cap; raise it before reading.
csv.field_size_limit(10**7)

# WDC zip entry pattern — same shape used by scrape/wdc.py.
_WDC_ENTRY_RE = re.compile(r"^Recipe_(.+)_October2023\.json\.gz$")

# --- Heuristic vocabulary --------------------------------------------------
#
# The component-noun whitelist was seeded from the bead's prior probe of the
# pigs-in-a-blanket Food Network row plus the canonical baking lexicon
# (crust, filling, frosting, topping, sauce, glaze, ganache, mousse, icing,
# streusel, spice mix, everything spice, relish, mustard) and extended with
# adjacent baking + savoury vocabulary that commonly headers a recipe section.
# Imperative-prefix list filters bare "X:" headers like "Stir:" / "Bake:" that
# look like component markers but are step instructions.

COMPONENT_NOUNS: frozenset[str] = frozenset(
    {
        # baking
        "crust", "filling", "frosting", "topping", "sauce", "glaze",
        "ganache", "mousse", "icing", "streusel", "crumble", "dough",
        "batter", "pastry", "base", "shell", "tart shell", "pie shell",
        "pie crust", "cookie crust", "graham crust", "graham cracker crust",
        "biscuit base",
        # cakes / cookies / breads
        "cake", "cake layer", "cake layers", "layer", "layers",
        "cupcake", "cupcakes", "cookie", "cookies", "biscuit", "biscuits",
        "scone", "scones", "muffin", "muffins", "bread", "rolls", "buns",
        "pancakes", "waffles", "buttercream", "whipped cream",
        # mixtures / spice / accompaniments
        "spice mix", "everything spice", "rub", "spice rub",
        "relish", "mustard", "marinade", "dressing", "dip", "syrup",
        "custard", "cream", "meringue", "garnish", "decoration",
        "decorations", "coating", "drizzle", "spread", "compote", "puree",
        "reduction", "soup", "stew", "salad",
        # mains / starches
        "rice", "noodles", "pasta", "pizza", "chicken", "pork", "beef",
        "fish", "salmon", "shrimp", "vegetable", "vegetables",
        # multi-word compounds seen in the wild
        "tomato sauce", "lemon glaze", "chocolate sauce",
        "cream cheese filling", "pigs-in-a-blanket", "pigs in a blanket",
        "cobbler", "tart", "pie",
    }
)

IMPERATIVE_PREFIXES: frozenset[str] = frozenset(
    {
        # cooking actions
        "stir", "bake", "mix", "preheat", "combine", "beat", "whisk", "add",
        "place", "pour", "serve", "prepare", "cook", "heat", "reduce",
        "simmer", "boil", "fry", "sear", "broil", "roast", "grill", "melt",
        "sprinkle", "brush", "store", "refrigerate", "chill", "freeze",
        "thaw", "slice", "dice", "chop", "mince", "peel", "rinse", "drain",
        "wash", "soak", "steep", "marinate", "season", "knead", "roll",
        "shape", "form", "spoon", "scoop", "remove", "transfer", "set",
        "let", "leave", "stand", "rest", "cool", "warm", "reheat",
        "discard", "cover", "uncover", "fold", "flip", "turn",
        # boilerplate section labels
        "ingredients", "directions", "instructions", "steps", "method",
        "step", "preparation", "note", "notes", "tip", "tips",
        "yield", "yields", "makes", "serves",
    }
)

# --- Regexes ---------------------------------------------------------------
#
# Both patterns allow a leading numbered/bulleted prefix ("1.", "2)", "*"),
# which covers the bead's #3 case (numbered subsections of "1. For the X:").

_FOR_THE_RE = re.compile(
    r"""
    ^\s*
    (?:[\d.]+[.)]\s*|[*\-]\s+)?     # optional bullet / numeric prefix
    for\s+(?:the\s+)?               # "For" or "For the"
    ([^:\n]{1,60}?)                 # X — up to 60 chars, non-greedy
    \s*:                            # terminating colon
    """,
    re.IGNORECASE | re.VERBOSE,
)

_HEADER_RE = re.compile(
    r"""
    ^\s*
    (?:[\d.]+[.)]\s*|[*\-]\s+)?
    ([A-Za-z][A-Za-z\s'\-]{1,60}?)
    \s*:\s*$                        # lone-line header — colon then EOL
    """,
    re.VERBOSE,
)


# --- Data classes ---------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ComponentMarker:
    """One detected component header within a recipe's directions."""

    line_index: int
    line_text: str
    matched: str
    component_name: str
    pattern: str  # "for_the" | "header" | "how_to_section"


@dataclass(frozen=True, slots=True)
class RecipeDetection:
    """Detection result for one recipe."""

    title: str
    source_id: str
    directions: str
    markers: tuple[ComponentMarker, ...]
    has_how_to_section: bool

    @property
    def is_component_structured(self) -> bool:
        """≥2 distinct component names, or a HowToSection container."""
        if self.has_how_to_section:
            return True
        names = {m.component_name for m in self.markers}
        return len(names) >= 2

    @property
    def has_any_marker(self) -> bool:
        """Looser threshold: ≥1 detected marker. Matches the bead's prior probe."""
        return bool(self.markers) or self.has_how_to_section


@dataclass
class CorpusReport:
    """Aggregate stats + sample examples for one corpus."""

    name: str
    n_sampled: int = 0
    n_any_marker: int = 0
    n_component_structured: int = 0
    n_how_to_section: int = 0
    examples: list[RecipeDetection] = field(default_factory=list)

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "n_sampled": self.n_sampled,
            "n_any_marker": self.n_any_marker,
            "n_component_structured": self.n_component_structured,
            "n_how_to_section": self.n_how_to_section,
            "fraction_any_marker": self._safe_frac(self.n_any_marker),
            "fraction_component_structured": self._safe_frac(
                self.n_component_structured
            ),
            "fraction_how_to_section": self._safe_frac(self.n_how_to_section),
            "examples": [
                {
                    "title": ex.title,
                    "source_id": ex.source_id,
                    "markers": [
                        {
                            "matched": m.matched,
                            "component_name": m.component_name,
                            "pattern": m.pattern,
                            "line_index": m.line_index,
                        }
                        for m in ex.markers
                    ],
                    "has_how_to_section": ex.has_how_to_section,
                }
                for ex in self.examples
            ],
        }

    def _safe_frac(self, n: int) -> float:
        return n / self.n_sampled if self.n_sampled else 0.0


# --- Heuristic core --------------------------------------------------------


def _strip_punct(s: str) -> str:
    return re.sub(r"[^\w\s'\-]", " ", s).strip()


def _is_imperative(name: str) -> bool:
    """Bare 'X:' headers whose first word is an imperative verb / boilerplate label."""
    words = _strip_punct(name).split()
    if not words:
        return True
    return words[0].lower() in IMPERATIVE_PREFIXES


def _has_component_noun(name: str) -> bool:
    n = _strip_punct(name).lower()
    if not n:
        return False
    if any(w in COMPONENT_NOUNS for w in n.split()):
        return True
    return any(phrase in n for phrase in COMPONENT_NOUNS if " " in phrase)


def find_markers_in_text(text: str) -> list[ComponentMarker]:
    """Detect component-section headers within a recipe's directions text.

    Looks for two shapes line by line:
      1. "For the X:" / "For X:" inline at start-of-line. High precision —
         the "for" prefix already filters most imperatives.
      2. Lone-line "X:" or "FOR THE X:" headers. Filtered to noun-phrases
         (component-noun whitelist) and rejected for imperatives like
         "Stir:" / "Bake:".

    Numbered-subsection variants ("1. For the X:") are picked up by the
    optional bullet prefix in the regexes.
    """
    if not text:
        return []
    out: list[ComponentMarker] = []
    for i, raw_line in enumerate(text.splitlines()):
        line = raw_line.strip()
        if not line:
            continue

        m_for = _FOR_THE_RE.match(line)
        if m_for:
            x = m_for.group(1).strip()
            if x and len(x) <= 60 and not _is_imperative(x):
                out.append(
                    ComponentMarker(
                        line_index=i,
                        line_text=raw_line,
                        matched=line[: m_for.end()],
                        component_name=x.lower(),
                        pattern="for_the",
                    )
                )
                continue

        m_hdr = _HEADER_RE.match(line)
        if m_hdr:
            x = m_hdr.group(1).strip()
            if not x or _is_imperative(x):
                continue
            is_caps = x == x.upper() and any(c.isalpha() for c in x)
            has_noun = _has_component_noun(x)
            # ALL-CAPS alone isn't enough (e.g. an emphasized step heading);
            # require it also smell like a component noun.
            if (is_caps and has_noun) or has_noun:
                out.append(
                    ComponentMarker(
                        line_index=i,
                        line_text=raw_line,
                        matched=line[: m_hdr.end()],
                        component_name=x.lower(),
                        pattern="header",
                    )
                )
    return out


# --- Corpus normalization -------------------------------------------------


def normalize_recipenlg_directions(raw: str) -> str:
    """Parse the stringified Python list and join steps with newlines."""
    if not raw:
        return ""
    try:
        parsed = ast.literal_eval(raw)
    except (ValueError, SyntaxError):
        return ""
    if not isinstance(parsed, list):
        return ""
    return "\n".join(str(s) for s in parsed)


def normalize_wdc_instructions(raw: object) -> tuple[str, bool]:
    """Convert WDC's recipeinstructions to step-per-line text + how_to_section flag.

    Shapes encountered (from the bead's prior 5000-row probe):
      list[{"text": "..."}], list[str], dict, string,
      list[{"@type": "HowToSection", "name": "...", "itemListElement": [...]}].

    HowToSection containers are flattened to synthesized "For the {name}:"
    headers followed by their inner steps, so the marker detector can use a
    single uniform path.
    """
    if raw is None:
        return ("", False)
    if isinstance(raw, str):
        return (raw, False)
    if isinstance(raw, dict):
        if raw.get("@type") == "HowToSection":
            text, _ = _flatten_how_to_section(raw)
            return (text, True)
        return (str(raw.get("text") or raw.get("name") or ""), False)
    if not isinstance(raw, list):
        return ("", False)

    has_section = False
    lines: list[str] = []
    for item in raw:
        if isinstance(item, dict):
            if item.get("@type") == "HowToSection":
                has_section = True
                section_text, _ = _flatten_how_to_section(item)
                lines.append(section_text)
                continue
            txt = item.get("text") or item.get("name") or ""
            lines.append(str(txt))
        elif isinstance(item, str):
            lines.append(item)
    return ("\n".join(line for line in lines if line), has_section)


def _flatten_how_to_section(section: dict[str, object]) -> tuple[str, bool]:
    name = str(section.get("name", "Section")).strip() or "Section"
    inner = section.get("itemListElement", [])
    out_lines: list[str] = [f"For the {name}:"]
    if isinstance(inner, list):
        for sub in inner:
            if isinstance(sub, dict):
                txt = sub.get("text") or sub.get("name") or ""
                out_lines.append(str(txt))
            elif isinstance(sub, str):
                out_lines.append(sub)
    return ("\n".join(out_lines), True)


# --- Streaming readers ----------------------------------------------------


def stream_recipenlg(path: Path) -> Iterator[tuple[int, str, str, str]]:
    """Yield (row_index, title, link, directions_raw) for every CSV row."""
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            yield (
                i,
                row.get("title", "").strip(),
                row.get("link", "").strip(),
                row.get("directions", ""),
            )


def stream_wdc(
    zip_path: Path, hosts: Iterable[str]
) -> Iterator[tuple[str, int, str, str, object]]:
    """Yield (host, row_index, title, page_url, recipeinstructions_raw)."""
    with zipfile.ZipFile(zip_path) as zf:
        for host in hosts:
            entry = f"Recipe_{host}_October2023.json.gz"
            try:
                info = zf.getinfo(entry)
            except KeyError:
                continue
            with zf.open(info) as raw:
                with gzip.open(raw, "rt", encoding="utf-8") as gz:
                    for i, line in enumerate(gz):
                        try:
                            row = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        yield (
                            host,
                            i,
                            str(row.get("name", "")),
                            str(row.get("page_url", "")),
                            row.get("recipeinstructions"),
                        )


def list_wdc_hosts(zip_path: Path) -> list[str]:
    with zipfile.ZipFile(zip_path) as zf:
        return sorted(
            m.group(1)
            for m in (_WDC_ENTRY_RE.match(n) for n in zf.namelist())
            if m
        )


# --- Sampling -------------------------------------------------------------


def reservoir_sample(
    stream: Iterable[tuple[object, ...]],
    sample_size: int,
    seed: int,
) -> list[tuple[object, ...]]:
    """Reservoir-sample sample_size items from stream with a fixed seed."""
    rng = random.Random(seed)
    reservoir: list[tuple[object, ...]] = []
    for i, item in enumerate(stream):
        if i < sample_size:
            reservoir.append(item)
        else:
            j = rng.randint(0, i)
            if j < sample_size:
                reservoir[j] = item
    return reservoir


# --- Diagnose drivers -----------------------------------------------------


def _detect_recipe(
    title: str, source_id: str, directions: str, has_how_to: bool
) -> RecipeDetection:
    markers = tuple(find_markers_in_text(directions))
    return RecipeDetection(
        title=title,
        source_id=source_id,
        directions=directions,
        markers=markers,
        has_how_to_section=has_how_to,
    )


def _update_report(
    report: CorpusReport,
    detection: RecipeDetection,
    examples_target: int,
    rng: random.Random,
) -> None:
    report.n_sampled += 1
    if detection.has_any_marker:
        report.n_any_marker += 1
    if detection.is_component_structured:
        report.n_component_structured += 1
    if detection.has_how_to_section:
        report.n_how_to_section += 1
    # Reservoir-sample DETECTED examples for display.
    if detection.is_component_structured:
        if len(report.examples) < examples_target:
            report.examples.append(detection)
        else:
            j = rng.randint(0, report.n_component_structured - 1)
            if j < examples_target:
                report.examples[j] = detection


def diagnose_recipenlg(
    path: Path,
    *,
    sample_size: int | None,
    seed: int,
    examples_target: int = 20,
) -> CorpusReport:
    """Stream RecipeNLG, reservoir-sample to sample_size, classify each row."""
    report = CorpusReport(name="recipenlg")
    rng = random.Random(seed + 1)

    if sample_size is None:
        for _, title, link, directions_raw in stream_recipenlg(path):
            text = normalize_recipenlg_directions(directions_raw)
            detection = _detect_recipe(title, link, text, has_how_to=False)
            _update_report(report, detection, examples_target, rng)
        return report

    sampled = reservoir_sample(
        stream_recipenlg(path), sample_size=sample_size, seed=seed
    )
    for item in sampled:
        _, title, link, directions_raw = item  # type: ignore[misc]
        text = normalize_recipenlg_directions(str(directions_raw))
        detection = _detect_recipe(
            str(title), str(link), text, has_how_to=False
        )
        _update_report(report, detection, examples_target, rng)
    return report


def diagnose_wdc(
    zip_path: Path,
    *,
    sample_size: int | None,
    n_hosts: int | None,
    seed: int,
    examples_target: int = 20,
) -> CorpusReport:
    """Iterate selected WDC hosts, reservoir-sample to sample_size, classify."""
    report = CorpusReport(name="wdc")
    rng = random.Random(seed + 2)

    all_hosts = list_wdc_hosts(zip_path)
    host_rng = random.Random(seed)
    host_rng.shuffle(all_hosts)
    selected_hosts = all_hosts if n_hosts is None else all_hosts[:n_hosts]

    if sample_size is None:
        for host, _, title, page_url, instr in stream_wdc(
            zip_path, selected_hosts
        ):
            text, has_section = normalize_wdc_instructions(instr)
            detection = _detect_recipe(
                title, page_url or f"{host}#{_}", text, has_section
            )
            _update_report(report, detection, examples_target, rng)
        return report

    sampled = reservoir_sample(
        stream_wdc(zip_path, selected_hosts),
        sample_size=sample_size,
        seed=seed,
    )
    for item in sampled:
        host, idx, title, page_url, instr = item  # type: ignore[misc]
        text, has_section = normalize_wdc_instructions(instr)
        source_id = (
            str(page_url) if page_url else f"{host}#{idx}"
        )
        detection = _detect_recipe(
            str(title), source_id, text, has_section
        )
        _update_report(report, detection, examples_target, rng)
    return report


# --- Output ---------------------------------------------------------------


def format_report(report: CorpusReport, examples_to_show: int) -> str:
    out: list[str] = []
    out.append(f"=== {report.name} ===")
    out.append(f"  sampled: {report.n_sampled}")
    out.append(
        f"  ≥1 marker: {report.n_any_marker} "
        f"({_pct(report.n_any_marker, report.n_sampled)})"
    )
    out.append(
        f"  component-structured (≥2 distinct OR HowToSection): "
        f"{report.n_component_structured} "
        f"({_pct(report.n_component_structured, report.n_sampled)})"
    )
    out.append(
        f"  HowToSection containers: {report.n_how_to_section} "
        f"({_pct(report.n_how_to_section, report.n_sampled)})"
    )
    if not report.examples:
        out.append("  (no component-structured examples found in this sample)")
        return "\n".join(out)

    out.append("")
    out.append(
        f"  --- up to {examples_to_show} component-structured examples ---"
    )
    for n, ex in enumerate(report.examples[:examples_to_show], 1):
        out.append("")
        out.append(f"  [{n}] {ex.title or '(untitled)'}  · {ex.source_id}")
        if ex.has_how_to_section:
            out.append("      (HowToSection container)")
        component_summary = ", ".join(
            sorted({m.component_name for m in ex.markers})
        )
        out.append(f"      components: {component_summary}")
        for marker in ex.markers:
            highlighted = _highlight_marker(marker)
            out.append(f"      L{marker.line_index:>3}  {highlighted}")
    return "\n".join(out)


def _pct(n: int, d: int) -> str:
    return f"{(n / d * 100):.2f}%" if d else "0.00%"


def _highlight_marker(marker: ComponentMarker) -> str:
    """Render a matched line with the matched header braced for visibility."""
    line = marker.line_text
    matched = marker.matched
    if matched and matched in line:
        idx = line.index(matched)
        return (
            f"{line[:idx]}<<{matched}>>{line[idx + len(matched):]}"
        ).rstrip()
    return line.rstrip()


def recommend_pilot_corpus(
    rnlg: CorpusReport, wdc: CorpusReport
) -> tuple[str, str]:
    """Pick the corpus with the higher component-structured rate."""
    rate_rnlg = rnlg.n_component_structured / rnlg.n_sampled if rnlg.n_sampled else 0.0
    rate_wdc = wdc.n_component_structured / wdc.n_sampled if wdc.n_sampled else 0.0
    if rate_wdc > rate_rnlg:
        winner, reason = (
            "wdc",
            f"{rate_wdc:.2%} vs RecipeNLG {rate_rnlg:.2%} — "
            "WDC is the natural pilot corpus for component extraction.",
        )
    elif rate_rnlg > rate_wdc:
        winner, reason = (
            "recipenlg",
            f"{rate_rnlg:.2%} vs WDC {rate_wdc:.2%} — "
            "surprising; RecipeNLG carries more component structure here.",
        )
    else:
        winner, reason = (
            "tie",
            f"both ~{rate_wdc:.2%} — neither corpus is clearly richer.",
        )
    return winner, reason


# --- CLI ------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--recipenlg",
        type=Path,
        default=Path("dataset/full_dataset.csv"),
        help="Path to RecipeNLG full_dataset.csv",
    )
    p.add_argument(
        "--wdc",
        type=Path,
        default=Path("dataset/wdc/Recipe_top100.zip"),
        help="Path to WDC Recipe_top100.zip",
    )
    p.add_argument(
        "--sample-size",
        type=int,
        default=50000,
        help="RecipeNLG sample size (reservoir). Default 50000.",
    )
    p.add_argument(
        "--wdc-sample-size",
        type=int,
        default=5000,
        help="WDC sample size (reservoir, across selected hosts). Default 5000.",
    )
    p.add_argument(
        "--wdc-hosts",
        type=int,
        default=5,
        help="Number of WDC host files to sample from. Default 5.",
    )
    p.add_argument(
        "--full",
        action="store_true",
        help="Process the entire RecipeNLG + all WDC hosts, no sampling.",
    )
    p.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Seed for reservoir sampling and host shuffling. Default 42.",
    )
    p.add_argument(
        "--examples-to-show",
        type=int,
        default=20,
        help="Number of component-structured examples to show per corpus. Default 20.",
    )
    p.add_argument(
        "--json",
        type=Path,
        default=None,
        help="If set, write a machine-readable JSON sidecar to this path.",
    )
    p.add_argument(
        "--skip-recipenlg", action="store_true", help="Skip RecipeNLG (debug only)."
    )
    p.add_argument(
        "--skip-wdc", action="store_true", help="Skip WDC (debug only)."
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    rnlg_report = CorpusReport(name="recipenlg")
    wdc_report = CorpusReport(name="wdc")

    if not args.skip_recipenlg:
        if not args.recipenlg.exists():
            print(
                f"ERROR: RecipeNLG CSV not found at {args.recipenlg}",
                file=sys.stderr,
            )
            return 1
        rnlg_report = diagnose_recipenlg(
            args.recipenlg,
            sample_size=None if args.full else args.sample_size,
            seed=args.seed,
            examples_target=max(args.examples_to_show, 20),
        )
        print(format_report(rnlg_report, args.examples_to_show))
        print()

    if not args.skip_wdc:
        if not args.wdc.exists():
            print(f"ERROR: WDC zip not found at {args.wdc}", file=sys.stderr)
            return 1
        wdc_report = diagnose_wdc(
            args.wdc,
            sample_size=None if args.full else args.wdc_sample_size,
            n_hosts=None if args.full else args.wdc_hosts,
            seed=args.seed,
            examples_target=max(args.examples_to_show, 20),
        )
        print(format_report(wdc_report, args.examples_to_show))
        print()

    winner, reason = recommend_pilot_corpus(rnlg_report, wdc_report)
    print("=== summary ===")
    print(f"  pilot recommendation: {winner}")
    print(f"  rationale: {reason}")

    if args.json is not None:
        payload = {
            "seed": args.seed,
            "recipenlg": rnlg_report.to_dict(),
            "wdc": wdc_report.to_dict(),
            "recommendation": {"winner": winner, "rationale": reason},
        }
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(json.dumps(payload, indent=2, sort_keys=True))
        print(f"  json sidecar: {args.json}")

    return 0


# --- Convenience: in-memory zip builder for CLI smoke tests --------------


def build_synthetic_wdc_zip(
    rows_by_host: dict[str, list[dict[str, object]]],
) -> bytes:
    """Pack {host: [rows]} into the on-disk WDC layout."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for host, rows in rows_by_host.items():
            inner = io.BytesIO()
            with gzip.GzipFile(fileobj=inner, mode="wb") as gz:
                for row in rows:
                    gz.write((json.dumps(row) + "\n").encode("utf-8"))
            zf.writestr(
                f"Recipe_{host}_October2023.json.gz", inner.getvalue()
            )
    return buf.getvalue()


if __name__ == "__main__":
    sys.exit(main())
