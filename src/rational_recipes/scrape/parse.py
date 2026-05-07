"""LLM-based ingredient line parsing via Ollama.

Turns natural-language ingredient strings ("1 1/2 cups flour, sifted") into
structured fields (quantity, unit, ingredient, preparation).

Uses the Ollama REST API for reliability. Default points at the production
remote's parse-fast endpoint (192.168.50.189:11444) since the canonical
scrape runs there; override with `--ollama-url http://localhost:11434`
for local dev.
"""

from __future__ import annotations

import json
import logging
import threading
import urllib.error
import urllib.request
from collections import defaultdict
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# parse-fast endpoint (NP=4, KEEP_ALIVE=5m) — tuned for batched parsing
# per ollama-tuning-report.md (2026-05-07). Was 11434 (auto-tuned NP=8,
# wrong for parsing); the legacy port survives as a fallback only.
OLLAMA_BASE_URL = "http://192.168.50.189:11444"

# Default per-call ``num_ctx`` for ingredient-line parsing
# (RationalRecipes-rjqg). Without an explicit ``num_ctx`` Ollama
# allocates each model's NATIVE context window per slot — for a
# 128 k-ctx model on the parse-fast endpoint (NP=4) that's ~150 GiB
# of KV demand, blowing past a single 24 GiB GPU and returning HTTP
# 500 ``model requires more system memory``. 4096 matches the parse-
# fast tuning report's measurements and is well above the longest
# single-line prompt we send (a batched prompt of 30 lines + system
# prompt + JSON wrapper is still well under 4 k tokens).
DEFAULT_NUM_CTX = 4096

_SYSTEM_PROMPT = """\
You are an ingredient parser. Given a recipe ingredient line, extract structured fields.

Return ONLY a JSON object with these fields:
- "quantity": number (float). For fractions like "1/2", convert to decimal (0.5). \
For "1 1/2", convert to 1.5. For ranges like "1-2", use the midpoint (1.5). \
For "a pinch" or "to taste", use 0. If no quantity, use 1.
- "unit": the unit of measurement (e.g. "cup", "tbsp", "tsp", "oz", "lb", "g", "ml"). \
Normalize abbreviations: "c." → "cup", "Tbsp." → "tbsp", "tsp." → "tsp". \
For whole items like "2 eggs" or "2 large eggs", use "MEDIUM" (or "LARGE"/"SMALL" if specified). \
If no unit, use "g" for weight items or "MEDIUM" for countable items.
- "ingredient": the base ingredient name, lowercase, no preparation notes. \
E.g. "all-purpose flour" → "flour", "unsalted butter" → "butter", \
"large eggs" → "egg". Remove plurals for countable items.
- "preparation": any preparation notes (e.g. "sifted", "melted", "chopped", "separated"). \
Empty string if none.

Examples:
Input: "1 c. all-purpose flour"
Output: {"quantity": 1.0, "unit": "cup", "ingredient": "flour", "preparation": ""}

Input: "1/2 c. evaporated milk"
Output: {"quantity": 0.5, "unit": "cup", "ingredient": "milk", "preparation": ""}

Input: "2 Tbsp. butter or margarine"
Output: {"quantity": 2.0, "unit": "tbsp", "ingredient": "butter", "preparation": ""}

Input: "3 eggs, separated"
Output: {"quantity": 3.0, "unit": "MEDIUM", "ingredient": "egg", "preparation": "separated"}

Input: "2 large eggs"
Output: {"quantity": 2.0, "unit": "LARGE", "ingredient": "egg", "preparation": ""}

Input: "1 1/2 c. water"
Output: {"quantity": 1.5, "unit": "cup", "ingredient": "water", "preparation": ""}

Input: "1 pinch salt"
Output: {"quantity": 1.0, "unit": "pinch", "ingredient": "salt", "preparation": ""}

Input: "1/4 tsp. salt"
Output: {"quantity": 0.25, "unit": "tsp", "ingredient": "salt", "preparation": ""}

Input: "1 cup milk"
Output: {"quantity": 1.0, "unit": "cup", "ingredient": "milk", "preparation": ""}

Input: "1/2 cup heavy cream, whipped"
Output: {"quantity": 0.5, "unit": "cup", "ingredient": "cream", "preparation": "whipped"}

Input: "1 tablespoon sugar"
Output: {"quantity": 1.0, "unit": "tbsp", "ingredient": "sugar", "preparation": ""}

Input: "1 Tbsp. margarine"
Output: {"quantity": 1.0, "unit": "tbsp", "ingredient": "margarine", "preparation": ""}
"""


_BATCH_SYSTEM_PROMPT = (
    _SYSTEM_PROMPT
    + """
When given multiple lines as a JSON array, return an object with a single
key "results" whose value is a JSON array of parsed objects in the SAME
ORDER as the input. The output array MUST have exactly the same length as
the input array — one parsed object per input line, no skips, no merges.

Example:
Input: ["1 c. flour", "2 large eggs"]
Output: {"results": [
  {"quantity": 1.0, "unit": "cup", "ingredient": "flour", "preparation": ""},
  {"quantity": 2.0, "unit": "LARGE", "ingredient": "egg", "preparation": ""}
]}
"""
)

# Cap batch size to avoid prompts that strain the model's reliable
# array-tracking. 30 lines covers >99% of real recipes; larger batches
# get split and merged.
_MAX_BATCH_SIZE = 30
# Token budget per parsed line (JSON object), with a safety floor for
# per-call overhead (the "results" wrapper, brackets, commas).
_TOKENS_PER_LINE = 80
_BATCH_OVERHEAD_TOKENS = 100


@dataclass(frozen=True, slots=True)
class ParsedIngredient:
    """Structured representation of a parsed ingredient line."""

    quantity: float
    unit: str
    ingredient: str
    preparation: str
    raw: str


def _ollama_generate(
    prompt: str,
    model: str,
    system: str = _SYSTEM_PROMPT,
    base_url: str = OLLAMA_BASE_URL,
    timeout: float = 120.0,
    num_predict: int = 256,
    num_ctx: int = DEFAULT_NUM_CTX,
) -> str | None:
    """Call Ollama REST API /api/generate, return the response text.

    ``num_predict`` caps the generated-token count. Ingredient JSONs are
    well under 100 tokens, so a small cap turns degenerate token-loop
    responses (seen on larger models like gemma4:26b) into bounded-length
    failures rather than 300s timeouts.

    ``num_ctx`` (RationalRecipes-rjqg) sizes the per-slot KV cache. We
    pass it explicitly so the parse-fast endpoint (NP=4) doesn't fall
    back to each model's native context window — that path OOMs on a
    single 24 GiB GPU for any 128 k-ctx model (e.g. mistral-nemo:12b).

    ``temperature=0`` + ``seed=42`` make the response deterministic so
    that downstream ``variant_id`` hashes are stable across reruns of
    the merged pipeline (RationalRecipes-toj). Without them, sampling
    noise was occasionally swapping ingredients ("sugar" vs "flour")
    between runs and producing different ``canonical_ingredient_set``s
    for the same recipe.
    """
    payload = json.dumps(
        {
            "model": model,
            "system": system,
            "prompt": prompt,
            "format": "json",
            "stream": False,
            "options": {
                "num_predict": num_predict,
                "num_ctx": num_ctx,
                "temperature": 0.0,
                "seed": 42,
            },
        }
    ).encode()

    req = urllib.request.Request(
        f"{base_url}/api/generate",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = json.loads(resp.read())
            # "response" is the visible output for non-thinking models; some
            # thinking models (e.g. qwen3.5) place the JSON in "thinking"
            # and leave "response" empty. Prefer response when non-empty.
            visible = body.get("response") or ""
            if visible.strip():
                return visible
            thinking = body.get("thinking") or ""
            return thinking or visible
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
        logger.warning("Ollama API call failed: %s", e)
        return None


def parse_ingredient_line(
    line: str,
    model: str = "gemma4:e2b",
    base_url: str = OLLAMA_BASE_URL,
    system_prompt: str | None = None,
    timeout: float = 120.0,
    num_predict: int = 256,
    num_ctx: int = DEFAULT_NUM_CTX,
) -> ParsedIngredient | None:
    """Parse a single ingredient line using Ollama.

    Returns None if parsing fails.
    """
    prompt = f'Parse this ingredient line:\nInput: "{line}"\nOutput:'

    raw_output = _ollama_generate(
        prompt,
        model=model,
        system=system_prompt or _SYSTEM_PROMPT,
        base_url=base_url,
        timeout=timeout,
        num_predict=num_predict,
        num_ctx=num_ctx,
    )
    if raw_output is None:
        return None

    try:
        data = json.loads(raw_output)
    except json.JSONDecodeError:
        # Try extracting JSON from the output
        start = raw_output.find("{")
        end = raw_output.rfind("}") + 1
        if start >= 0 and end > start:
            try:
                data = json.loads(raw_output[start:end])
            except json.JSONDecodeError:
                logger.warning(
                    "Could not parse JSON from Ollama for %r: %s",
                    line,
                    raw_output[:200],
                )
                return None
        else:
            logger.warning(
                "No JSON in Ollama output for %r: %s",
                line,
                raw_output[:200],
            )
            return None

    # Smaller Ollama models (notably gemma3n:e2b) occasionally misspell the
    # "ingredient" key as "ingruedient"/"ingrredient"/etc.; accept any key
    # whose name starts with "ingr" as the ingredient field so we don't
    # discard an otherwise-usable parse.
    ingredient_key = next(
        (k for k in data if isinstance(k, str) and k.lower().startswith("ingr")),
        None,
    )
    if ingredient_key is None:
        logger.warning("No ingredient key in parsed data for %r: %s", line, data)
        return None

    try:
        return ParsedIngredient(
            quantity=float(data["quantity"]),
            unit=str(data["unit"]),
            ingredient=str(data[ingredient_key]).lower().strip(),
            preparation=str(data.get("preparation", "")),
            raw=line,
        )
    except (KeyError, ValueError, TypeError) as e:
        logger.warning("Invalid parsed data for %r: %s — %s", line, data, e)
        return None


def parse_ingredient_lines(
    lines: list[str],
    model: str = "gemma4:e2b",
    base_url: str = OLLAMA_BASE_URL,
    system_prompt: str | None = None,
    timeout: float = 120.0,
    num_predict: int = 256,
    num_ctx: int = DEFAULT_NUM_CTX,
    *,
    use_regex_prefilter: bool = True,
) -> list[ParsedIngredient | None]:
    """Parse multiple ingredient lines, regex-first then LLM (vwt.17).

    Hybrid path: each line is first run through ``regex_parse_line``
    (microseconds, deterministic, USDA-confidence-gated). Lines the
    regex declines fall through to the existing batched LLM parse. The
    contract is unchanged: returns a list parallel to the input — index
    i is the parse for line i, or None on failure.

    ``use_regex_prefilter=False`` reverts to LLM-only behavior — handy
    for shadow A/B comparisons against the pre-vwt.17 baseline.
    """
    if not lines:
        return []

    if use_regex_prefilter:
        # Local import to avoid a hard dep cycle; regex_parse imports
        # ParsedIngredient from this module.
        from rational_recipes.scrape.regex_parse import regex_parse_line

        results: list[ParsedIngredient | None] = [None] * len(lines)
        residue_indices: list[int] = []
        residue_lines: list[str] = []
        for i, line in enumerate(lines):
            hit = regex_parse_line(line)
            if hit is not None:
                results[i] = hit.parsed
            else:
                residue_indices.append(i)
                residue_lines.append(line)

        if not residue_lines:
            return results

        llm_results = _llm_parse_lines(
            residue_lines,
            model=model,
            base_url=base_url,
            system_prompt=system_prompt,
            timeout=timeout,
            num_predict=num_predict,
            num_ctx=num_ctx,
        )
        for idx, parsed in zip(residue_indices, llm_results, strict=True):
            results[idx] = parsed
        return results

    return _llm_parse_lines(
        lines,
        model=model,
        base_url=base_url,
        system_prompt=system_prompt,
        timeout=timeout,
        num_predict=num_predict,
        num_ctx=num_ctx,
    )


def _llm_parse_lines(
    lines: list[str],
    *,
    model: str,
    base_url: str,
    system_prompt: str | None,
    timeout: float,
    num_predict: int,
    num_ctx: int,
) -> list[ParsedIngredient | None]:
    """LLM-only parse path: one batched call per ``_MAX_BATCH_SIZE`` chunk."""
    if not lines:
        return []
    if len(lines) == 1:
        return [
            parse_ingredient_line(
                lines[0],
                model=model,
                base_url=base_url,
                system_prompt=system_prompt,
                timeout=timeout,
                num_predict=num_predict,
                num_ctx=num_ctx,
            )
        ]

    results: list[ParsedIngredient | None] = []
    for i in range(0, len(lines), _MAX_BATCH_SIZE):
        chunk = lines[i : i + _MAX_BATCH_SIZE]
        results.extend(
            _parse_batch_with_fallback(
                chunk,
                model=model,
                base_url=base_url,
                system_prompt=system_prompt,
                timeout=timeout,
                num_predict=num_predict,
                num_ctx=num_ctx,
            )
        )
    return results


@dataclass
class _BatchTreeState:
    """Per-original-batch state passed through the bisect recursion.

    Tracks the maximum recursion depth reached so the top-level call can
    classify the original batch as depth-0 (full success), depth-1 (one
    bisect needed), depth-N, etc. (bead RationalRecipes-6bc).
    """

    original_size: int
    max_depth: int = 0


@dataclass
class BatchStatsSnapshot:
    """Snapshot of bisect-rate counters from a Pass 1 run (RationalRecipes-6bc).

    All counters are top-level (per-original-batch) except total_llm_calls
    which counts every actual Ollama call (batched + singleton fallback).
    """

    total_top_level_batches: int = 0
    total_top_level_lines: int = 0
    total_llm_calls: int = 0
    max_depth_counts: dict[int, int] = field(default_factory=dict)
    size_histogram_total: dict[int, int] = field(default_factory=dict)
    size_histogram_succeeded_root: dict[int, int] = field(default_factory=dict)


class _BatchStats:
    """Thread-safe accumulator for batched-parse bisect metrics."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._reset_locked()

    def _reset_locked(self) -> None:
        self.total_top_level_batches = 0
        self.total_top_level_lines = 0
        self.total_llm_calls = 0
        self.max_depth_counts: dict[int, int] = defaultdict(int)
        self.size_histogram_total: dict[int, int] = defaultdict(int)
        self.size_histogram_succeeded_root: dict[int, int] = defaultdict(int)

    def reset(self) -> None:
        with self._lock:
            self._reset_locked()

    def record_llm_call(self, n: int = 1) -> None:
        with self._lock:
            self.total_llm_calls += n

    def record_top_level_start(self, size: int) -> None:
        with self._lock:
            self.total_top_level_batches += 1
            self.total_top_level_lines += size
            self.size_histogram_total[size] += 1

    def record_top_level_finish(self, state: _BatchTreeState) -> None:
        with self._lock:
            self.max_depth_counts[state.max_depth] += 1
            if state.max_depth == 0:
                self.size_histogram_succeeded_root[state.original_size] += 1

    def snapshot(self) -> BatchStatsSnapshot:
        with self._lock:
            return BatchStatsSnapshot(
                total_top_level_batches=self.total_top_level_batches,
                total_top_level_lines=self.total_top_level_lines,
                total_llm_calls=self.total_llm_calls,
                max_depth_counts=dict(self.max_depth_counts),
                size_histogram_total=dict(self.size_histogram_total),
                size_histogram_succeeded_root=dict(self.size_histogram_succeeded_root),
            )


_BATCH_STATS = _BatchStats()


def reset_batch_stats() -> None:
    """Zero the bisect-rate counters (RationalRecipes-6bc)."""
    _BATCH_STATS.reset()


def get_batch_stats() -> BatchStatsSnapshot:
    """Read-only snapshot of the current bisect-rate counters."""
    return _BATCH_STATS.snapshot()


def format_batch_stats_summary(snapshot: BatchStatsSnapshot | None = None) -> str:
    """Human-readable summary of bisect-rate metrics (RationalRecipes-6bc).

    Reports the seven metrics called out by the bead:
      1. Total batches attempted
      2-5. % of batches at max-depth 0 / 1 / 2 / >=3
      6. Per-size success/total histogram
      7. Effective LLM calls per line
    """
    if snapshot is None:
        snapshot = get_batch_stats()
    if snapshot.total_top_level_batches == 0:
        return "Batched-parse stats: no batches recorded."

    total = snapshot.total_top_level_batches
    lines = snapshot.total_top_level_lines
    calls = snapshot.total_llm_calls

    depth_pct: dict[int, float] = {}
    for depth, count in snapshot.max_depth_counts.items():
        depth_pct[depth] = 100.0 * count / total

    lines_out: list[str] = []
    lines_out.append("Batched-parse stats (bead RationalRecipes-6bc):")
    lines_out.append(
        f"  total top-level batches: {total} "
        f"(total lines: {lines}, total LLM calls: {calls})"
    )
    lines_out.append(
        f"  effective calls/line: {calls / lines:.3f}" if lines else
        "  effective calls/line: n/a"
    )

    def _depth_line(depth: int) -> str:
        c = snapshot.max_depth_counts.get(depth, 0)
        p = depth_pct.get(depth, 0.0)
        return f"    max-depth {depth}: {c} ({p:.1f}%)"

    lines_out.append("  bisect tree (max recursion depth per original batch):")
    for d in sorted(snapshot.max_depth_counts):
        lines_out.append(_depth_line(d))

    lines_out.append("  size histogram (size: succeeded@root / total):")
    for size in sorted(snapshot.size_histogram_total):
        total_at_size = snapshot.size_histogram_total[size]
        ok_at_size = snapshot.size_histogram_succeeded_root.get(size, 0)
        pct = 100.0 * ok_at_size / total_at_size if total_at_size else 0.0
        lines_out.append(
            f"    size {size}: {ok_at_size}/{total_at_size} ({pct:.1f}%)"
        )

    return "\n".join(lines_out)


def _parse_batch_with_fallback(
    lines: list[str],
    *,
    model: str,
    base_url: str,
    system_prompt: str | None,
    timeout: float,
    num_predict: int,
    num_ctx: int,
    _state: _BatchTreeState | None = None,
    _depth: int = 0,
) -> list[ParsedIngredient | None]:
    """Try one batched call; on failure, bisect (vwt.21).

    A failed batch is split in half and each half retried as its own batch.
    This costs O(log N) extra calls instead of N when only a few items in a
    big batch trip up the model, while still degrading to per-line for the
    singletons at the leaves.

    Records bisect-rate stats into the module-level counter so a Pass 1 run
    can report effective calls-per-line (bead RationalRecipes-6bc). The
    ``_state`` and ``_depth`` parameters are private — callers always pass
    neither, and recursion threads them through.
    """
    is_root = _state is None
    if _state is None:
        _state = _BatchTreeState(original_size=len(lines))
        _BATCH_STATS.record_top_level_start(len(lines))
    if _depth > _state.max_depth:
        _state.max_depth = _depth

    try:
        if len(lines) <= 1:
            # Skip the batched-call hop for singletons — the per-line system
            # prompt and response shape are simpler and more reliable.
            _BATCH_STATS.record_llm_call(len(lines))
            return [
                parse_ingredient_line(
                    line,
                    model=model,
                    base_url=base_url,
                    system_prompt=system_prompt,
                    timeout=timeout,
                    num_predict=num_predict,
                    num_ctx=num_ctx,
                )
                for line in lines
            ]

        _BATCH_STATS.record_llm_call(1)
        batched = _parse_batch(
            lines,
            model=model,
            base_url=base_url,
            system_prompt=system_prompt,
            timeout=timeout,
            num_predict=num_predict,
            num_ctx=num_ctx,
        )
        if batched is not None:
            return batched

        mid = len(lines) // 2
        logger.warning(
            "Batched parse failed for %d lines; bisecting into %d + %d",
            len(lines),
            mid,
            len(lines) - mid,
        )
        left = _parse_batch_with_fallback(
            lines[:mid],
            model=model,
            base_url=base_url,
            system_prompt=system_prompt,
            timeout=timeout,
            num_predict=num_predict,
            num_ctx=num_ctx,
            _state=_state,
            _depth=_depth + 1,
        )
        right = _parse_batch_with_fallback(
            lines[mid:],
            model=model,
            base_url=base_url,
            system_prompt=system_prompt,
            timeout=timeout,
            num_predict=num_predict,
            num_ctx=num_ctx,
            _state=_state,
            _depth=_depth + 1,
        )
        return left + right
    finally:
        if is_root:
            _BATCH_STATS.record_top_level_finish(_state)


def _parse_batch(
    lines: list[str],
    *,
    model: str,
    base_url: str,
    system_prompt: str | None,
    timeout: float,
    num_predict: int,
    num_ctx: int,
) -> list[ParsedIngredient | None] | None:
    """One batched LLM call. Returns None on any structural failure."""
    # Scale num_predict with batch size so the model isn't truncated mid-array.
    batch_num_predict = max(num_predict, _TOKENS_PER_LINE * len(lines) + _BATCH_OVERHEAD_TOKENS)

    # JSON-encode the input list so the model sees a clean array literal —
    # numbering ("1.", "2.") confused the model into rewriting indices.
    prompt = (
        f'Parse these {len(lines)} ingredient lines and return one parsed '
        f'object per input line, in input order:\nInput: {json.dumps(lines)}\nOutput:'
    )

    raw_output = _ollama_generate(
        prompt,
        model=model,
        system=system_prompt or _BATCH_SYSTEM_PROMPT,
        base_url=base_url,
        timeout=timeout,
        num_predict=batch_num_predict,
        num_ctx=num_ctx,
    )
    if raw_output is None:
        return None

    try:
        data = json.loads(raw_output)
    except json.JSONDecodeError:
        start = raw_output.find("{")
        end = raw_output.rfind("}") + 1
        if start < 0 or end <= start:
            logger.warning("No JSON object in batched output: %s", raw_output[:200])
            return None
        try:
            data = json.loads(raw_output[start:end])
        except json.JSONDecodeError:
            logger.warning("Could not parse batched JSON: %s", raw_output[:200])
            return None

    items = data.get("results") if isinstance(data, dict) else None
    if not isinstance(items, list):
        logger.warning("Batched output missing 'results' array: %s", str(data)[:200])
        return None
    if len(items) != len(lines):
        logger.warning(
            "Batched output length mismatch: got %d, expected %d",
            len(items),
            len(lines),
        )
        return None

    parsed: list[ParsedIngredient | None] = []
    for line, obj in zip(lines, items, strict=True):
        parsed.append(_dict_to_parsed(obj, line))
    return parsed


def _dict_to_parsed(data: object, line: str) -> ParsedIngredient | None:
    """Turn one LLM-returned dict into a ParsedIngredient, or None on failure.

    Mirrors the validation logic from parse_ingredient_line so batched and
    per-line paths produce identical results for the same input dict.
    """
    if not isinstance(data, dict):
        return None
    ingredient_key = next(
        (k for k in data if isinstance(k, str) and k.lower().startswith("ingr")),
        None,
    )
    if ingredient_key is None:
        return None
    try:
        return ParsedIngredient(
            quantity=float(data["quantity"]),
            unit=str(data["unit"]),
            ingredient=str(data[ingredient_key]).lower().strip(),
            preparation=str(data.get("preparation", "")),
            raw=line,
        )
    except (KeyError, ValueError, TypeError):
        return None
