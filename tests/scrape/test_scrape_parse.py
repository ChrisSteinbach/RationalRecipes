"""Tests for num_ctx threading through parse._ollama_generate (RationalRecipes-rjqg).

Without an explicit num_ctx Ollama allocates each model's NATIVE context
window per slot. On the parse-fast endpoint (NP=4) that means a 128 k-ctx
model demands ~150 GiB of KV cache and OOMs the GPU. These tests pin the
contract that production parse_ingredient_line carries num_ctx=4096 by
default, and that callers can override it.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import patch

from rational_recipes.scrape.parse import (
    DEFAULT_NUM_CTX,
    parse_ingredient_line,
)


def _fake_response(body: dict[str, Any]) -> Any:
    """Build an object with the minimal shape urlopen's context manager exposes."""

    class _Resp:
        def read(self) -> bytes:
            return json.dumps(body).encode()

        def __enter__(self) -> _Resp:
            return self

        def __exit__(self, *exc: object) -> None:
            return None

    return _Resp()


def _capture_request_body(captured: list[bytes]) -> Any:
    """Return a urlopen replacement that captures the POST body."""

    def _fake_urlopen(req: Any, timeout: float = 0) -> Any:
        # urllib.request.Request stores the body on .data; .get_data() also works.
        captured.append(req.data if hasattr(req, "data") else req.get_data())
        return _fake_response(
            {
                "response": json.dumps(
                    {
                        "quantity": 1.0,
                        "unit": "cup",
                        "ingredient": "flour",
                        "preparation": "",
                    }
                )
            }
        )

    return _fake_urlopen


class TestNumCtxThreading:
    """The Ollama API request body must carry options.num_ctx."""

    def test_default_num_ctx_is_4096(self) -> None:
        """parse_ingredient_line with no num_ctx kwarg → options.num_ctx=4096."""
        assert DEFAULT_NUM_CTX == 4096
        captured: list[bytes] = []
        with patch(
            "urllib.request.urlopen",
            side_effect=_capture_request_body(captured),
        ):
            parse_ingredient_line("1 cup flour")
        assert len(captured) == 1
        body = json.loads(captured[0])
        assert body["options"]["num_ctx"] == 4096

    def test_explicit_num_ctx_overrides_default(self) -> None:
        """An explicit num_ctx kwarg flows into options.num_ctx unchanged."""
        captured: list[bytes] = []
        with patch(
            "urllib.request.urlopen",
            side_effect=_capture_request_body(captured),
        ):
            parse_ingredient_line("1 cup flour", num_ctx=8192)
        assert len(captured) == 1
        body = json.loads(captured[0])
        assert body["options"]["num_ctx"] == 8192

    def test_determinism_options_preserved_alongside_num_ctx(self) -> None:
        """Adding num_ctx must not displace temperature=0 / seed=42 — these
        pin variant_id stability across reruns (Phase 2 closed bead)."""
        captured: list[bytes] = []
        with patch(
            "urllib.request.urlopen",
            side_effect=_capture_request_body(captured),
        ):
            parse_ingredient_line("1 cup flour", num_ctx=2048)
        body = json.loads(captured[0])
        opts = body["options"]
        assert opts["num_ctx"] == 2048
        assert opts["temperature"] == 0.0
        assert opts["seed"] == 42
        # format=json is at the top level of the payload, not inside options.
        assert body["format"] == "json"

    def test_batched_path_threads_num_ctx(self) -> None:
        """parse_ingredient_lines (batched path) also forwards num_ctx."""
        from rational_recipes.scrape.parse import parse_ingredient_lines

        captured: list[bytes] = []

        def _batch_response(req: Any, timeout: float = 0) -> Any:
            captured.append(req.data)
            # Return a 2-line batched response so the call succeeds without
            # bisection — we only care about the request body shape.
            return _fake_response(
                {
                    "response": json.dumps(
                        {
                            "results": [
                                {
                                    "quantity": 1.0,
                                    "unit": "cup",
                                    "ingredient": "flour",
                                    "preparation": "",
                                },
                                {
                                    "quantity": 2.0,
                                    "unit": "MEDIUM",
                                    "ingredient": "egg",
                                    "preparation": "",
                                },
                            ]
                        }
                    )
                }
            )

        with patch(
            "urllib.request.urlopen",
            side_effect=_batch_response,
        ):
            parse_ingredient_lines(
                ["unparseable line A", "unparseable line B"],
                num_ctx=1024,
                use_regex_prefilter=False,
            )
        # At least one batched call hit the wire; every captured body has
        # the supplied num_ctx.
        assert captured
        for raw in captured:
            assert json.loads(raw)["options"]["num_ctx"] == 1024


