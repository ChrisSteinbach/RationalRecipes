"""Manifest contract for the merged pipeline output.

The merged pipeline (``RationalRecipes-toj``) emits per-variant CSVs
alongside a ``manifest.json`` that downstream consumers (review shell,
SQLite writer, L3 variant-splitting) rely on. The per-variant CSVs stay
compatible with ``rr-stats``; the manifest is the structured index.

Schema — one entry per variant in ``manifest.json``::

    {
      "manifest_version": 1,
      "variants": [
        {
          "variant_id": "3fa8c91d7e42",
          "title": "pannkakor",
          "canonical_ingredients": ["butter", "egg", "flour", "milk", "salt"],
          "cooking_methods": ["stekt"],
          "n_recipes": 42,
          "csv_path": "pannkakor_3fa8c91d7e42.csv",
          "source_urls": ["https://example.com/recipe/..."],
          "row_outlier_scores": [0.0, 1.23, ...]
        }
      ]
    }

``row_outlier_scores`` is optional (bead 0g3) and aligned row-for-row
with the per-variant CSV. Readers that predate the field treat it as
empty; writers that have no meaningful scores to emit (n_recipes ≤ 1)
may omit it.

``variant_id`` is the 12-char SHA1 prefix of
``normalized_l1_title | sorted(canonical_ingredients) | sorted(cooking_methods)``.
See ``compute_variant_id`` for the exact hashing spec.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import cast

MANIFEST_VERSION = 1

_VARIANT_ID_LEN = 12


def compute_variant_id(
    normalized_title: str,
    canonical_ingredients: Iterable[str],
    cooking_methods: Iterable[str],
) -> str:
    """Stable 12-hex-char id for a merged-pipeline variant.

    Hash input is ``normalized_title | sorted(canonical_ingredients) |
    sorted(cooking_methods)`` with ``|`` as the separator and individual
    set elements comma-joined after sorting. Canonicalization (bead 3cu)
    maps raw ingredient names to a shared English vocabulary before this
    function sees them, so the id is stable across re-runs as long as
    canonicalization is stable.

    Empty ``cooking_methods`` is valid and common (RecipeNLG side carries
    no method field; WDC hosts without cookingMethod produce an empty
    set). It hashes identically to any other empty set, so a later run
    that adds L3 method data to a variant will change that variant's id
    — which is the correct behavior: a method-refined variant is a new
    variant.
    """
    title = normalized_title.strip()
    ing_str = ",".join(sorted(canonical_ingredients))
    method_str = ",".join(sorted(cooking_methods))
    payload = f"{title}|{ing_str}|{method_str}".encode()
    return hashlib.sha1(payload, usedforsecurity=False).hexdigest()[:_VARIANT_ID_LEN]


@dataclass(frozen=True, slots=True)
class VariantManifestEntry:
    variant_id: str
    title: str
    canonical_ingredients: tuple[str, ...]
    cooking_methods: tuple[str, ...]
    n_recipes: int
    csv_path: str
    source_urls: tuple[str, ...]
    row_outlier_scores: tuple[float, ...] = ()

    def to_json_dict(self) -> dict[str, object]:
        data: dict[str, object] = {
            "variant_id": self.variant_id,
            "title": self.title,
            "canonical_ingredients": list(self.canonical_ingredients),
            "cooking_methods": list(self.cooking_methods),
            "n_recipes": self.n_recipes,
            "csv_path": self.csv_path,
            "source_urls": list(self.source_urls),
        }
        if self.row_outlier_scores:
            data["row_outlier_scores"] = list(self.row_outlier_scores)
        return data

    @classmethod
    def from_json_dict(cls, data: dict[str, object]) -> VariantManifestEntry:
        raw_scores = data.get("row_outlier_scores", [])
        if not isinstance(raw_scores, list):
            raise ValueError("row_outlier_scores must be a list when present")
        return cls(
            variant_id=str(data["variant_id"]),
            title=str(data["title"]),
            canonical_ingredients=tuple(cast(list[str], data["canonical_ingredients"])),
            cooking_methods=tuple(cast(list[str], data["cooking_methods"])),
            n_recipes=cast(int, data["n_recipes"]),
            csv_path=str(data["csv_path"]),
            source_urls=tuple(cast(list[str], data["source_urls"])),
            row_outlier_scores=tuple(float(x) for x in raw_scores),
        )


@dataclass
class Manifest:
    variants: list[VariantManifestEntry] = field(default_factory=list)
    manifest_version: int = MANIFEST_VERSION

    def to_json_dict(self) -> dict[str, object]:
        return {
            "manifest_version": self.manifest_version,
            "variants": [v.to_json_dict() for v in self.variants],
        }

    @classmethod
    def from_json_dict(cls, data: dict[str, object]) -> Manifest:
        version = cast(int, data.get("manifest_version", 0))
        if version != MANIFEST_VERSION:
            raise ValueError(
                f"Unsupported manifest_version {version}; expected {MANIFEST_VERSION}"
            )
        raw_variants = data.get("variants", [])
        if not isinstance(raw_variants, list):
            raise ValueError("variants must be a list")
        return cls(
            variants=[VariantManifestEntry.from_json_dict(v) for v in raw_variants],
            manifest_version=version,
        )

    def write(self, path: Path) -> None:
        path.write_text(
            json.dumps(self.to_json_dict(), indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

    @classmethod
    def read(cls, path: Path) -> Manifest:
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError("manifest.json root must be an object")
        return cls.from_json_dict(data)
