"""End-to-end tests for the catalog → JSON exporter (vwt.y43)."""

from __future__ import annotations

import json
from pathlib import Path

from rational_recipes.catalog_db import CatalogDB
from rational_recipes.cli.export_catalog_json import export, main


def _seed_variant(
    db: CatalogDB,
    *,
    variant_id: str,
    normalized_title: str,
    display_title: str,
    category: str | None,
    n_recipes: int,
    review_status: str | None = None,
    description: str | None = None,
    confidence_level: float | None = 0.95,
    base_ingredient: str = "flour",
    ingredients: list[dict[str, object]] | None = None,
    sources: list[dict[str, str]] | None = None,
) -> None:
    if ingredients is None:
        ingredients = [
            {
                "name": "flour",
                "ratio": 1.0,
                "proportion": 0.25,
                "stddev": 0.05,
                "ci_lower": 0.24,
                "ci_upper": 0.26,
                "min_sample_size": 100,
                "density_g_per_ml": 0.5283,
                "whole_unit_name": None,
                "whole_unit_grams": None,
            },
        ]
    canonical_set = ",".join(sorted(str(i["name"]) for i in ingredients))
    conn = db.connection
    with conn:
        conn.execute(
            """
            INSERT INTO variants (
              variant_id, normalized_title, display_title, category, description,
              base_ingredient, cooking_methods, canonical_ingredient_set,
              n_recipes, confidence_level, review_status
            ) VALUES (?, ?, ?, ?, ?, ?, '', ?, ?, ?, ?)
            """,
            (
                variant_id,
                normalized_title,
                display_title,
                category,
                description,
                base_ingredient,
                canonical_set,
                n_recipes,
                confidence_level,
                review_status,
            ),
        )
        for i, ing in enumerate(ingredients):
            conn.execute(
                """
                INSERT INTO variant_ingredient_stats (
                  variant_id, canonical_name, ordinal, mean_proportion, stddev,
                  ci_lower, ci_upper, ratio, min_sample_size, density_g_per_ml,
                  whole_unit_name, whole_unit_grams
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    variant_id,
                    ing["name"],
                    i,
                    ing["proportion"],
                    ing["stddev"],
                    ing["ci_lower"],
                    ing["ci_upper"],
                    ing["ratio"],
                    ing["min_sample_size"],
                    ing["density_g_per_ml"],
                    ing["whole_unit_name"],
                    ing["whole_unit_grams"],
                ),
            )
        for i, src in enumerate(sources or []):
            conn.execute(
                """
                INSERT INTO variant_sources (
                  variant_id, ordinal, source_type, title, ref
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    variant_id,
                    i,
                    src["type"],
                    src.get("title"),
                    src["ref"],
                ),
            )


class TestExport:
    def test_exports_only_variants_above_threshold(self, tmp_path: Path) -> None:
        db_path = tmp_path / "recipes.db"
        out_path = tmp_path / "catalog.json"
        db = CatalogDB.open(db_path)
        try:
            _seed_variant(
                db,
                variant_id="big",
                normalized_title="big",
                display_title="Big Bread",
                category="bread",
                n_recipes=200,
            )
            _seed_variant(
                db,
                variant_id="small",
                normalized_title="small",
                display_title="Small Bread",
                category="bread",
                n_recipes=50,
            )
        finally:
            db.close()

        n = export(db_path, out_path, min_recipes=100)
        assert n == 1

        catalog = json.loads(out_path.read_text(encoding="utf-8"))
        assert catalog["version"] == 1
        assert [r["id"] for r in catalog["recipes"]] == ["big"]

    def test_drops_review_status_drop(self, tmp_path: Path) -> None:
        db_path = tmp_path / "recipes.db"
        out_path = tmp_path / "catalog.json"
        db = CatalogDB.open(db_path)
        try:
            _seed_variant(
                db,
                variant_id="kept",
                normalized_title="kept",
                display_title="Kept",
                category="bread",
                n_recipes=200,
            )
            _seed_variant(
                db,
                variant_id="dropped",
                normalized_title="dropped",
                display_title="Dropped",
                category="bread",
                n_recipes=300,
                review_status="drop",
            )
        finally:
            db.close()

        n = export(db_path, out_path, min_recipes=100)
        assert n == 1
        catalog = json.loads(out_path.read_text(encoding="utf-8"))
        assert [r["id"] for r in catalog["recipes"]] == ["kept"]

    def test_smoke_path_emits_empty_catalog_without_error(
        self, tmp_path: Path
    ) -> None:
        # Smoke runs may produce zero variants meeting the default threshold;
        # the exporter must not raise — it should write `{"version":1,"recipes":[]}`.
        db_path = tmp_path / "recipes.db"
        out_path = tmp_path / "catalog.json"
        db = CatalogDB.open(db_path)
        try:
            _seed_variant(
                db,
                variant_id="tiny",
                normalized_title="tiny",
                display_title="Tiny",
                category="bread",
                n_recipes=3,
            )
        finally:
            db.close()

        n = export(db_path, out_path, min_recipes=100)
        assert n == 0
        catalog = json.loads(out_path.read_text(encoding="utf-8"))
        assert catalog == {"version": 1, "recipes": []}

    def test_sibling_variants_get_distinct_ids(self, tmp_path: Path) -> None:
        # Regression: prior to the y43 fix the exporter wrote
        # `id = normalized_title`, which collides across siblings in the
        # same L1 group (e.g. all "pancakes" variants share that title).
        # The PWA's findRecipe(id) returns the first match, so every card
        # in the L1 group navigated to the same detail page. Each emitted
        # recipe must carry its unique variant_id.
        db_path = tmp_path / "recipes.db"
        out_path = tmp_path / "catalog.json"
        db = CatalogDB.open(db_path)
        try:
            _seed_variant(
                db,
                variant_id="vid-buttermilk",
                normalized_title="pancakes",
                display_title="Buttermilk Pancakes",
                category="breakfast",
                n_recipes=200,
            )
            _seed_variant(
                db,
                variant_id="vid-shortening",
                normalized_title="pancakes",
                display_title="Shortening Pancakes",
                category="breakfast",
                n_recipes=150,
            )
        finally:
            db.close()

        n = export(db_path, out_path, min_recipes=100)
        assert n == 2
        catalog = json.loads(out_path.read_text(encoding="utf-8"))
        ids = [r["id"] for r in catalog["recipes"]]
        assert len(ids) == len(set(ids)), f"duplicate ids in export: {ids}"
        assert set(ids) == {"vid-buttermilk", "vid-shortening"}

    def test_min_recipes_override_picks_up_smoke_data(
        self, tmp_path: Path
    ) -> None:
        # The smoke path uses `--min-recipes 1` so a 3-recipe variant ends up
        # in the spot-check output.
        db_path = tmp_path / "recipes.db"
        out_path = tmp_path / "catalog.json"
        db = CatalogDB.open(db_path)
        try:
            _seed_variant(
                db,
                variant_id="tiny",
                normalized_title="tiny",
                display_title="Tiny",
                category="bread",
                n_recipes=3,
            )
        finally:
            db.close()

        n = export(db_path, out_path, min_recipes=1)
        assert n == 1
        catalog = json.loads(out_path.read_text(encoding="utf-8"))
        assert catalog["recipes"][0]["sample_size"] == 3

    def test_full_round_trip_shape(self, tmp_path: Path) -> None:
        # The shape must match what the PWA's `Catalog` type expects —
        # see web/src/catalog.ts.
        db_path = tmp_path / "recipes.db"
        out_path = tmp_path / "catalog.json"
        db = CatalogDB.open(db_path)
        try:
            _seed_variant(
                db,
                variant_id="vid",
                normalized_title="swedish-pancakes",
                display_title="Swedish Pancakes",
                category="crepes",
                description="Thin Scandinavian pancakes.",
                base_ingredient="flour",
                confidence_level=0.95,
                n_recipes=200,
                ingredients=[
                    {
                        "name": "flour",
                        "ratio": 1.0,
                        "proportion": 0.17,
                        "stddev": 0.05,
                        "ci_lower": 0.16,
                        "ci_upper": 0.18,
                        "min_sample_size": 116,
                        "density_g_per_ml": 0.5283,
                        "whole_unit_name": None,
                        "whole_unit_grams": None,
                    },
                    {
                        "name": "egg",
                        "ratio": 0.83,
                        "proportion": 0.20,
                        "stddev": 0.05,
                        "ci_lower": 0.19,
                        "ci_upper": 0.21,
                        "min_sample_size": 36,
                        "density_g_per_ml": 1.0271,
                        "whole_unit_name": "medium",
                        "whole_unit_grams": 44.0,
                    },
                ],
                sources=[
                    {
                        "type": "text",
                        "title": "Aggregated Swedish recipes",
                        "ref": "Swedish pannkakor.",
                    },
                ],
            )
        finally:
            db.close()

        n = export(db_path, out_path, min_recipes=100)
        assert n == 1

        catalog = json.loads(out_path.read_text(encoding="utf-8"))
        recipe = catalog["recipes"][0]
        assert recipe["id"] == "vid"  # variant_id, not normalized_title
        assert recipe["title"] == "Swedish Pancakes"
        assert recipe["category"] == "crepes"
        assert recipe["description"] == "Thin Scandinavian pancakes."
        assert recipe["base_ingredient"] == "flour"
        assert recipe["sample_size"] == 200
        assert recipe["confidence_level"] == 0.95
        assert len(recipe["ingredients"]) == 2
        flour, egg = recipe["ingredients"]
        assert flour["name"] == "flour"
        assert flour["ratio"] == 1.0
        assert flour["density_g_per_ml"] == 0.5283
        assert flour["whole_unit"] is None
        assert egg["whole_unit"] == {"name": "medium", "grams": 44.0}
        assert recipe["sources"] == [
            {
                "type": "text",
                "title": "Aggregated Swedish recipes",
                "ref": "Swedish pannkakor.",
            },
        ]


class TestMain:
    def test_cli_writes_default_path(self, tmp_path: Path) -> None:
        db_path = tmp_path / "recipes.db"
        out_path = tmp_path / "catalog.json"
        db = CatalogDB.open(db_path)
        try:
            _seed_variant(
                db,
                variant_id="vid",
                normalized_title="t",
                display_title="T",
                category="bread",
                n_recipes=200,
            )
        finally:
            db.close()

        rc = main([
            "--db",
            str(db_path),
            "--output",
            str(out_path),
            "--min-recipes",
            "100",
        ])
        assert rc == 0
        assert out_path.exists()
        catalog = json.loads(out_path.read_text(encoding="utf-8"))
        assert len(catalog["recipes"]) == 1

    def test_cli_returns_nonzero_on_missing_db(self, tmp_path: Path) -> None:
        rc = main([
            "--db",
            str(tmp_path / "nope.db"),
            "--output",
            str(tmp_path / "out.json"),
        ])
        assert rc == 1


def _seed_recipe_with_parsed(
    db: CatalogDB,
    *,
    recipe_id: str,
    parsed: list[tuple[str, float | None, str | None]],
    corpus: str = "recipenlg",
) -> None:
    conn = db.connection
    with conn:
        conn.execute(
            "INSERT INTO recipes (recipe_id, corpus) VALUES (?, ?)",
            (recipe_id, corpus),
        )
        for canonical_name, quantity, unit in parsed:
            conn.execute(
                """
                INSERT INTO parsed_ingredients (
                  recipe_id, canonical_name, quantity, unit
                ) VALUES (?, ?, ?, ?)
                """,
                (recipe_id, canonical_name, quantity, unit),
            )


def _attach_member(
    db: CatalogDB,
    *,
    variant_id: str,
    recipe_id: str,
    outlier_score: float | None = None,
) -> None:
    conn = db.connection
    with conn:
        conn.execute(
            "INSERT INTO variant_members (variant_id, recipe_id, outlier_score)"
            " VALUES (?, ?, ?)",
            (variant_id, recipe_id, outlier_score),
        )


class TestExportSources:
    """Per-variant source-ingredient sidecars (bead zh6)."""

    def test_writes_one_file_per_shipped_variant(self, tmp_path: Path) -> None:
        db_path = tmp_path / "recipes.db"
        out_path = tmp_path / "catalog.json"
        sources_dir = tmp_path / "sources"
        db = CatalogDB.open(db_path)
        try:
            _seed_variant(
                db,
                variant_id="big",
                normalized_title="big",
                display_title="Big Bread",
                category="bread",
                n_recipes=200,
            )
            _seed_recipe_with_parsed(
                db,
                recipe_id="r1",
                parsed=[("flour", 500.0, "g"), ("water", 1.0, "cup")],
            )
            _seed_recipe_with_parsed(
                db,
                recipe_id="r2",
                parsed=[("flour", 250.0, "g")],
            )
            _attach_member(
                db, variant_id="big", recipe_id="r1", outlier_score=0.1
            )
            _attach_member(
                db, variant_id="big", recipe_id="r2", outlier_score=0.5
            )
            # Below-threshold variant — should not get a sidecar.
            _seed_variant(
                db,
                variant_id="small",
                normalized_title="small",
                display_title="Small Bread",
                category="bread",
                n_recipes=50,
            )
            _seed_recipe_with_parsed(
                db, recipe_id="r3", parsed=[("flour", 100.0, "g")]
            )
            _attach_member(db, variant_id="small", recipe_id="r3")
        finally:
            db.close()

        n = export(
            db_path, out_path, min_recipes=100, sources_dir=sources_dir
        )
        assert n == 1
        assert (sources_dir / "big.json").exists()
        assert not (sources_dir / "small.json").exists()

        payload = json.loads((sources_dir / "big.json").read_text())
        assert payload["variant_id"] == "big"
        assert len(payload["source_recipes"]) == 2
        # Best outlier score (0.1) sorts first, so r1 → #1, r2 → #2.
        first, second = payload["source_recipes"]
        assert {ing["name"] for ing in first["ingredients"]} == {"flour", "water"}
        assert {ing["name"] for ing in second["ingredients"]} == {"flour"}

    def test_omits_sidecars_for_dropped_variants(self, tmp_path: Path) -> None:
        db_path = tmp_path / "recipes.db"
        out_path = tmp_path / "catalog.json"
        sources_dir = tmp_path / "sources"
        db = CatalogDB.open(db_path)
        try:
            _seed_variant(
                db,
                variant_id="kept",
                normalized_title="kept",
                display_title="Kept",
                category="bread",
                n_recipes=200,
            )
            _seed_variant(
                db,
                variant_id="dropped",
                normalized_title="dropped",
                display_title="Dropped",
                category="bread",
                n_recipes=300,
                review_status="drop",
            )
            _seed_recipe_with_parsed(
                db, recipe_id="r1", parsed=[("flour", 100.0, "g")]
            )
            _attach_member(db, variant_id="kept", recipe_id="r1")
            _seed_recipe_with_parsed(
                db, recipe_id="r2", parsed=[("flour", 200.0, "g")]
            )
            _attach_member(db, variant_id="dropped", recipe_id="r2")
        finally:
            db.close()

        export(db_path, out_path, min_recipes=100, sources_dir=sources_dir)
        assert (sources_dir / "kept.json").exists()
        assert not (sources_dir / "dropped.json").exists()

    def test_sidecar_includes_quantity_and_unit_when_present(
        self, tmp_path: Path
    ) -> None:
        db_path = tmp_path / "recipes.db"
        out_path = tmp_path / "catalog.json"
        sources_dir = tmp_path / "sources"
        db = CatalogDB.open(db_path)
        try:
            _seed_variant(
                db,
                variant_id="vid",
                normalized_title="bread",
                display_title="Bread",
                category="bread",
                n_recipes=200,
            )
            _seed_recipe_with_parsed(
                db,
                recipe_id="r1",
                parsed=[
                    ("flour", 500.0, "g"),
                    ("salt", None, None),  # quantity-less ingredient
                ],
            )
            _attach_member(db, variant_id="vid", recipe_id="r1")
        finally:
            db.close()

        export(db_path, out_path, min_recipes=100, sources_dir=sources_dir)

        payload = json.loads((sources_dir / "vid.json").read_text())
        ings = {i["name"]: i for i in payload["source_recipes"][0]["ingredients"]}
        assert ings["flour"]["quantity"] == 500.0
        assert ings["flour"]["unit"] == "g"
        assert "quantity" not in ings["salt"]
        assert "unit" not in ings["salt"]

    def test_sidecar_skips_grams(self, tmp_path: Path) -> None:
        # The bead is explicit: skip grams (the parsed grams column is
        # mostly NULL — quantity + unit only).
        db_path = tmp_path / "recipes.db"
        out_path = tmp_path / "catalog.json"
        sources_dir = tmp_path / "sources"
        db = CatalogDB.open(db_path)
        try:
            _seed_variant(
                db,
                variant_id="vid",
                normalized_title="bread",
                display_title="Bread",
                category="bread",
                n_recipes=200,
            )
            conn = db.connection
            with conn:
                conn.execute(
                    "INSERT INTO recipes (recipe_id, corpus) VALUES (?, ?)",
                    ("r1", "recipenlg"),
                )
                conn.execute(
                    """
                    INSERT INTO parsed_ingredients (
                      recipe_id, canonical_name, quantity, unit, grams
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    ("r1", "flour", 500.0, "g", 500.0),
                )
            _attach_member(db, variant_id="vid", recipe_id="r1")
        finally:
            db.close()

        export(db_path, out_path, min_recipes=100, sources_dir=sources_dir)

        payload = json.loads((sources_dir / "vid.json").read_text())
        ing = payload["source_recipes"][0]["ingredients"][0]
        assert "grams" not in ing
        assert set(ing.keys()) <= {"name", "quantity", "unit"}

    def test_sidecar_handles_member_without_parsed_rows(
        self, tmp_path: Path
    ) -> None:
        # If parsed_ingredients has no rows for a member, the source still
        # appears in the list (with empty ingredients) so #N indices stay
        # stable across the source list.
        db_path = tmp_path / "recipes.db"
        out_path = tmp_path / "catalog.json"
        sources_dir = tmp_path / "sources"
        db = CatalogDB.open(db_path)
        try:
            _seed_variant(
                db,
                variant_id="vid",
                normalized_title="bread",
                display_title="Bread",
                category="bread",
                n_recipes=200,
            )
            conn = db.connection
            with conn:
                conn.execute(
                    "INSERT INTO recipes (recipe_id, corpus) VALUES (?, ?)",
                    ("r-empty", "recipenlg"),
                )
            _attach_member(
                db, variant_id="vid", recipe_id="r-empty", outlier_score=0.1
            )
            _seed_recipe_with_parsed(
                db, recipe_id="r-full", parsed=[("flour", 500.0, "g")]
            )
            _attach_member(
                db, variant_id="vid", recipe_id="r-full", outlier_score=0.2
            )
        finally:
            db.close()

        export(db_path, out_path, min_recipes=100, sources_dir=sources_dir)

        payload = json.loads((sources_dir / "vid.json").read_text())
        assert len(payload["source_recipes"]) == 2
        assert payload["source_recipes"][0]["ingredients"] == []
        assert payload["source_recipes"][1]["ingredients"] == [
            {"name": "flour", "quantity": 500.0, "unit": "g"},
        ]

    def test_sidecar_omitted_when_flag_not_passed(self, tmp_path: Path) -> None:
        db_path = tmp_path / "recipes.db"
        out_path = tmp_path / "catalog.json"
        sources_dir = tmp_path / "sources"
        db = CatalogDB.open(db_path)
        try:
            _seed_variant(
                db,
                variant_id="vid",
                normalized_title="bread",
                display_title="Bread",
                category="bread",
                n_recipes=200,
            )
        finally:
            db.close()

        export(db_path, out_path, min_recipes=100)
        # No sources_dir passed → directory should not be created.
        assert not sources_dir.exists()

    def test_cli_writes_sources_dir(self, tmp_path: Path) -> None:
        db_path = tmp_path / "recipes.db"
        out_path = tmp_path / "catalog.json"
        sources_dir = tmp_path / "sources"
        db = CatalogDB.open(db_path)
        try:
            _seed_variant(
                db,
                variant_id="vid",
                normalized_title="bread",
                display_title="Bread",
                category="bread",
                n_recipes=200,
            )
            _seed_recipe_with_parsed(
                db, recipe_id="r1", parsed=[("flour", 500.0, "g")]
            )
            _attach_member(db, variant_id="vid", recipe_id="r1")
        finally:
            db.close()

        rc = main([
            "--db", str(db_path),
            "--output", str(out_path),
            "--min-recipes", "100",
            "--sources-dir", str(sources_dir),
        ])
        assert rc == 0
        assert (sources_dir / "vid.json").exists()
