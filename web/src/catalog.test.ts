import initSqlJs, { type Database } from "sql.js";
import { beforeAll, describe, expect, it } from "vitest";
import {
  type Catalog,
  type CuratedRecipe,
  categoriesOf,
  filterRecipes,
  toRatio,
  validateCatalog,
} from "./catalog.ts";
import { CatalogRepo } from "./catalog_repo.ts";

let SQL: Awaited<ReturnType<typeof initSqlJs>>;

beforeAll(async () => {
  SQL = await initSqlJs();
});

const SCHEMA = `
CREATE TABLE recipes (
  recipe_id TEXT PRIMARY KEY, url TEXT, title TEXT,
  corpus TEXT NOT NULL, language TEXT,
  source_type TEXT DEFAULT 'url', cooking_method TEXT,
  cook_time_min INTEGER, total_time_min INTEGER, extracted_at TEXT
);
CREATE TABLE variants (
  variant_id TEXT PRIMARY KEY, normalized_title TEXT NOT NULL,
  display_title TEXT, category TEXT, description TEXT,
  base_ingredient TEXT, cooking_methods TEXT,
  canonical_ingredient_set TEXT NOT NULL, n_recipes INTEGER NOT NULL,
  confidence_level REAL, review_status TEXT, review_note TEXT,
  reviewed_at TEXT
);
CREATE TABLE variant_members (
  variant_id TEXT NOT NULL, recipe_id TEXT NOT NULL,
  outlier_score REAL, PRIMARY KEY (variant_id, recipe_id)
);
CREATE TABLE variant_ingredient_stats (
  variant_id TEXT NOT NULL, canonical_name TEXT NOT NULL,
  ordinal INTEGER NOT NULL, mean_proportion REAL NOT NULL,
  stddev REAL, ci_lower REAL, ci_upper REAL, ratio REAL,
  min_sample_size INTEGER NOT NULL, density_g_per_ml REAL,
  whole_unit_name TEXT, whole_unit_grams REAL,
  PRIMARY KEY (variant_id, canonical_name)
);
CREATE TABLE variant_sources (
  variant_id TEXT NOT NULL, ordinal INTEGER NOT NULL,
  source_type TEXT NOT NULL, title TEXT, ref TEXT NOT NULL,
  PRIMARY KEY (variant_id, ordinal)
);
`;

interface SeedIngredient {
  name: string;
  proportion: number;
  ratio: number;
  min_sample_size: number;
  density?: number | null;
  wholeUnit?: { name: string; grams: number } | null;
}

interface SeedVariant {
  id: string;
  normalizedTitle: string;
  displayTitle?: string;
  category: string | null;
  description?: string;
  baseIngredient: string;
  sampleSize: number;
  confidenceLevel?: number;
  reviewStatus?: string;
  ingredients: SeedIngredient[];
  sources?: Array<{ type: "url" | "book" | "text"; title?: string; ref: string }>;
}

// ----- Builders (Object Mother avoidance) -----
//
// Defaults are deliberately uninteresting (id="v", sampleSize=10, single
// flour ingredient). Every test passes the fields that matter for the
// behaviour under test, so the test reads in isolation.

function aSeedVariant(overrides: Partial<SeedVariant> = {}): SeedVariant {
  return {
    id: "v",
    normalizedTitle: "v",
    category: "cat",
    baseIngredient: "flour",
    sampleSize: 10,
    ingredients: [
      { name: "flour", proportion: 0.5, ratio: 1.0, min_sample_size: 5 },
    ],
    ...overrides,
  };
}

function aRecipe(overrides: Partial<CuratedRecipe> = {}): CuratedRecipe {
  return {
    id: "r",
    title: "r",
    category: "cat",
    base_ingredient: "flour",
    sample_size: 10,
    ingredients: [
      {
        name: "flour",
        ratio: 1.0,
        proportion: 0.5,
        std_deviation: 0.05,
        ci_lower: 0.45,
        ci_upper: 0.55,
      },
    ],
    ...overrides,
  };
}

function aCatalog(recipes: CuratedRecipe[]): Catalog {
  return { version: 1, recipes };
}

function seedDb(variants: SeedVariant[]): Database {
  const db = new SQL.Database();
  db.exec(SCHEMA);
  for (const v of variants) {
    const canonicalSet = [...v.ingredients]
      .map((i) => i.name)
      .sort()
      .join(",");
    db.run(
      "INSERT INTO variants (variant_id, normalized_title, display_title," +
        " category, description, base_ingredient, cooking_methods," +
        " canonical_ingredient_set, n_recipes, confidence_level, review_status)" +
        " VALUES (?, ?, ?, ?, ?, ?, '', ?, ?, ?, ?)",
      [
        v.id,
        v.normalizedTitle,
        v.displayTitle ?? v.normalizedTitle,
        v.category,
        v.description ?? null,
        v.baseIngredient,
        canonicalSet,
        v.sampleSize,
        v.confidenceLevel ?? null,
        v.reviewStatus ?? null,
      ],
    );
    v.ingredients.forEach((ing, i) => {
      db.run(
        "INSERT INTO variant_ingredient_stats (variant_id, canonical_name," +
          " ordinal, mean_proportion, stddev, ci_lower, ci_upper, ratio," +
          " min_sample_size, density_g_per_ml, whole_unit_name, whole_unit_grams)" +
          " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
          v.id,
          ing.name,
          i,
          ing.proportion,
          0.05,
          Math.max(0, ing.proportion - 0.01),
          ing.proportion + 0.01,
          ing.ratio,
          ing.min_sample_size,
          ing.density ?? null,
          ing.wholeUnit?.name ?? null,
          ing.wholeUnit?.grams ?? null,
        ],
      );
    });
    (v.sources ?? []).forEach((s, i) => {
      db.run(
        "INSERT INTO variant_sources (variant_id, ordinal, source_type, title, ref)" +
          " VALUES (?, ?, ?, ?, ?)",
        [v.id, i, s.type, s.title ?? null, s.ref],
      );
    });
  }
  return db;
}

describe("validateCatalog", () => {
  it("accepts a well-formed catalog", () => {
    const c = aCatalog([aRecipe()]);
    expect(validateCatalog(c)).toBe(c);
  });

  it("rejects non-object root", () => {
    expect(() => validateCatalog(null)).toThrow(/object/);
    expect(() => validateCatalog([])).toThrow(/Unsupported catalog version/);
  });

  it("rejects unsupported versions", () => {
    expect(() => validateCatalog({ version: 2, recipes: [] })).toThrow(
      /Unsupported catalog version/,
    );
  });
});

describe("categoriesOf", () => {
  it("returns unique categories in first-seen order", () => {
    const catalog = aCatalog([
      aRecipe({ id: "a", category: "crepes" }),
      aRecipe({ id: "b", category: "crepes" }),
      aRecipe({ id: "c", category: "bread" }),
    ]);
    expect(categoriesOf(catalog)).toEqual(["crepes", "bread"]);
  });
});

describe("filterRecipes", () => {
  it("returns all recipes with empty query + all category", () => {
    const catalog = aCatalog([
      aRecipe({ id: "a" }),
      aRecipe({ id: "b" }),
      aRecipe({ id: "c" }),
    ]);
    expect(filterRecipes(catalog, "", "all")).toHaveLength(3);
  });

  it("filters by title substring case-insensitively", () => {
    const catalog = aCatalog([
      aRecipe({ id: "swedish", title: "Swedish Pancakes (Pannkakor)" }),
      aRecipe({ id: "other", title: "Other" }),
    ]);
    const result = filterRecipes(catalog, "pannkakor", "all");
    expect(result.map((r) => r.id)).toEqual(["swedish"]);
  });

  it("combines query and category", () => {
    const catalog = aCatalog([
      aRecipe({ id: "french-crepes", title: "French Crêpes", category: "crepes" }),
      aRecipe({ id: "french-bread", title: "French Bread", category: "bread" }),
    ]);
    const result = filterRecipes(catalog, "french", "crepes");
    expect(result.map((r) => r.id)).toEqual(["french-crepes"]);
  });
});

describe("toRatio", () => {
  it("produces a Ratio with baker's percentage values", () => {
    const recipe = aRecipe({
      ingredients: [
        {
          name: "flour",
          ratio: 1.0,
          proportion: 0.1673,
          std_deviation: 0.05,
          ci_lower: 0.16,
          ci_upper: 0.17,
          density_g_per_ml: 0.5283,
        },
        {
          name: "milk",
          ratio: 3.6,
          proportion: 0.6019,
          std_deviation: 0.05,
          ci_lower: 0.59,
          ci_upper: 0.61,
          density_g_per_ml: 1.0313,
        },
      ],
    });
    const ratio = toRatio(recipe);
    expect(ratio.values()).toEqual([1.0, 3.6]);
    expect(ratio.ingredients[0].densityGPerMl).toBe(0.5283);
  });
});

describe("CatalogRepo.listVariants", () => {
  it("returns all variants except drop-reviewed by default", () => {
    const repo = new CatalogRepo(
      seedDb([
        aSeedVariant({ id: "kept-1", normalizedTitle: "kept-1", sampleSize: 30 }),
        aSeedVariant({ id: "kept-2", normalizedTitle: "kept-2", sampleSize: 20 }),
        aSeedVariant({
          id: "dropped",
          normalizedTitle: "dropped",
          sampleSize: 100,
          reviewStatus: "drop",
        }),
      ]),
    );
    expect(repo.listVariants().map((v) => v.normalizedTitle)).toEqual([
      "kept-1",
      "kept-2",
    ]);
  });

  it("filters by minSampleSize", () => {
    const repo = new CatalogRepo(
      seedDb([
        aSeedVariant({ id: "big", normalizedTitle: "big", sampleSize: 200 }),
        aSeedVariant({ id: "small", normalizedTitle: "small", sampleSize: 100 }),
      ]),
    );
    expect(
      repo.listVariants({ minSampleSize: 150 }).map((v) => v.normalizedTitle),
    ).toEqual(["big"]);
  });

  it("filters by minSampleSize=10 cutoff", () => {
    const repo = new CatalogRepo(
      seedDb([
        aSeedVariant({ id: "above", normalizedTitle: "above", sampleSize: 30 }),
        aSeedVariant({ id: "tiny", normalizedTitle: "tiny", sampleSize: 5 }),
      ]),
    );
    const titles = repo
      .listVariants({ minSampleSize: 10 })
      .map((v) => v.normalizedTitle);
    expect(titles).toContain("above");
    expect(titles).not.toContain("tiny");
  });

  it("filters by category", () => {
    const repo = new CatalogRepo(
      seedDb([
        aSeedVariant({ id: "boule", normalizedTitle: "sourdough-boule", category: "bread" }),
        aSeedVariant({ id: "crepe", normalizedTitle: "crepe", category: "crepes" }),
      ]),
    );
    expect(
      repo.listVariants({ category: "bread" }).map((v) => v.normalizedTitle),
    ).toEqual(["sourdough-boule"]);
  });

  it("filters by case-insensitive titleSearch", () => {
    const repo = new CatalogRepo(
      seedDb([
        aSeedVariant({ id: "swedish", normalizedTitle: "swedish-pancakes" }),
        aSeedVariant({ id: "boule", normalizedTitle: "sourdough-boule" }),
      ]),
    );
    expect(
      repo.listVariants({ titleSearch: "PANCAKES" }).map((v) => v.normalizedTitle),
    ).toEqual(["swedish-pancakes"]);
  });

  it("combines category and minSampleSize", () => {
    const repo = new CatalogRepo(
      seedDb([
        aSeedVariant({
          id: "big-crepe",
          normalizedTitle: "big-crepe",
          category: "crepes",
          sampleSize: 200,
        }),
        aSeedVariant({
          id: "small-crepe",
          normalizedTitle: "small-crepe",
          category: "crepes",
          sampleSize: 100,
        }),
        aSeedVariant({
          id: "big-bread",
          normalizedTitle: "big-bread",
          category: "bread",
          sampleSize: 200,
        }),
      ]),
    );
    expect(
      repo
        .listVariants({ category: "crepes", minSampleSize: 150 })
        .map((v) => v.normalizedTitle),
    ).toEqual(["big-crepe"]);
  });

  it("composes category + titleSearch + minSampleSize additively", () => {
    const repo = new CatalogRepo(
      seedDb([
        aSeedVariant({
          id: "swedish",
          normalizedTitle: "swedish-pancakes",
          category: "crepes",
          sampleSize: 200,
        }),
        aSeedVariant({
          id: "tiny-pancakes",
          normalizedTitle: "tiny-pancakes",
          category: "crepes",
          sampleSize: 30,
        }),
        aSeedVariant({
          id: "french",
          normalizedTitle: "french-crepes",
          category: "crepes",
          sampleSize: 100,
        }),
      ]),
    );
    expect(
      repo
        .listVariants({
          category: "crepes",
          titleSearch: "pancakes",
          minSampleSize: 50,
        })
        .map((v) => v.normalizedTitle),
    ).toEqual(["swedish-pancakes"]);
  });

  it("includes drop-reviewed when includeDropped=true", () => {
    const repo = new CatalogRepo(
      seedDb([
        aSeedVariant({ id: "kept", normalizedTitle: "kept" }),
        aSeedVariant({
          id: "dropped",
          normalizedTitle: "dropped",
          reviewStatus: "drop",
        }),
      ]),
    );
    expect(
      repo.listVariants({ includeDropped: true }).map((v) => v.normalizedTitle),
    ).toContain("dropped");
  });

  it("orders by sample_size desc by default", () => {
    const repo = new CatalogRepo(
      seedDb([
        aSeedVariant({ id: "small", normalizedTitle: "small", sampleSize: 30 }),
        aSeedVariant({ id: "big", normalizedTitle: "big", sampleSize: 200 }),
        aSeedVariant({ id: "mid", normalizedTitle: "mid", sampleSize: 119 }),
      ]),
    );
    expect(repo.listVariants().map((v) => v.sampleSize)).toEqual([200, 119, 30]);
  });

  it("orders by title alphabetically with orderBy='title'", () => {
    const repo = new CatalogRepo(
      seedDb([
        aSeedVariant({ id: "swedish", normalizedTitle: "swedish-pancakes" }),
        aSeedVariant({ id: "boule", normalizedTitle: "sourdough-boule" }),
        aSeedVariant({ id: "french", normalizedTitle: "french-crepes" }),
      ]),
    );
    expect(
      repo.listVariants({ orderBy: "title" }).map((v) => v.normalizedTitle),
    ).toEqual(["french-crepes", "sourdough-boule", "swedish-pancakes"]);
  });

  it("orders by sample_size desc with explicit orderBy", () => {
    const repo = new CatalogRepo(
      seedDb([
        aSeedVariant({ id: "a", normalizedTitle: "a", sampleSize: 200 }),
        aSeedVariant({ id: "b", normalizedTitle: "b", sampleSize: 119 }),
        aSeedVariant({ id: "c", normalizedTitle: "c", sampleSize: 30 }),
      ]),
    );
    expect(
      repo.listVariants({ orderBy: "sample_size" }).map((v) => v.sampleSize),
    ).toEqual([200, 119, 30]);
  });
});

describe("CatalogRepo.listRecipes", () => {
  it("returns hydrated CuratedRecipes matching filters", () => {
    const repo = new CatalogRepo(
      seedDb([
        aSeedVariant({
          id: "swedish-vid",
          normalizedTitle: "swedish-pancakes",
          sampleSize: 200,
          ingredients: [
            { name: "flour", proportion: 0.17, ratio: 1.0, min_sample_size: 116 },
            { name: "milk", proportion: 0.6, ratio: 3.6, min_sample_size: 36 },
          ],
        }),
        aSeedVariant({
          id: "french-vid",
          normalizedTitle: "french-crepes",
          sampleSize: 119,
        }),
        aSeedVariant({ id: "tiny", normalizedTitle: "tiny", sampleSize: 30 }),
      ]),
    );
    const recipes = repo.listRecipes({ minSampleSize: 100 });
    expect(recipes.map((r) => r.id)).toEqual(["swedish-pancakes", "french-crepes"]);
    expect(recipes[0].ingredients).toHaveLength(2);
    expect(recipes[0].ingredients[0].name).toBe("flour");
  });

  it("honors orderBy='title'", () => {
    const repo = new CatalogRepo(
      seedDb([
        aSeedVariant({ id: "a", normalizedTitle: "swedish-pancakes" }),
        aSeedVariant({ id: "b", normalizedTitle: "sourdough-boule" }),
        aSeedVariant({ id: "c", normalizedTitle: "french-crepes" }),
      ]),
    );
    const recipes = repo.listRecipes({ orderBy: "title" });
    expect(recipes.map((r) => r.id)).toEqual([
      "french-crepes",
      "sourdough-boule",
      "swedish-pancakes",
    ]);
  });
});

describe("CatalogRepo.getVariant", () => {
  it("hydrates a full CuratedRecipe", () => {
    const repo = new CatalogRepo(
      seedDb([
        aSeedVariant({
          id: "swedish-pancakes-vid",
          normalizedTitle: "swedish-pancakes",
          displayTitle: "Swedish Pancakes (Pannkakor)",
          category: "crepes",
          description: "Thin, eggy Scandinavian pancakes.",
          baseIngredient: "flour",
          sampleSize: 200,
          confidenceLevel: 0.95,
          ingredients: [
            {
              name: "flour",
              proportion: 0.1673,
              ratio: 1.0,
              min_sample_size: 116,
              density: 0.5283,
            },
            {
              name: "milk",
              proportion: 0.6019,
              ratio: 3.6,
              min_sample_size: 36,
              density: 1.0313,
            },
          ],
          sources: [
            {
              type: "text",
              title: "Aggregated Swedish recipes",
              ref: "Swedish pannkakor.",
            },
          ],
        }),
      ]),
    );
    const recipe = repo.getVariant("swedish-pancakes-vid");
    expect(recipe).not.toBeNull();
    expect(recipe!.id).toBe("swedish-pancakes");
    expect(recipe!.title).toBe("Swedish Pancakes (Pannkakor)");
    expect(recipe!.category).toBe("crepes");
    expect(recipe!.description).toBe("Thin, eggy Scandinavian pancakes.");
    expect(recipe!.base_ingredient).toBe("flour");
    expect(recipe!.sample_size).toBe(200);
    expect(recipe!.confidence_level).toBe(0.95);
    expect(recipe!.ingredients).toHaveLength(2);
    expect(recipe!.ingredients[0].name).toBe("flour");
    expect(recipe!.ingredients[0].ratio).toBe(1.0);
    expect(recipe!.ingredients[0].density_g_per_ml).toBe(0.5283);
    expect(recipe!.sources).toHaveLength(1);
    expect(recipe!.sources![0].type).toBe("text");
  });

  it("returns null for missing id", () => {
    const repo = new CatalogRepo(seedDb([aSeedVariant()]));
    expect(repo.getVariant("xxx")).toBeNull();
  });
});

describe("CatalogRepo.toCatalog", () => {
  it("hydrates a full catalog consumable by filterRecipes/categoriesOf", () => {
    const repo = new CatalogRepo(
      seedDb([
        aSeedVariant({
          id: "swedish",
          normalizedTitle: "swedish-pancakes",
          displayTitle: "Swedish Pancakes (Pannkakor)",
          category: "crepes",
          sampleSize: 200,
        }),
        aSeedVariant({
          id: "french",
          normalizedTitle: "french-crepes",
          category: "crepes",
          sampleSize: 100,
        }),
        aSeedVariant({
          id: "boule",
          normalizedTitle: "sourdough-boule",
          category: "bread",
          sampleSize: 30,
        }),
      ]),
    );
    const catalog = repo.toCatalog();
    expect(catalog.version).toBe(1);
    expect(catalog.recipes.map((r) => r.id)).toEqual([
      "swedish-pancakes",
      "french-crepes",
      "sourdough-boule",
    ]);
    expect(categoriesOf(catalog)).toEqual(["crepes", "bread"]);
    expect(
      filterRecipes(catalog, "pannkakor", "all").map((r) => r.id),
    ).toEqual(["swedish-pancakes"]);
  });
});
