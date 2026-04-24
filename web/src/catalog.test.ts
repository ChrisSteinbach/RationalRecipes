import initSqlJs, { type Database } from "sql.js";
import { beforeAll, describe, expect, it } from "vitest";
import {
  type Catalog,
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

function sampleVariants(): SeedVariant[] {
  return [
    {
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
    },
    {
      id: "french-crepes-vid",
      normalizedTitle: "french-crepes",
      displayTitle: "French Crêpes",
      category: "crepes",
      description: "Classic thin crêpes.",
      baseIngredient: "flour",
      sampleSize: 119,
      ingredients: [
        { name: "flour", proportion: 0.2473, ratio: 1.0, min_sample_size: 113 },
      ],
    },
    {
      id: "boule-vid",
      normalizedTitle: "sourdough-boule",
      displayTitle: "Sourdough Boule",
      category: "bread",
      baseIngredient: "flour",
      sampleSize: 30,
      ingredients: [
        { name: "flour", proportion: 0.6, ratio: 1.0, min_sample_size: 25 },
      ],
    },
    {
      id: "dropped-vid",
      normalizedTitle: "dropped",
      displayTitle: "Dropped",
      category: "bread",
      baseIngredient: "flour",
      sampleSize: 100,
      reviewStatus: "drop",
      ingredients: [
        { name: "flour", proportion: 0.5, ratio: 1.0, min_sample_size: 20 },
      ],
    },
  ];
}

function sampleCatalog(): Catalog {
  return {
    version: 1,
    recipes: [
      {
        id: "swedish-pancakes",
        title: "Swedish Pancakes (Pannkakor)",
        category: "crepes",
        description: "Thin, eggy Scandinavian pancakes.",
        base_ingredient: "flour",
        sample_size: 200,
        confidence_level: 0.95,
        ingredients: [
          {
            name: "flour",
            ratio: 1.0,
            proportion: 0.1673,
            std_deviation: 0.05,
            ci_lower: 0.16,
            ci_upper: 0.17,
            density_g_per_ml: 0.5283,
            whole_unit: null,
          },
          {
            name: "milk",
            ratio: 3.6,
            proportion: 0.6019,
            std_deviation: 0.05,
            ci_lower: 0.59,
            ci_upper: 0.61,
            density_g_per_ml: 1.0313,
            whole_unit: null,
          },
        ],
      },
      {
        id: "french-crepes",
        title: "French Crêpes",
        category: "crepes",
        description: "Classic thin crêpes.",
        base_ingredient: "flour",
        sample_size: 50,
        ingredients: [
          {
            name: "flour",
            ratio: 1.0,
            proportion: 0.2,
            std_deviation: 0.05,
            ci_lower: 0.18,
            ci_upper: 0.22,
          },
        ],
      },
      {
        id: "sourdough-boule",
        title: "Sourdough Boule",
        category: "bread",
        base_ingredient: "flour",
        sample_size: 30,
        ingredients: [
          {
            name: "flour",
            ratio: 1.0,
            proportion: 0.6,
            std_deviation: 0.05,
            ci_lower: 0.55,
            ci_upper: 0.65,
          },
        ],
      },
    ],
  };
}

describe("validateCatalog", () => {
  it("accepts a well-formed catalog", () => {
    const c = sampleCatalog();
    const validated = validateCatalog(c);
    expect(validated).toBe(c);
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
    expect(categoriesOf(sampleCatalog())).toEqual(["crepes", "bread"]);
  });
});

describe("filterRecipes", () => {
  const catalog = sampleCatalog();

  it("returns all recipes with empty query + all category", () => {
    expect(filterRecipes(catalog, "", "all")).toHaveLength(3);
  });

  it("filters by title substring case-insensitively", () => {
    const result = filterRecipes(catalog, "pannkakor", "all");
    expect(result.map((r) => r.id)).toEqual(["swedish-pancakes"]);
  });

  it("combines query and category", () => {
    const result = filterRecipes(catalog, "french", "crepes");
    expect(result.map((r) => r.id)).toEqual(["french-crepes"]);
  });
});

describe("toRatio", () => {
  it("produces a Ratio with baker's percentage values", () => {
    const recipe = sampleCatalog().recipes[0];
    const ratio = toRatio(recipe);
    expect(ratio.values()).toEqual([1.0, 3.6]);
    expect(ratio.ingredients[0].densityGPerMl).toBe(0.5283);
  });
});

describe("CatalogRepo.listVariants", () => {
  it("returns all variants except drop-reviewed by default", () => {
    const repo = new CatalogRepo(seedDb(sampleVariants()));
    const listed = repo.listVariants();
    expect(listed.map((v) => v.normalizedTitle)).toEqual([
      "swedish-pancakes",
      "french-crepes",
      "sourdough-boule",
    ]);
  });

  it("filters by minSampleSize", () => {
    const repo = new CatalogRepo(seedDb(sampleVariants()));
    const listed = repo.listVariants({ minSampleSize: 150 });
    expect(listed.map((v) => v.normalizedTitle)).toEqual([
      "swedish-pancakes",
    ]);
  });

  it("filters by category", () => {
    const repo = new CatalogRepo(seedDb(sampleVariants()));
    const listed = repo.listVariants({ category: "bread" });
    expect(listed.map((v) => v.normalizedTitle)).toEqual(["sourdough-boule"]);
  });

  it("filters by case-insensitive titleSearch", () => {
    const repo = new CatalogRepo(seedDb(sampleVariants()));
    const listed = repo.listVariants({ titleSearch: "PANCAKES" });
    expect(listed.map((v) => v.normalizedTitle)).toEqual(["swedish-pancakes"]);
  });

  it("combines filters", () => {
    const repo = new CatalogRepo(seedDb(sampleVariants()));
    const listed = repo.listVariants({ category: "crepes", minSampleSize: 150 });
    expect(listed.map((v) => v.normalizedTitle)).toEqual(["swedish-pancakes"]);
  });

  it("includes drop-reviewed when includeDropped=true", () => {
    const repo = new CatalogRepo(seedDb(sampleVariants()));
    const listed = repo.listVariants({ includeDropped: true });
    expect(listed.map((v) => v.normalizedTitle)).toContain("dropped");
  });

  it("orders by n_recipes desc by default", () => {
    const repo = new CatalogRepo(seedDb(sampleVariants()));
    const listed = repo.listVariants();
    const sizes = listed.map((v) => v.sampleSize);
    expect(sizes).toEqual([...sizes].sort((a, b) => b - a));
  });
});

describe("CatalogRepo.getVariant", () => {
  it("hydrates a full CuratedRecipe", () => {
    const repo = new CatalogRepo(seedDb(sampleVariants()));
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
    const repo = new CatalogRepo(seedDb(sampleVariants()));
    expect(repo.getVariant("xxx")).toBeNull();
  });
});

describe("CatalogRepo.toCatalog", () => {
  it("hydrates a full catalog consumable by filterRecipes/categoriesOf", () => {
    const repo = new CatalogRepo(seedDb(sampleVariants()));
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
