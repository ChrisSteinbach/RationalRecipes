import { describe, expect, it, vi } from "vitest";
import {
  type Catalog,
  type CuratedRecipe,
  CATALOG_PATH,
  categoriesOf,
  filterRecipes,
  loadCatalog,
  toRatio,
  validateCatalog,
} from "./catalog.ts";

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

  it("rejects non-array recipes", () => {
    expect(() => validateCatalog({ version: 1, recipes: "nope" })).toThrow(
      /must be an array/,
    );
  });
});

describe("loadCatalog", () => {
  it("fetches and validates the manifest at CATALOG_PATH", async () => {
    const fixture: Catalog = {
      version: 1,
      recipes: [
        aRecipe({
          id: "swedish-pancakes",
          title: "Swedish Pancakes",
          category: "crepes",
          sample_size: 200,
        }),
      ],
    };
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify(fixture), {
        status: 200,
        headers: { "content-type": "application/json" },
      }),
    );
    vi.stubGlobal("fetch", fetchMock);
    try {
      const catalog = await loadCatalog();
      expect(fetchMock).toHaveBeenCalledWith(CATALOG_PATH);
      expect(catalog.version).toBe(1);
      expect(catalog.recipes.map((r) => r.id)).toEqual(["swedish-pancakes"]);
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it("matches the v1 export shape produced by export_catalog_json.py", () => {
    // Mirrors a row produced by `python3 scripts/export_catalog_json.py`
    // — exact field set we expect to round-trip through the validator.
    const exported: Catalog = {
      version: 1,
      recipes: [
        {
          id: "pumpkin bread",
          title: "Soda Pumpkin Bread",
          category: "bread",
          base_ingredient: "flour",
          sample_size: 431,
          ingredients: [
            {
              name: "flour",
              ratio: 1.0,
              proportion: 0.252,
              std_deviation: 0.058,
              ci_lower: 0.246,
              ci_upper: 0.258,
              min_sample_size: 425,
              density_g_per_ml: null,
              whole_unit: null,
            },
          ],
        },
      ],
    };
    expect(validateCatalog(exported)).toBe(exported);
    const recipe = exported.recipes[0];
    expect(recipe.ingredients[0].whole_unit).toBeNull();
    expect(recipe.ingredients[0].min_sample_size).toBe(425);
  });

  it("throws when the response is not ok", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response("nope", { status: 404, statusText: "Not Found" }),
    );
    vi.stubGlobal("fetch", fetchMock);
    try {
      await expect(loadCatalog()).rejects.toThrow(/404/);
    } finally {
      vi.unstubAllGlobals();
    }
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
