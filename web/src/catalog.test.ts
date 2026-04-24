import { describe, expect, it } from "vitest";
import {
  type Catalog,
  categoriesOf,
  filterRecipes,
  toRatio,
  validateCatalog,
} from "./catalog.ts";

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
            std_deviation: 0.0459,
            ci_lower: 0.1609,
            ci_upper: 0.1737,
            density_g_per_ml: 0.5283,
            whole_unit: null,
          },
          {
            name: "milk",
            ratio: 3.6,
            proportion: 0.6019,
            std_deviation: 0.0918,
            ci_lower: 0.5891,
            ci_upper: 0.6146,
            density_g_per_ml: 1.0313,
            whole_unit: null,
          },
        ],
        sources: [{ type: "text", ref: "Aggregated Swedish recipes" }],
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
    // Arrays hit the version check before the array check (typeof []==="object").
    expect(() => validateCatalog([])).toThrow(/Unsupported catalog version/);
  });

  it("rejects unsupported versions", () => {
    expect(() => validateCatalog({ version: 2, recipes: [] })).toThrow(
      /Unsupported catalog version/,
    );
  });

  it("rejects non-array recipes", () => {
    expect(() => validateCatalog({ version: 1, recipes: {} })).toThrow(
      /recipes must be an array/,
    );
  });
});

describe("categoriesOf", () => {
  it("returns unique categories in first-seen order", () => {
    expect(categoriesOf(sampleCatalog())).toEqual(["crepes", "bread"]);
  });

  it("returns empty array for empty catalog", () => {
    expect(categoriesOf({ version: 1, recipes: [] })).toEqual([]);
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

  it("also matches description", () => {
    expect(filterRecipes(catalog, "scandinavian", "all")).toHaveLength(1);
  });

  it("also matches base_ingredient", () => {
    // All three recipes have base_ingredient=flour, so this returns all.
    expect(filterRecipes(catalog, "flour", "all")).toHaveLength(3);
  });

  it("filters by category", () => {
    expect(filterRecipes(catalog, "", "bread")).toHaveLength(1);
    expect(filterRecipes(catalog, "", "crepes")).toHaveLength(2);
  });

  it("combines query and category", () => {
    // 'crêpes' only in french-crepes description and french-crepes title,
    // but accented 'ê' plus description contains "thin" not "crepes"…
    // Actually french-crepes title is "French Crêpes" which lowercases to
    // "french crêpes". Searching for "french" plus category=crepes.
    const result = filterRecipes(catalog, "french", "crepes");
    expect(result.map((r) => r.id)).toEqual(["french-crepes"]);
  });

  it("returns empty when nothing matches", () => {
    expect(filterRecipes(catalog, "xxxnonexistent", "all")).toEqual([]);
  });

  it("trims whitespace in query", () => {
    expect(filterRecipes(catalog, "  pannkakor  ", "all")).toHaveLength(1);
  });
});

describe("toRatio", () => {
  it("produces a Ratio with baker's percentage values and ingredient metadata", () => {
    const recipe = sampleCatalog().recipes[0];
    const ratio = toRatio(recipe);
    expect(ratio.length).toBe(2);
    expect(ratio.values()).toEqual([1.0, 3.6]);
    expect(ratio.ingredients[0].name).toBe("flour");
    expect(ratio.ingredients[0].densityGPerMl).toBe(0.5283);
    expect(ratio.ingredients[0].wholeUnit).toBe(null);
    expect(ratio.ingredients[1].name).toBe("milk");
    expect(ratio.ingredients[1].densityGPerMl).toBe(1.0313);
  });

  it("treats missing density as null", () => {
    const recipe = sampleCatalog().recipes[1]; // french-crepes: no density
    const ratio = toRatio(recipe);
    expect(ratio.ingredients[0].densityGPerMl).toBe(null);
  });
});
