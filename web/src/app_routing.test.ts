import { describe, expect, it } from "vitest";
import {
  type Route,
  findRecipe,
  inMemoryFilter,
  parseRoute,
  routeToHash,
  viewStateToFilters,
} from "./app_routing.ts";
import type { Catalog, CuratedRecipe } from "./catalog.ts";
import type { CatalogViewState } from "./catalog_view.ts";

function aRecipe(overrides: Partial<CuratedRecipe> = {}): CuratedRecipe {
  return {
    id: "swedish-pancakes",
    title: "Swedish Pancakes",
    category: "crepes",
    base_ingredient: "flour",
    sample_size: 200,
    ingredients: [
      {
        name: "flour",
        ratio: 1.0,
        proportion: 0.2,
        std_deviation: 0.01,
        ci_lower: 0.19,
        ci_upper: 0.21,
      },
    ],
    ...overrides,
  };
}

function aCatalog(recipes: CuratedRecipe[]): Catalog {
  return { version: 1, recipes };
}

function aViewState(overrides: Partial<CatalogViewState> = {}): CatalogViewState {
  return {
    query: "",
    category: "all",
    minSampleSize: 0,
    orderBy: "sample_size",
    ...overrides,
  };
}

describe("parseRoute", () => {
  it("returns catalog for empty hash", () => {
    expect(parseRoute("")).toEqual({ kind: "catalog" });
  });

  it("returns catalog for #/", () => {
    expect(parseRoute("#/")).toEqual({ kind: "catalog" });
  });

  it("parses #/recipe/<id> into a detail route", () => {
    expect(parseRoute("#/recipe/swedish-pancakes")).toEqual({
      kind: "detail",
      recipeId: "swedish-pancakes",
    });
  });

  it("decodes URL-encoded recipe ids", () => {
    expect(parseRoute("#/recipe/swedish%20pancakes")).toEqual({
      kind: "detail",
      recipeId: "swedish pancakes",
    });
  });

  it("falls back to catalog for unknown hash shapes", () => {
    expect(parseRoute("#/garbage/stuff")).toEqual({ kind: "catalog" });
  });
});

describe("routeToHash", () => {
  it("renders catalog as #/", () => {
    expect(routeToHash({ kind: "catalog" })).toBe("#/");
  });

  it("renders detail with encoded id", () => {
    expect(routeToHash({ kind: "detail", recipeId: "swedish pancakes" })).toBe(
      "#/recipe/swedish%20pancakes",
    );
  });

  it("round-trips with parseRoute for catalog", () => {
    const route: Route = { kind: "catalog" };
    expect(parseRoute(routeToHash(route))).toEqual(route);
  });

  it("round-trips with parseRoute for detail (incl. unsafe chars)", () => {
    const route: Route = { kind: "detail", recipeId: "Pâte à crêpes" };
    expect(parseRoute(routeToHash(route))).toEqual(route);
  });
});

describe("findRecipe", () => {
  it("returns the matching recipe", () => {
    const r = aRecipe();
    expect(findRecipe(aCatalog([r]), "swedish-pancakes")).toBe(r);
  });

  it("returns null when no match", () => {
    expect(findRecipe(aCatalog([aRecipe()]), "missing")).toBeNull();
  });
});

describe("viewStateToFilters", () => {
  it("default state passes only orderBy", () => {
    expect(viewStateToFilters(aViewState())).toEqual({ orderBy: "sample_size" });
  });

  it("includes minSampleSize when > 0", () => {
    expect(viewStateToFilters(aViewState({ minSampleSize: 10 }))).toEqual({
      orderBy: "sample_size",
      minSampleSize: 10,
    });
  });

  it("omits minSampleSize when 0", () => {
    expect(viewStateToFilters(aViewState({ minSampleSize: 0 }))).not.toHaveProperty(
      "minSampleSize",
    );
  });

  it("includes category unless 'all'", () => {
    expect(viewStateToFilters(aViewState({ category: "bread" }))).toMatchObject({
      category: "bread",
    });
    expect(viewStateToFilters(aViewState({ category: "all" }))).not.toHaveProperty(
      "category",
    );
  });

  it("trims and forwards non-empty query as titleSearch", () => {
    expect(viewStateToFilters(aViewState({ query: "  pancakes  " }))).toMatchObject({
      titleSearch: "pancakes",
    });
  });

  it("omits titleSearch when query is whitespace-only", () => {
    expect(viewStateToFilters(aViewState({ query: "   " }))).not.toHaveProperty(
      "titleSearch",
    );
  });

  it("forwards orderBy='title'", () => {
    expect(viewStateToFilters(aViewState({ orderBy: "title" }))).toMatchObject({
      orderBy: "title",
    });
  });
});

describe("inMemoryFilter", () => {
  const swedish = aRecipe({
    id: "swedish-pancakes",
    title: "Swedish Pancakes",
    category: "crepes",
    sample_size: 200,
  });
  const french = aRecipe({
    id: "french-crepes",
    title: "French Crêpes",
    category: "crepes",
    sample_size: 50,
  });
  const boule = aRecipe({
    id: "sourdough-boule",
    title: "Sourdough Boule",
    category: "bread",
    sample_size: 30,
  });
  const catalog = aCatalog([french, swedish, boule]);

  it("returns all recipes for default state, sorted by sample_size desc", () => {
    const result = inMemoryFilter(catalog, aViewState());
    expect(result.map((r) => r.id)).toEqual([
      "swedish-pancakes",
      "french-crepes",
      "sourdough-boule",
    ]);
  });

  it("filters by category", () => {
    const result = inMemoryFilter(catalog, aViewState({ category: "bread" }));
    expect(result.map((r) => r.id)).toEqual(["sourdough-boule"]);
  });

  it("filters by minSampleSize", () => {
    const result = inMemoryFilter(catalog, aViewState({ minSampleSize: 100 }));
    expect(result.map((r) => r.id)).toEqual(["swedish-pancakes"]);
  });

  it("matches title query case-insensitively", () => {
    const result = inMemoryFilter(catalog, aViewState({ query: "PANCAKES" }));
    expect(result.map((r) => r.id)).toEqual(["swedish-pancakes"]);
  });

  it("title match ignores leading/trailing whitespace", () => {
    const result = inMemoryFilter(catalog, aViewState({ query: "  french  " }));
    expect(result.map((r) => r.id)).toEqual(["french-crepes"]);
  });

  it("returns [] when nothing matches", () => {
    expect(inMemoryFilter(catalog, aViewState({ query: "nothing" }))).toEqual([]);
  });

  it("orderBy='title' sorts alphabetically", () => {
    const result = inMemoryFilter(catalog, aViewState({ orderBy: "title" }));
    expect(result.map((r) => r.title)).toEqual([
      "French Crêpes",
      "Sourdough Boule",
      "Swedish Pancakes",
    ]);
  });

  it("ties on sample_size break alphabetically", () => {
    const tieA = aRecipe({ id: "a", title: "Apple", sample_size: 10 });
    const tieB = aRecipe({ id: "b", title: "Banana", sample_size: 10 });
    const result = inMemoryFilter(aCatalog([tieB, tieA]), aViewState());
    expect(result.map((r) => r.id)).toEqual(["a", "b"]);
  });

  it("does not mutate the source catalog order", () => {
    const recipes = [french, swedish, boule];
    const before = recipes.map((r) => r.id);
    inMemoryFilter(aCatalog(recipes), aViewState());
    expect(recipes.map((r) => r.id)).toEqual(before);
  });
});
