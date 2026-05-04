import { describe, expect, it } from "vitest";
import {
  type Route,
  detectAdminMode,
  findRecipe,
  inMemoryFilter,
  parseRoute,
  routeToHash,
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

  it("parses #/admin into the admin route", () => {
    expect(parseRoute("#/admin")).toEqual({ kind: "admin" });
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

  it("renders admin as #/admin", () => {
    expect(routeToHash({ kind: "admin" })).toBe("#/admin");
  });

  it("round-trips with parseRoute for catalog", () => {
    const route: Route = { kind: "catalog" };
    expect(parseRoute(routeToHash(route))).toEqual(route);
  });

  it("round-trips with parseRoute for detail (incl. unsafe chars)", () => {
    const route: Route = { kind: "detail", recipeId: "Pâte à crêpes" };
    expect(parseRoute(routeToHash(route))).toEqual(route);
  });

  it("round-trips with parseRoute for admin", () => {
    const route: Route = { kind: "admin" };
    expect(parseRoute(routeToHash(route))).toEqual(route);
  });
});

describe("detectAdminMode", () => {
  function memoryStorage(): Storage {
    const map = new Map<string, string>();
    return {
      getItem: (k) => map.get(k) ?? null,
      setItem: (k, v) => {
        map.set(k, v);
      },
      removeItem: (k) => {
        map.delete(k);
      },
      clear: () => map.clear(),
      key: (i) => Array.from(map.keys())[i] ?? null,
      get length() {
        return map.size;
      },
    };
  }

  it("returns false with no param and empty storage", () => {
    expect(detectAdminMode("", memoryStorage())).toBe(false);
  });

  it("flips on with ?admin=1 and persists in storage", () => {
    const storage = memoryStorage();
    expect(detectAdminMode("?admin=1", storage)).toBe(true);
    expect(storage.getItem("rr-admin-mode")).toBe("1");
  });

  it("stays on for subsequent calls with no param once stored", () => {
    const storage = memoryStorage();
    detectAdminMode("?admin=1", storage);
    expect(detectAdminMode("", storage)).toBe(true);
  });

  it("ignores admin=0 (only =1 unlocks)", () => {
    expect(detectAdminMode("?admin=0", memoryStorage())).toBe(false);
  });

  it("ignores other params", () => {
    expect(detectAdminMode("?foo=bar", memoryStorage())).toBe(false);
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
