// @vitest-environment jsdom
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { Catalog, CuratedRecipe } from "./catalog.ts";
import {
  type CatalogViewCallbacks,
  type CatalogViewState,
  initialCatalogState,
  renderCatalog,
} from "./catalog_view.ts";

function aRecipe(overrides: Partial<CuratedRecipe> = {}): CuratedRecipe {
  return {
    id: "swedish-pancakes",
    title: "Swedish Pancakes",
    category: "crepes",
    description: "Thin Scandinavian pancakes.",
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

function aCatalog(recipes: CuratedRecipe[] = [], release = false): Catalog {
  const c: Catalog = { version: 1, recipes };
  if (release) {
    c.metadata = {
      dataset_version: "vwt.5",
      released: "2026-04-25",
      pipeline_revision: "abc123",
      notes: "test build",
    };
  }
  return c;
}

function noopCallbacks(
  overrides: Partial<CatalogViewCallbacks> = {},
): CatalogViewCallbacks {
  return {
    onQueryChange: () => {},
    onCategoryChange: () => {},
    onMinSampleSizeChange: () => {},
    onOrderByChange: () => {},
    onRecipeSelect: () => {},
    ...overrides,
  };
}

let container: HTMLElement;

beforeEach(() => {
  container = document.createElement("div");
  document.body.appendChild(container);
});

describe("initialCatalogState", () => {
  it("starts empty / unfiltered / sorted by sample size", () => {
    expect(initialCatalogState()).toEqual({
      query: "",
      category: "all",
      minSampleSize: 0,
      orderBy: "sample_size",
    });
  });
});

describe("renderCatalog — empty visible list", () => {
  it("shows the empty-state message and no list", () => {
    renderCatalog(container, aCatalog([]), [], initialCatalogState(), noopCallbacks());

    expect(container.querySelector(".catalog-empty")?.textContent).toBe(
      "No recipes match the current filters.",
    );
    expect(container.querySelector(".catalog-list")).toBeNull();
  });

  it("count line reads '0 of N recipes'", () => {
    const catalog = aCatalog([aRecipe(), aRecipe({ id: "b", title: "B" })]);
    renderCatalog(container, catalog, [], initialCatalogState(), noopCallbacks());
    expect(container.querySelector(".catalog-count")?.textContent).toBe(
      "0 of 2 recipes",
    );
  });
});

describe("renderCatalog — populated list", () => {
  it("renders one recipe-card per visible recipe", () => {
    const recipes = [
      aRecipe({ id: "a", title: "Alpha" }),
      aRecipe({ id: "b", title: "Beta", description: undefined }),
    ];
    renderCatalog(container, aCatalog(recipes), recipes, initialCatalogState(), noopCallbacks());

    const cards = container.querySelectorAll(".recipe-card");
    expect(cards).toHaveLength(2);
    expect(cards[0].querySelector(".recipe-card-title")?.textContent).toBe("Alpha");
    expect(cards[1].querySelector(".recipe-card-title")?.textContent).toBe("Beta");
  });

  it("renders title, category, sample-size meta on each card", () => {
    const recipe = aRecipe({ sample_size: 42, category: "crepes" });
    renderCatalog(container, aCatalog([recipe]), [recipe], initialCatalogState(), noopCallbacks());
    expect(container.querySelector(".recipe-card-meta")?.textContent).toBe(
      "crepes · 42 source recipes",
    );
  });

  it("singular 'source recipe' wording when sample_size == 1", () => {
    const recipe = aRecipe({ sample_size: 1 });
    renderCatalog(container, aCatalog([recipe]), [recipe], initialCatalogState(), noopCallbacks());
    expect(container.querySelector(".recipe-card-meta")?.textContent).toContain(
      "1 source recipe",
    );
  });

  it("description is rendered when present, omitted when missing", () => {
    const withDesc = aRecipe({ id: "a", description: "Hello" });
    const noDesc = aRecipe({ id: "b", title: "B", description: undefined });
    renderCatalog(
      container,
      aCatalog([withDesc, noDesc]),
      [withDesc, noDesc],
      initialCatalogState(),
      noopCallbacks(),
    );
    const cards = container.querySelectorAll(".recipe-card");
    expect(cards[0].querySelector(".recipe-card-desc")?.textContent).toBe("Hello");
    expect(cards[1].querySelector(".recipe-card-desc")).toBeNull();
  });

  it("count line reads 'M of N recipes'", () => {
    const all = [aRecipe({ id: "a" }), aRecipe({ id: "b", title: "B" }), aRecipe({ id: "c", title: "C" })];
    renderCatalog(container, aCatalog(all), all.slice(0, 2), initialCatalogState(), noopCallbacks());
    expect(container.querySelector(".catalog-count")?.textContent).toBe(
      "2 of 3 recipes",
    );
  });

  it("is idempotent: re-render replaces previous content", () => {
    const recipes = [aRecipe({ id: "a", title: "Alpha" })];
    renderCatalog(container, aCatalog(recipes), recipes, initialCatalogState(), noopCallbacks());
    renderCatalog(container, aCatalog(recipes), recipes, initialCatalogState(), noopCallbacks());
    expect(container.querySelectorAll(".catalog-list")).toHaveLength(1);
    expect(container.querySelectorAll(".recipe-card")).toHaveLength(1);
  });
});

describe("renderCatalog — toolbar", () => {
  it("seeds search input from state.query", () => {
    const state: CatalogViewState = { ...initialCatalogState(), query: "panna" };
    renderCatalog(container, aCatalog([]), [], state, noopCallbacks());
    const search = container.querySelector<HTMLInputElement>(".catalog-search");
    expect(search?.value).toBe("panna");
  });

  it("category select includes 'All' + each unique category from the catalog", () => {
    const recipes = [
      aRecipe({ id: "a", category: "crepes" }),
      aRecipe({ id: "b", category: "bread" }),
      aRecipe({ id: "c", category: "crepes" }),
    ];
    renderCatalog(container, aCatalog(recipes), recipes, initialCatalogState(), noopCallbacks());
    const opts = container.querySelectorAll<HTMLOptionElement>(".catalog-category option");
    expect(Array.from(opts).map((o) => o.value)).toEqual(["all", "crepes", "bread"]);
  });

  it("min-sample select offers the All / ≥3 / ≥10 / ≥30 tiers", () => {
    renderCatalog(container, aCatalog([]), [], initialCatalogState(), noopCallbacks());
    const opts = container.querySelectorAll<HTMLOptionElement>(".catalog-min-sample option");
    expect(Array.from(opts).map((o) => o.value)).toEqual(["0", "3", "10", "30"]);
  });

  it("sort-by select offers sample_size and title", () => {
    renderCatalog(container, aCatalog([]), [], initialCatalogState(), noopCallbacks());
    const opts = container.querySelectorAll<HTMLOptionElement>(".catalog-order-by option");
    expect(Array.from(opts).map((o) => o.value)).toEqual(["sample_size", "title"]);
  });
});

describe("renderCatalog — callbacks fire", () => {
  it("typing in search → onQueryChange with new value", () => {
    const onQueryChange = vi.fn();
    renderCatalog(container, aCatalog([]), [], initialCatalogState(), noopCallbacks({ onQueryChange }));
    const search = container.querySelector<HTMLInputElement>(".catalog-search")!;
    search.value = "boule";
    search.dispatchEvent(new Event("input"));
    expect(onQueryChange).toHaveBeenCalledWith("boule");
  });

  it("changing category → onCategoryChange with new value", () => {
    const onCategoryChange = vi.fn();
    const recipes = [aRecipe({ category: "crepes" }), aRecipe({ id: "b", category: "bread" })];
    renderCatalog(
      container,
      aCatalog(recipes),
      recipes,
      initialCatalogState(),
      noopCallbacks({ onCategoryChange }),
    );
    const select = container.querySelector<HTMLSelectElement>(".catalog-category")!;
    select.value = "bread";
    select.dispatchEvent(new Event("change"));
    expect(onCategoryChange).toHaveBeenCalledWith("bread");
  });

  it("changing min sample → onMinSampleSizeChange with numeric value", () => {
    const onMinSampleSizeChange = vi.fn();
    renderCatalog(
      container,
      aCatalog([]),
      [],
      initialCatalogState(),
      noopCallbacks({ onMinSampleSizeChange }),
    );
    const select = container.querySelector<HTMLSelectElement>(".catalog-min-sample")!;
    select.value = "10";
    select.dispatchEvent(new Event("change"));
    expect(onMinSampleSizeChange).toHaveBeenCalledWith(10);
  });

  it("changing sort → onOrderByChange with new value", () => {
    const onOrderByChange = vi.fn();
    renderCatalog(container, aCatalog([]), [], initialCatalogState(), noopCallbacks({ onOrderByChange }));
    const select = container.querySelector<HTMLSelectElement>(".catalog-order-by")!;
    select.value = "title";
    select.dispatchEvent(new Event("change"));
    expect(onOrderByChange).toHaveBeenCalledWith("title");
  });

  it("clicking a recipe card → onRecipeSelect with that recipe id", () => {
    const onRecipeSelect = vi.fn();
    const recipes = [aRecipe({ id: "boule", title: "Boule" })];
    renderCatalog(
      container,
      aCatalog(recipes),
      recipes,
      initialCatalogState(),
      noopCallbacks({ onRecipeSelect }),
    );
    container.querySelector<HTMLButtonElement>(".recipe-card-button")!.click();
    expect(onRecipeSelect).toHaveBeenCalledWith("boule");
  });
});

describe("renderCatalog — release badge", () => {
  it("renders nothing when catalog has no metadata", () => {
    renderCatalog(container, aCatalog([]), [], initialCatalogState(), noopCallbacks());
    expect(container.querySelector(".catalog-release")).toBeNull();
  });

  it("renders dataset/released/rev when metadata is present", () => {
    renderCatalog(container, aCatalog([], true), [], initialCatalogState(), noopCallbacks());
    const badge = container.querySelector<HTMLElement>(".catalog-release");
    expect(badge?.textContent).toBe("Dataset vwt.5 · released 2026-04-25 · rev abc123");
    expect(badge?.title).toBe("test build");
  });
});
