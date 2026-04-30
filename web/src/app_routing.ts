// Pure helpers extracted from main.ts so they can be tested without
// triggering the app shell's module-scope `void main()` side effect.
//
// Hash-route parsing, in-memory catalog filtering, and view-state →
// SQL-filter translation all live here. main.ts wires them up to the
// DOM; this module stays framework-free.

import type { Catalog, CuratedRecipe } from "./catalog.ts";
import type { ListFilters } from "./catalog_repo.ts";
import type { CatalogViewState } from "./catalog_view.ts";

export type Route = { kind: "catalog" } | { kind: "detail"; recipeId: string };

/** Parse a `location.hash` string into a Route. Unknown shapes → catalog. */
export function parseRoute(hash: string): Route {
  const m = /^#\/recipe\/([^/]+)\/?$/.exec(hash);
  if (m) return { kind: "detail", recipeId: decodeURIComponent(m[1]) };
  return { kind: "catalog" };
}

/** Inverse of parseRoute. parseRoute(routeToHash(r)) === r for all r. */
export function routeToHash(route: Route): string {
  if (route.kind === "detail") {
    return `#/recipe/${encodeURIComponent(route.recipeId)}`;
  }
  return "#/";
}

/** Look up a recipe by id; null if not present. */
export function findRecipe(catalog: Catalog, id: string): CuratedRecipe | null {
  return catalog.recipes.find((r) => r.id === id) ?? null;
}

/** Translate the toolbar's view state into CatalogRepo.listRecipes filters. */
export function viewStateToFilters(state: CatalogViewState): ListFilters {
  const filters: ListFilters = { orderBy: state.orderBy };
  if (state.minSampleSize > 0) filters.minSampleSize = state.minSampleSize;
  if (state.category !== "all") filters.category = state.category;
  const q = state.query.trim();
  if (q) filters.titleSearch = q;
  return filters;
}

/**
 * JSON fallback path (?source=json) — keeps the in-browser filter alive
 * for dev without a recipes.db. Title-only LIKE semantics intentionally
 * mirror the SQL path so behavior stays consistent.
 */
export function inMemoryFilter(
  catalog: Catalog,
  view: CatalogViewState,
): CuratedRecipe[] {
  const q = view.query.trim().toLowerCase();
  const filtered = catalog.recipes.filter((r) => {
    if (view.category !== "all" && r.category !== view.category) return false;
    if (view.minSampleSize > 0 && r.sample_size < view.minSampleSize) return false;
    if (q && !r.title.toLowerCase().includes(q)) return false;
    return true;
  });
  if (view.orderBy === "title") {
    filtered.sort((a, b) => a.title.localeCompare(b.title));
  } else {
    filtered.sort(
      (a, b) => b.sample_size - a.sample_size || a.title.localeCompare(b.title),
    );
  }
  return filtered;
}
