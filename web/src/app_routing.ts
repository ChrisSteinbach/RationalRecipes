// Pure helpers extracted from main.ts so they can be tested without
// triggering the app shell's module-scope `void main()` side effect.
//
// Hash-route parsing and in-memory catalog filtering both live here.
// main.ts wires them up to the DOM; this module stays framework-free.

import type { Catalog, CuratedRecipe } from "./catalog.ts";
import type { CatalogViewState } from "./catalog_view.ts";

export type Route =
  | { kind: "catalog" }
  | { kind: "detail"; recipeId: string }
  | { kind: "admin" };

/** Parse a `location.hash` string into a Route. Unknown shapes → catalog. */
export function parseRoute(hash: string): Route {
  const m = /^#\/recipe\/([^/]+)\/?$/.exec(hash);
  if (m) return { kind: "detail", recipeId: decodeURIComponent(m[1]) };
  if (/^#\/admin\/?$/.test(hash)) return { kind: "admin" };
  return { kind: "catalog" };
}

/** Inverse of parseRoute. parseRoute(routeToHash(r)) === r for all r. */
export function routeToHash(route: Route): string {
  if (route.kind === "detail") {
    return `#/recipe/${encodeURIComponent(route.recipeId)}`;
  }
  if (route.kind === "admin") return "#/admin";
  return "#/";
}

/** Look up a recipe by id; null if not present. */
export function findRecipe(catalog: Catalog, id: string): CuratedRecipe | null {
  return catalog.recipes.find((r) => r.id === id) ?? null;
}

const ADMIN_SESSION_KEY = "rr-admin-mode";

/**
 * Decide whether admin mode should be active for this session.
 *
 * `?admin=1` in the URL flips it on and writes a sticky flag into
 * sessionStorage so navigating between catalog / detail / admin
 * doesn't keep dropping the param. Closing the tab clears the flag —
 * intentional weak gating; the bead notes we'll harden it later.
 */
export function detectAdminMode(
  search: string = typeof location !== "undefined" ? location.search : "",
  storage: Storage | null = typeof sessionStorage !== "undefined"
    ? sessionStorage
    : null,
): boolean {
  if (search) {
    const params = new URLSearchParams(
      search.startsWith("?") ? search.slice(1) : search,
    );
    if (params.get("admin") === "1") {
      storage?.setItem(ADMIN_SESSION_KEY, "1");
      return true;
    }
  }
  return storage?.getItem(ADMIN_SESSION_KEY) === "1";
}

/**
 * Filter the in-memory catalog by the toolbar's view state.
 *
 * The only filter path since RationalRecipes-y43 retired sql.js — the
 * 198-variant v1 catalog fits comfortably in memory, so client-side
 * `Array.filter` plus a sort handles search/category/min-sample/order
 * directly.
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
