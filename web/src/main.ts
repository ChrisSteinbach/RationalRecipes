// App shell: load the catalog, route between catalog list and detail.
//
// Route state lives in the URL hash so refreshing preserves the view
// and back/forward navigation works. Hash format:
//   #/              → catalog
//   #/recipe/<id>   → detail

import "./styles.css";
import { type Catalog, type CuratedRecipe, loadCatalog } from "./catalog.ts";
import {
  type CatalogViewState,
  initialCatalogState,
  renderCatalog,
} from "./catalog_view.ts";
import {
  type DetailViewState,
  initialDetailState,
  renderDetail,
} from "./detail_view.ts";
import { registerServiceWorker } from "./sw-register.ts";

interface AppState {
  catalog: Catalog;
  catalogView: CatalogViewState;
  detailView: DetailViewState;
  route: Route;
}

type Route = { kind: "catalog" } | { kind: "detail"; recipeId: string };

function parseRoute(hash: string): Route {
  const m = /^#\/recipe\/([^/]+)\/?$/.exec(hash);
  if (m) return { kind: "detail", recipeId: decodeURIComponent(m[1]) };
  return { kind: "catalog" };
}

function routeToHash(route: Route): string {
  if (route.kind === "detail") return `#/recipe/${encodeURIComponent(route.recipeId)}`;
  return "#/";
}

function findRecipe(catalog: Catalog, id: string): CuratedRecipe | null {
  return catalog.recipes.find((r) => r.id === id) ?? null;
}

function render(container: HTMLElement, state: AppState): void {
  if (state.route.kind === "detail") {
    const recipe = findRecipe(state.catalog, state.route.recipeId);
    if (recipe) {
      renderDetail(container, recipe, state.detailView, {
        onBack: () => navigate({ kind: "catalog" }),
      });
      return;
    }
    // Unknown id → fall back to catalog.
    state.route = { kind: "catalog" };
  }
  renderCatalog(container, state.catalog, state.catalogView, {
    onQueryChange(q) {
      state.catalogView.query = q;
      render(container, state);
    },
    onCategoryChange(c) {
      state.catalogView.category = c;
      render(container, state);
    },
    onRecipeSelect(id) {
      state.detailView = initialDetailState();
      navigate({ kind: "detail", recipeId: id });
    },
  });
}

let rootState: AppState | null = null;
let rootContainer: HTMLElement | null = null;

function navigate(route: Route): void {
  if (!rootState || !rootContainer) return;
  rootState.route = route;
  const nextHash = routeToHash(route);
  if (location.hash !== nextHash) {
    location.hash = nextHash;
    // hashchange handler will call render; avoid a double-render.
    return;
  }
  render(rootContainer, rootState);
}

async function main(): Promise<void> {
  const app = document.querySelector<HTMLDivElement>("#app");
  if (!app) return;
  app.innerHTML = `<p class="app-loading">Loading catalog…</p>`;

  let catalog: Catalog;
  try {
    catalog = await loadCatalog();
  } catch (err) {
    app.innerHTML = `<p class="app-error">Failed to load catalog: ${(err as Error).message}</p>`;
    throw err;
  }

  rootState = {
    catalog,
    catalogView: initialCatalogState(),
    detailView: initialDetailState(),
    route: parseRoute(location.hash),
  };
  rootContainer = app;

  window.addEventListener("hashchange", () => {
    if (!rootState || !rootContainer) return;
    rootState.route = parseRoute(location.hash);
    render(rootContainer, rootState);
  });

  render(app, rootState);
  registerServiceWorker();
}

void main();
