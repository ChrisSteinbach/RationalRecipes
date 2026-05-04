// App shell: load the catalog, route between catalog list and detail.
//
// Route state lives in the URL hash so refreshing preserves the view
// and back/forward navigation works. Hash format:
//   #/              → catalog
//   #/recipe/<id>   → detail
//
// Since RationalRecipes-y43 the catalog ships as a static JSON manifest
// (catalog.json). The PWA fetches it once on boot and filters in
// memory via `inMemoryFilter` from app_routing.ts.

import "./styles.css";
import {
  type Route,
  findRecipe,
  inMemoryFilter,
  parseRoute,
  routeToHash,
} from "./app_routing.ts";
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

function filteredRecipes(state: AppState): CuratedRecipe[] {
  return inMemoryFilter(state.catalog, state.catalogView);
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
    state.route = { kind: "catalog" };
  }
  renderCatalog(container, state.catalog, filteredRecipes(state), state.catalogView, {
    onQueryChange(q) {
      state.catalogView.query = q;
      render(container, state);
    },
    onCategoryChange(c) {
      state.catalogView.category = c;
      render(container, state);
    },
    onMinSampleSizeChange(n) {
      state.catalogView.minSampleSize = n;
      render(container, state);
    },
    onOrderByChange(o) {
      state.catalogView.orderBy = o;
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
