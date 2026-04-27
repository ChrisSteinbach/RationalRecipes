// App shell: load the catalog, route between catalog list and detail.
//
// Route state lives in the URL hash so refreshing preserves the view
// and back/forward navigation works. Hash format:
//   #/              → catalog
//   #/recipe/<id>   → detail
//
// Since vwt.4 the catalog list queries recipes.db through CatalogRepo
// per render, so the filter UI compiles to SQL WHERE clauses. The full
// in-memory Catalog stays loaded for detail-view lookup and toolbar
// metadata (categories, total count).

import "./styles.css";
import {
  type Route,
  findRecipe,
  inMemoryFilter,
  parseRoute,
  routeToHash,
  viewStateToFilters,
} from "./app_routing.ts";
import { type Catalog, type CuratedRecipe, loadCatalog } from "./catalog.ts";
import { type CatalogRepo, loadCatalogRepo } from "./catalog_repo.ts";
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
  repo: CatalogRepo | null;
  catalogView: CatalogViewState;
  detailView: DetailViewState;
  route: Route;
}

function filteredRecipes(state: AppState): CuratedRecipe[] {
  if (state.repo) return state.repo.listRecipes(viewStateToFilters(state.catalogView));
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

  const useJson = new URLSearchParams(location.search).get("source") === "json";
  let catalog: Catalog;
  let repo: CatalogRepo | null = null;
  try {
    if (useJson) {
      catalog = await loadCatalog();
    } else {
      repo = await loadCatalogRepo();
      catalog = repo.toCatalog();
    }
  } catch (err) {
    app.innerHTML = `<p class="app-error">Failed to load catalog: ${(err as Error).message}</p>`;
    throw err;
  }

  rootState = {
    catalog,
    repo,
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
