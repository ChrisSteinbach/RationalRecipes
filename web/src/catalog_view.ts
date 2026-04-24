// Catalog list view: browse / search / category-filter the CuratedRecipeCatalog.
//
// Pure DOM — no framework. Render is idempotent: the caller owns a
// container element and calls render(state) after any state change.

import {
  type Catalog,
  type CuratedRecipe,
  categoriesOf,
  filterRecipes,
} from "./catalog.ts";

export interface CatalogViewState {
  query: string;
  category: string;
}

export interface CatalogViewCallbacks {
  onQueryChange(q: string): void;
  onCategoryChange(c: string): void;
  onRecipeSelect(id: string): void;
}

const ALL_CATEGORIES = "all";

export function initialCatalogState(): CatalogViewState {
  return { query: "", category: ALL_CATEGORIES };
}

export function renderCatalog(
  container: HTMLElement,
  catalog: Catalog,
  state: CatalogViewState,
  callbacks: CatalogViewCallbacks,
): void {
  container.replaceChildren();
  container.appendChild(renderToolbar(catalog, state, callbacks));

  const filtered = filterRecipes(catalog, state.query, state.category);
  const countLine = document.createElement("p");
  countLine.className = "catalog-count";
  countLine.textContent = `${filtered.length} of ${catalog.recipes.length} recipes`;
  container.appendChild(countLine);

  const release = renderReleaseBadge(catalog);
  if (release) container.appendChild(release);

  if (filtered.length === 0) {
    const empty = document.createElement("p");
    empty.className = "catalog-empty";
    empty.textContent = "No recipes match the current filters.";
    container.appendChild(empty);
    return;
  }

  const list = document.createElement("ul");
  list.className = "catalog-list";
  for (const recipe of filtered) {
    list.appendChild(renderRecipeCard(recipe, callbacks));
  }
  container.appendChild(list);
}

function renderToolbar(
  catalog: Catalog,
  state: CatalogViewState,
  callbacks: CatalogViewCallbacks,
): HTMLElement {
  const toolbar = document.createElement("div");
  toolbar.className = "catalog-toolbar";

  const searchLabel = document.createElement("label");
  searchLabel.className = "toolbar-field";
  const searchText = document.createElement("span");
  searchText.textContent = "Search";
  const search = document.createElement("input");
  search.type = "search";
  search.placeholder = "pannkakor, flour, …";
  search.value = state.query;
  search.className = "catalog-search";
  search.addEventListener("input", () => callbacks.onQueryChange(search.value));
  searchLabel.append(searchText, search);

  const categoryLabel = document.createElement("label");
  categoryLabel.className = "toolbar-field";
  const catText = document.createElement("span");
  catText.textContent = "Category";
  const select = document.createElement("select");
  select.className = "catalog-category";
  const allOption = document.createElement("option");
  allOption.value = ALL_CATEGORIES;
  allOption.textContent = "All";
  select.appendChild(allOption);
  for (const cat of categoriesOf(catalog)) {
    const opt = document.createElement("option");
    opt.value = cat;
    opt.textContent = cat;
    select.appendChild(opt);
  }
  select.value = state.category;
  select.addEventListener("change", () => callbacks.onCategoryChange(select.value));
  categoryLabel.append(catText, select);

  toolbar.append(searchLabel, categoryLabel);
  return toolbar;
}

function renderReleaseBadge(catalog: Catalog): HTMLElement | null {
  const meta = catalog.metadata;
  if (!meta) return null;
  const parts: string[] = [];
  if (meta.dataset_version) parts.push(`Dataset ${meta.dataset_version}`);
  if (meta.released) parts.push(`released ${meta.released}`);
  if (meta.pipeline_revision) parts.push(`rev ${meta.pipeline_revision}`);
  if (parts.length === 0) return null;
  const el = document.createElement("p");
  el.className = "catalog-release";
  el.textContent = parts.join(" · ");
  if (meta.notes) el.title = meta.notes;
  return el;
}

function renderRecipeCard(
  recipe: CuratedRecipe,
  callbacks: CatalogViewCallbacks,
): HTMLElement {
  const li = document.createElement("li");
  li.className = "recipe-card";

  const button = document.createElement("button");
  button.type = "button";
  button.className = "recipe-card-button";
  button.setAttribute("data-recipe-id", recipe.id);
  button.addEventListener("click", () => callbacks.onRecipeSelect(recipe.id));

  const title = document.createElement("h2");
  title.className = "recipe-card-title";
  title.textContent = recipe.title;

  const meta = document.createElement("p");
  meta.className = "recipe-card-meta";
  const sampleLabel =
    recipe.sample_size === 1 ? "1 source recipe" : `${recipe.sample_size} source recipes`;
  meta.textContent = `${recipe.category} · ${sampleLabel}`;

  button.append(title, meta);
  if (recipe.description) {
    const desc = document.createElement("p");
    desc.className = "recipe-card-desc";
    desc.textContent = recipe.description;
    button.appendChild(desc);
  }

  li.appendChild(button);
  return li;
}
