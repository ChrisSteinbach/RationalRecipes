// Catalog list view: browse / search / category- and sample-size-filter.
//
// Pure DOM — no framework. Render is idempotent: the caller owns a
// container element and calls render(state) after any state change.
// Filters compile to SQL in main.ts via CatalogRepo.listRecipes (vwt.4);
// this view receives the pre-filtered list and only paints it.

import { type Catalog, type CuratedRecipe, categoriesOf } from "./catalog.ts";
import type { CatalogOrderBy } from "./catalog_repo.ts";

export interface CatalogViewState {
  query: string;
  category: string;
  minSampleSize: number;
  orderBy: CatalogOrderBy;
}

export interface CatalogViewCallbacks {
  onQueryChange(q: string): void;
  onCategoryChange(c: string): void;
  onMinSampleSizeChange(n: number): void;
  onOrderByChange(o: CatalogOrderBy): void;
  onRecipeSelect(id: string): void;
}

const ALL_CATEGORIES = "all";

const SAMPLE_SIZE_TIERS: ReadonlyArray<{ label: string; value: number }> = [
  { label: "All", value: 0 },
  { label: "≥3", value: 3 },
  { label: "≥10", value: 10 },
  { label: "≥30", value: 30 },
];

const ORDER_BY_OPTIONS: ReadonlyArray<{ label: string; value: CatalogOrderBy }> = [
  { label: "Sample size", value: "sample_size" },
  { label: "Alphabetical", value: "title" },
];

export function initialCatalogState(): CatalogViewState {
  return {
    query: "",
    category: ALL_CATEGORIES,
    minSampleSize: 0,
    orderBy: "sample_size",
  };
}

export function renderCatalog(
  container: HTMLElement,
  catalog: Catalog,
  visibleRecipes: CuratedRecipe[],
  state: CatalogViewState,
  callbacks: CatalogViewCallbacks,
): void {
  // Preserve the toolbar across re-renders so the search input keeps
  // focus (and cursor position) while typing. Only the content below
  // the toolbar is rebuilt each cycle.
  let toolbar = container.querySelector<HTMLElement>(".catalog-toolbar");
  if (toolbar) {
    syncToolbarValues(toolbar, state);
    while (toolbar.nextSibling) toolbar.nextSibling.remove();
  } else {
    container.replaceChildren();
    toolbar = renderToolbar(catalog, state, callbacks);
    container.appendChild(toolbar);
  }

  const countLine = document.createElement("p");
  countLine.className = "catalog-count";
  countLine.textContent = `${visibleRecipes.length} of ${catalog.recipes.length} recipes`;
  container.appendChild(countLine);

  const release = renderReleaseBadge(catalog);
  if (release) container.appendChild(release);

  if (visibleRecipes.length === 0) {
    const empty = document.createElement("p");
    empty.className = "catalog-empty";
    empty.textContent = "No recipes match the current filters.";
    container.appendChild(empty);
    return;
  }

  const list = document.createElement("ul");
  list.className = "catalog-list";
  for (const recipe of visibleRecipes) {
    list.appendChild(renderRecipeCard(recipe, callbacks));
  }
  container.appendChild(list);
}

function syncToolbarValues(
  toolbar: HTMLElement,
  state: CatalogViewState,
): void {
  const search = toolbar.querySelector<HTMLInputElement>(".catalog-search");
  if (search && search.value !== state.query) search.value = state.query;
  const category = toolbar.querySelector<HTMLSelectElement>(".catalog-category");
  if (category) category.value = state.category;
  const sample = toolbar.querySelector<HTMLSelectElement>(".catalog-min-sample");
  if (sample) sample.value = String(state.minSampleSize);
  const order = toolbar.querySelector<HTMLSelectElement>(".catalog-order-by");
  if (order) order.value = state.orderBy;
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

  const sampleLabel = document.createElement("label");
  sampleLabel.className = "toolbar-field";
  const sampleText = document.createElement("span");
  sampleText.textContent = "Min sample size";
  const sampleSelect = document.createElement("select");
  sampleSelect.className = "catalog-min-sample";
  for (const tier of SAMPLE_SIZE_TIERS) {
    const opt = document.createElement("option");
    opt.value = String(tier.value);
    opt.textContent = tier.label;
    sampleSelect.appendChild(opt);
  }
  sampleSelect.value = String(state.minSampleSize);
  sampleSelect.addEventListener("change", () =>
    callbacks.onMinSampleSizeChange(Number(sampleSelect.value)),
  );
  sampleLabel.append(sampleText, sampleSelect);

  const orderLabel = document.createElement("label");
  orderLabel.className = "toolbar-field";
  const orderText = document.createElement("span");
  orderText.textContent = "Sort by";
  const orderSelect = document.createElement("select");
  orderSelect.className = "catalog-order-by";
  for (const option of ORDER_BY_OPTIONS) {
    const opt = document.createElement("option");
    opt.value = option.value;
    opt.textContent = option.label;
    orderSelect.appendChild(opt);
  }
  orderSelect.value = state.orderBy;
  orderSelect.addEventListener("change", () =>
    callbacks.onOrderByChange(orderSelect.value as CatalogOrderBy),
  );
  orderLabel.append(orderText, orderSelect);

  toolbar.append(searchLabel, categoryLabel, sampleLabel, orderLabel);
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
