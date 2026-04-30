// Recipe detail view: ratio, weight input, scaled ingredient list, sources.
//
// Stateful weight input — the scaled list re-renders on every input.
// Pure DOM, no framework.

import { type CuratedRecipe, toRatio } from "./catalog.ts";
import { formatRatio, formatRecipe } from "./format.ts";

export interface DetailViewCallbacks {
  onBack(): void;
}

export interface DetailViewState {
  targetWeight: number;
}

export const WEIGHT_PRESETS = [250, 500, 1000] as const;

export function initialDetailState(): DetailViewState {
  return { targetWeight: 500 };
}

export function renderDetail(
  container: HTMLElement,
  recipe: CuratedRecipe,
  state: DetailViewState,
  callbacks: DetailViewCallbacks,
): void {
  container.replaceChildren();

  container.appendChild(renderHeader(recipe, callbacks));
  container.appendChild(renderRatioLine(recipe));
  container.appendChild(renderWeightControls(state, (newWeight) => {
    state.targetWeight = newWeight;
    renderDetail(container, recipe, state, callbacks);
  }));
  container.appendChild(renderScaledIngredients(recipe, state.targetWeight));
  container.appendChild(renderStatsTable(recipe));
  if (recipe.sources && recipe.sources.length > 0) {
    container.appendChild(renderSources(recipe.sources));
  }
}

function renderHeader(
  recipe: CuratedRecipe,
  callbacks: DetailViewCallbacks,
): HTMLElement {
  const header = document.createElement("header");
  header.className = "detail-header";

  const back = document.createElement("button");
  back.type = "button";
  back.className = "detail-back";
  back.textContent = "← Catalog";
  back.addEventListener("click", () => callbacks.onBack());
  header.appendChild(back);

  const title = document.createElement("h1");
  title.className = "detail-title";
  title.textContent = recipe.title;
  header.appendChild(title);

  const meta = document.createElement("p");
  meta.className = "detail-meta";
  meta.textContent = `Category: ${recipe.category}   Sample size: ${recipe.sample_size}`;
  header.appendChild(meta);

  if (recipe.description) {
    const desc = document.createElement("p");
    desc.className = "detail-description";
    desc.textContent = recipe.description;
    header.appendChild(desc);
  }

  return header;
}

function renderRatioLine(recipe: CuratedRecipe): HTMLElement {
  const section = document.createElement("section");
  section.className = "detail-ratio";
  const label = document.createElement("h2");
  label.textContent = "Ratio (baker's percentage)";
  const code = document.createElement("code");
  code.className = "detail-ratio-value";
  code.textContent = formatRatio(toRatio(recipe), { precision: 2 });
  section.append(label, code);
  return section;
}

function renderWeightControls(
  state: DetailViewState,
  onChange: (weight: number) => void,
): HTMLElement {
  const section = document.createElement("section");
  section.className = "detail-weight-controls";

  const h = document.createElement("h2");
  h.textContent = "Scale to total weight";
  section.appendChild(h);

  const label = document.createElement("label");
  label.className = "weight-input-label";
  const labelText = document.createElement("span");
  labelText.textContent = "Grams";
  const input = document.createElement("input");
  input.type = "number";
  input.min = "1";
  input.step = "10";
  input.value = String(state.targetWeight);
  input.className = "detail-weight-input";
  input.addEventListener("input", () => {
    const v = Number(input.value);
    if (Number.isFinite(v) && v > 0) onChange(v);
  });
  label.append(labelText, input);
  section.appendChild(label);

  const presets = document.createElement("div");
  presets.className = "weight-presets";
  for (const w of WEIGHT_PRESETS) {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "weight-preset";
    btn.textContent = `${w} g`;
    if (w === state.targetWeight) btn.classList.add("weight-preset-active");
    btn.addEventListener("click", () => onChange(w));
    presets.appendChild(btn);
  }
  section.appendChild(presets);

  return section;
}

function renderScaledIngredients(
  recipe: CuratedRecipe,
  targetWeight: number,
): HTMLElement {
  const section = document.createElement("section");
  section.className = "detail-scaled";

  const h = document.createElement("h2");
  h.textContent = "Ingredients";
  section.appendChild(h);

  const ratio = toRatio(recipe);
  const { totalWeight, text } = formatRecipe(ratio, targetWeight, { precision: 1 });

  const ul = document.createElement("ul");
  ul.className = "scaled-ingredients";
  for (const line of text.split("\n")) {
    const li = document.createElement("li");
    li.textContent = line;
    ul.appendChild(li);
  }
  section.appendChild(ul);

  const total = document.createElement("p");
  total.className = "detail-total";
  total.textContent = `Total: ${totalWeight.toFixed(1)} g`;
  section.appendChild(total);

  return section;
}

function renderStatsTable(recipe: CuratedRecipe): HTMLElement {
  const section = document.createElement("section");
  section.className = "detail-stats";

  const h = document.createElement("h2");
  h.textContent = "Per-ingredient proportion (95% CI)";
  section.appendChild(h);

  const table = document.createElement("table");
  table.className = "stats-table";
  const thead = document.createElement("thead");
  thead.innerHTML =
    "<tr><th>Ingredient</th><th>Ratio</th><th>Proportion</th><th>95% CI</th><th>Stddev</th></tr>";
  table.appendChild(thead);
  const tbody = document.createElement("tbody");
  for (const ing of recipe.ingredients) {
    const tr = document.createElement("tr");
    const cells = [
      ing.name,
      ing.ratio.toFixed(3),
      (ing.proportion * 100).toFixed(2) + "%",
      `${(ing.ci_lower * 100).toFixed(2)}–${(ing.ci_upper * 100).toFixed(2)}%`,
      (ing.std_deviation * 100).toFixed(2) + "%",
    ];
    for (const c of cells) {
      const td = document.createElement("td");
      td.textContent = c;
      tr.appendChild(td);
    }
    tbody.appendChild(tr);
  }
  table.appendChild(tbody);
  section.appendChild(table);
  return section;
}

function renderSources(
  sources: NonNullable<CuratedRecipe["sources"]>,
): HTMLElement {
  const section = document.createElement("section");
  section.className = "detail-sources";

  const h = document.createElement("h2");
  h.textContent = `Sources (${sources.length})`;
  section.appendChild(h);

  const ul = document.createElement("ul");
  ul.className = "source-list";
  for (const s of sources) {
    const li = document.createElement("li");
    li.className = `source-item source-${s.type}`;
    if (s.type === "url") {
      const a = document.createElement("a");
      a.href = s.ref;
      a.target = "_blank";
      a.rel = "noreferrer noopener";
      a.textContent = s.title ?? s.ref;
      li.appendChild(a);
    } else {
      const text = document.createElement("span");
      text.textContent = s.title ? `${s.title}: ${s.ref}` : s.ref;
      li.appendChild(text);
    }
    ul.appendChild(li);
  }
  section.appendChild(ul);
  return section;
}
