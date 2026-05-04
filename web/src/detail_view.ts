// Recipe detail view: ratio, weight input, scaled ingredient list, sources.
//
// Stateful weight input — the scaled list re-renders on every input.
// Pure DOM, no framework.

import {
  FEEDBACK_TAGS,
  type FeedbackTag,
  formatLastEdited,
  getEntry,
  saveEntry,
} from "./admin_feedback.ts";
import { type CuratedRecipe, toRatio } from "./catalog.ts";
import { formatRatio, formatRecipe } from "./format.ts";

/** One source recipe's parsed ingredient list, as shipped in
 *  `<variant_id>.json` sidecars (bead zh6). */
export interface SourceIngredient {
  name: string;
  quantity?: number | null;
  unit?: string | null;
}

export interface SourceRecipe {
  ingredients: SourceIngredient[];
}

export interface SourceRecipesPayload {
  variant_id: string;
  source_recipes: SourceRecipe[];
}

/** Loader for the per-variant source-recipes sidecar JSON. Tests
 *  inject a stub; production wires `defaultSourcesLoader` which fetches
 *  `${BASE_URL}sources/<id>.json`. */
export type SourcesLoader = (
  variantId: string,
) => Promise<SourceRecipesPayload>;

export interface DetailViewCallbacks {
  onBack(): void;
  loadSources?: SourcesLoader;
  /** When true, the detail view renders the admin-only feedback panel
   *  below the source-recipes section. */
  adminMode?: boolean;
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
  if (recipe.sample_size > 0) {
    container.appendChild(renderSourceRecipes(recipe, callbacks));
  }
  if (callbacks.adminMode) {
    container.appendChild(renderFeedbackPanel(recipe));
  }
}

/** Default loader: fetch `${BASE_URL}sources/<variantId>.json` and
 *  parse as JSON. Network errors propagate via the returned promise. */
export const defaultSourcesLoader: SourcesLoader = async (variantId) => {
  const url = `${import.meta.env.BASE_URL}sources/${encodeURIComponent(variantId)}.json`;
  const res = await fetch(url);
  if (!res.ok) {
    throw new Error(
      `Failed to fetch ${url}: ${res.status} ${res.statusText}`,
    );
  }
  return (await res.json()) as SourceRecipesPayload;
};

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

/** Lazy-loaded 'Source recipes' section (bead zh6).
 *
 *  Variants can have hundreds of sources, so the sidecar JSON is only
 *  fetched once the user expands the section. The element renders as a
 *  native ``<details>`` so collapse state is browser-managed and
 *  keyboard-accessible. State machine:
 *
 *    closed (initial) ──open──▶ loading ──ok──▶ loaded
 *                                       └─err─▶ error
 *
 *  Re-collapsing keeps the loaded content; re-opening doesn't refetch.
 */
function renderSourceRecipes(
  recipe: CuratedRecipe,
  callbacks: DetailViewCallbacks,
): HTMLElement {
  const details = document.createElement("details");
  details.className = "detail-source-recipes";

  const summary = document.createElement("summary");
  summary.className = "detail-source-recipes-summary";
  summary.textContent = `Source recipes (${recipe.sample_size})`;
  details.appendChild(summary);

  const content = document.createElement("div");
  content.className = "source-recipes-content";
  details.appendChild(content);

  const loader = callbacks.loadSources ?? defaultSourcesLoader;
  let loadStarted = false;
  details.addEventListener("toggle", () => {
    if (!details.open || loadStarted) return;
    loadStarted = true;

    content.replaceChildren();
    const placeholder = document.createElement("p");
    placeholder.className = "source-recipes-loading";
    placeholder.textContent = "Loading source recipes…";
    content.appendChild(placeholder);

    loader(recipe.id).then(
      (payload) => {
        content.replaceChildren(renderSourceRecipesList(payload.source_recipes));
      },
      (err: unknown) => {
        const msg = err instanceof Error ? err.message : String(err);
        content.replaceChildren(renderSourceRecipesError(msg));
      },
    );
  });

  return details;
}

function renderSourceRecipesList(sources: SourceRecipe[]): HTMLElement {
  if (sources.length === 0) {
    const p = document.createElement("p");
    p.className = "source-recipes-empty";
    p.textContent = "No source recipes available.";
    return p;
  }
  const ol = document.createElement("ol");
  ol.className = "source-recipes-list";
  sources.forEach((source, index) => {
    const li = document.createElement("li");
    li.className = "source-recipe";

    const label = document.createElement("span");
    label.className = "source-recipe-label";
    label.textContent = `#${index + 1}`;
    li.appendChild(label);

    const list = document.createElement("span");
    list.className = "source-recipe-ingredients";
    list.textContent = source.ingredients.length === 0
      ? "(no parsed ingredients)"
      : source.ingredients.map(formatSourceIngredient).join(", ");
    li.appendChild(list);

    ol.appendChild(li);
  });
  return ol;
}

function renderSourceRecipesError(message: string): HTMLElement {
  const p = document.createElement("p");
  p.className = "source-recipes-error";
  p.textContent = `Couldn't load source recipes: ${message}`;
  return p;
}

function formatSourceIngredient(ing: SourceIngredient): string {
  const parts: string[] = [];
  if (ing.quantity != null && Number.isFinite(ing.quantity)) {
    parts.push(formatQuantity(ing.quantity));
  }
  if (ing.unit) parts.push(ing.unit);
  if (parts.length === 0) return ing.name;
  return `${ing.name} (${parts.join(" ")})`;
}

function formatQuantity(q: number): string {
  // Trim trailing zeroes / unnecessary decimal point for readability —
  // 2 → "2", 0.5 → "0.5", 1.50 → "1.5". Cap at 3 decimals to keep the
  // one-line display compact.
  const rounded = Math.round(q * 1000) / 1000;
  return Number.isInteger(rounded) ? String(rounded) : rounded.toString();
}

/** Admin-only free-text feedback panel. Auto-saves on blur for both
 *  the tag dropdown and the notes textarea — no explicit save button.
 *  See `admin_feedback.ts` for the storage model. */
function renderFeedbackPanel(recipe: CuratedRecipe): HTMLElement {
  const section = document.createElement("section");
  section.className = "detail-feedback";

  const h = document.createElement("h2");
  h.textContent = "Feedback (admin)";
  section.appendChild(h);

  const existing = getEntry(recipe.id);

  const tagLabel = document.createElement("label");
  tagLabel.className = "feedback-field";
  const tagText = document.createElement("span");
  tagText.textContent = "Tag";
  const tagSelect = document.createElement("select");
  tagSelect.className = "feedback-tag";
  for (const tag of FEEDBACK_TAGS) {
    const opt = document.createElement("option");
    opt.value = tag;
    opt.textContent = tag;
    tagSelect.appendChild(opt);
  }
  tagSelect.value = existing?.tag ?? "Other";
  tagLabel.append(tagText, tagSelect);
  section.appendChild(tagLabel);

  const notesLabel = document.createElement("label");
  notesLabel.className = "feedback-field";
  const notesText = document.createElement("span");
  notesText.textContent = "Notes";
  const notes = document.createElement("textarea");
  notes.className = "feedback-notes";
  notes.rows = 4;
  notes.placeholder = "What's wrong / right with this recipe?";
  notes.value = existing?.notes ?? "";
  notesLabel.append(notesText, notes);
  section.appendChild(notesLabel);

  const status = document.createElement("p");
  status.className = "feedback-status";
  const updateStatus = (iso: string | null): void => {
    status.textContent = iso
      ? `Last edited ${formatLastEdited(iso)}`
      : "No feedback saved yet.";
  };
  updateStatus(existing?.updatedAt ?? null);
  section.appendChild(status);

  const persist = (): void => {
    const saved = saveEntry({
      variantId: recipe.id,
      tag: tagSelect.value as FeedbackTag,
      notes: notes.value,
    });
    updateStatus(saved?.updatedAt ?? null);
  };

  tagSelect.addEventListener("blur", persist);
  tagSelect.addEventListener("change", persist);
  notes.addEventListener("blur", persist);

  return section;
}
