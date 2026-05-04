// Admin feedback list view: per-tag groups, sort selector, and the
// three export controls (markdown copy, JSON download, clear-all).
//
// Pure DOM. The route is reachable at `#/admin` once admin mode is on
// — see `detectAdminMode` in `app_routing.ts`. Storage lives in
// localStorage via `admin_feedback.ts`.

import {
  type FeedbackEntry,
  type ResolvedFeedbackEntry,
  type SortKey,
  clearAll,
  exportJSON,
  exportMarkdown,
  formatLastEdited,
  groupByTag,
  loadFeedback,
  resolveEntries,
  sortEntries,
} from "./admin_feedback.ts";
import type { Catalog } from "./catalog.ts";

export interface AdminViewCallbacks {
  onBack(): void;
  onOpenRecipe(id: string): void;
}

const SORT_OPTIONS: ReadonlyArray<{ label: string; value: SortKey }> = [
  { label: "Most recently edited", value: "updatedAt" },
  { label: "Sample size", value: "sampleSize" },
  { label: "Alphabetical", value: "title" },
];

interface AdminViewState {
  sortBy: SortKey;
}

export function renderAdminView(
  container: HTMLElement,
  catalog: Catalog,
  callbacks: AdminViewCallbacks,
): void {
  const state: AdminViewState = { sortBy: "updatedAt" };
  const recipesById = new Map(catalog.recipes.map((r) => [r.id, r]));

  function paint(): void {
    container.replaceChildren();

    container.appendChild(renderHeader(callbacks));

    const entries = loadFeedback();
    const resolved = resolveEntries(entries, recipesById);

    container.appendChild(renderToolbar(state, () => paint(), entries, resolved));
    container.appendChild(renderSummary(resolved));
    container.appendChild(renderBody(resolved, state.sortBy, callbacks));
  }

  paint();
}

function renderHeader(callbacks: AdminViewCallbacks): HTMLElement {
  const header = document.createElement("header");
  header.className = "admin-header";

  const back = document.createElement("button");
  back.type = "button";
  back.className = "admin-back";
  back.textContent = "← Catalog";
  back.addEventListener("click", () => callbacks.onBack());
  header.appendChild(back);

  const title = document.createElement("h1");
  title.className = "admin-title";
  title.textContent = "Admin feedback";
  header.appendChild(title);

  const note = document.createElement("p");
  note.className = "admin-note";
  note.textContent =
    "Free-text observations stored locally in this browser. Use the buttons below to copy a markdown report or download a JSON backup.";
  header.appendChild(note);

  return header;
}

function renderToolbar(
  state: AdminViewState,
  rerender: () => void,
  entries: FeedbackEntry[],
  resolved: ResolvedFeedbackEntry[],
): HTMLElement {
  const toolbar = document.createElement("div");
  toolbar.className = "admin-toolbar";

  const sortLabel = document.createElement("label");
  sortLabel.className = "toolbar-field";
  const sortText = document.createElement("span");
  sortText.textContent = "Sort by";
  const sortSelect = document.createElement("select");
  sortSelect.className = "admin-sort";
  for (const opt of SORT_OPTIONS) {
    const o = document.createElement("option");
    o.value = opt.value;
    o.textContent = opt.label;
    sortSelect.appendChild(o);
  }
  sortSelect.value = state.sortBy;
  sortSelect.addEventListener("change", () => {
    state.sortBy = sortSelect.value as SortKey;
    rerender();
  });
  sortLabel.append(sortText, sortSelect);
  toolbar.appendChild(sortLabel);

  const actions = document.createElement("div");
  actions.className = "admin-actions";

  const copyBtn = document.createElement("button");
  copyBtn.type = "button";
  copyBtn.className = "admin-action admin-action-copy";
  copyBtn.textContent = "Copy markdown report";
  copyBtn.disabled = entries.length === 0;
  copyBtn.addEventListener("click", () => {
    void copyMarkdownReport(copyBtn, resolved);
  });
  actions.appendChild(copyBtn);

  const downloadBtn = document.createElement("button");
  downloadBtn.type = "button";
  downloadBtn.className = "admin-action admin-action-download";
  downloadBtn.textContent = "Download JSON";
  downloadBtn.disabled = entries.length === 0;
  downloadBtn.addEventListener("click", () => downloadJSON(entries));
  actions.appendChild(downloadBtn);

  const clearBtn = document.createElement("button");
  clearBtn.type = "button";
  clearBtn.className = "admin-action admin-action-clear";
  clearBtn.textContent = "Clear all";
  clearBtn.disabled = entries.length === 0;
  clearBtn.addEventListener("click", () => {
    if (entries.length === 0) return;
    // Double-confirm — wiping local feedback is irreversible.
    if (!confirm(`Delete all ${entries.length} feedback ${entries.length === 1 ? "entry" : "entries"}? This cannot be undone.`)) {
      return;
    }
    if (!confirm("Are you absolutely sure? Click OK to permanently delete.")) {
      return;
    }
    clearAll();
    rerender();
  });
  actions.appendChild(clearBtn);

  toolbar.appendChild(actions);
  return toolbar;
}

async function copyMarkdownReport(
  btn: HTMLButtonElement,
  resolved: ResolvedFeedbackEntry[],
): Promise<void> {
  const markdown = exportMarkdown(resolved);
  const original = btn.textContent;
  try {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(markdown);
      btn.textContent = "Copied!";
    } else {
      // Fallback: open a textarea so the user can copy manually.
      window.prompt("Copy this markdown report:", markdown);
      return;
    }
  } catch {
    btn.textContent = "Copy failed";
  }
  setTimeout(() => {
    btn.textContent = original;
  }, 1500);
}

function downloadJSON(entries: FeedbackEntry[]): void {
  const blob = new Blob([exportJSON(entries)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `rr-feedback-${new Date().toISOString().slice(0, 10)}.json`;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

function renderSummary(resolved: ResolvedFeedbackEntry[]): HTMLElement {
  const p = document.createElement("p");
  p.className = "admin-summary";
  if (resolved.length === 0) {
    p.textContent = "No feedback yet. Open a recipe with ?admin=1 to leave notes.";
    return p;
  }
  const orphaned = resolved.filter((e) => e.orphaned).length;
  const present = resolved.length - orphaned;
  const parts = [`${resolved.length} ${resolved.length === 1 ? "entry" : "entries"}`];
  if (orphaned > 0) parts.push(`${orphaned} orphaned`);
  parts.push(`${present} live`);
  p.textContent = parts.join(" · ");
  return p;
}

function renderBody(
  resolved: ResolvedFeedbackEntry[],
  sortBy: SortKey,
  callbacks: AdminViewCallbacks,
): HTMLElement {
  const wrapper = document.createElement("div");
  wrapper.className = "admin-body";
  if (resolved.length === 0) return wrapper;

  const present = resolved.filter((e) => !e.orphaned);
  const orphaned = resolved.filter((e) => e.orphaned);

  for (const group of groupByTag(present, sortBy)) {
    const section = document.createElement("section");
    section.className = "admin-group";
    const h2 = document.createElement("h2");
    h2.className = "admin-group-title";
    h2.textContent = `${group.tag} (${group.entries.length})`;
    section.appendChild(h2);

    const list = document.createElement("ul");
    list.className = "admin-entry-list";
    for (const entry of group.entries) {
      list.appendChild(renderEntry(entry, callbacks));
    }
    section.appendChild(list);
    wrapper.appendChild(section);
  }

  if (orphaned.length > 0) {
    const section = document.createElement("section");
    section.className = "admin-group admin-group-orphans";
    const h2 = document.createElement("h2");
    h2.className = "admin-group-title";
    h2.textContent = `Orphaned (variant_id no longer in catalog) (${orphaned.length})`;
    section.appendChild(h2);

    const list = document.createElement("ul");
    list.className = "admin-entry-list";
    for (const entry of sortEntries(orphaned, sortBy)) {
      list.appendChild(renderEntry(entry, callbacks));
    }
    section.appendChild(list);
    wrapper.appendChild(section);
  }

  return wrapper;
}

function renderEntry(
  entry: ResolvedFeedbackEntry,
  callbacks: AdminViewCallbacks,
): HTMLElement {
  const li = document.createElement("li");
  li.className = "admin-entry";
  if (entry.orphaned) li.classList.add("admin-entry-orphaned");

  const header = document.createElement("div");
  header.className = "admin-entry-header";

  if (entry.recipe) {
    const link = document.createElement("button");
    link.type = "button";
    link.className = "admin-entry-link";
    link.textContent = entry.recipe.title;
    link.addEventListener("click", () => callbacks.onOpenRecipe(entry.variantId));
    header.appendChild(link);

    const meta = document.createElement("span");
    meta.className = "admin-entry-meta";
    meta.textContent = ` · ${entry.recipe.category} · ${entry.recipe.sample_size} ${entry.recipe.sample_size === 1 ? "source" : "sources"}`;
    header.appendChild(meta);
  } else {
    const title = document.createElement("span");
    title.className = "admin-entry-title";
    title.textContent = "(unknown variant)";
    header.appendChild(title);
  }

  const id = document.createElement("code");
  id.className = "admin-entry-id";
  id.textContent = entry.variantId.slice(0, 12);
  header.appendChild(id);

  li.appendChild(header);

  if (entry.notes.trim() !== "") {
    const notes = document.createElement("p");
    notes.className = "admin-entry-notes";
    notes.textContent = entry.notes;
    li.appendChild(notes);
  }

  const time = document.createElement("p");
  time.className = "admin-entry-time";
  time.textContent = `Last edited ${formatLastEdited(entry.updatedAt)}`;
  li.appendChild(time);

  return li;
}
