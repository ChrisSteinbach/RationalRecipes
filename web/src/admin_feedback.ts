// Admin feedback model: free-text per-recipe notes with a tag category,
// stored in localStorage so observations survive between sessions.
//
// Pure module — no DOM, no fetch. Views import the helpers below to
// load / save / list / categorize / export. The PWA gates the admin
// surface separately (`?admin=1` → sessionStorage), but this module
// doesn't know or care; storage is always available.
//
// Out of scope: review_status (formal accept/reject) lives in
// recipes.db via scripts/review_variants.py. This is the informal
// scratchpad next to that workflow.

import type { CuratedRecipe } from "./catalog.ts";

export const FEEDBACK_TAGS = [
  "Title issue",
  "Category issue",
  "Ingredient issue",
  "Ratio issue",
  "Merge candidate",
  "Split candidate",
  "Looks great",
  "Other",
] as const;

export type FeedbackTag = (typeof FEEDBACK_TAGS)[number];

export interface FeedbackEntry {
  variantId: string;
  tag: FeedbackTag;
  notes: string;
  /** ISO-8601 timestamp of the last edit. */
  updatedAt: string;
}

interface FeedbackStorePayload {
  version: 1;
  entries: Record<string, FeedbackEntry>;
}

export const STORAGE_KEY = "rr-admin-feedback";

const DEFAULT_TAG: FeedbackTag = "Other";

/** Storage adapter — defaults to window.localStorage but is injectable
 *  so tests can drive the module without a DOM. */
export interface FeedbackStorage {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem(key: string): void;
}

function defaultStorage(): FeedbackStorage {
  if (typeof localStorage !== "undefined") return localStorage;
  // Fallback in-memory shim — used in unit tests that don't set up jsdom.
  const map = new Map<string, string>();
  return {
    getItem: (k) => map.get(k) ?? null,
    setItem: (k, v) => {
      map.set(k, v);
    },
    removeItem: (k) => {
      map.delete(k);
    },
  };
}

/** Load all stored entries. Returns an empty store if nothing is
 *  persisted yet, or if the stored payload is malformed (we don't want
 *  a corrupt blob to brick the admin view — just start fresh). */
export function loadFeedback(
  storage: FeedbackStorage = defaultStorage(),
): FeedbackEntry[] {
  const raw = storage.getItem(STORAGE_KEY);
  if (!raw) return [];
  try {
    const parsed = JSON.parse(raw) as unknown;
    return entriesFromPayload(parsed);
  } catch {
    return [];
  }
}

function entriesFromPayload(parsed: unknown): FeedbackEntry[] {
  if (!parsed || typeof parsed !== "object") return [];
  const obj = parsed as { entries?: unknown };
  if (!obj.entries || typeof obj.entries !== "object") return [];
  const out: FeedbackEntry[] = [];
  for (const [variantId, raw] of Object.entries(
    obj.entries as Record<string, unknown>,
  )) {
    const e = coerceEntry(variantId, raw);
    if (e) out.push(e);
  }
  return out;
}

function coerceEntry(variantId: string, raw: unknown): FeedbackEntry | null {
  if (!raw || typeof raw !== "object") return null;
  const r = raw as Record<string, unknown>;
  const tag = isFeedbackTag(r.tag) ? r.tag : DEFAULT_TAG;
  const notes = typeof r.notes === "string" ? r.notes : "";
  const updatedAt =
    typeof r.updatedAt === "string" ? r.updatedAt : new Date(0).toISOString();
  return { variantId, tag, notes, updatedAt };
}

function isFeedbackTag(v: unknown): v is FeedbackTag {
  return typeof v === "string" && (FEEDBACK_TAGS as readonly string[]).includes(v);
}

/** Get the entry for a single variant, or null if none stored. */
export function getEntry(
  variantId: string,
  storage: FeedbackStorage = defaultStorage(),
): FeedbackEntry | null {
  return loadFeedback(storage).find((e) => e.variantId === variantId) ?? null;
}

/** Persist (insert or replace) a feedback entry. An entry with empty
 *  notes AND the default tag is treated as a delete — the user blurred
 *  out an empty form, no need to keep an empty record. */
export function saveEntry(
  entry: { variantId: string; tag: FeedbackTag; notes: string },
  storage: FeedbackStorage = defaultStorage(),
  now: () => Date = () => new Date(),
): FeedbackEntry | null {
  const trimmed = entry.notes.replace(/\s+$/g, "");
  const isEmpty = trimmed === "" && entry.tag === DEFAULT_TAG;
  const existing = loadFeedback(storage);
  const others = existing.filter((e) => e.variantId !== entry.variantId);
  if (isEmpty) {
    writeAll(others, storage);
    return null;
  }
  const next: FeedbackEntry = {
    variantId: entry.variantId,
    tag: entry.tag,
    notes: entry.notes,
    updatedAt: now().toISOString(),
  };
  writeAll([...others, next], storage);
  return next;
}

/** Remove a single entry (no-op if missing). */
export function deleteEntry(
  variantId: string,
  storage: FeedbackStorage = defaultStorage(),
): void {
  const existing = loadFeedback(storage);
  writeAll(
    existing.filter((e) => e.variantId !== variantId),
    storage,
  );
}

/** Wipe every entry. Caller is responsible for the confirmation UX. */
export function clearAll(storage: FeedbackStorage = defaultStorage()): void {
  storage.removeItem(STORAGE_KEY);
}

function writeAll(entries: FeedbackEntry[], storage: FeedbackStorage): void {
  if (entries.length === 0) {
    storage.removeItem(STORAGE_KEY);
    return;
  }
  const dedup = new Map<string, FeedbackEntry>();
  for (const e of entries) dedup.set(e.variantId, e);
  const payload: FeedbackStorePayload = {
    version: 1,
    entries: Object.fromEntries(dedup),
  };
  storage.setItem(STORAGE_KEY, JSON.stringify(payload));
}

/** Annotated entry used by the admin list view: pulls in the recipe's
 *  current title/category/sample size if it's still in the catalog,
 *  otherwise marks the entry as orphaned. */
export interface ResolvedFeedbackEntry extends FeedbackEntry {
  recipe: CuratedRecipe | null;
  orphaned: boolean;
}

/** Join entries against a catalog and return resolved entries. The
 *  caller decides how to render orphans (the admin view shows them in
 *  a separate section so nothing silently disappears). */
export function resolveEntries(
  entries: FeedbackEntry[],
  recipesById: Map<string, CuratedRecipe>,
): ResolvedFeedbackEntry[] {
  return entries.map((e) => {
    const recipe = recipesById.get(e.variantId) ?? null;
    return { ...e, recipe, orphaned: recipe === null };
  });
}

export type SortKey = "updatedAt" | "sampleSize" | "title";

/** Group entries by their tag in FEEDBACK_TAGS order. Each group is
 *  sorted by the requested key. Empty groups are dropped. */
export function groupByTag(
  entries: ResolvedFeedbackEntry[],
  sortBy: SortKey = "updatedAt",
): Array<{ tag: FeedbackTag; entries: ResolvedFeedbackEntry[] }> {
  const buckets = new Map<FeedbackTag, ResolvedFeedbackEntry[]>();
  for (const tag of FEEDBACK_TAGS) buckets.set(tag, []);
  for (const e of entries) {
    const bucket = buckets.get(e.tag);
    if (bucket) bucket.push(e);
  }
  const out: Array<{ tag: FeedbackTag; entries: ResolvedFeedbackEntry[] }> = [];
  for (const tag of FEEDBACK_TAGS) {
    const list = buckets.get(tag) ?? [];
    if (list.length === 0) continue;
    out.push({ tag, entries: sortEntries(list, sortBy) });
  }
  return out;
}

export function sortEntries(
  entries: ResolvedFeedbackEntry[],
  sortBy: SortKey,
): ResolvedFeedbackEntry[] {
  const copy = [...entries];
  copy.sort((a, b) => {
    if (sortBy === "sampleSize") {
      const sa = a.recipe?.sample_size ?? -1;
      const sb = b.recipe?.sample_size ?? -1;
      if (sa !== sb) return sb - sa;
    } else if (sortBy === "title") {
      const ta = a.recipe?.title ?? a.variantId;
      const tb = b.recipe?.title ?? b.variantId;
      const cmp = ta.localeCompare(tb);
      if (cmp !== 0) return cmp;
    } else {
      // updatedAt: most-recent-first
      if (a.updatedAt !== b.updatedAt) {
        return a.updatedAt < b.updatedAt ? 1 : -1;
      }
    }
    return a.variantId.localeCompare(b.variantId);
  });
  return copy;
}

/** Render all entries as a markdown report. The format is shaped to be
 *  pasted straight into a triage bead — heading + per-tag sections,
 *  one bullet per entry with its short variant id, dish, sample size,
 *  and the freeform notes. Orphans get their own trailing section. */
export function exportMarkdown(
  entries: ResolvedFeedbackEntry[],
  options: { date?: Date } = {},
): string {
  const date = options.date ?? new Date();
  const dateStr = date.toISOString().slice(0, 10);
  const lines: string[] = [];
  const total = entries.length;
  lines.push(`# RationalRecipes feedback — ${dateStr} (${total} ${pluralize("entry", "entries", total)})`);
  lines.push("");

  if (total === 0) {
    lines.push("_No entries._");
    return lines.join("\n");
  }

  const present = entries.filter((e) => !e.orphaned);
  const orphaned = entries.filter((e) => e.orphaned);

  for (const group of groupByTag(present, "updatedAt")) {
    lines.push(`## ${group.tag} (${group.entries.length})`);
    for (const entry of group.entries) {
      lines.push(...renderEntryMarkdown(entry));
    }
    lines.push("");
  }

  if (orphaned.length > 0) {
    lines.push(`## Orphaned (variant_id no longer in catalog) (${orphaned.length})`);
    for (const entry of sortEntries(orphaned, "updatedAt")) {
      lines.push(...renderEntryMarkdown(entry));
    }
    lines.push("");
  }

  // Trim trailing blank line so the report ends cleanly.
  while (lines.length > 0 && lines[lines.length - 1] === "") lines.pop();
  return lines.join("\n");
}

function renderEntryMarkdown(entry: ResolvedFeedbackEntry): string[] {
  const out: string[] = [];
  const shortId = entry.variantId.slice(0, 12);
  if (entry.recipe) {
    const r = entry.recipe;
    out.push(
      `- **${r.title}** (\`${shortId}\`, ${r.category}, ${r.sample_size} ${pluralize("source", "sources", r.sample_size)})`,
    );
  } else {
    out.push(`- **(unknown variant)** (\`${shortId}\`)`);
  }
  const notes = entry.notes.trim();
  if (notes) {
    for (const line of notes.split("\n")) {
      out.push(`  ${line}`);
    }
  }
  return out;
}

function pluralize(singular: string, plural: string, n: number): string {
  return n === 1 ? singular : plural;
}

/** Pretty-printed JSON dump, suitable for download/backup. Stable key
 *  order so two backups taken back-to-back diff cleanly. */
export function exportJSON(entries: FeedbackEntry[]): string {
  const sorted = [...entries].sort((a, b) =>
    a.variantId.localeCompare(b.variantId),
  );
  const payload: FeedbackStorePayload = {
    version: 1,
    entries: Object.fromEntries(
      sorted.map((e) => [
        e.variantId,
        { tag: e.tag, notes: e.notes, updatedAt: e.updatedAt },
      ]),
    ) as Record<string, FeedbackEntry>,
  };
  return JSON.stringify(payload, null, 2);
}

/** Format a stored ISO timestamp as a short human-readable string for
 *  the 'last edited' indicator in the detail view. */
export function formatLastEdited(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "never";
  return d.toLocaleString();
}
