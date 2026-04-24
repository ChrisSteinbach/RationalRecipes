// CuratedRecipeCatalog types and loader.
//
// Schema source of truth: schema/curated_recipes.schema.json (at the
// repo root). Populated by the Python pipeline — scripts/export_curated_recipes.py
// for hand-curated inputs, scripts/merged_to_catalog.py for pipeline
// output. Synced into /curated_recipes.json by web/scripts/sync-catalog.mjs.

import { Ratio, type RatioIngredient, type WholeUnit } from "./ratio.ts";

export interface CatalogIngredient {
  name: string;
  ratio: number;
  proportion: number;
  std_deviation: number;
  ci_lower: number;
  ci_upper: number;
  min_sample_size?: number;
  density_g_per_ml?: number | null;
  whole_unit?: WholeUnit | null;
}

export type CatalogSourceType = "url" | "book" | "text";

export interface CatalogSource {
  type: CatalogSourceType;
  title?: string;
  ref: string;
}

export interface CuratedRecipe {
  id: string;
  title: string;
  category: string;
  description?: string;
  base_ingredient: string;
  sample_size: number;
  confidence_level?: number;
  ingredients: CatalogIngredient[];
  sources?: CatalogSource[];
}

export interface Catalog {
  version: 1;
  recipes: CuratedRecipe[];
}

const CATALOG_PATH = "/curated_recipes.json";

/** Fetch the catalog from the static asset path and sanity-check its shape. */
export async function loadCatalog(path: string = CATALOG_PATH): Promise<Catalog> {
  const response = await fetch(path);
  if (!response.ok) {
    throw new Error(
      `Failed to fetch ${path}: ${response.status} ${response.statusText}`,
    );
  }
  const data = (await response.json()) as unknown;
  return validateCatalog(data);
}

/** Minimal runtime shape check — full validation lives in the JSON schema. */
export function validateCatalog(data: unknown): Catalog {
  if (!data || typeof data !== "object") {
    throw new Error("Catalog root must be an object");
  }
  const obj = data as Record<string, unknown>;
  if (obj.version !== 1) {
    throw new Error(`Unsupported catalog version: ${String(obj.version)}`);
  }
  if (!Array.isArray(obj.recipes)) {
    throw new Error("Catalog.recipes must be an array");
  }
  return obj as unknown as Catalog;
}

/**
 * Convert a catalog entry into a Ratio model.
 *
 * Uses baker's percentages (the `ratio` field on each CatalogIngredient),
 * which are pre-computed by the Python pipeline so the base ingredient is
 * 1.0. The in-browser Ratio math uses these directly — it doesn't need
 * proportions for scaling.
 */
export function toRatio(recipe: CuratedRecipe): Ratio {
  const values = recipe.ingredients.map((i) => i.ratio);
  const ingredients: RatioIngredient[] = recipe.ingredients.map((i) => ({
    name: i.name,
    densityGPerMl: i.density_g_per_ml ?? null,
    wholeUnit: i.whole_unit ?? null,
  }));
  return new Ratio(values, ingredients);
}

/** Unique categories in the order they first appear. */
export function categoriesOf(catalog: Catalog): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const r of catalog.recipes) {
    if (!seen.has(r.category)) {
      seen.add(r.category);
      out.push(r.category);
    }
  }
  return out;
}

/**
 * Filter recipes by a case-insensitive substring match against title,
 * description, and base_ingredient — plus an optional category filter.
 * Empty query + "all" category returns everything.
 */
export function filterRecipes(
  catalog: Catalog,
  query: string,
  category: string,
): CuratedRecipe[] {
  const q = query.trim().toLowerCase();
  return catalog.recipes.filter((r) => {
    if (category !== "all" && r.category !== category) return false;
    if (!q) return true;
    const haystack = [r.title, r.description ?? "", r.base_ingredient]
      .join(" ")
      .toLowerCase();
    return haystack.includes(q);
  });
}
