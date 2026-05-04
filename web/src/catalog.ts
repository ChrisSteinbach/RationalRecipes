// CuratedRecipe types and catalog-loading façade.
//
// Data source since bead vwt.y43: a static JSON manifest
// (catalog.json) produced by `scripts/export_catalog_json.py`. The
// historical sql.js + recipes.db path was retired — gzipped JSON at
// the v1 scope (n_recipes >= 100) is ~75 KB, so the WASM/DB overhead
// bought nothing. See `docs/design/full-catalog.md` for the inflection
// point at ~5,000 variants where sql.js becomes worth re-introducing.

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

export interface CatalogMetadata {
  dataset_version?: string;
  released?: string;
  pipeline_revision?: string;
  recipe_count?: number;
  notes?: string;
}

export interface Catalog {
  version: 1;
  metadata?: CatalogMetadata;
  recipes: CuratedRecipe[];
}

const CATALOG_FILE = "catalog.json";

/** Path to the JSON catalog manifest under the Vite base URL. */
export const CATALOG_PATH = `${import.meta.env.BASE_URL}${CATALOG_FILE}`;

/** Fetch and validate the JSON catalog. */
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
 * Uses baker's percentages (the `ratio` field on each CatalogIngredient).
 * The Python side pre-computes these so the base ingredient is 1.0; the
 * in-browser Ratio math consumes them directly.
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
