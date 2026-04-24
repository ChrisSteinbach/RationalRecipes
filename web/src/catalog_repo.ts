// sql.js-backed reader for the Phase 5 recipes.db catalog (bead vwt.6).
//
// Wraps a sql.js Database handle and exposes queries that hydrate
// CuratedRecipe-shaped rows from the CatalogDB schema. The PWA
// (main.ts, catalog_view.ts, detail_view.ts) consumes CuratedRecipe
// unchanged — only the source swaps from a JSON payload to SQL rows.

import type { Database } from "sql.js";
import type {
  Catalog,
  CatalogIngredient,
  CatalogMetadata,
  CatalogSource,
  CuratedRecipe,
} from "./catalog.ts";

export interface ListFilters {
  minSampleSize?: number;
  category?: string;
  titleSearch?: string;
  orderBy?: "n_recipes_desc" | "n_recipes_asc" | "title_asc" | "title_desc";
  includeDropped?: boolean;
}

const ORDER_BY_SQL: Record<NonNullable<ListFilters["orderBy"]>, string> = {
  n_recipes_desc: "n_recipes DESC, normalized_title ASC",
  n_recipes_asc: "n_recipes ASC, normalized_title ASC",
  title_asc: "normalized_title ASC",
  title_desc: "normalized_title DESC",
};

export interface VariantSummary {
  id: string;
  normalizedTitle: string;
  displayTitle: string;
  category: string | null;
  description: string | null;
  baseIngredient: string | null;
  sampleSize: number;
  confidenceLevel: number | null;
}

export class CatalogRepo {
  private readonly db: Database;

  constructor(db: Database) {
    this.db = db;
  }

  listVariants(filters: ListFilters = {}): VariantSummary[] {
    const where: string[] = [];
    const params: (string | number)[] = [];
    if (filters.minSampleSize !== undefined) {
      where.push("n_recipes >= ?");
      params.push(filters.minSampleSize);
    }
    if (filters.category !== undefined) {
      where.push("category = ?");
      params.push(filters.category);
    }
    if (filters.titleSearch) {
      where.push("LOWER(normalized_title) LIKE ?");
      params.push(`%${filters.titleSearch.toLowerCase()}%`);
    }
    if (!filters.includeDropped) {
      where.push("(review_status IS NULL OR review_status != 'drop')");
    }
    const orderBy = ORDER_BY_SQL[filters.orderBy ?? "n_recipes_desc"];
    let sql =
      "SELECT variant_id, normalized_title, display_title, category," +
      " description, base_ingredient, n_recipes, confidence_level" +
      " FROM variants";
    if (where.length > 0) sql += " WHERE " + where.join(" AND ");
    sql += " ORDER BY " + orderBy;

    const stmt = this.db.prepare(sql);
    try {
      stmt.bind(params);
      const out: VariantSummary[] = [];
      while (stmt.step()) {
        const r = stmt.get() as [
          string,
          string,
          string | null,
          string | null,
          string | null,
          string | null,
          number,
          number | null,
        ];
        out.push({
          id: r[0],
          normalizedTitle: r[1],
          displayTitle: r[2] ?? r[1],
          category: r[3],
          description: r[4],
          baseIngredient: r[5],
          sampleSize: r[6],
          confidenceLevel: r[7],
        });
      }
      return out;
    } finally {
      stmt.free();
    }
  }

  getVariant(id: string): CuratedRecipe | null {
    const summaries = this.runSingleVariantQuery(id);
    if (summaries === null) return null;
    const ingredients = this.getIngredientStats(id);
    const sources = this.getVariantSources(id);
    const recipe: CuratedRecipe = {
      id: summaries.normalizedTitle,
      title: summaries.displayTitle,
      category: summaries.category ?? "uncategorized",
      base_ingredient: summaries.baseIngredient ?? ingredients[0]?.name ?? "",
      sample_size: summaries.sampleSize,
      ingredients,
    };
    if (summaries.description) recipe.description = summaries.description;
    if (summaries.confidenceLevel !== null) {
      recipe.confidence_level = summaries.confidenceLevel;
    }
    if (sources.length > 0) recipe.sources = sources;
    return recipe;
  }

  getVariantByNormalizedTitle(normalizedTitle: string): CuratedRecipe | null {
    const stmt = this.db.prepare(
      "SELECT variant_id FROM variants WHERE normalized_title = ?",
    );
    try {
      stmt.bind([normalizedTitle]);
      if (!stmt.step()) return null;
      const [variantId] = stmt.get() as [string];
      return this.getVariant(variantId);
    } finally {
      stmt.free();
    }
  }

  private runSingleVariantQuery(id: string): VariantSummary | null {
    const stmt = this.db.prepare(
      "SELECT variant_id, normalized_title, display_title, category," +
        " description, base_ingredient, n_recipes, confidence_level" +
        " FROM variants WHERE variant_id = ?",
    );
    try {
      stmt.bind([id]);
      if (!stmt.step()) return null;
      const r = stmt.get() as [
        string,
        string,
        string | null,
        string | null,
        string | null,
        string | null,
        number,
        number | null,
      ];
      return {
        id: r[0],
        normalizedTitle: r[1],
        displayTitle: r[2] ?? r[1],
        category: r[3],
        description: r[4],
        baseIngredient: r[5],
        sampleSize: r[6],
        confidenceLevel: r[7],
      };
    } finally {
      stmt.free();
    }
  }

  getIngredientStats(variantId: string): CatalogIngredient[] {
    const stmt = this.db.prepare(
      "SELECT canonical_name, mean_proportion, stddev, ci_lower, ci_upper," +
        " ratio, min_sample_size, density_g_per_ml, whole_unit_name," +
        " whole_unit_grams" +
        " FROM variant_ingredient_stats WHERE variant_id = ?" +
        " ORDER BY ordinal ASC",
    );
    try {
      stmt.bind([variantId]);
      const out: CatalogIngredient[] = [];
      while (stmt.step()) {
        const r = stmt.get() as [
          string,
          number,
          number | null,
          number | null,
          number | null,
          number | null,
          number,
          number | null,
          string | null,
          number | null,
        ];
        const ingredient: CatalogIngredient = {
          name: r[0],
          ratio: r[5] ?? 0,
          proportion: r[1],
          std_deviation: r[2] ?? 0,
          ci_lower: r[3] ?? r[1],
          ci_upper: r[4] ?? r[1],
          min_sample_size: r[6],
          density_g_per_ml: r[7],
          whole_unit:
            r[8] !== null && r[9] !== null
              ? { name: r[8], grams: r[9] }
              : null,
        };
        out.push(ingredient);
      }
      return out;
    } finally {
      stmt.free();
    }
  }

  getVariantMembers(
    variantId: string,
  ): Array<{ recipeId: string; url: string | null; title: string | null; corpus: string; sourceType: string; outlierScore: number | null }> {
    const stmt = this.db.prepare(
      "SELECT r.recipe_id, r.url, r.title, r.corpus, r.source_type," +
        " m.outlier_score" +
        " FROM variant_members m" +
        " JOIN recipes r ON r.recipe_id = m.recipe_id" +
        " WHERE m.variant_id = ?" +
        " ORDER BY m.outlier_score IS NULL, m.outlier_score ASC",
    );
    try {
      stmt.bind([variantId]);
      const out: Array<{
        recipeId: string;
        url: string | null;
        title: string | null;
        corpus: string;
        sourceType: string;
        outlierScore: number | null;
      }> = [];
      while (stmt.step()) {
        const r = stmt.get() as [
          string,
          string | null,
          string | null,
          string,
          string | null,
          number | null,
        ];
        out.push({
          recipeId: r[0],
          url: r[1],
          title: r[2],
          corpus: r[3],
          sourceType: r[4] ?? "url",
          outlierScore: r[5],
        });
      }
      return out;
    } finally {
      stmt.free();
    }
  }

  getVariantSources(variantId: string): CatalogSource[] {
    const stmt = this.db.prepare(
      "SELECT source_type, title, ref FROM variant_sources" +
        " WHERE variant_id = ? ORDER BY ordinal ASC",
    );
    try {
      stmt.bind([variantId]);
      const out: CatalogSource[] = [];
      while (stmt.step()) {
        const r = stmt.get() as [string, string | null, string];
        const s: CatalogSource = { type: r[0] as CatalogSource["type"], ref: r[2] };
        if (r[1]) s.title = r[1];
        out.push(s);
      }
      return out;
    } finally {
      stmt.free();
    }
  }

  categories(): string[] {
    const stmt = this.db.prepare(
      "SELECT DISTINCT category FROM variants WHERE category IS NOT NULL" +
        " ORDER BY category ASC",
    );
    try {
      const out: string[] = [];
      while (stmt.step()) {
        const [c] = stmt.get() as [string];
        out.push(c);
      }
      return out;
    } finally {
      stmt.free();
    }
  }

  toCatalog(metadata?: CatalogMetadata): Catalog {
    const recipes: CuratedRecipe[] = [];
    for (const summary of this.listVariants()) {
      const recipe = this.getVariant(summary.id);
      if (recipe !== null) recipes.push(recipe);
    }
    const catalog: Catalog = { version: 1, recipes };
    if (metadata) catalog.metadata = metadata;
    return catalog;
  }
}

export async function loadCatalogRepo(): Promise<CatalogRepo> {
  const { loadRecipesDb } = await import("./db.ts");
  const db = await loadRecipesDb();
  return new CatalogRepo(db);
}
