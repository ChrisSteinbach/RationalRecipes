// SQLite loaders for the two client-side databases used by the PWA.
//
// Both databases ship as static assets under the Vite base URL:
// - ingredients.db: USDA/FAO ingredient reference (densities, portions).
// - recipes.db: the variant catalog written by CatalogDB (bead vwt.6).
//
// sql.js ships a WebAssembly build of SQLite. The .wasm file is imported
// as a Vite asset URL so it gets fingerprinted and served correctly in
// both dev and production, then passed to initSqlJs via `locateFile`.
import initSqlJs, { type Database } from "sql.js";
import sqlWasmUrl from "sql.js/dist/sql-wasm.wasm?url";

const INGREDIENTS_PATH = `${import.meta.env.BASE_URL}ingredients.db`;
const RECIPES_PATH = `${import.meta.env.BASE_URL}recipes.db`;

let sqlReady: Promise<Awaited<ReturnType<typeof initSqlJs>>> | null = null;

function getSql() {
  if (sqlReady === null) {
    sqlReady = initSqlJs({ locateFile: () => sqlWasmUrl });
  }
  return sqlReady;
}

async function loadDbFromUrl(path: string): Promise<Database> {
  const SQL = await getSql();
  const response = await fetch(path);
  if (!response.ok) {
    throw new Error(
      `Failed to fetch ${path}: ${response.status} ${response.statusText}`,
    );
  }
  const bytes = new Uint8Array(await response.arrayBuffer());
  return new SQL.Database(bytes);
}

export async function loadIngredientsDb(): Promise<Database> {
  return loadDbFromUrl(INGREDIENTS_PATH);
}

export async function loadRecipesDb(): Promise<Database> {
  return loadDbFromUrl(RECIPES_PATH);
}

// Test-only helper: build an in-memory sql.js Database from a byte buffer
// or from nothing (applies the recipes schema so tests can seed rows).
export async function databaseFromBytes(
  bytes: Uint8Array | null = null,
): Promise<Database> {
  const SQL = await getSql();
  return bytes ? new SQL.Database(bytes) : new SQL.Database();
}
