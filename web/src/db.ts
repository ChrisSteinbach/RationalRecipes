// Loads the ingredients SQLite database into sql.js in the browser.
//
// sql.js ships a WebAssembly build of SQLite. We import the .wasm file as
// a Vite asset URL so it gets fingerprinted and served correctly in both
// dev and production, then point sql.js at it via `locateFile`.
import initSqlJs, { type Database } from "sql.js";
import sqlWasmUrl from "sql.js/dist/sql-wasm.wasm?url";

const DB_PATH = "/ingredients.db";

export async function loadIngredientsDb(): Promise<Database> {
  const SQL = await initSqlJs({ locateFile: () => sqlWasmUrl });

  const response = await fetch(DB_PATH);
  if (!response.ok) {
    throw new Error(
      `Failed to fetch ${DB_PATH}: ${response.status} ${response.statusText}`,
    );
  }
  const bytes = new Uint8Array(await response.arrayBuffer());
  return new SQL.Database(bytes);
}
