// Node smoke test: verifies sql.js can load and query the ingredients DB
// using the same SQL the browser code runs. The Vite-specific ?url import
// for the .wasm file is separately validated by `npm run build`.
import initSqlJs from "sql.js";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const __dirname = dirname(fileURLToPath(import.meta.url));
const dbPath = join(__dirname, "..", "public", "ingredients.db");

const SQL = await initSqlJs();
const db = new SQL.Database(new Uint8Array(readFileSync(dbPath)));

for (const table of ["food", "synonym", "density", "portion"]) {
  const n = db.exec(`SELECT COUNT(*) FROM ${table}`)[0].values[0][0];
  console.log(`${table}: ${n}`);
}

for (const name of ["all purpose flour", "water"]) {
  const rows = db.exec(
    `SELECT f.name, d.g_per_ml
     FROM synonym s
     JOIN food f ON f.id = s.food_id
     LEFT JOIN density d ON d.food_id = f.id
     WHERE s.name = ? COLLATE NOCASE
     LIMIT 1`,
    [name],
  );
  if (rows.length === 0) {
    console.log(`${name}: NOT FOUND`);
  } else {
    const [food, density] = rows[0].values[0];
    console.log(`${name}: ${food} (${density ?? "no density"} g/ml)`);
  }
}
