import { loadIngredientsDb } from "./db.ts";

type Counts = {
  foods: number;
  synonyms: number;
  densities: number;
  portions: number;
};

function countRows(db: import("sql.js").Database, table: string): number {
  const result = db.exec(`SELECT COUNT(*) FROM ${table}`);
  return result[0].values[0][0] as number;
}

function sampleLookup(
  db: import("sql.js").Database,
  name: string,
): { food: string; density?: number } | null {
  // Mirrors the Python Factory.get_by_name() lookup: match on synonym, then
  // join back to food and any density row.
  const rows = db.exec(
    `SELECT f.name, d.g_per_ml
     FROM synonym s
     JOIN food f ON f.id = s.food_id
     LEFT JOIN density d ON d.food_id = f.id
     WHERE s.name = ? COLLATE NOCASE
     LIMIT 1`,
    [name],
  );
  if (rows.length === 0) return null;
  const [foodName, density] = rows[0].values[0];
  return {
    food: foodName as string,
    density: density == null ? undefined : (density as number),
  };
}

async function main(): Promise<void> {
  const app = document.querySelector<HTMLDivElement>("#app")!;
  app.innerHTML = `<h1>RationalRecipes</h1><p id="status">Loading ingredients database…</p>`;
  const status = app.querySelector<HTMLParagraphElement>("#status")!;

  try {
    const db = await loadIngredientsDb();

    const counts: Counts = {
      foods: countRows(db, "food"),
      synonyms: countRows(db, "synonym"),
      densities: countRows(db, "density"),
      portions: countRows(db, "portion"),
    };

    const flour = sampleLookup(db, "all purpose flour");
    const water = sampleLookup(db, "water");

    status.innerHTML = `
      <strong>Database loaded.</strong>
      ${counts.foods.toLocaleString()} foods,
      ${counts.synonyms.toLocaleString()} synonyms,
      ${counts.densities.toLocaleString()} densities,
      ${counts.portions.toLocaleString()} portions.
      <br>Sample lookups —
      all-purpose flour: <code>${flour ? `${flour.food} (${flour.density ?? "no density"} g/ml)` : "not found"}</code>,
      water: <code>${water ? `${water.food} (${water.density ?? "no density"} g/ml)` : "not found"}</code>
    `;

    // Expose for ad-hoc console poking during development.
    (window as unknown as { db: typeof db }).db = db;
  } catch (err) {
    status.textContent = `Failed to load database: ${(err as Error).message}`;
    throw err;
  }
}

void main();
