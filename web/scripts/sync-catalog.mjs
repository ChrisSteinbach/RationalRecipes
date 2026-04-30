// Copy the catalog artifact(s) from artifacts/ into the Vite public/
// directory so `npm run dev` and `npm run build` can serve them.
//
// Default source is SQLite: artifacts/recipes.db → public/recipes.db.
// The JSON file (curated_recipes.json) ships as a dev fallback for
// ?source=json. Pass --source=json to copy only the JSON path.
//
// The artifacts live outside web/ because the Python side owns them
// (scripts/migrate_curated_to_db.py writes recipes.db from the
// historical curated_recipes.json seed). Re-run this after rebuilding.
import { copyFileSync, existsSync, mkdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const __dirname = dirname(fileURLToPath(import.meta.url));
const repoRoot = join(__dirname, "..", "..");
const destDir = join(__dirname, "..", "public");
mkdirSync(destDir, { recursive: true });

const args = new Set(process.argv.slice(2));
const source = [...args]
  .find((a) => a.startsWith("--source="))
  ?.split("=")[1] ?? "db";

function copy(srcName, destName, options = {}) {
  // Pipeline output is the primary source; fall back to artifacts/
  // for legacy paths (e.g. curated_recipes.json).
  const primary = join(repoRoot, "output", "catalog", srcName);
  const fallback = join(repoRoot, "artifacts", srcName);
  const src = existsSync(primary) ? primary : fallback;
  const dest = join(destDir, destName);
  if (!existsSync(src)) {
    if (options.optional) {
      console.warn(`Optional source missing (checked ${primary} and ${fallback}) (skipping)`);
      return false;
    }
    console.error(`Required source not found (checked ${primary} and ${fallback})`);
    if (srcName === "recipes.db") {
      console.error(
        "Run `python3 scripts/scrape_catalog.py` from the repo root first.",
      );
    }
    process.exit(1);
  }
  copyFileSync(src, dest);
  console.log(`Copied ${src} → ${dest}`);
  return true;
}

if (source === "json") {
  copy("curated_recipes.json", "curated_recipes.json");
} else {
  copy("recipes.db", "recipes.db");
  copy("curated_recipes.json", "curated_recipes.json", { optional: true });
}
