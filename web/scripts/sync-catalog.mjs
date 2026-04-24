// Copy the canonical CuratedRecipeCatalog JSON from artifacts/ into the
// Vite public/ directory so `npm run dev` and `npm run build` can serve
// it at /curated_recipes.json.
//
// The source of truth lives outside web/ because the Python pipeline
// emits it (scripts/export_curated_recipes.py or merged_to_catalog.py)
// and the PWA is a consumer. Re-run this after regenerating the
// artifact.
import { copyFileSync, existsSync, mkdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const __dirname = dirname(fileURLToPath(import.meta.url));
const src = join(__dirname, "..", "..", "artifacts", "curated_recipes.json");
const destDir = join(__dirname, "..", "public");
const dest = join(destDir, "curated_recipes.json");

if (!existsSync(src)) {
  console.error(`Catalog not found at ${src}`);
  console.error(
    "Run `python3 scripts/export_curated_recipes.py` from the repo root first.",
  );
  process.exit(1);
}

mkdirSync(destDir, { recursive: true });
copyFileSync(src, dest);
console.log(`Copied ${src} → ${dest}`);
