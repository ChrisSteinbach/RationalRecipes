// Copy the catalog artifact(s) from artifacts/ or output/catalog/ into
// the Vite public/ directory so `npm run dev` and `npm run build` can
// serve them.
//
// Since RationalRecipes-y43 the catalog ships as a static JSON manifest
// (catalog.json) — sql.js is gone. Source of truth is
// `output/catalog/catalog.json`, written by
// `scripts/export_catalog_json.py` after the pipeline finishes.
//
// `curated_recipes.json` still ships as a small dev/seed asset so
// designers can iterate on the UI without rebuilding the catalog.
import {
  copyFileSync,
  existsSync,
  mkdirSync,
  readdirSync,
  rmSync,
  statSync,
} from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const __dirname = dirname(fileURLToPath(import.meta.url));
const repoRoot = join(__dirname, "..", "..");
const destDir = join(__dirname, "..", "public");
mkdirSync(destDir, { recursive: true });

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
    if (srcName === "catalog.json") {
      console.error(
        "Run `python3 scripts/export_catalog_json.py` from the repo root first.",
      );
    }
    process.exit(1);
  }
  copyFileSync(src, dest);
  console.log(`Copied ${src} → ${dest}`);
  return true;
}

copy("catalog.json", "catalog.json");
copy("curated_recipes.json", "curated_recipes.json", { optional: true });
mirrorSourcesDir();

// Per-variant sidecar JSON files for the detail view's 'Source recipes'
// section (bead zh6). The catalog manifest itself doesn't carry the
// per-source ingredient lists — they live in <variant_id>.json files
// and are lazy-fetched on user expand. We mirror the whole directory
// rather than copying file-by-file so removed variants don't linger.
function mirrorSourcesDir() {
  const srcDir = join(repoRoot, "output", "catalog", "sources");
  const dstDir = join(destDir, "sources");
  if (!existsSync(srcDir) || !statSync(srcDir).isDirectory()) {
    console.warn(`Sources sidecar dir missing (${srcDir}) (skipping)`);
    return;
  }
  if (existsSync(dstDir)) rmSync(dstDir, { recursive: true, force: true });
  mkdirSync(dstDir, { recursive: true });
  const entries = readdirSync(srcDir);
  let copied = 0;
  for (const name of entries) {
    if (!name.endsWith(".json")) continue;
    copyFileSync(join(srcDir, name), join(dstDir, name));
    copied += 1;
  }
  console.log(`Mirrored ${copied} source sidecar(s) → ${dstDir}`);
}
