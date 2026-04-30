// Generate web/public/manifest.webmanifest at build time with the
// correct base path. Vite's `base` is passed via the VITE_BASE env var;
// defaults to "/".
//
// The manifest's start_url, scope, and icon src must resolve against
// the deployment root (e.g. "/RationalRecipes/" on GitHub Pages project
// URLs). Keeping this in a pre-build script avoids a runtime
// substitution pass and keeps the webmanifest discoverable as a plain
// static file.
import { writeFileSync, mkdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const __dirname = dirname(fileURLToPath(import.meta.url));
const outDir = join(__dirname, "..", "public");
mkdirSync(outDir, { recursive: true });

let base = process.env.VITE_BASE ?? "/";
if (!base.endsWith("/")) base += "/";

const manifest = {
  name: "RationalRecipes",
  short_name: "RationalRecipes",
  description:
    "Averaged recipe ratios with confidence intervals for serious bakers.",
  start_url: base,
  scope: base,
  display: "standalone",
  background_color: "#faf9f6",
  theme_color: "#8b5e3c",
  icons: [
    {
      src: `${base}favicon.svg`,
      sizes: "any",
      type: "image/svg+xml",
      purpose: "any",
    },
    {
      src: `${base}favicon.svg`,
      sizes: "any",
      type: "image/svg+xml",
      purpose: "maskable",
    },
  ],
  categories: ["food", "lifestyle", "utilities"],
};

const path = join(outDir, "manifest.webmanifest");
writeFileSync(path, JSON.stringify(manifest, null, 2) + "\n");
console.log(`Wrote ${path} (base=${base})`);
