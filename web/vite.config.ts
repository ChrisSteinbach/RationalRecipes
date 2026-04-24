import { defineConfig } from "vite";

// VITE_BASE lets the deploy workflow target a sub-path
// (e.g. "/RationalRecipes/" for GitHub Pages project URLs). Default is
// "/" for root-domain deploys and local dev.
const base = process.env.VITE_BASE ?? "/";

export default defineConfig({
  base,
});
