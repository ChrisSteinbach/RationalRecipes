// RationalRecipes service worker.
//
// Two-layer cache strategy:
//
// 1. Precache (install): app shell — /, /index.html, /manifest.webmanifest,
//    /favicon.svg. Tiny; grabbed at install so the first offline open works
//    immediately. Cached under a versioned name; activate clears older
//    versions.
//
// 2. Runtime cache (fetch): network-first for /catalog.json and
//    /curated_recipes.json (so a fresh pipeline export is picked up on
//    the next online load) and cache-first for everything else
//    same-origin (fingerprinted JS/CSS, SVGs). Responses are cached on
//    success.
//
// The cache version must bump whenever the precache list changes or
// the asset set changes (e.g. the y43 sql.js retirement).

const CACHE_VERSION = "v2";
const PRECACHE = `rr-precache-${CACHE_VERSION}`;
const RUNTIME = `rr-runtime-${CACHE_VERSION}`;

// Scope-relative — derived from the service worker's registration scope
// so this file works at both "/" (root domain) and "/RationalRecipes/"
// (GitHub Pages project URL) without edits.
const BASE = new URL("./", self.registration.scope).pathname;

const APP_SHELL = [
  BASE,
  `${BASE}index.html`,
  `${BASE}manifest.webmanifest`,
  `${BASE}favicon.svg`,
];

// Same-origin paths that should always go network-first so a new
// pipeline export overrides the cached copy as soon as online.
const NETWORK_FIRST = new Set([
  `${BASE}catalog.json`,
  `${BASE}curated_recipes.json`,
]);

self.addEventListener("install", (event) => {
  event.waitUntil(
    (async () => {
      const cache = await caches.open(PRECACHE);
      await cache.addAll(APP_SHELL);
      // Take over active clients as soon as activation finishes.
      await self.skipWaiting();
    })(),
  );
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    (async () => {
      const names = await caches.keys();
      await Promise.all(
        names
          .filter((n) => n !== PRECACHE && n !== RUNTIME)
          .map((n) => caches.delete(n)),
      );
      await self.clients.claim();
    })(),
  );
});

self.addEventListener("fetch", (event) => {
  const req = event.request;
  if (req.method !== "GET") return;

  const url = new URL(req.url);
  if (url.origin !== self.location.origin) return;

  // Only handle requests inside the SW's scope.
  if (!url.pathname.startsWith(BASE)) return;

  if (NETWORK_FIRST.has(url.pathname)) {
    event.respondWith(networkFirst(req));
  } else {
    event.respondWith(cacheFirst(req));
  }
});

async function cacheFirst(request) {
  const cached = await caches.match(request);
  if (cached) return cached;
  try {
    const response = await fetch(request);
    if (response.ok && response.status === 200) {
      const cache = await caches.open(RUNTIME);
      cache.put(request, response.clone());
    }
    return response;
  } catch (err) {
    // Last-resort fallback for navigations: serve the shell's index.
    if (request.mode === "navigate") {
      const fallback = await caches.match(`${BASE}index.html`);
      if (fallback) return fallback;
    }
    throw err;
  }
}

async function networkFirst(request) {
  try {
    const response = await fetch(request);
    if (response.ok) {
      const cache = await caches.open(RUNTIME);
      cache.put(request, response.clone());
    }
    return response;
  } catch (err) {
    const cached = await caches.match(request);
    if (cached) return cached;
    throw err;
  }
}
