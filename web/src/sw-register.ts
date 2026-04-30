// Service worker registration.
//
// Registered at window `load` so it doesn't compete with the initial
// app render. Failures are logged but non-fatal — the app works without
// offline support, just without the caching layer.

export function registerServiceWorker(): void {
  if (!("serviceWorker" in navigator)) return;

  const base = import.meta.env.BASE_URL;
  window.addEventListener("load", () => {
    navigator.serviceWorker
      .register(`${base}sw.js`, { scope: base })
      .catch((err: unknown) => {
        console.warn("Service worker registration failed:", err);
      });
  });
}
