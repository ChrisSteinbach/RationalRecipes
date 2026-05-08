# Bluesky thread sketch — Chocolate Chip Cookies drop

> Sketch only. Posts are sized for Bluesky's 300-char limit but
> formatting (line breaks, link previews) hasn't been validated in a
> real client. The canonical-home link in post 5 is a placeholder
> pending RationalRecipes-z9cz.

---

## Post 1 — Hook

> Chocolate Chip Cookies — averaged across 98 independent recipes.
>
> Mean ratios (per kg of dough):
> 32% flour · 21% chocolate chips · 14% brown sugar · 12% margarine ·
> 9% egg · 9% sugar · 0.5% each of salt + soda + vanilla
>
> The averaged-recipe methodology, applied. 🧵

(280 chars)

## Post 2 — Per-kilo recipe

> Per 1 kg of dough, the 95%-CI averaged amounts:
>
> · 324 g flour
> · 213 g chocolate chips
> · 139 g brown sugar
> · 125 g margarine
> · 95 g sugar
> · 89 g egg (≈ 2 large)
> · 6 g vanilla
> · 5 g salt
> · 5 g baking soda

(245 chars)

## Post 3 — Method (median-source verbatim)

> Method, taken verbatim from the most-central source in the cluster:
>
> 1. Cream margarine, shortening, and sugar.
> 2. Add eggs and vanilla, then dry ingredients. Chocolate chips last.
> 3. Bake at 350°F (175°C) for 10–12 minutes.
>
> Brief by design — see thread end.

(269 chars)

## Post 4 — Caveat

> Caveats:
>
> · High variance on fat type (margarine / shortening / butter all
>   appear in the cluster — pick what you have)
> · Chip volume: 21% is generous, common in older recipes
> · 95% CIs in the canonical post if you want the error bars

(255 chars)

## Post 5 — Methodology + canonical home

> How: I averaged 98 independent CCC recipes from RecipeNLG + WDC,
> mass-normalized to per-100g of batch, dropped outliers, computed
> 95% CIs.
>
> Full numbers, sources, methodology:
> [canonical home — pending RationalRecipes-z9cz]
>
> Code: github.com/ChrisSteinbach/RationalRecipes

(287 chars)

---

## Notes for the editor

- **Tone**: posts 1–4 are utilitarian; post 5 is the "trust establishment"
  + traffic post. Adjust to taste.
- **Per-1kg-of-dough framing** in post 2 is concrete; mass percentages
  are abstract for most readers. Picked 1 kg because it produces a
  recognizable batch size (~36 cookies).
- **Method post (3) is terse** because the median source's instructions
  are terse (F10 in the friction journal). For a wider audience consider:
  - Pulling instructions from id=1017218 or id=883123 (more detailed
    top-5 sources) instead.
  - Lightly expanding with universals (preheat oven, drop by teaspoonfuls,
    cool on wire rack).
- **Engagement hook**: the variance numbers in post 4 are the most
  novel part of the drop — bakers know the recipe varies but seeing
  CV=110% on margarine is concrete. Could lead the thread with
  variance instead of means if testing engagement.
- **Image**: a single chart showing the 9 ingredients with error bars
  could replace post 2 entirely and save ~245 chars for narration
  elsewhere.
