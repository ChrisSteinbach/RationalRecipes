# Drops

Per-drop artifacts for the recipe-drops pivot
(see [`../design/recipe-drops.md`](../design/recipe-drops.md)).

## Layout

```
docs/drops/
  README.md                       # this file
  <bead-id>-<slug>/
    drop.md                       # the published recipe (markdown)
    friction-journal.md           # what was friction during this drop
    notes/                        # optional working notes
```

The bead-id prefix on each subdirectory ties the artifact back to the
beads tracker (`bd show <id>`). For drops shipped after the pivot
validates, the bead-id may stop being prefixed once a stable cadence
exists.

## What lives here vs. elsewhere

- **The published recipe** lives here and (eventually) on the
  canonical home (per `RationalRecipes-z9cz`).
- **The social post text** is short and might just live in the
  drop's `drop.md` as a section, or alongside as `social.md` if it
  warrants its own file.
- **The friction journal** is the value the hand-cycle preserves;
  keeps the loop closed between "what was annoying" and "what we
  should build."
- **Ingredients DB / source recipes / parsed cache** live in
  `output/catalog/recipes.db` (gitignored). Drop artifacts here
  should be self-contained: if you have to re-derive a quantity, it
  should be possible from the source URLs listed in the drop.

## Status

Empty until `RationalRecipes-ehe7` (chocolate chip cookies hand-cycle)
completes. That entry is the pivot validation.
