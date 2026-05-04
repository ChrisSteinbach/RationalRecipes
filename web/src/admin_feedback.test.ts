import { beforeEach, describe, expect, it } from "vitest";
import type { CuratedRecipe } from "./catalog.ts";
import {
  type FeedbackStorage,
  STORAGE_KEY,
  clearAll,
  deleteEntry,
  exportJSON,
  exportMarkdown,
  formatLastEdited,
  getEntry,
  groupByTag,
  loadFeedback,
  resolveEntries,
  saveEntry,
  sortEntries,
} from "./admin_feedback.ts";

function memoryStorage(seed: Record<string, string> = {}): FeedbackStorage {
  const map = new Map<string, string>(Object.entries(seed));
  return {
    getItem: (k) => map.get(k) ?? null,
    setItem: (k, v) => {
      map.set(k, v);
    },
    removeItem: (k) => {
      map.delete(k);
    },
  };
}

function aRecipe(overrides: Partial<CuratedRecipe> = {}): CuratedRecipe {
  return {
    id: "ea4f8312ce2d",
    title: "Flour Potato",
    category: "side",
    base_ingredient: "potato",
    sample_size: 169,
    ingredients: [],
    ...overrides,
  };
}

let storage: FeedbackStorage;

beforeEach(() => {
  storage = memoryStorage();
});

describe("loadFeedback", () => {
  it("returns [] when nothing is stored", () => {
    expect(loadFeedback(storage)).toEqual([]);
  });

  it("returns [] for malformed JSON without throwing", () => {
    storage.setItem(STORAGE_KEY, "{not json");
    expect(loadFeedback(storage)).toEqual([]);
  });

  it("returns [] for a payload without an entries map", () => {
    storage.setItem(STORAGE_KEY, JSON.stringify({ version: 1 }));
    expect(loadFeedback(storage)).toEqual([]);
  });

  it("coerces unknown tags to 'Other' so an old payload still loads", () => {
    storage.setItem(
      STORAGE_KEY,
      JSON.stringify({
        version: 1,
        entries: {
          a: { tag: "Bogus", notes: "n", updatedAt: "2026-05-04T00:00:00.000Z" },
        },
      }),
    );
    const entries = loadFeedback(storage);
    expect(entries).toHaveLength(1);
    expect(entries[0].tag).toBe("Other");
  });
});

describe("saveEntry / round-trip", () => {
  it("persists an entry and reads it back via loadFeedback", () => {
    const fixedNow = new Date("2026-05-04T12:34:56.000Z");
    const saved = saveEntry(
      { variantId: "abc", tag: "Title issue", notes: "Pass 3 dropped 'scalloped'" },
      storage,
      () => fixedNow,
    );
    expect(saved).toEqual({
      variantId: "abc",
      tag: "Title issue",
      notes: "Pass 3 dropped 'scalloped'",
      updatedAt: "2026-05-04T12:34:56.000Z",
    });
    expect(loadFeedback(storage)).toEqual([saved]);
  });

  it("replaces an existing entry rather than duplicating", () => {
    saveEntry({ variantId: "a", tag: "Other", notes: "first" }, storage);
    saveEntry({ variantId: "a", tag: "Ratio issue", notes: "second" }, storage);
    const entries = loadFeedback(storage);
    expect(entries).toHaveLength(1);
    expect(entries[0].notes).toBe("second");
    expect(entries[0].tag).toBe("Ratio issue");
  });

  it("treats an empty-notes 'Other' save as a delete", () => {
    saveEntry({ variantId: "a", tag: "Other", notes: "first" }, storage);
    saveEntry({ variantId: "a", tag: "Other", notes: "  \n " }, storage);
    expect(loadFeedback(storage)).toEqual([]);
  });

  it("keeps an entry when the notes are empty but the tag is set", () => {
    saveEntry({ variantId: "a", tag: "Looks great", notes: "" }, storage);
    const entries = loadFeedback(storage);
    expect(entries).toHaveLength(1);
    expect(entries[0].tag).toBe("Looks great");
  });

  it("getEntry round-trips a single variant", () => {
    saveEntry({ variantId: "a", tag: "Other", notes: "hello" }, storage);
    expect(getEntry("a", storage)?.notes).toBe("hello");
    expect(getEntry("missing", storage)).toBeNull();
  });

  it("storage payload has stable shape (version + entries map)", () => {
    saveEntry({ variantId: "a", tag: "Other", notes: "x" }, storage);
    const raw = JSON.parse(storage.getItem(STORAGE_KEY) as string) as {
      version: number;
      entries: Record<string, unknown>;
    };
    expect(raw.version).toBe(1);
    expect(Object.keys(raw.entries)).toEqual(["a"]);
  });
});

describe("dedup on save", () => {
  it("entries map is keyed by variantId so a re-save can never duplicate", () => {
    saveEntry({ variantId: "a", tag: "Other", notes: "1" }, storage);
    saveEntry({ variantId: "a", tag: "Other", notes: "2" }, storage);
    saveEntry({ variantId: "a", tag: "Other", notes: "3" }, storage);
    saveEntry({ variantId: "b", tag: "Other", notes: "x" }, storage);
    expect(loadFeedback(storage)).toHaveLength(2);
  });
});

describe("deleteEntry / clearAll", () => {
  it("deleteEntry removes a single variant", () => {
    saveEntry({ variantId: "a", tag: "Other", notes: "x" }, storage);
    saveEntry({ variantId: "b", tag: "Other", notes: "y" }, storage);
    deleteEntry("a", storage);
    const remaining = loadFeedback(storage);
    expect(remaining.map((e) => e.variantId)).toEqual(["b"]);
  });

  it("clearAll wipes the storage key entirely", () => {
    saveEntry({ variantId: "a", tag: "Other", notes: "x" }, storage);
    clearAll(storage);
    expect(storage.getItem(STORAGE_KEY)).toBeNull();
    expect(loadFeedback(storage)).toEqual([]);
  });
});

describe("resolveEntries / groupByTag / sortEntries", () => {
  function entry(
    overrides: Partial<{
      variantId: string;
      tag:
        | "Title issue"
        | "Ratio issue"
        | "Looks great"
        | "Other";
      notes: string;
      updatedAt: string;
    }> = {},
  ) {
    return {
      variantId: "a",
      tag: "Other" as const,
      notes: "",
      updatedAt: "2026-05-04T00:00:00.000Z",
      ...overrides,
    };
  }

  it("marks variants missing from the catalog as orphaned", () => {
    const recipes = new Map([["a", aRecipe({ id: "a" })]]);
    const resolved = resolveEntries(
      [entry({ variantId: "a" }), entry({ variantId: "missing" })],
      recipes,
    );
    expect(resolved[0].orphaned).toBe(false);
    expect(resolved[1].orphaned).toBe(true);
  });

  it("groupByTag emits groups in canonical FEEDBACK_TAGS order", () => {
    const recipes = new Map([["a", aRecipe({ id: "a" })]]);
    const resolved = resolveEntries(
      [
        entry({ variantId: "a", tag: "Looks great" }),
        entry({ variantId: "a", tag: "Title issue" }),
      ],
      recipes,
    );
    const groups = groupByTag(resolved);
    expect(groups.map((g) => g.tag)).toEqual(["Title issue", "Looks great"]);
  });

  it("groupByTag drops empty groups", () => {
    const groups = groupByTag([]);
    expect(groups).toEqual([]);
  });

  it("sortEntries by sampleSize prefers higher counts", () => {
    const big = aRecipe({ id: "big", sample_size: 200 });
    const small = aRecipe({ id: "small", sample_size: 5 });
    const recipes = new Map([
      ["big", big],
      ["small", small],
    ]);
    const resolved = resolveEntries(
      [entry({ variantId: "small" }), entry({ variantId: "big" })],
      recipes,
    );
    const sorted = sortEntries(resolved, "sampleSize");
    expect(sorted.map((e) => e.variantId)).toEqual(["big", "small"]);
  });

  it("sortEntries by title is alphabetical", () => {
    const recipes = new Map([
      ["a", aRecipe({ id: "a", title: "Boule" })],
      ["b", aRecipe({ id: "b", title: "Apple Pie" })],
    ]);
    const resolved = resolveEntries(
      [entry({ variantId: "a" }), entry({ variantId: "b" })],
      recipes,
    );
    const sorted = sortEntries(resolved, "title");
    expect(sorted.map((e) => e.variantId)).toEqual(["b", "a"]);
  });

  it("sortEntries by updatedAt is most-recent-first", () => {
    const recipes = new Map([["a", aRecipe({ id: "a" })]]);
    const resolved = resolveEntries(
      [
        entry({
          variantId: "a",
          updatedAt: "2026-04-01T00:00:00.000Z",
        }),
        entry({
          variantId: "z",
          updatedAt: "2026-05-04T00:00:00.000Z",
        }),
      ],
      recipes,
    );
    const sorted = sortEntries(resolved, "updatedAt");
    expect(sorted.map((e) => e.variantId)).toEqual(["z", "a"]);
  });
});

describe("exportMarkdown", () => {
  it("renders an empty placeholder when no entries", () => {
    const out = exportMarkdown([], { date: new Date("2026-05-04T00:00:00Z") });
    expect(out).toContain("# RationalRecipes feedback — 2026-05-04 (0 entries)");
    expect(out).toContain("_No entries._");
  });

  it("groups by tag with counts and renders one bullet per entry", () => {
    const recipes = new Map([
      [
        "ea4f8312ce2d12345",
        aRecipe({
          id: "ea4f8312ce2d12345",
          title: "Flour Potato",
          category: "scalloped potatoes",
          sample_size: 169,
        }),
      ],
    ]);
    const resolved = resolveEntries(
      [
        {
          variantId: "ea4f8312ce2d12345",
          tag: "Title issue",
          notes: "Should be \"Flour Scalloped Potatoes\" — Pass 3 dropped \"scalloped\"",
          updatedAt: "2026-05-04T00:00:00.000Z",
        },
      ],
      recipes,
    );
    const out = exportMarkdown(resolved, { date: new Date("2026-05-04T00:00:00Z") });
    expect(out).toContain("# RationalRecipes feedback — 2026-05-04 (1 entry)");
    expect(out).toContain("## Title issue (1)");
    expect(out).toContain(
      "- **Flour Potato** (`ea4f8312ce2d`, scalloped potatoes, 169 sources)",
    );
    expect(out).toContain(
      "  Should be \"Flour Scalloped Potatoes\" — Pass 3 dropped \"scalloped\"",
    );
  });

  it("uses singular wording for sample_size==1", () => {
    const recipes = new Map([
      ["a", aRecipe({ id: "a", title: "Lonely", sample_size: 1 })],
    ]);
    const resolved = resolveEntries(
      [
        {
          variantId: "a",
          tag: "Other",
          notes: "",
          updatedAt: "2026-05-04T00:00:00.000Z",
        },
      ],
      recipes,
    );
    const out = exportMarkdown(resolved);
    expect(out).toMatch(/1 source\b/);
  });

  it("places orphaned entries in a trailing 'unknown variant' section", () => {
    const resolved = resolveEntries(
      [
        {
          variantId: "missing",
          tag: "Title issue",
          notes: "old note",
          updatedAt: "2026-05-04T00:00:00.000Z",
        },
      ],
      new Map(),
    );
    const out = exportMarkdown(resolved, {
      date: new Date("2026-05-04T00:00:00Z"),
    });
    expect(out).toContain("Orphaned (variant_id no longer in catalog) (1)");
    expect(out).toContain("- **(unknown variant)** (`missing`)");
    // No regular Title issue section should appear, since the only entry
    // is orphaned.
    expect(out).not.toContain("## Title issue");
  });

  it("renders multi-line notes as continuation lines", () => {
    const recipes = new Map([["a", aRecipe({ id: "a" })]]);
    const resolved = resolveEntries(
      [
        {
          variantId: "a",
          tag: "Other",
          notes: "first line\nsecond line",
          updatedAt: "2026-05-04T00:00:00.000Z",
        },
      ],
      recipes,
    );
    const out = exportMarkdown(resolved);
    expect(out).toContain("  first line");
    expect(out).toContain("  second line");
  });
});

describe("exportJSON", () => {
  it("emits a versioned payload with stable key ordering", () => {
    const json = exportJSON([
      {
        variantId: "b",
        tag: "Other",
        notes: "x",
        updatedAt: "2026-05-04T00:00:00.000Z",
      },
      {
        variantId: "a",
        tag: "Title issue",
        notes: "y",
        updatedAt: "2026-05-03T00:00:00.000Z",
      },
    ]);
    const parsed = JSON.parse(json) as {
      version: number;
      entries: Record<string, { tag: string; notes: string; updatedAt: string }>;
    };
    expect(parsed.version).toBe(1);
    expect(Object.keys(parsed.entries)).toEqual(["a", "b"]);
    expect(parsed.entries.a.tag).toBe("Title issue");
  });

  it("round-trips through loadFeedback when the same payload is restored", () => {
    saveEntry({ variantId: "x", tag: "Ratio issue", notes: "n" }, storage);
    const original = loadFeedback(storage);
    const json = exportJSON(original);
    const fresh = memoryStorage({ [STORAGE_KEY]: json });
    expect(loadFeedback(fresh)).toEqual(original);
  });

  it("emits an empty payload when entries is empty", () => {
    expect(JSON.parse(exportJSON([]))).toEqual({ version: 1, entries: {} });
  });
});

describe("formatLastEdited", () => {
  it("returns 'never' for an unparseable input", () => {
    expect(formatLastEdited("not-a-date")).toBe("never");
  });

  it("returns a non-empty string for a valid ISO timestamp", () => {
    expect(formatLastEdited("2026-05-04T12:00:00Z")).not.toBe("");
  });
});
