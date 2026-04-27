// @vitest-environment jsdom
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { CuratedRecipe } from "./catalog.ts";
import {
  WEIGHT_PRESETS,
  type DetailViewCallbacks,
  initialDetailState,
  renderDetail,
} from "./detail_view.ts";

function aRecipe(overrides: Partial<CuratedRecipe> = {}): CuratedRecipe {
  return {
    id: "swedish-pancakes",
    title: "Swedish Pancakes",
    category: "crepes",
    description: "Thin Scandinavian pancakes.",
    base_ingredient: "flour",
    sample_size: 200,
    ingredients: [
      {
        name: "flour",
        ratio: 1.0,
        proportion: 0.16,
        std_deviation: 0.05,
        ci_lower: 0.15,
        ci_upper: 0.17,
      },
      {
        name: "milk",
        ratio: 3.6,
        proportion: 0.6,
        std_deviation: 0.05,
        ci_lower: 0.59,
        ci_upper: 0.61,
      },
    ],
    ...overrides,
  };
}

function noopCallbacks(
  overrides: Partial<DetailViewCallbacks> = {},
): DetailViewCallbacks {
  return {
    onBack: () => {},
    ...overrides,
  };
}

let container: HTMLElement;

beforeEach(() => {
  container = document.createElement("div");
  document.body.appendChild(container);
});

describe("initialDetailState", () => {
  it("targets 500 g by default", () => {
    expect(initialDetailState()).toEqual({ targetWeight: 500 });
  });
});

describe("renderDetail — header", () => {
  it("renders title, category/sample meta, description, back button", () => {
    renderDetail(container, aRecipe(), initialDetailState(), noopCallbacks());

    expect(container.querySelector(".detail-title")?.textContent).toBe(
      "Swedish Pancakes",
    );
    expect(container.querySelector(".detail-meta")?.textContent).toContain(
      "Category: crepes",
    );
    expect(container.querySelector(".detail-meta")?.textContent).toContain(
      "Sample size: 200",
    );
    expect(container.querySelector(".detail-description")?.textContent).toBe(
      "Thin Scandinavian pancakes.",
    );
    expect(container.querySelector<HTMLButtonElement>(".detail-back")).not.toBeNull();
  });

  it("omits description when missing", () => {
    renderDetail(
      container,
      aRecipe({ description: undefined }),
      initialDetailState(),
      noopCallbacks(),
    );
    expect(container.querySelector(".detail-description")).toBeNull();
  });

  it("back button click invokes onBack", () => {
    const onBack = vi.fn();
    renderDetail(container, aRecipe(), initialDetailState(), noopCallbacks({ onBack }));
    container.querySelector<HTMLButtonElement>(".detail-back")!.click();
    expect(onBack).toHaveBeenCalledOnce();
  });
});

describe("renderDetail — ratio line", () => {
  it("renders the baker's-percentage ratio with 2 decimals + ingredient names", () => {
    renderDetail(container, aRecipe(), initialDetailState(), noopCallbacks());
    expect(container.querySelector(".detail-ratio-value")?.textContent).toBe(
      "1.00:3.60 (flour:milk)",
    );
  });
});

describe("renderDetail — weight controls", () => {
  it("seeds the weight input from state.targetWeight", () => {
    renderDetail(
      container,
      aRecipe(),
      { targetWeight: 750 },
      noopCallbacks(),
    );
    expect(
      container.querySelector<HTMLInputElement>(".detail-weight-input")?.value,
    ).toBe("750");
  });

  it("renders one button per WEIGHT_PRESETS entry", () => {
    renderDetail(container, aRecipe(), initialDetailState(), noopCallbacks());
    const buttons = container.querySelectorAll(".weight-preset");
    expect(buttons).toHaveLength(WEIGHT_PRESETS.length);
    expect(Array.from(buttons).map((b) => b.textContent)).toEqual(
      WEIGHT_PRESETS.map((w) => `${w} g`),
    );
  });

  it("marks the preset matching the current target as active", () => {
    renderDetail(container, aRecipe(), { targetWeight: 1000 }, noopCallbacks());
    const active = container.querySelectorAll(".weight-preset-active");
    expect(active).toHaveLength(1);
    expect(active[0].textContent).toBe("1000 g");
  });

  it("clicking a preset re-renders with that preset as active", () => {
    const state = initialDetailState();
    renderDetail(container, aRecipe(), state, noopCallbacks());
    expect(container.querySelector(".weight-preset-active")?.textContent).toBe(
      "500 g",
    );

    const presets = container.querySelectorAll<HTMLButtonElement>(".weight-preset");
    const target = Array.from(presets).find((b) => b.textContent === "1000 g")!;
    target.click();

    expect(state.targetWeight).toBe(1000);
    expect(
      container.querySelector<HTMLInputElement>(".detail-weight-input")?.value,
    ).toBe("1000");
    expect(container.querySelector(".weight-preset-active")?.textContent).toBe(
      "1000 g",
    );
  });

  it("typing a positive number into the weight input re-renders", () => {
    const state = initialDetailState();
    renderDetail(container, aRecipe(), state, noopCallbacks());
    const input = container.querySelector<HTMLInputElement>(".detail-weight-input")!;
    input.value = "250";
    input.dispatchEvent(new Event("input"));
    expect(state.targetWeight).toBe(250);
    expect(container.querySelector(".weight-preset-active")?.textContent).toBe(
      "250 g",
    );
  });

  it("ignores non-positive / non-finite weight input", () => {
    const state = initialDetailState();
    renderDetail(container, aRecipe(), state, noopCallbacks());
    const input = container.querySelector<HTMLInputElement>(".detail-weight-input")!;
    input.value = "0";
    input.dispatchEvent(new Event("input"));
    input.value = "abc";
    input.dispatchEvent(new Event("input"));
    expect(state.targetWeight).toBe(500);
  });
});

describe("renderDetail — scaled ingredients", () => {
  it("renders one li per ingredient and a total line", () => {
    renderDetail(container, aRecipe(), { targetWeight: 460 }, noopCallbacks());
    const items = container.querySelectorAll(".scaled-ingredients li");
    expect(items).toHaveLength(2);
    expect(items[0].textContent).toContain("flour");
    expect(items[1].textContent).toContain("milk");
    // ratios 1.0:3.6 sum to 4.6; target=460 → flour=100g, milk=360g.
    expect(items[0].textContent).toContain("100.0g");
    expect(items[1].textContent).toContain("360.0g");
    expect(container.querySelector(".detail-total")?.textContent).toBe(
      "Total: 460.0 g",
    );
  });
});

describe("renderDetail — stats table", () => {
  it("renders a header row and one data row per ingredient", () => {
    renderDetail(container, aRecipe(), initialDetailState(), noopCallbacks());
    const headers = container.querySelectorAll(".stats-table thead th");
    expect(Array.from(headers).map((h) => h.textContent)).toEqual([
      "Ingredient",
      "Ratio",
      "Proportion",
      "95% CI",
      "Stddev",
    ]);
    const rows = container.querySelectorAll(".stats-table tbody tr");
    expect(rows).toHaveLength(2);
  });

  it("renders ingredient name + formatted ratio/proportion/CI/stddev", () => {
    renderDetail(container, aRecipe(), initialDetailState(), noopCallbacks());
    const firstRow = container.querySelectorAll<HTMLTableCellElement>(
      ".stats-table tbody tr:first-child td",
    );
    expect(firstRow[0].textContent).toBe("flour");
    expect(firstRow[1].textContent).toBe("1.000");
    expect(firstRow[2].textContent).toBe("16.00%");
    expect(firstRow[3].textContent).toBe("15.00–17.00%");
    expect(firstRow[4].textContent).toBe("5.00%");
  });
});

describe("renderDetail — sources", () => {
  it("renders no sources section when sources is empty/missing", () => {
    renderDetail(container, aRecipe(), initialDetailState(), noopCallbacks());
    expect(container.querySelector(".detail-sources")).toBeNull();
  });

  it("renders url sources as anchors with target=_blank", () => {
    const recipe = aRecipe({
      sources: [
        { type: "url", title: "King Arthur Pancakes", ref: "https://example.com/a" },
      ],
    });
    renderDetail(container, recipe, initialDetailState(), noopCallbacks());
    const anchor = container.querySelector<HTMLAnchorElement>(".source-url a");
    expect(anchor?.getAttribute("href")).toBe("https://example.com/a");
    expect(anchor?.getAttribute("target")).toBe("_blank");
    expect(anchor?.textContent).toBe("King Arthur Pancakes");
  });

  it("renders text sources inline (no anchor)", () => {
    const recipe = aRecipe({
      sources: [{ type: "text", title: "Cookbook", ref: "p. 42" }],
    });
    renderDetail(container, recipe, initialDetailState(), noopCallbacks());
    const item = container.querySelector(".source-text");
    expect(item?.querySelector("a")).toBeNull();
    expect(item?.textContent).toBe("Cookbook: p. 42");
  });

  it("section header includes the source count", () => {
    const recipe = aRecipe({
      sources: [
        { type: "url", ref: "https://a" },
        { type: "url", ref: "https://b" },
        { type: "text", ref: "Cookbook" },
      ],
    });
    renderDetail(container, recipe, initialDetailState(), noopCallbacks());
    expect(container.querySelector(".detail-sources h2")?.textContent).toBe(
      "Sources (3)",
    );
  });
});

describe("renderDetail — idempotence", () => {
  it("re-render replaces previous content (no duplicates)", () => {
    renderDetail(container, aRecipe(), initialDetailState(), noopCallbacks());
    renderDetail(container, aRecipe(), initialDetailState(), noopCallbacks());
    expect(container.querySelectorAll(".detail-header")).toHaveLength(1);
    expect(container.querySelectorAll(".detail-ratio")).toHaveLength(1);
    expect(container.querySelectorAll(".stats-table")).toHaveLength(1);
  });
});
