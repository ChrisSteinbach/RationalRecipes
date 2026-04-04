import { describe, expect, it } from "vitest";
import { formatIngredient, formatRatio, formatRecipe } from "./format.ts";
import { Ratio, type RatioIngredient } from "./ratio.ts";

const FLOUR: RatioIngredient = {
  name: "flour",
  densityGPerMl: 0.53,
  wholeUnit: null,
};
const EGG: RatioIngredient = {
  name: "egg",
  densityGPerMl: 1.0271,
  wholeUnit: { name: "medium", grams: 44 },
};
const BUTTER: RatioIngredient = {
  name: "butter",
  densityGPerMl: 0.9595,
  wholeUnit: null,
};

const INGREDIENTS = [FLOUR, EGG, BUTTER];

describe("formatRatio", () => {
  it("precision 0 renders '1:2:3 (flour:egg:butter)'", () => {
    const ratio = new Ratio([1, 2, 3], INGREDIENTS);
    expect(formatRatio(ratio, { precision: 0 }).split(" ")[0]).toBe("1:2:3");
  });

  it("default precision is 2 decimals", () => {
    const ratio = new Ratio([1, 2, 3], INGREDIENTS);
    expect(formatRatio(ratio).split(" ")[0]).toBe("1.00:2.00:3.00");
  });

  it("includes the ingredient names", () => {
    const ratio = new Ratio([1, 2, 3], INGREDIENTS);
    expect(formatRatio(ratio)).toBe("1.00:2.00:3.00 (flour:egg:butter)");
  });
});

describe("formatIngredient", () => {
  it("grams + ml for ingredients with density only", () => {
    const ratio = new Ratio([1, 2, 3], INGREDIENTS);
    // 1 / 0.53 ≈ 1.887 → '1.89'
    expect(formatIngredient(ratio, "flour")).toBe("1.00g or 1.89ml flour");
  });

  it("grams + ml + wholeunit count for countable ingredients", () => {
    const ratio = new Ratio([1, 2, 3], INGREDIENTS);
    // grams=2, ml=2/1.0271≈1.947 → '1.95', count=2/44≈0.0455 → '0.05'
    expect(formatIngredient(ratio, "egg")).toBe(
      "2.00g, 1.95ml or 0.05 egg(s) where each egg is 44.00g",
    );
  });
});

describe("formatRecipe", () => {
  it("scales to total weight with precision 0", () => {
    const ratio = new Ratio([1, 2, 3], INGREDIENTS);
    const { text } = formatRecipe(ratio, 200, { precision: 0 });
    const expected = [
      "33g or 63ml flour",
      "67g, 65ml or 2 egg(s) where each egg is 44g",
      "100g or 104ml butter",
    ].join("\n");
    expect(text).toBe(expected);
  });

  it("honours single-column weight restrictions", () => {
    const ratio = new Ratio([1, 2, 3], INGREDIENTS);
    ratio.setRestrictions([{ column: 1, weight: 31.5 }]);
    const { text } = formatRecipe(ratio, 200, { precision: 0 });
    const expected = [
      "16g or 30ml flour",
      "32g, 31ml or 1 egg(s) where each egg is 44g",
      "47g or 49ml butter",
    ].join("\n");
    expect(text).toBe(expected);
  });

  it("honours restrictions targeted by ingredient name", () => {
    const ratio = new Ratio([1, 2, 3], INGREDIENTS);
    ratio.setRestrictions([{ name: "egg", weight: 31.5 }]);
    const { text } = formatRecipe(ratio, 200, { precision: 0 });
    const expected = [
      "16g or 30ml flour",
      "32g, 31ml or 1 egg(s) where each egg is 44g",
      "47g or 49ml butter",
    ].join("\n");
    expect(text).toBe(expected);
  });

  it("restrictions on repeated ingredients sum their columns", () => {
    const ingredients = [FLOUR, EGG, BUTTER, BUTTER];
    const ratio = new Ratio([1, 2, 3, 3], ingredients);
    ratio.setRestrictions([{ name: "butter", weight: 94 }]);
    const { text } = formatRecipe(ratio, 200, { precision: 0 });
    const expected = [
      "16g or 30ml flour",
      "31g, 31ml or 1 egg(s) where each egg is 44g",
      "47g or 49ml butter",
      "47g or 49ml butter",
    ].join("\n");
    expect(text).toBe(expected);
  });

  it("multiple restrictions: tightest wins", () => {
    const ratio = new Ratio([1, 2, 3], INGREDIENTS);
    ratio.setRestrictions([
      { column: 0, weight: 17 },
      { column: 1, weight: 31.5 },
      { column: 2, weight: 48 },
    ]);
    const { text } = formatRecipe(ratio, 200, { precision: 0 });
    const expected = [
      "16g or 30ml flour",
      "32g, 31ml or 1 egg(s) where each egg is 44g",
      "47g or 49ml butter",
    ].join("\n");
    expect(text).toBe(expected);
  });
});
