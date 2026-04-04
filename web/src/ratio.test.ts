import { describe, expect, it } from "vitest";
import { Ratio, type RatioIngredient } from "./ratio.ts";

// Test fixtures — match the densities and egg whole-unit used by the
// Python test suite so expected output numbers transfer directly.
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

describe("Ratio data model", () => {
  it("values returns raw ratio values", () => {
    const ratio = new Ratio([1, 2, 3], INGREDIENTS);
    expect(ratio.values()).toEqual([1, 2, 3]);
  });

  it("values(scale) multiplies by the scale factor", () => {
    const ratio = new Ratio([1, 2, 3], INGREDIENTS);
    expect(ratio.values(2)).toEqual([2, 4, 6]);
  });

  it("asPercentages returns values summing to 100", () => {
    const ratio = new Ratio([1, 2, 3], INGREDIENTS);
    const pct = ratio.asPercentages();
    expect(pct.reduce((s, v) => s + v, 0)).toBeCloseTo(100);
    expect(pct[0]).toBeCloseTo(100 / 6);
  });

  it("recipe returns correct total weight", () => {
    const ratio = new Ratio([1, 2, 3], INGREDIENTS);
    const { totalWeight, values } = ratio.recipe(200);
    expect(totalWeight).toBeCloseTo(200);
    expect(values.reduce((s, v) => s + v, 0)).toBeCloseTo(200);
  });

  it("recipe preserves proportions", () => {
    const ratio = new Ratio([1, 2, 3], INGREDIENTS);
    const { values } = ratio.recipe(600);
    expect(values[0]).toBeCloseTo(100);
    expect(values[1]).toBeCloseTo(200);
    expect(values[2]).toBeCloseTo(300);
  });

  it("recipe with restriction reduces total weight", () => {
    const ratio = new Ratio([1, 2, 3], INGREDIENTS);
    ratio.setRestrictions([{ column: 1, weight: 31.5 }]);
    const { totalWeight, values } = ratio.recipe(200);
    expect(totalWeight).toBeLessThan(200);
    expect(values[1]).toBeLessThanOrEqual(31.5 + 0.001);
  });

  it("ingredientValues returns (grams, ingredient) pairs", () => {
    const ratio = new Ratio([1, 2, 3], INGREDIENTS);
    const pairs = ratio.ingredientValues("flour");
    expect(pairs).toHaveLength(1);
    expect(pairs[0].grams).toBeCloseTo(1);
    expect(pairs[0].ingredient.name).toBe("flour");
  });

  it("multiple restrictions: tightest constrains the scale", () => {
    // target 600 → unrestricted scale=100. Limits: flour≤50 (scale≤50),
    // egg≤150 (scale≤75), butter≤300 (scale≤100). Flour wins.
    const ratio = new Ratio([1, 2, 3], INGREDIENTS);
    ratio.setRestrictions([
      { column: 0, weight: 50 },
      { column: 1, weight: 150 },
      { column: 2, weight: 300 },
    ]);
    const { totalWeight, values } = ratio.recipe(600);
    expect(values[0]).toBeCloseTo(50);
    expect(values[1]).toBeCloseTo(100);
    expect(values[2]).toBeCloseTo(150);
    expect(totalWeight).toBeCloseTo(300);
  });

  it("pantry constraint by ingredient name", () => {
    // target 1000 → unrestricted scale≈166.7. flour≤80 constrains to 80.
    const ratio = new Ratio([1, 2, 3], INGREDIENTS);
    ratio.setRestrictions([{ name: "flour", weight: 80 }]);
    const { totalWeight, values } = ratio.recipe(1000);
    expect(values[0]).toBeCloseTo(80);
    expect(values[1]).toBeCloseTo(160);
    expect(values[2]).toBeCloseTo(240);
    expect(totalWeight).toBeCloseTo(480);
  });

  it("non-constraining restriction has no effect", () => {
    // target 200 → scale≈33.3, butter≈100. butter≤500 does not constrain.
    const ratio = new Ratio([1, 2, 3], INGREDIENTS);
    ratio.setRestrictions([{ column: 2, weight: 500 }]);
    const { totalWeight, values } = ratio.recipe(200);
    expect(totalWeight).toBeCloseTo(200);
    expect(values.reduce((s, v) => s + v, 0)).toBeCloseTo(200);
  });

  it("restriction on repeated ingredient sums both columns", () => {
    // ingredients [flour, egg, butter, butter], proportions [1,2,3,3].
    // total=9, target 200. butter (cols 2+3) unscaled=6, limit 94 →
    // scale≤94/6≈15.67.
    const ingredients = [FLOUR, EGG, BUTTER, BUTTER];
    const ratio = new Ratio([1, 2, 3, 3], ingredients);
    ratio.setRestrictions([{ name: "butter", weight: 94 }]);
    const { totalWeight, values } = ratio.recipe(200);
    const butterTotal = values[2] + values[3];
    expect(butterTotal).toBeLessThanOrEqual(94 + 0.001);
    expect(butterTotal).toBeCloseTo(94, 1);
    expect(totalWeight).toBeLessThan(200);
  });

  it("length returns number of elements", () => {
    const ratio = new Ratio([1, 2, 3], INGREDIENTS);
    expect(ratio.length).toBe(3);
  });

  it("resolving an unknown name throws", () => {
    const ratio = new Ratio([1, 2, 3], INGREDIENTS);
    expect(() => ratio.ingredientValues("sugar")).toThrow(/sugar/);
  });

  it("resolving an out-of-range index throws", () => {
    const ratio = new Ratio([1, 2, 3], INGREDIENTS);
    expect(() => ratio.ingredientValues(5)).toThrow(/out of range/);
  });

  it("mismatched values/ingredients length throws", () => {
    expect(() => new Ratio([1, 2], INGREDIENTS)).toThrow(/length/);
  });
});
