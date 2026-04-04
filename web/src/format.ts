// Formatting helpers for Ratio data: translate numeric values into
// text suitable for the catalog detail view.
//
// Mirrors `rational_recipes.ratio_format`. Presentation only — all math
// lives in ratio.ts and stats.ts.

import type { Ratio, RatioIngredient } from "./ratio.ts";

export interface FormatOptions {
  /** Digits after the decimal point. Defaults to 2. */
  precision?: number;
}

function formatNumber(value: number, precision: number): string {
  return value.toFixed(precision);
}

function gramsToMilliliters(
  grams: number,
  ingredient: RatioIngredient,
): number | null {
  if (ingredient.densityGPerMl == null || ingredient.densityGPerMl === 0) {
    return null;
  }
  return grams / ingredient.densityGPerMl;
}

function gramsToWholeUnits(
  grams: number,
  ingredient: RatioIngredient,
): number | null {
  if (ingredient.wholeUnit == null) return null;
  return grams / ingredient.wholeUnit.grams;
}

function describeGramsAndMilliliters(
  grams: number,
  ingredient: RatioIngredient,
  precision: number,
): string {
  const g = formatNumber(grams, precision);
  const ml = gramsToMilliliters(grams, ingredient);
  if (ml === null) {
    return `${g}g ${ingredient.name}`;
  }
  return `${g}g or ${formatNumber(ml, precision)}ml ${ingredient.name}`;
}

function describeWholeUnits(
  grams: number,
  ingredient: RatioIngredient,
  precision: number,
): string {
  const whole = ingredient.wholeUnit;
  if (whole == null) {
    return describeGramsAndMilliliters(grams, ingredient, precision);
  }
  const g = formatNumber(grams, precision);
  const ml = gramsToMilliliters(grams, ingredient);
  const count = gramsToWholeUnits(grams, ingredient) ?? 0;
  const name = ingredient.name;
  const mlPart = ml === null ? "" : `${formatNumber(ml, precision)}ml or `;
  return (
    `${g}g, ${mlPart}${formatNumber(count, precision)} ${name}(s)` +
    ` where each ${name} is ${formatNumber(whole.grams, precision)}g`
  );
}

function describeIngredientValue(
  grams: number,
  ingredient: RatioIngredient,
  precision: number,
): string {
  if (ingredient.wholeUnit != null) {
    return describeWholeUnits(grams, ingredient, precision);
  }
  return describeGramsAndMilliliters(grams, ingredient, precision);
}

/** Format a ratio as '1.00:2.00:3.00 (flour:egg:butter)'. */
export function formatRatio(ratio: Ratio, options: FormatOptions = {}): string {
  const precision = options.precision ?? 2;
  const values = ratio
    .values()
    .map((v) => formatNumber(v, precision))
    .join(":");
  const names = ratio.ingredients.map((i) => i.name).join(":");
  return `${values} (${names})`;
}

/**
 * Format a scaled recipe as a newline-separated ingredient list.
 * Returns the achieved total weight (restrictions may lower it below
 * the requested target).
 */
export function formatRecipe(
  ratio: Ratio,
  targetWeight: number,
  options: FormatOptions = {},
): { totalWeight: number; text: string } {
  const precision = options.precision ?? 2;
  const { totalWeight, values } = ratio.recipe(targetWeight);
  const lines = values.map((grams, i) =>
    describeIngredientValue(grams, ratio.ingredients[i], precision),
  );
  return { totalWeight, text: lines.join("\n") };
}

/**
 * Format one or more rows describing the ingredient(s) matching
 * `identifier`. Used for per-ingredient detail display.
 */
export function formatIngredient(
  ratio: Ratio,
  identifier: string | number,
  options: FormatOptions = {},
): string {
  const precision = options.precision ?? 2;
  return ratio
    .ingredientValues(identifier)
    .map(({ grams, ingredient }) =>
      describeIngredientValue(grams, ingredient, precision),
    )
    .join("\n");
}
