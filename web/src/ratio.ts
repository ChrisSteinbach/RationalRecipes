// Data model for ingredient ratios.
//
// Holds proportions, performs scaling and restriction math, and returns
// numeric values. Presentation lives in format.ts.
//
// Mirrors `rational_recipes.ratio` (and ColumnTranslator from columns.py)
// but simplified: the curated_recipes artifact contains fully-resolved
// ingredient names per column, so name lookup is a direct match without
// synonym tables.

export interface WholeUnit {
  name: string;
  grams: number;
}

export interface RatioIngredient {
  name: string;
  densityGPerMl: number | null;
  wholeUnit: WholeUnit | null;
}

/**
 * A ceiling on how much of an ingredient (or group of columns sharing a
 * name) can appear in the scaled recipe. Either addressed by column index
 * or by ingredient name.
 */
export type Restriction =
  | { column: number; weight: number }
  | { name: string; weight: number };

/** Pure numeric ratio model: values plus scaling and restriction math. */
export class Ratio {
  readonly ingredients: readonly RatioIngredient[];
  private readonly _values: number[];
  private _restrictions: ReadonlyArray<{
    indexes: number[];
    weight: number;
  }> = [];

  constructor(values: readonly number[], ingredients: readonly RatioIngredient[]) {
    if (values.length !== ingredients.length) {
      throw new Error(
        `Ratio: values length ${values.length} does not match ingredients length ${ingredients.length}`,
      );
    }
    this._values = [...values];
    this.ingredients = ingredients;
  }

  get length(): number {
    return this._values.length;
  }

  /** Resolve a column identifier to one or more column indexes. */
  private resolveIndexes(identifier: string | number): number[] {
    if (typeof identifier === "number") {
      if (identifier < 0 || identifier >= this.ingredients.length) {
        throw new Error(`Ratio: column index ${identifier} out of range`);
      }
      return [identifier];
    }
    const needle = identifier.toLowerCase();
    const matches: number[] = [];
    for (let i = 0; i < this.ingredients.length; i++) {
      if (this.ingredients[i].name.toLowerCase() === needle) {
        matches.push(i);
      }
    }
    if (matches.length === 0) {
      throw new Error(`Ratio: no column matches '${identifier}'`);
    }
    return matches;
  }

  /** Return ratio values, optionally scaled. */
  values(scale = 1): number[] {
    return this._values.map((v) => v * scale);
  }

  /** Return ratio values rescaled so they sum to 100. Ignores restrictions. */
  asPercentages(): number[] {
    const total = this._values.reduce((s, v) => s + v, 0);
    const scale = 100 / total;
    return this.values(scale);
  }

  /** Replace the set of per-ingredient weight limits. */
  setRestrictions(restrictions: readonly Restriction[]): void {
    this._restrictions = restrictions.map((r) => {
      const indexes =
        "column" in r ? this.resolveIndexes(r.column) : this.resolveIndexes(r.name);
      return { indexes, weight: r.weight };
    });
  }

  /**
   * Compute a recipe scaled toward `targetWeight`. Restrictions may
   * reduce the scale below the target so no ingredient exceeds its cap.
   */
  recipe(targetWeight: number): { totalWeight: number; values: number[] } {
    const total = this._values.reduce((s, v) => s + v, 0);
    let scale = targetWeight / total;
    for (const { indexes, weight } of this._restrictions) {
      const unscaledWeight = indexes.reduce((s, i) => s + this._values[i], 0);
      if (unscaledWeight > 0) {
        scale = Math.min(scale, weight / unscaledWeight);
      }
    }
    const values = this.values(scale);
    const totalWeight = values.reduce((s, v) => s + v, 0);
    return { totalWeight, values };
  }

  /** Return (grams, ingredient) pairs for the given column identifier. */
  ingredientValues(
    identifier: string | number,
    scale = 1,
  ): Array<{ grams: number; ingredient: RatioIngredient }> {
    return this.resolveIndexes(identifier).map((i) => ({
      grams: this._values[i] * scale,
      ingredient: this.ingredients[i],
    }));
  }
}
