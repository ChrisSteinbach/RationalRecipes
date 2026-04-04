// Statistical computations over recipe row data.
//
// Mirrors the Python `rational_recipes.statistics` and `normalize` modules.
// No numpy equivalent is needed — plain arithmetic over number[][].

// 1.96 corresponds to a 95% confidence level.
export const Z_VALUE = 1.96;

export interface ColumnStats {
  mean: number;
  stdDeviation: number;
  confidenceInterval: number;
  minSampleSize: number;
}

/** Rescale each row so its values sum to 100. */
export function normalizeTo100g(
  rows: readonly (readonly number[])[],
): number[][] {
  return rows.map((row) => {
    const total = row.reduce((sum, v) => sum + v, 0);
    const multiplier = 100 / total;
    return row.map((v) => v * multiplier);
  });
}

/** Transpose a row-major matrix to column-major. */
function transpose(rows: readonly (readonly number[])[]): number[][] {
  if (rows.length === 0) return [];
  const ncols = rows[0].length;
  const cols: number[][] = Array.from({ length: ncols }, () => []);
  for (const row of rows) {
    for (let j = 0; j < ncols; j++) {
      cols[j].push(row[j]);
    }
  }
  return cols;
}

/** Arithmetic mean. */
export function mean(values: readonly number[]): number {
  if (values.length === 0) return 0;
  return values.reduce((s, v) => s + v, 0) / values.length;
}

/**
 * Population standard deviation (ddof=0), matching numpy's default
 * `array.std()` behaviour that the Python pipeline relies on.
 */
export function stdDeviation(values: readonly number[]): number {
  if (values.length === 0) return 0;
  const m = mean(values);
  const variance =
    values.reduce((s, v) => s + (v - m) * (v - m), 0) / values.length;
  return Math.sqrt(variance);
}

/** Half-width of the 95% confidence interval for the sample mean. */
export function confidenceInterval(std: number, sampleSize: number): number {
  if (sampleSize <= 0) return 0;
  return (std * Z_VALUE) / Math.sqrt(sampleSize);
}

/**
 * Minimum sample size needed so that the confidence interval is within
 * `desiredInterval` of the mean at 95% confidence.
 */
export function minimumSampleSize(
  std: number,
  meanValue: number,
  desiredInterval: number,
): number {
  if (meanValue === 0) return 0;
  return Math.ceil(((Z_VALUE * std) / (meanValue * desiredInterval)) ** 2);
}

/**
 * Compute per-column statistics from raw rows. Rows are normalized to
 * sum to 100 before aggregation, so results are expressed as grams per
 * 100g of recipe.
 */
export function calculateStatistics(
  rows: readonly (readonly number[])[],
  desiredInterval = 0.05,
): ColumnStats[] {
  const normalized = normalizeTo100g(rows);
  const columns = transpose(normalized);
  return columns.map((col) => {
    const m = mean(col);
    const std = stdDeviation(col);
    return {
      mean: m,
      stdDeviation: std,
      confidenceInterval: confidenceInterval(std, col.length),
      minSampleSize: minimumSampleSize(std, m, desiredInterval),
    };
  });
}

/**
 * Express means as baker's percentages: each value divided by the first,
 * so the base ingredient (typically flour) is 1.0.
 */
export function bakersPercentage(means: readonly number[]): number[] {
  if (means.length === 0) return [];
  const base = means[0];
  return means.map((m) => m / base);
}
