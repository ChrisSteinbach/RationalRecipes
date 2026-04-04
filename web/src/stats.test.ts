import { describe, expect, it } from "vitest";
import {
  Z_VALUE,
  bakersPercentage,
  calculateStatistics,
  confidenceInterval,
  mean,
  minimumSampleSize,
  normalizeTo100g,
  stdDeviation,
} from "./stats.ts";

describe("normalizeTo100g", () => {
  it("rescales each row to sum to 100", () => {
    const result = normalizeTo100g([
      [1, 2, 3],
      [10, 20, 30],
    ]);
    expect(result[0][0]).toBeCloseTo(100 / 6);
    expect(result[0].reduce((s, v) => s + v, 0)).toBeCloseTo(100);
    expect(result[1].reduce((s, v) => s + v, 0)).toBeCloseTo(100);
  });

  it("normalizes rows with different totals independently", () => {
    const result = normalizeTo100g([
      [100, 100],
      [30, 10],
    ]);
    expect(result[0]).toEqual([50, 50]);
    expect(result[1][0]).toBeCloseTo(75);
    expect(result[1][1]).toBeCloseTo(25);
  });
});

describe("mean and stdDeviation", () => {
  it("mean computes arithmetic average", () => {
    expect(mean([10, 20, 30])).toBeCloseTo(20);
  });

  it("stdDeviation matches numpy population std (ddof=0)", () => {
    // numpy.array([10, 20, 30]).std() == sqrt(200/3) ≈ 8.16497
    expect(stdDeviation([10, 20, 30])).toBeCloseTo(Math.sqrt(200 / 3));
  });

  it("stdDeviation of constant array is zero", () => {
    expect(stdDeviation([5, 5, 5, 5])).toBe(0);
  });
});

describe("confidenceInterval", () => {
  it("interval = Z * std / sqrt(n)", () => {
    expect(confidenceInterval(1.0, 4)).toBeCloseTo((Z_VALUE * 1.0) / 2);
  });

  it("single sample: interval = Z * std", () => {
    expect(confidenceInterval(3.0, 1)).toBeCloseTo(Z_VALUE * 3.0);
  });
});

describe("minimumSampleSize", () => {
  it("ceil((1.96*10/(50*0.05))^2) = 62", () => {
    expect(minimumSampleSize(10, 50, 0.05)).toBe(62);
  });

  it("zero mean yields zero to avoid division by zero", () => {
    expect(minimumSampleSize(5, 0, 0.05)).toBe(0);
  });

  it("small std needs few samples", () => {
    expect(minimumSampleSize(1, 100, 0.05)).toBe(1);
  });
});

describe("calculateStatistics", () => {
  it("all-equal rows give means [50, 50]", () => {
    const stats = calculateStatistics([
      [50, 50],
      [50, 50],
      [50, 50],
    ]);
    expect(stats[0].mean).toBeCloseTo(50);
    expect(stats[1].mean).toBeCloseTo(50);
  });

  it("varying rows give the expected means", () => {
    const stats = calculateStatistics([
      [50, 50],
      [60, 40],
      [70, 30],
    ]);
    expect(stats[0].mean).toBeCloseTo(60);
    expect(stats[1].mean).toBeCloseTo(40);
  });

  it("normalizes rows with different totals", () => {
    const stats = calculateStatistics([
      [100, 100],
      [30, 10],
    ]);
    expect(stats[0].mean).toBeCloseTo(62.5);
    expect(stats[1].mean).toBeCloseTo(37.5);
  });

  it("confidence interval matches hand calculation", () => {
    const stats = calculateStatistics([
      [50, 50],
      [60, 40],
      [70, 30],
    ]);
    const expected = (Z_VALUE * stdDeviation([50, 60, 70])) / Math.sqrt(3);
    expect(stats[0].confidenceInterval).toBeCloseTo(expected);
  });
});

describe("bakersPercentage", () => {
  it("divides all means by the first", () => {
    expect(bakersPercentage([60, 40])).toEqual([1, 40 / 60]);
  });

  it("[25, 50, 25] -> [1, 2, 1]", () => {
    const bp = bakersPercentage([25, 50, 25]);
    expect(bp[0]).toBeCloseTo(1);
    expect(bp[1]).toBeCloseTo(2);
    expect(bp[2]).toBeCloseTo(1);
  });

  it("single ingredient always 1.0", () => {
    expect(bakersPercentage([42])).toEqual([1]);
  });

  it("empty input returns empty", () => {
    expect(bakersPercentage([])).toEqual([]);
  });
});
