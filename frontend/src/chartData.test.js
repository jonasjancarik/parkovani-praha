import assert from "node:assert/strict";
import test from "node:test";

import {
  formatMonth,
  hasDrawableObservationAverage,
  materializeMonthlyRows,
  monthsBetween,
  niceAxisMax,
  spreadLabels,
  trailingObservationAverage,
} from "./chartData.js";

function monthEndFromOffset(offset) {
  return new Date(Date.UTC(2025, offset + 1, 0)).toISOString().slice(0, 10);
}

test("monthly chart rows expose missing calendar months as null", () => {
  const result = materializeMonthlyRows(
    [
      { date: "2025-01-31", permits: 10 },
      { date: "2025-03-31", permits: 30 },
    ],
    ["permits"],
  );

  assert.deepEqual(result, [
    { date: "2025-01-31", permits: 10 },
    { date: "2025-02-28", permits: null },
    { date: "2025-03-31", permits: 30 },
  ]);
});

test("trailing average starts after a complete window", () => {
  const rows = Array.from({ length: 12 }, (_, index) => ({
    date: monthEndFromOffset(index),
    permits: index + 1,
  }));

  const result = trailingObservationAverage(rows, "permits", 12);

  assert.equal(result[10][1], null);
  assert.equal(result[11][1], 6.5);
});

test("observation average exposes gaps without becoming stale", () => {
  const rows = Array.from({ length: 13 }, (_, index) => ({
    date: monthEndFromOffset(index),
    permits: index === 4 ? null : index + 1,
  }));

  const result = trailingObservationAverage(rows, "permits", 12);

  assert.equal(result[4][1], null);
  assert.equal(result[12][1], (86 / 12));
});

test("observation average is enabled only when it can draw a segment", () => {
  const rows = Array.from({ length: 13 }, (_, index) => ({
    date: monthEndFromOffset(index),
    permits: index + 1,
  }));

  assert.equal(hasDrawableObservationAverage(rows.slice(0, 11), "permits"), false);
  assert.equal(hasDrawableObservationAverage(rows.slice(0, 12), "permits"), false);
  assert.equal(hasDrawableObservationAverage(rows, "permits"), true);
});

test("observation average becomes drawable again after a data gap", () => {
  const rows = Array.from({ length: 14 }, (_, index) => ({
    date: monthEndFromOffset(index),
    permits: index === 4 ? null : index + 1,
  }));

  assert.equal(hasDrawableObservationAverage(rows.slice(0, 13), "permits"), false);
  assert.equal(hasDrawableObservationAverage(rows, "permits"), true);
});

test("month helpers list every month and label it in Czech", () => {
  assert.deepEqual(monthsBetween("2024-11-30", "2025-02-28"), ["2024-11", "2024-12", "2025-01", "2025-02"]);
  assert.equal(formatMonth("2025-02-28"), "únor 2025");
  assert.equal(formatMonth(Date.UTC(2023, 1, 28)), "únor 2023");
});

test("axis maximum rounds up to a readable value", () => {
  assert.equal(niceAxisMax(218542), 250000);
  assert.equal(niceAxisMax(15752), 20000);
  assert.equal(niceAxisMax(0), 1);
});

test("overlapping labels are pushed apart without leaving the plot", () => {
  assert.deepEqual(spreadLabels([100, 105, 20], 40, 300), [100, 140, 20]);
  assert.deepEqual(spreadLabels([290, 295, null], 40, 300), [260, 300, null]);
});
