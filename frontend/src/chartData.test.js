import assert from "node:assert/strict";
import test from "node:test";

import {
  hasDrawableObservationAverage,
  materializeMonthlyRows,
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
