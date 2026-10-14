import assert from "node:assert/strict";
import test from "node:test";

import { mergeColumns, mergeSeries } from "./parkerGroups.js";

test("a series is added to another one with the same key", () => {
  const rows = [
    { date: "2026-07-31", series: "Rezidenti", value: 60 },
    { date: "2026-07-31", series: "Sociální", value: 10 },
    { date: "2026-08-31", series: "Sociální", value: 12 },
    { date: "2026-07-31", series: "Návštěvníci", value: 30 },
  ];
  assert.deepEqual(mergeSeries(rows, "Sociální", "Rezidenti", ["date"]), [
    { date: "2026-07-31", series: "Rezidenti", value: 70 },
    { date: "2026-08-31", series: "Rezidenti", value: 12 },
    { date: "2026-07-31", series: "Návštěvníci", value: 30 },
  ]);
});

test("shares are added too", () => {
  const rows = [
    { district: "P01", series: "Rezidenti", value: 6, share: 0.6 },
    { district: "P01", series: "Sociální", value: 1, share: 0.1 },
  ];
  const [merged] = mergeSeries(rows, "Sociální", "Rezidenti", ["district"]);
  assert.equal(merged.value, 7);
  assert.equal(Math.round(merged.share * 10) / 10, 0.7);
});

test("columns are merged and missing values stay missing", () => {
  assert.deepEqual(mergeColumns({ rezidentska: 5, socialni: 2, abonentska: 1 }, "socialni", "rezidentska"), { rezidentska: 7, abonentska: 1 });
  assert.deepEqual(mergeColumns({ rezidentska: null, socialni: null }, "socialni", "rezidentska"), { rezidentska: null });
  assert.deepEqual(mergeColumns({ rezidentska: 5 }, "socialni", "rezidentska"), { rezidentska: 5 });
});
