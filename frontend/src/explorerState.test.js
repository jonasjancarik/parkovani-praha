import assert from "node:assert/strict";
import test from "node:test";

import {
  DEFAULT_FILTERS,
  defaultFiltersFor,
  initialStateFromSearch,
  monthEnd,
  monthStart,
  selectionIsAll,
  stateToSearch,
} from "./explorerState.js";

test("shareable state round-trips filters, view, and metric", () => {
  const filters = {
    start: "2024-01-01",
    end: "2025-12-31",
    castDne: ["noc"],
    districts: ["P07"],
    zoneTypes: ["RES", "MIX"],
    sameZones: true,
  };

  const search = stateToSearch(filters, "table", "pressure", false);
  assert.deepEqual(initialStateFromSearch(search), {
    view: "table",
    metric: "pressure",
    socialAsResidents: false,
    filters,
  });
  assert.equal(initialStateFromSearch("page=explorer").socialAsResidents, true);
});

test("the default selection keeps the address short", () => {
  assert.equal(stateToSearch(DEFAULT_FILTERS, "charts", "permits-spaces"), "page=explorer");
  assert.deepEqual(initialStateFromSearch("").filters, DEFAULT_FILTERS);
  assert.deepEqual(
    defaultFiltersFor({ cast_dne: ["noc", "den"], zone_types: ["RES"] }),
    { ...DEFAULT_FILTERS, castDne: ["noc"], zoneTypes: ["RES"] },
  );
});

test("invalid URL state falls back to the primary view", () => {
  const state = initialStateFromSearch("view=unknown&metric=unknown");
  assert.equal(state.view, "charts");
  assert.equal(state.metric, "permits-spaces");
});

test("month boundaries use the full selected month", () => {
  assert.equal(monthStart("2024-02"), "2024-02-01");
  assert.equal(monthEnd("2024-02"), "2024-02-29");
  assert.equal(monthEnd("2025-02"), "2025-02-28");
});

test("empty and complete multiselects both mean all values", () => {
  assert.equal(selectionIsAll([], ["A", "B"]), true);
  assert.equal(selectionIsAll(["B", "A"], ["A", "B"]), true);
  assert.equal(selectionIsAll(["A"], ["A", "B"]), false);
});
