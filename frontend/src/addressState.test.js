import assert from "node:assert/strict";
import test from "node:test";

import {
  addressStateFromSearch,
  addressStateToSearch,
  fileStem,
  historyRange,
  historySummary,
  reportQuery,
  sortZones,
  sparklinePath,
  zonesCsv,
} from "./addressState.js";

const defaults = { radius: 500, castDne: "den" };

test("a shared link restores the point, radius and part of the day", () => {
  const state = addressStateFromSearch("?page=address&lon=14.4035&lat=50.0755&label=Štefánikova+12&radius=800&time=noc");
  assert.deepEqual(state, {
    point: { lon: 14.4035, lat: 50.0755, label: "Štefánikova 12" },
    radius: 800,
    castDne: "noc",
  });
  assert.deepEqual(addressStateFromSearch("?page=address&lon=abc"), { point: null, radius: null, castDne: "" });
});

test("the address keeps default choices out of the link", () => {
  const point = { lon: 14.4035, lat: 50.0755, label: "" };
  assert.equal(
    addressStateToSearch({ point, label: "Štefánikova 12", radius: 500, castDne: "den" }, defaults),
    "page=address&lon=14.403500&lat=50.075500&label=%C5%A0tef%C3%A1nikova+12",
  );
  assert.equal(
    addressStateToSearch({ point: null, label: "", radius: 800, castDne: "noc" }, defaults),
    "page=address&radius=800&time=noc",
  );
});

test("the report request only names the point when its address is known", () => {
  const base = { radius: 300, castDne: "den" };
  assert.equal(
    reportQuery({ ...base, point: { lon: 14.4, lat: 50.08, label: "" } }),
    "lon=14.400000&lat=50.080000&radius=300&cast_dne=den",
  );
  assert.match(reportQuery({ ...base, point: { lon: 14.4, lat: 50.08, label: "Újezd 1" } }), /&label=%C3%9Ajezd\+1$/);
});

test("sparklines share one time axis and break at missing months", () => {
  const zones = [
    { history: { start: "2026-01-31", values: [10, 10, null, 20, 20] } },
    { history: { start: "2026-03-31", values: [5, 5, 5] } },
    { history: null },
  ];
  const range = historyRange(zones);
  assert.deepEqual(range, { first: 2026 * 12, months: 5 });
  assert.equal(sparklinePath(zones[0].history, range, 40, 20), "M0.0 10.0L10.0 10.0M30.0 0.0L40.0 0.0");
  assert.equal(sparklinePath(zones[1].history, range, 40, 20), "M20.0 0.0L30.0 0.0L40.0 0.0");
  assert.equal(sparklinePath(null, range, 40, 20), "");
  assert.deepEqual(historySummary(zones[0].history), { first: 10, last: 20, from: "2026-01", to: "2026-05" });
});

test("zones without a value sort last in both directions", () => {
  const zones = [
    { code: "A", distance_m: 10, occupancy: 0.5 },
    { code: "B", distance_m: 20, occupancy: null },
    { code: "C", distance_m: 30, occupancy: 0.9 },
  ];
  assert.deepEqual(sortZones(zones, "occupancy", true).map((zone) => zone.code), ["C", "A", "B"]);
  assert.deepEqual(sortZones(zones, "occupancy", false).map((zone) => zone.code), ["A", "C", "B"]);
});

test("exports get a readable file name and quoted cells", () => {
  assert.equal(fileStem("Štefánikova 12, Praha 5"), "stefanikova-12-praha-5");
  assert.equal(fileStem(""), "okoli-adresy");
  const csv = zonesCsv([{ code: "P5-1", distance_m: 12, locality: "Anděl, sever", district: "P05", zone_type: "RES", spaces: 20, permits: null, permits_per_space: null, occupancy: 0.9, respect: 0.8 }]);
  assert.equal(csv.split("\n")[1], 'P5-1,12,"Anděl, sever",P05,Rezidentní,20,,,0.9,0.8');
});
