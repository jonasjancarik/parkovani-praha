import assert from "node:assert/strict";
import test from "node:test";

import {
  biggestChanges,
  districtLocative,
  explorerHref,
  formatPercentChange,
  formatRatioChange,
  pageFromSearch,
  ratioTrend,
  shareOf,
  yearChangeClause,
} from "./overviewState.js";

test("the overview is the first page and older explorer links still open the explorer", () => {
  assert.equal(pageFromSearch(""), "overview");
  assert.equal(pageFromSearch("?district=P07&view=table"), "explorer");
  assert.equal(pageFromSearch("?page=explorer"), "explorer");
  assert.equal(pageFromSearch("?page=address&lon=14.4&lat=50.1"), "address");
});

test("explorer links preselect a district or a metric", () => {
  assert.equal(explorerHref(), "./?page=explorer");
  assert.equal(explorerHref({ district: "P07" }), "./?page=explorer&district=P07");
  assert.equal(explorerHref({ metric: "pressure" }), "./?page=explorer&metric=pressure");
  assert.equal(
    explorerHref({ metric: "pressure", start: "2019-03-01", sameZones: true }),
    "./?page=explorer&start=2019-03-01&same_zones=true&metric=pressure",
  );
});

test("district names are declined after a preposition", () => {
  assert.equal(districtLocative("P01"), "Praze 1");
  assert.equal(districtLocative("P10"), "Praze 10");
});

test("changes too small to show are reported as no change", () => {
  assert.equal(ratioTrend(0.05), "up");
  assert.equal(ratioTrend(-0.02), "down");
  assert.equal(ratioTrend(0.004), "flat");
  assert.equal(ratioTrend(null), null);
});

test("the biggest rise and fall ignore districts without a comparison", () => {
  const districts = [
    { district: "P01", ratio_change: 0.34 },
    { district: "P02", ratio_change: 0.01 },
    { district: "P03", ratio_change: null },
  ];
  assert.deepEqual(biggestChanges(districts), { rise: districts[0], fall: null });
  assert.equal(
    biggestChanges([...districts, { district: "P04", ratio_change: -0.1 }]).fall.district,
    "P04",
  );
  assert.deepEqual(biggestChanges([]), { rise: null, fall: null });
});

test("changes carry their sign", () => {
  assert.equal(formatRatioChange(0.342), "+0,34");
  assert.equal(formatRatioChange(-0.05), "−0,05");
  assert.equal(formatPercentChange(0.0359).replace(/\s/g, " "), "+3,6 %");
  assert.equal(formatPercentChange(null), "—");
});

test("a parker share is looked up by name", () => {
  const period = { groups: [{ series: "Rezidenti", share: 0.6 }] };
  assert.equal(shareOf(period, "Rezidenti"), 0.6);
  assert.equal(shareOf(period, "Návštěvníci"), null);
  assert.equal(shareOf(undefined, "Rezidenti"), null);
});

test("a yearly change reads as a rise, a fall, or no change", () => {
  assert.equal(yearChangeClause("oprávnění", 0.0359).replace(/\s/g, " "), "oprávnění přibylo o 3,6 %");
  assert.equal(yearChangeClause("parkovacích míst", -0.0023).replace(/\s/g, " "), "parkovacích míst ubylo o 0,2 %");
  assert.equal(yearChangeClause("parkovacích míst", 0.0001), "parkovacích míst zůstalo stejně");
});
