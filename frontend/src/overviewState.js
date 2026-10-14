import { districtName } from "./format.js";

// Explorer links shared before the overview existed carry filters but no page name.
export function pageFromSearch(search) {
  const params = new URLSearchParams(search);
  return params.get("page") ?? ([...params.keys()].length > 0 ? "explorer" : "overview");
}

export function explorerHref({ district, metric, start, sameZones } = {}) {
  const params = new URLSearchParams({ page: "explorer" });
  if (start) params.set("start", start);
  if (district) params.set("district", district);
  if (sameZones) params.set("same_zones", "true");
  if (metric) params.set("metric", metric);
  return `./?${params}`;
}

// "Praha 1" in the form that follows "v": "v Praze 1".
export function districtLocative(code) {
  return districtName(code).replace(/^Praha /, "Praze ");
}

// Differences that would round to 0,00 are not worth calling a rise or a fall.
const NOTICEABLE_RATIO_CHANGE = 0.005;

export function ratioTrend(change) {
  if (change == null) return null;
  if (Math.abs(change) < NOTICEABLE_RATIO_CHANGE) return "flat";
  return change > 0 ? "up" : "down";
}

// The districts where permits per space rose and fell the most over the year.
export function biggestChanges(districts) {
  const known = districts.filter((row) => row.ratio_change != null);
  const byChange = [...known].sort((first, second) => second.ratio_change - first.ratio_change);
  const rise = byChange[0];
  const fall = byChange.at(-1);
  return {
    rise: rise && ratioTrend(rise.ratio_change) === "up" ? rise : null,
    fall: fall && ratioTrend(fall.ratio_change) === "down" ? fall : null,
  };
}

const signedRatioFormat = new Intl.NumberFormat("cs-CZ", {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
  signDisplay: "exceptZero",
});
const signedPercentFormat = new Intl.NumberFormat("cs-CZ", {
  style: "percent",
  maximumFractionDigits: 1,
  signDisplay: "exceptZero",
});

// Czech number formatting writes a hyphen where a minus sign belongs.
function signed(format, value) {
  return value == null ? "—" : format.format(value).replace("-", "−");
}

export function formatRatioChange(value) {
  return signed(signedRatioFormat, value);
}

export function formatPercentChange(value) {
  return signed(signedPercentFormat, value);
}

const percentChangeFormat = new Intl.NumberFormat("cs-CZ", { style: "percent", maximumFractionDigits: 1 });

// "oprávnění přibylo o 3,6 %": how a count changed over the year, for use inside a sentence.
export function yearChangeClause(noun, change) {
  if (Math.abs(change) < 0.0005) return `${noun} zůstalo stejně`;
  return `${noun} ${change > 0 ? "přibylo" : "ubylo"} o ${percentChangeFormat.format(Math.abs(change))}`;
}

export function shareOf(period, series) {
  return period?.groups.find((group) => group.series === series)?.share ?? null;
}
