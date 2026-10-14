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

function monthNumber(date) {
  const [year, month] = date.slice(0, 7).split("-").map(Number);
  return year * 12 + month;
}

// Direction of permits per space over the whole series and over its last year:
// "up" or "down" only when both agree, so a heading never claims a lasting trend
// that the latest year contradicts.
export function longRunTrend(series) {
  const rows = series.filter((row) => Number.isFinite(row.permits_per_space));
  if (rows.length < 2) return null;
  const first = rows[0];
  const last = rows.at(-1);
  const yearAgo = rows.find((row) => monthNumber(row.date) === monthNumber(last.date) - 12);
  const whole = ratioTrend(last.permits_per_space - first.permits_per_space);
  const recent = yearAgo ? ratioTrend(last.permits_per_space - yearAgo.permits_per_space) : whole;
  return whole === recent ? whole : "mixed";
}

// "vzrostl z 1,20 na 1,42"
export function ratioChangeClause(from, to) {
  const trend = ratioTrend(to - from);
  if (trend === "flat") return `zůstal na ${ratioText(to)}`;
  return `${trend === "up" ? "vzrostl" : "klesl"} z ${ratioText(from)} na ${ratioText(to)}`;
}

const ratioNumber = new Intl.NumberFormat("cs-CZ", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
function ratioText(value) {
  return ratioNumber.format(value);
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

// Groups too small to read as a tile in every period are shown together as "Ostatní".
export function mergeSmallGroups(periods, minShare = 0.03) {
  const small = new Set(
    periods[0]?.groups
      .map((group) => group.series)
      .filter((name) => periods.every((period) => (shareOf(period, name) ?? 0) < minShare)) ?? [],
  );
  if (small.size < 2) {
    return periods.map((period) => ({
      ...period,
      groups: period.groups.map((group) => ({ ...group, parts: group.parts ?? [group.series] })),
    }));
  }
  return periods.map((period) => {
    const kept = period.groups
      .filter((group) => !small.has(group.series))
      .map((group) => ({ ...group, parts: group.parts ?? [group.series] }));
    const merged = period.groups.filter((group) => small.has(group.series));
    return {
      ...period,
      groups: [
        ...kept,
        {
          series: "Ostatní",
          value: merged.reduce((total, group) => total + group.value, 0),
          share: merged.reduce((total, group) => total + group.share, 0),
          parts: merged.flatMap((group) => group.parts ?? [group.series]),
        },
      ],
    };
  });
}
