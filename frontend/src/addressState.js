import { ZONE_LABELS } from "./format.js";

function monthIndex(date) {
  const [year, month] = date.slice(0, 7).split("-").map(Number);
  return year * 12 + month - 1;
}

export function monthFromIndex(index) {
  return `${Math.floor(index / 12)}-${String((index % 12) + 1).padStart(2, "0")}`;
}

function finite(value) {
  const number = Number.parseFloat(value);
  return Number.isFinite(number) ? number : null;
}

export function addressStateFromSearch(search) {
  const params = new URLSearchParams(search);
  const lon = finite(params.get("lon"));
  const lat = finite(params.get("lat"));
  const radius = Number.parseInt(params.get("radius"), 10);
  return {
    point: lon == null || lat == null ? null : { lon, lat, label: params.get("label") ?? "" },
    radius: Number.isFinite(radius) ? radius : null,
    castDne: params.get("time") ?? "",
  };
}

export function addressStateToSearch({ point, label, radius, castDne }, defaults) {
  const params = new URLSearchParams({ page: "address" });
  if (point) {
    params.set("lon", point.lon.toFixed(6));
    params.set("lat", point.lat.toFixed(6));
    if (label) params.set("label", label);
  }
  if (radius && radius !== defaults.radius) params.set("radius", String(radius));
  if (castDne && castDne !== defaults.castDne) params.set("time", castDne);
  return params.toString();
}

export function reportQuery({ point, radius, castDne }) {
  const params = new URLSearchParams({
    lon: point.lon.toFixed(6),
    lat: point.lat.toFixed(6),
    radius: String(radius),
    cast_dne: castDne,
  });
  if (point.label) params.set("label", point.label);
  return params.toString();
}

export function radiusOptions({ min, max, step }) {
  const values = [];
  for (let value = min; value <= max; value += step) values.push(value);
  return values;
}

// First and last month covered by any zone, so that every sparkline shares one time axis.
export function historyRange(zones) {
  let first = Infinity;
  let last = -Infinity;
  zones.forEach(({ history }) => {
    if (!history) return;
    const start = monthIndex(history.start);
    first = Math.min(first, start);
    last = Math.max(last, start + history.values.length - 1);
  });
  return Number.isFinite(first) ? { first, months: last - first + 1 } : null;
}

// SVG path for one zone's capacity; missing months break the line.
export function sparklinePath(history, range, width, height) {
  if (!history || !range) return "";
  const values = history.values;
  const highest = Math.max(...values.filter(Number.isFinite), 1);
  const offset = monthIndex(history.start) - range.first;
  const stepX = range.months > 1 ? width / (range.months - 1) : 0;
  let path = "";
  let drawing = false;
  values.forEach((value, index) => {
    if (!Number.isFinite(value)) {
      drawing = false;
      return;
    }
    const x = (offset + index) * stepX;
    const y = height - (value / highest) * height;
    path += `${drawing ? "L" : "M"}${x.toFixed(1)} ${y.toFixed(1)}`;
    drawing = true;
  });
  return path;
}

export function historySummary(history) {
  if (!history) return null;
  const known = history.values.filter(Number.isFinite);
  if (known.length === 0) return null;
  const start = monthIndex(history.start);
  const end = start + history.values.length - 1;
  return { first: known[0], last: known.at(-1), from: monthFromIndex(start), to: monthFromIndex(end) };
}

export function sortZones(zones, key, descending) {
  const direction = descending ? -1 : 1;
  return [...zones].sort((first, second) => {
    const a = first[key];
    const b = second[key];
    if (a == null && b == null) return first.distance_m - second.distance_m;
    if (a == null) return 1;
    if (b == null) return -1;
    const order = typeof a === "string" ? a.localeCompare(b, "cs", { numeric: true }) : a - b;
    return order === 0 ? first.distance_m - second.distance_m : order * direction;
  });
}

export function fileStem(label) {
  const stem = (label ?? "")
    .normalize("NFD")
    .replace(/[̀-ͯ]/g, "")
    .replace(/[^0-9A-Za-z]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .toLowerCase()
    .slice(0, 60);
  return stem || "okoli-adresy";
}

const CSV_COLUMNS = [
  ["usek", (zone) => zone.code],
  ["vzdalenost_m", (zone) => zone.distance_m],
  ["lokalita", (zone) => zone.locality],
  ["mestska_cast", (zone) => zone.district],
  ["typ_zony", (zone) => ZONE_LABELS[zone.zone_type] ?? zone.zone_type],
  ["parkovaci_mista", (zone) => zone.spaces],
  ["parkovaci_opravneni", (zone) => zone.permits],
  ["opravneni_na_misto", (zone) => zone.permits_per_space],
  ["obsazenost", (zone) => zone.occupancy],
  ["respektovanost", (zone) => zone.respect],
];

function csvCell(value) {
  if (value == null) return "";
  const text = String(value);
  return /[",\n]/.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
}

export function zonesCsv(zones) {
  return [
    CSV_COLUMNS.map(([name]) => name).join(","),
    ...zones.map((zone) => CSV_COLUMNS.map(([, read]) => csvCell(read(zone))).join(",")),
  ].join("\n");
}

export function zonesGeoJson(report) {
  const zoneByCode = new Map(report.zones.map((zone) => [zone.code, zone]));
  return {
    type: "FeatureCollection",
    properties: {
      adresa: report.selection.label,
      okruh_m: report.selection.radius_m,
      cast_dne: report.selection.cast_dne,
    },
    features: report.geometry.features.map((feature) => {
      const { history, ...zone } = zoneByCode.get(feature.properties.code) ?? {};
      return { ...feature, properties: { ...feature.properties, ...zone } };
    }),
  };
}
