export const DEFAULT_FILTERS = {
  start: "",
  end: "",
  castDne: ["Po-Pá", "Po-Pá (MPD)", "So-Ne", "noc"],
  districts: [],
  zoneTypes: ["MIX", "OST", "RES", "VIS"],
  sameZones: false,
};

const VALID_VIEWS = new Set(["charts", "table"]);
const VALID_METRICS = new Set(["permits-spaces", "pressure"]);

export function initialStateFromSearch(search) {
  const params = new URLSearchParams(search);
  const view = params.get("view");
  const metric = params.get("metric");
  return {
    view: VALID_VIEWS.has(view) ? view : "charts",
    metric: VALID_METRICS.has(metric) ? metric : "permits-spaces",
    socialAsResidents: params.get("social") !== "separate",
    filters: {
      start: params.get("start") ?? "",
      end: params.get("end") ?? "",
      castDne: params.has("cast_dne") ? params.getAll("cast_dne") : DEFAULT_FILTERS.castDne,
      districts: params.getAll("district"),
      zoneTypes: params.has("zone_type") ? params.getAll("zone_type") : DEFAULT_FILTERS.zoneTypes,
      sameZones: params.get("same_zones") === "true",
    },
  };
}

export function queryFor(filters) {
  const params = new URLSearchParams();
  if (filters.start) params.set("start", filters.start);
  if (filters.end) params.set("end", filters.end);
  filters.castDne.forEach((value) => params.append("cast_dne", value));
  filters.districts.forEach((value) => params.append("district", value));
  filters.zoneTypes.forEach((value) => params.append("zone_type", value));
  if (filters.sameZones) params.set("same_zones", "true");
  return params.toString();
}

export function sameSelection(first, second) {
  return first.length === second.length && first.every((value) => second.includes(value));
}

export function defaultFiltersFor(options) {
  if (!options) return DEFAULT_FILTERS;
  return {
    ...DEFAULT_FILTERS,
    castDne: DEFAULT_FILTERS.castDne.filter((value) => options.cast_dne.includes(value)),
    zoneTypes: DEFAULT_FILTERS.zoneTypes.filter((value) => options.zone_types.includes(value)),
  };
}

export function stateToSearch(filters, view, metric, socialAsResidents = true) {
  const params = new URLSearchParams({ page: "explorer" });
  if (filters.start) params.set("start", filters.start);
  if (filters.end) params.set("end", filters.end);
  if (!sameSelection(filters.castDne, DEFAULT_FILTERS.castDne)) {
    filters.castDne.forEach((value) => params.append("cast_dne", value));
  }
  filters.districts.forEach((value) => params.append("district", value));
  if (!sameSelection(filters.zoneTypes, DEFAULT_FILTERS.zoneTypes)) {
    filters.zoneTypes.forEach((value) => params.append("zone_type", value));
  }
  if (filters.sameZones) params.set("same_zones", "true");
  if (view !== "charts") params.set("view", view);
  if (metric !== "permits-spaces") params.set("metric", metric);
  if (!socialAsResidents) params.set("social", "separate");
  return params.toString();
}

export function monthStart(value) {
  return value ? `${value}-01` : "";
}

export function monthEnd(value) {
  if (!value) return "";
  const [year, month] = value.split("-").map(Number);
  return new Date(Date.UTC(year, month, 0)).toISOString().slice(0, 10);
}

export function selectionIsAll(selected, available) {
  return selected.length === 0
    || (selected.length === available.length && available.every((value) => selected.includes(value)));
}
