export const TIME_LABELS = {
  "Po-Pá": "Pracovní dny",
  "Po-Pá (MPD)": "Pracovní dny · měření",
  "So-Ne": "Víkend",
  den: "Den",
  noc: "Noc",
};
export const ZONE_LABELS = {
  MIX: "Smíšená",
  OST: "Ostatní",
  RES: "Rezidentní",
  VIS: "Návštěvnická",
};

export const numberFormat = new Intl.NumberFormat("cs-CZ", { maximumFractionDigits: 0 });
export const ratioFormat = new Intl.NumberFormat("cs-CZ", {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});
const percentFormat = new Intl.NumberFormat("cs-CZ", { style: "percent", maximumFractionDigits: 0 });
const dateFormat = new Intl.DateTimeFormat("cs-CZ");

export function formatNumber(value) {
  return value == null ? "—" : numberFormat.format(value);
}

export function formatRatio(value) {
  return value == null ? "—" : ratioFormat.format(value);
}

export function formatPercent(value) {
  return value == null ? "—" : percentFormat.format(value);
}

export function formatSigned(value) {
  if (value == null) return "—";
  return `${value > 0 ? "+" : value < 0 ? "−" : ""}${numberFormat.format(Math.abs(value))}`;
}

export function formatDistance(metres) {
  if (metres == null) return "—";
  return metres < 1000
    ? `${numberFormat.format(metres)} m`
    : `${new Intl.NumberFormat("cs-CZ", { maximumFractionDigits: 1 }).format(metres / 1000)} km`;
}

export function isoToCzech(value) {
  if (!value) return "—";
  return dateFormat.format(new Date(`${value}T12:00:00`));
}

export function districtName(value) {
  return value ? value.replace(/^P0?/, "Praha ") : "";
}

const dayMonthFormat = new Intl.DateTimeFormat("cs-CZ", { day: "numeric", month: "long", year: "numeric", timeZone: "UTC" });

// "února 2025": the month in the form that follows "od" and "do".
export function formatMonthGenitive(value) {
  if (!value) return "—";
  const parts = dayMonthFormat.formatToParts(new Date(`${value.slice(0, 7)}-01T00:00:00Z`));
  const part = (type) => parts.find((item) => item.type === type)?.value;
  return `${part("month")} ${part("year")}`;
}
