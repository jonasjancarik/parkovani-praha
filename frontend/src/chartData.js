function monthIndex(date) {
  const [year, month] = date.slice(0, 7).split("-").map(Number);
  return year * 12 + month - 1;
}

function monthEndFromIndex(index) {
  const year = Math.floor(index / 12);
  const month = index % 12;
  return new Date(Date.UTC(year, month + 1, 0)).toISOString().slice(0, 10);
}

export function materializeMonthlyRows(rows, valueKeys) {
  if (rows.length === 0) return [];
  const rowByMonth = new Map(rows.map((row) => [row.date.slice(0, 7), row]));
  const indexes = rows.map((row) => monthIndex(row.date));
  const first = Math.min(...indexes);
  const last = Math.max(...indexes);
  const result = [];
  for (let index = first; index <= last; index += 1) {
    const date = monthEndFromIndex(index);
    const existing = rowByMonth.get(date.slice(0, 7));
    result.push(existing ?? Object.fromEntries([
      ["date", date],
      ...valueKeys.map((key) => [key, null]),
    ]));
  }
  return result;
}

export function trailingObservationAverage(rows, key, windowSize = 12) {
  const observations = [];
  return rows.map((row) => {
    const value = row[key];
    if (!Number.isFinite(value)) return [row.date, null];

    observations.push(value);
    if (observations.length < windowSize) return [row.date, null];
    const window = observations.slice(-windowSize);
    return [
      row.date,
      window.reduce((total, observation) => total + observation, 0) / windowSize,
    ];
  });
}

export function hasDrawableObservationAverage(rows, key, windowSize = 12) {
  const trend = trailingObservationAverage(rows, key, windowSize);
  return trend.some(([, value], index) => (
    Number.isFinite(value) && Number.isFinite(trend[index - 1]?.[1])
  ));
}

const monthFormat = new Intl.DateTimeFormat("cs-CZ", { month: "long", year: "numeric", timeZone: "UTC" });

export function formatMonth(value) {
  if (value == null || value === "") return "—";
  const date = typeof value === "string" ? new Date(`${value.slice(0, 7)}-01T00:00:00Z`) : new Date(value);
  return monthFormat.format(date);
}

export function monthsBetween(min, max) {
  if (!min || !max) return [];
  const months = [];
  for (let index = monthIndex(min); index <= monthIndex(max); index += 1) {
    months.push(monthEndFromIndex(index).slice(0, 7));
  }
  return months;
}

export function niceAxisMax(value) {
  if (!Number.isFinite(value) || value <= 0) return 1;
  const magnitude = 10 ** Math.floor(Math.log10(value));
  const step = [1, 1.5, 2, 2.5, 3, 4, 5, 6, 8, 10].find((candidate) => candidate * magnitude >= value);
  return step * magnitude;
}

// Moves labels apart vertically so that neighbours keep at least `gap` pixels
// between their centres and none ends up below `limit`.
export function spreadLabels(positions, gap, limit) {
  const sorted = positions
    .map((position, index) => ({ position, index }))
    .filter((item) => Number.isFinite(item.position))
    .sort((a, b) => a.position - b.position);
  const placed = [];
  sorted.forEach((item, order) => {
    placed.push(order === 0 ? item.position : Math.max(item.position, placed[order - 1] + gap));
  });
  for (let order = placed.length - 1; order >= 0; order -= 1) {
    const ceiling = order === placed.length - 1 ? limit : placed[order + 1] - gap;
    placed[order] = Math.min(placed[order], ceiling);
  }
  const result = positions.map(() => null);
  sorted.forEach((item, order) => { result[item.index] = placed[order]; });
  return result;
}
