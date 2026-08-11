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
