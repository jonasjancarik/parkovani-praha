// Social permits belong to residents with a discount, so by default they are
// shown as part of the residents. "Podnikatelé" are called "abonenti" by TSK.
export const SOCIAL_NOTE = "Sociální oprávnění mají rezidenti se zvýhodněním. Podnikatelé jsou v datech TSK vedeni jako abonenti.";

export const SOCIAL_OPTIONS = [
  { value: true, label: "Sociální k rezidentům" },
  { value: false, label: "Sociální zvlášť" },
];

function addValues(first, second) {
  return first == null && second == null ? null : (first ?? 0) + (second ?? 0);
}

// Adds the rows of series `from` to series `into` that share the same `keyFields`.
export function mergeSeries(rows, from, into, keyFields) {
  const merged = new Map();
  rows.forEach((row) => {
    const series = row.series === from ? into : row.series;
    const key = [...keyFields.map((field) => row[field]), series].join("|");
    const current = merged.get(key);
    if (!current) {
      merged.set(key, { ...row, series });
      return;
    }
    current.value = addValues(current.value, row.value);
    if ("share" in current || "share" in row) current.share = addValues(current.share, row.share);
  });
  return [...merged.values()];
}

// The same for one row of named columns, e.g. { rezidentska, socialni }.
export function mergeColumns(row, from, into) {
  if (!(from in row)) return row;
  const { [from]: added, ...rest } = row;
  return { ...rest, [into]: addValues(row[into], added) };
}
