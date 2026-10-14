import { useMemo } from "react";

import { EChart } from "./EChart.jsx";
import { formatMonth, materializeMonthlyRows } from "./chartData.js";
import { formatNumber, formatPercent } from "./format.js";
import { SOCIAL_NOTE, SOCIAL_OPTIONS, mergeColumns } from "./parkerGroups.js";

const RED = "#e33b32";
const TEAL = "#114f50";
// Breakdowns stay clear of red and slate, which mean permits and permits per space.
const MUTED = ["#114f50", "#9a684f", "#8a866c", "#b48b87", "#5d8a8b", "#c2a878", "#77736a", "#d6cfc2"];
const EMPTY_SPACE = "#cfc8bb";
const PARKER_LABELS = {
  rezidentska: "Rezidenti",
  socialni: "Sociální",
  vlastnicka: "Vlastníci",
  abonentska: "Podnikatelé",
  navstevnici: "Návštěvníci",
  volna_mista: "Volná místa",
};
const PERMIT_LABELS = {
  pop_rezidentska: "Rezidentní",
  pop_vlastnicka: "Vlastnická",
  pop_abonentska: "Podnikatelská",
  pop_prenosna: "Přenosná",
  pop_carsharing: "Carsharing",
  pop_ekologicka: "Ekologická",
  pop_ostatni: "Ostatní",
  pop_socialni: "Sociální",
};
const DAY = 24 * 3600 * 1000;
const axisText = { color: "#77736a", fontFamily: "Inter, sans-serif", fontSize: 11 };
const pointValue = (value) => (Array.isArray(value) ? value.at(-1) : value);

function timeOption(rows, lines, { format = formatNumber, fromZero = true, stacked = false, reduceMotion }) {
  const monthly = materializeMonthlyRows(rows, lines.map((line) => line.key));
  const withLegend = lines.length > 1;
  const yearly = monthly.length > 30;
  return {
    animationDuration: reduceMotion ? 0 : 500,
    grid: { left: 52, right: 16, top: withLegend ? 44 : 16, bottom: 34 },
    legend: withLegend
      ? {
          top: 3,
          left: 0,
          itemWidth: 16,
          itemHeight: 3,
          textStyle: { color: "#4f4c45", fontFamily: "Inter, sans-serif", fontSize: 11 },
        }
      : { show: false },
    tooltip: {
      trigger: "axis",
      backgroundColor: "#fffdf8",
      borderColor: "#d9d4ca",
      textStyle: { color: "#26251f", fontFamily: "Inter, sans-serif" },
      valueFormatter: (value) => format(pointValue(value)),
    },
    xAxis: {
      type: "time",
      boundaryGap: false,
      axisLine: { lineStyle: { color: "#aaa59a" } },
      axisTick: { show: false },
      // One label per year; a span of a year or two gets months instead.
      minInterval: (yearly ? 365 : 28) * DAY,
      axisLabel: { ...axisText, formatter: yearly ? "{yyyy}" : "{M}/{yyyy}", hideOverlap: true },
      axisPointer: { label: { formatter: ({ value }) => formatMonth(value) } },
    },
    yAxis: {
      type: "value",
      scale: !fromZero,
      minInterval: format === formatNumber ? 1 : undefined,
      axisLabel: { ...axisText, formatter: format },
      splitLine: { lineStyle: { color: "#e4dfd5", type: "dashed" } },
    },
    series: lines.map((line) => ({
      name: line.name,
      type: "line",
      stack: stacked ? "total" : undefined,
      showSymbol: monthly.length < 3,
      connectNulls: false,
      data: monthly.map((row) => [row.date, row[line.key]]),
      itemStyle: { color: line.color },
      lineStyle: { color: line.color, width: stacked ? 1.2 : 2.2 },
      areaStyle: stacked ? { color: line.color, opacity: 0.22 } : undefined,
      emphasis: { focus: "series" },
    })),
  };
}

export function SocialToggle({ value, onChange }) {
  return (
    <div className="social-toggle">
      <div className="chart-toggle" role="group" aria-label="Sociální oprávnění">
        {SOCIAL_OPTIONS.map((option) => (
          <button
            key={String(option.value)}
            type="button"
            className={value === option.value ? "active" : ""}
            aria-pressed={value === option.value}
            onClick={() => onChange(option.value)}
          >
            {option.label}
          </button>
        ))}
      </div>
      <p>{SOCIAL_NOTE}</p>
    </div>
  );
}

export function CapacityChart({ series, reduceMotion }) {
  const option = useMemo(
    () => timeOption(series, [{ key: "spaces", name: "Parkovací místa", color: TEAL }], { fromZero: false, reduceMotion }),
    [series, reduceMotion],
  );
  return <EChart option={option} className="address-chart" ariaLabel="Vývoj počtu parkovacích míst ve vybraném okruhu" />;
}

function breakdownLines(rows, labels) {
  return Object.keys(labels)
    .filter((key) => rows.some((row) => row[key] > 0))
    .map((key, index) => ({
      key,
      name: labels[key],
      color: key === "volna_mista" ? EMPTY_SPACE : MUTED[index % MUTED.length],
    }));
}

const WHERE = {
  zone: { in: "v úseku", permits: "pro adresy u tohoto úseku" },
  circle: { in: "ve všech úsecích v okruhu", permits: "pro adresy u úseků v okruhu" },
};

export function ReferenceCharts({ series, reduceMotion, scope = "zone", socialAsResidents = true }) {
  const where = WHERE[scope];
  const charts = useMemo(() => {
    const rows = series.map((row) => {
      const parkers = socialAsResidents ? mergeColumns(row.parkers, "socialni", "rezidentska") : row.parkers;
      const permits = socialAsResidents
        ? mergeColumns(row.permit_types, "pop_socialni", "pop_rezidentska")
        : row.permit_types;
      return { ...row, ...parkers, ...permits };
    });
    const settings = { reduceMotion };
    return [
      {
        title: "Oprávnění a místa",
        note: `Počet vydaných oprávnění a parkovacích míst ${where.in}`,
        option: timeOption(rows, [
          { key: "permits", name: "Parkovací oprávnění", color: RED },
          { key: "spaces", name: "Parkovací místa", color: TEAL },
        ], settings),
      },
      {
        title: "Obsazenost a respektovanost",
        note: scope === "circle"
          ? "Podíl obsazených míst a podíl aut, která parkují oprávněně, vážený počtem míst v úsecích"
          : "Podíl obsazených míst a podíl aut, která parkují oprávněně",
        option: timeOption(rows, [
          { key: "occupancy", name: "Obsazenost", color: MUTED[1] },
          { key: "respect", name: "Respektovanost", color: MUTED[4] },
        ], { ...settings, format: formatPercent }),
      },
      {
        title: "Kdo tu parkuje",
        note: `Odhad počtu obsazených míst podle skupiny ${where.in}`,
        option: timeOption(rows, breakdownLines(rows, PARKER_LABELS), { ...settings, stacked: true }),
      },
      {
        title: "Oprávnění podle typu",
        note: `Složení vydaných oprávnění ${where.permits}`,
        option: timeOption(rows, breakdownLines(rows, PERMIT_LABELS), { ...settings, stacked: true }),
      },
    ];
  }, [series, reduceMotion, where, scope, socialAsResidents]);

  return (
    <div className="reference-charts">
      {charts.map((chart) => (
        <div key={chart.title}>
          <h3>{chart.title}</h3>
          <p>{chart.note}</p>
          <EChart option={chart.option} className="address-chart" ariaLabel={`${chart.title}: ${chart.note}`} />
        </div>
      ))}
    </div>
  );
}
