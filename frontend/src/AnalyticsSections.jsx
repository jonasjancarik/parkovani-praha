import { useEffect, useMemo, useState } from "react";
import { IconInfoCircle } from "@tabler/icons-react";
import { EChart } from "./EChart.jsx";
import { formatMonth, materializeMonthlyRows, niceAxisMax, spreadLabels } from "./chartData.js";
import { SOCIAL_NOTE, SOCIAL_OPTIONS, mergeSeries } from "./parkerGroups.js";

// Red and slate stay reserved for permits and for permits per space, so breakdowns use other hues.
const COLORS = ["#114f50", "#9a684f", "#8a866c", "#b48b87", "#5d8a8b", "#c2a878"];
const SLATE = "#667386";
const QUIET_LINE = "#bdb6aa";
const ZONE_LABELS = { MIX: "Smíšená", OST: "Ostatní", RES: "Rezidentní", VIS: "Návštěvnická" };
const ZONE_COLORS = { RES: "#114f50", MIX: "#9a684f", VIS: "#8a866c", OST: "#b48b87" };
const numberFormat = new Intl.NumberFormat("cs-CZ", { maximumFractionDigits: 0 });
const ratioFormat = new Intl.NumberFormat("cs-CZ", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const scalarValue = (value) => Array.isArray(value) ? value.at(-1) : value;
const formatNumber = (value) => scalarValue(value) == null ? "—" : numberFormat.format(scalarValue(value));
const formatRatio = (value) => scalarValue(value) == null ? "—" : ratioFormat.format(scalarValue(value));

function useMediaQuery(query) {
  const [matches, setMatches] = useState(() => window.matchMedia(query).matches);
  useEffect(() => {
    const media = window.matchMedia(query);
    const update = () => setMatches(media.matches);
    media.addEventListener("change", update);
    return () => media.removeEventListener("change", update);
  }, [query]);
  return matches;
}

function motionOption(option, reduceMotion) {
  return reduceMotion ? { ...option, animationDuration: 0 } : option;
}
const percentFormat = new Intl.NumberFormat("cs-CZ", { style: "percent", maximumFractionDigits: 0 });

const axisText = { color: "#77736a", fontFamily: "Inter, sans-serif", fontSize: 11 };
const legendText = { color: "#4f4c45", fontFamily: "Inter, sans-serif", fontSize: 11 };
const monthPointer = { label: { formatter: ({ value }) => formatMonth(value) } };
const splitLine = { lineStyle: { color: "#e4dfd5", type: "dashed" } };

function lineGradient(color, faded) {
  return {
    type: "linear",
    x: 0,
    y: 0,
    x2: 1,
    y2: 0,
    colorStops: [
      { offset: 0, color: faded },
      { offset: 0.58, color },
      { offset: 1, color },
    ],
  };
}

function areaGradient(color) {
  return {
    type: "linear",
    x: 0,
    y: 0,
    x2: 0,
    y2: 1,
    colorStops: [
      { offset: 0, color: `${color}24` },
      { offset: 1, color: `${color}02` },
    ],
  };
}

function districtLabel(value) {
  return value.replace(/^P0?/, "Praha ");
}

function compactSeries(rows, limit = 5) {
  const totals = new Map();
  rows.forEach((row) => totals.set(row.series, (totals.get(row.series) ?? 0) + row.value));
  const leaders = new Set(
    [...totals.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, limit)
      .map(([name]) => name),
  );
  const merged = new Map();
  rows.forEach((row) => {
    const series = leaders.has(row.series) ? row.series : "Ostatní typy";
    const key = `${row.date}|${series}`;
    merged.set(key, { date: row.date, series, value: (merged.get(key)?.value ?? 0) + row.value });
  });
  return [...merged.values()];
}

function timeSeriesOption(rows, { stacked = true, seriesLabels = {}, seriesColors = {} } = {}) {
  const compactRows = compactSeries(rows);
  const dates = [...new Set(compactRows.map((row) => row.date))].sort();
  const calendarDates = materializeMonthlyRows(
    dates.map((date) => ({ date, present: true })),
    ["present"],
  ).map((row) => row.date);
  const names = [...new Set(compactRows.map((row) => row.series))];
  const valueByKey = new Map(compactRows.map((row) => [`${row.date}|${row.series}`, row.value]));
  const colorOf = (name, index) => seriesColors[name] ?? COLORS[index % COLORS.length];
  return {
    animationDuration: 500,
    color: COLORS,
    grid: { left: 54, right: 18, top: 44, bottom: 38 },
    legend: {
      top: 3,
      left: 0,
      itemWidth: 16,
      itemHeight: 3,
      formatter: (name) => seriesLabels[name] ?? name,
      textStyle: legendText,
    },
    tooltip: {
      trigger: "axis",
      backgroundColor: "#fffdf8",
      borderColor: "#d9d4ca",
      textStyle: { color: "#26251f", fontFamily: "Inter, sans-serif" },
      valueFormatter: formatNumber,
    },
    xAxis: {
      type: "time",
      boundaryGap: false,
      axisLine: { lineStyle: { color: "#aaa59a" } },
      axisTick: { show: false },
      axisLabel: { ...axisText, formatter: "{yyyy}", hideOverlap: true },
      axisPointer: monthPointer,
    },
    yAxis: {
      type: "value",
      axisLabel: { ...axisText, formatter: (value) => numberFormat.format(value) },
      splitLine,
    },
    series: names.map((name, index) => ({
      name,
      type: "line",
      stack: stacked ? "total" : undefined,
      showSymbol: false,
      smooth: false,
      connectNulls: false,
      data: calendarDates.map((date) => [date, valueByKey.get(`${date}|${name}`) ?? null]),
      itemStyle: { color: colorOf(name, index) },
      lineStyle: {
        width: 1.5,
        color: lineGradient(colorOf(name, index), `${colorOf(name, index)}70`),
      },
      areaStyle: stacked ? { color: areaGradient(colorOf(name, index)) } : undefined,
      emphasis: { focus: "series" },
    })),
  };
}

function ChartToggle({ options, value, onChange, label }) {
  return (
    <div className="chart-toggle" role="group" aria-label={label}>
      {options.map((option) => (
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
  );
}

function AnalysisHeader({ title, subtitle, children }) {
  return (
    <div className="analysis-heading">
      <div>
        <h2>{title}</h2>
        <p>{subtitle}</p>
      </div>
      {children}
    </div>
  );
}

function TrendBreakdown({ analytics, reduceMotion }) {
  const [mode, setMode] = useState("spaces");
  const configuration = {
    spaces: {
      rows: analytics.spaces_by_zone,
      seriesLabels: ZONE_LABELS,
      seriesColors: ZONE_COLORS,
      title: "Kapacita podle typu zóny",
      subtitle: "Vývoj počtu parkovacích míst v regulovaných zónách",
    },
    permits: {
      rows: analytics.permits_by_type,
      title: "Oprávnění podle typu",
      subtitle: "Složení vydaných parkovacích oprávnění v čase; bez souhrnných záznamů a budov dál než 500 m od úseku zóny",
    },
    parkers: {
      rows: analytics.parkers_by_type,
      seriesColors: PARKER_COLORS,
      title: "Kdo v zónách parkuje",
      subtitle: "Vývoj odhadovaného počtu parkujících podle skupiny",
    },
  }[mode];
  const option = useMemo(
    () => motionOption(
      timeSeriesOption(configuration.rows, {
        seriesLabels: configuration.seriesLabels,
        seriesColors: configuration.seriesColors,
      }),
      reduceMotion,
    ),
    [configuration.rows, configuration.seriesLabels, configuration.seriesColors, reduceMotion],
  );
  return (
    <section className="analysis-section trend-breakdown">
      <AnalysisHeader title={configuration.title} subtitle={configuration.subtitle}>
        <ChartToggle
          label="Podrobnost vývoje"
          value={mode}
          onChange={setMode}
          options={[
            { value: "spaces", label: "Místa" },
            { value: "permits", label: "Oprávnění" },
            { value: "parkers", label: "Parkující" },
          ]}
        />
      </AnalysisHeader>
      <EChart option={option} className="analysis-chart analysis-chart-wide" ariaLabel={`${configuration.title}: ${configuration.subtitle}`} />
    </section>
  );
}

// Each group keeps its colour in every chart; red and slate stay reserved.
const PARKER_ORDER = ["Rezidenti", "Sociální", "Návštěvníci", "Podnikatelé", "Vlastníci", "Ostatní"];
const PARKER_COLORS = {
  Rezidenti: "#114f50",
  Sociální: "#5d8a8b",
  Návštěvníci: "#9a684f",
  Podnikatelé: "#8a866c",
  Vlastníci: "#c2a878",
  Ostatní: "#b48b87",
};
const PARKER_ROW_HEIGHT = 34;

function spacesText(count) {
  const noun = count === 1 ? "místo" : count >= 2 && count <= 4 ? "místa" : "míst";
  return `${numberFormat.format(count)} ${noun}`;
}

function parkerOrder(names) {
  return [...names].sort((a, b) => (PARKER_ORDER.indexOf(a) + 1 || 99) - (PARKER_ORDER.indexOf(b) + 1 || 99));
}

function parkerShareOption(rows) {
  const shareByKey = new Map(rows.map((row) => [`${row.district}|${row.series}`, row.share]));
  // Districts with the most residents first, so that the bars read as a ranking.
  const districts = [...new Set(rows.map((row) => row.district))]
    .sort((a, b) => (shareByKey.get(`${b}|Rezidenti`) ?? 0) - (shareByKey.get(`${a}|Rezidenti`) ?? 0));
  const names = parkerOrder(new Set(rows.map((row) => row.series)));
  return {
    animationDuration: 450,
    grid: { left: 70, right: 12, top: 40, bottom: 26 },
    legend: { top: 0, left: 0, itemWidth: 13, itemHeight: 8, textStyle: legendText, data: names },
    tooltip: { trigger: "axis", axisPointer: { type: "shadow" }, valueFormatter: (value) => percentFormat.format(value) },
    xAxis: { type: "value", max: 1, axisLabel: { ...axisText, formatter: (value) => percentFormat.format(value) }, splitLine },
    yAxis: {
      type: "category",
      inverse: true,
      data: districts.map(districtLabel),
      axisTick: { show: false },
      axisLine: { show: false },
      axisLabel: axisText,
    },
    series: names.map((name) => ({
      name,
      type: "bar",
      stack: "share",
      barWidth: 18,
      itemStyle: { color: PARKER_COLORS[name] ?? QUIET_LINE },
      label: {
        show: true,
        position: "inside",
        formatter: ({ value }) => (value >= 0.08 ? percentFormat.format(value) : ""),
        color: ["Vlastníci", "Ostatní"].includes(name) ? "#24231f" : "#fffdf8",
        fontFamily: "Inter, sans-serif",
        fontSize: 11,
      },
      data: districts.map((district) => shareByKey.get(`${district}|${name}`) ?? 0),
      emphasis: { focus: "series" },
    })),
  };
}

function CompositionSection({ analytics, reduceMotion }) {
  const parkerOption = useMemo(
    () => motionOption(parkerShareOption(analytics.parker_share_by_district), reduceMotion),
    [analytics.parker_share_by_district, reduceMotion],
  );
  const districtCount = new Set(analytics.parker_share_by_district.map((row) => row.district)).size;
  return (
    <section className="analysis-section composition-section">
      <div className="composition-grid">
        <div className="composition-panel zone-composition">
          <AnalysisHeader title="Složení parkovací kapacity" subtitle="Podíl míst podle typu regulované zóny" />
          <div className="share-bar" aria-hidden="true">
            {analytics.zone_mix.map((row, index) => (
              <span
                key={row.zone_type}
                style={{ flexGrow: row.share, background: ZONE_COLORS[row.zone_type] ?? COLORS[index % COLORS.length] }}
              />
            ))}
          </div>
          <table className="share-legend">
            <caption className="sr-only">Parkovací místa podle typu zóny</caption>
            <tbody>
              {analytics.zone_mix.map((row, index) => (
                <tr key={row.zone_type}>
                  <th scope="row">
                    <i style={{ background: ZONE_COLORS[row.zone_type] ?? COLORS[index % COLORS.length] }} aria-hidden="true" />
                    {ZONE_LABELS[row.zone_type] ?? row.zone_type}
                  </th>
                  <td>{spacesText(row.spaces)}</td>
                  <td className="share-legend-value">{percentFormat.format(row.share)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <div className="composition-panel parker-composition">
          <AnalysisHeader
            title="Složení parkujících podle městské části"
            subtitle="Podíl skupin na zaparkovaných autech, seřazeno podle podílu rezidentů"
          />
          <EChart
            option={parkerOption}
            className="parker-share-chart"
            style={{ height: 66 + Math.max(districtCount, 1) * PARKER_ROW_HEIGHT }}
            ariaLabel="Složení skupin parkujících podle městské části"
          />
        </div>
      </div>
    </section>
  );
}

function changeScatterOption(rows) {
  return {
    animationDuration: 450,
    grid: { left: 58, right: 20, top: 24, bottom: 42 },
    tooltip: {
      trigger: "item",
      formatter: ({ data }) => `${data[3]}<br/>Start: ${numberFormat.format(data[0])}<br/>Roční změna: ${numberFormat.format(data[1])}`,
    },
    xAxis: { type: "value", name: "Oprávnění na začátku", nameLocation: "middle", nameGap: 28, nameTextStyle: axisText, axisLabel: axisText, splitLine },
    yAxis: { type: "value", name: "Roční změna", nameTextStyle: axisText, axisLabel: axisText, splitLine },
    series: [{
      type: "scatter",
      symbolSize: (value) => Math.max(6, Math.min(20, Math.sqrt(value[2]) / 5)),
      data: rows.map((row) => [row.start_permits, row.annual_change, row.end_permits, row.name]),
      itemStyle: { color: "#114f50", opacity: 0.72 },
      emphasis: { itemStyle: { color: "#24231f", opacity: 1 } },
    }],
  };
}

function histogramOption(rows) {
  const values = rows.reduce((result, row) => {
    if (Number.isFinite(row.annual_change)) result.push(row.annual_change);
    return result;
  }, []);
  if (values.length === 0) {
    return {
      animationDuration: 0,
      xAxis: { type: "category", data: [] },
      yAxis: { type: "value" },
      series: [{ type: "bar", data: [] }],
    };
  }
  const min = Math.min(...values);
  const max = Math.max(...values);
  const binCount = 14;
  const size = max === min ? 1 : (max - min) / binCount;
  const bins = Array.from({ length: binCount }, (_, index) => ({ start: min + index * size, count: 0 }));
  values.forEach((value) => {
    const index = Math.min(binCount - 1, Math.floor((value - min) / size));
    bins[index].count += 1;
  });
  return {
    animationDuration: 450,
    grid: { left: 48, right: 18, top: 20, bottom: 42 },
    tooltip: { trigger: "axis", axisPointer: { type: "shadow" } },
    xAxis: {
      type: "category",
      data: bins.map((bin) => numberFormat.format(bin.start)),
      name: "Roční změna oprávnění",
      nameLocation: "middle",
      nameGap: 30,
      nameTextStyle: axisText,
      axisLabel: { ...axisText, interval: 1, rotate: 24 },
    },
    yAxis: { type: "value", minInterval: 1, axisLabel: axisText, splitLine },
    series: [{ type: "bar", barWidth: "72%", data: bins.map((bin) => bin.count), itemStyle: { color: "#5d8a8b" } }],
  };
}

function ChangeSection({ analytics, reduceMotion }) {
  const [mode, setMode] = useState("scatter");
  const option = useMemo(
    () => motionOption(
      mode === "scatter" ? changeScatterOption(analytics.zsj_changes) : histogramOption(analytics.zsj_changes),
      reduceMotion,
    ),
    [analytics.zsj_changes, mode, reduceMotion],
  );
  return (
    <section className="analysis-section change-section">
      <AnalysisHeader
        title="Jak se mění počet oprávnění v jednotlivých lokalitách"
        subtitle="Každý bod představuje jednu základní sídelní jednotku; změna je přepočtená na jeden rok"
      >
        <ChartToggle
          label="Zobrazení změn"
          value={mode}
          onChange={setMode}
          options={[{ value: "scatter", label: "Souvislosti" }, { value: "histogram", label: "Rozložení" }]}
        />
      </AnalysisHeader>
      <EChart option={option} className="analysis-chart change-chart" ariaLabel="Roční změna počtu parkovacích oprávnění v základních sídelních jednotkách" />
    </section>
  );
}

// Height of `.pressure-chart` in styles.css and the margins of its plot when lines carry their own labels.
const PRESSURE_CHART_HEIGHT = 310;
const PRESSURE_GRID = { left: 48, right: 176, top: 16, bottom: 38 };

function zsjPressureOption(rows, compact = false) {
  const dates = [...new Set(rows.map((row) => row.date))].sort();
  const calendarDates = materializeMonthlyRows(
    dates.map((date) => ({ date, present: true })),
    ["present"],
  ).map((row) => row.date);
  const names = [...new Set(rows.map((row) => row.name))];
  const valueByKey = new Map(rows.map((row) => [`${row.date}|${row.name}`, row.value]));
  const axisMax = niceAxisMax(Math.max(0, ...rows.map((row) => row.value ?? 0)));
  const plotHeight = PRESSURE_CHART_HEIGHT - PRESSURE_GRID.top - PRESSURE_GRID.bottom;
  const lastPositions = names.map((name) => {
    const last = rows.findLast((row) => row.name === name && Number.isFinite(row.value));
    return last ? plotHeight * (1 - last.value / axisMax) : null;
  });
  const labelPositions = spreadLabels(lastPositions, 15, plotHeight);
  // The locality that currently has the most permits per space is the one drawn in colour.
  const leader = lastPositions.reduce(
    (best, position, index) => (position != null && (best < 0 || position < lastPositions[best]) ? index : best),
    -1,
  );
  return {
    animationDuration: 450,
    color: compact ? COLORS : names.map((_, index) => (index === leader ? SLATE : QUIET_LINE)),
    grid: compact ? { left: 48, right: 18, top: 84, bottom: 38 } : PRESSURE_GRID,
    legend: compact
      ? { top: 0, left: 0, itemWidth: 15, itemHeight: 3, textStyle: legendText }
      : { show: false },
    tooltip: { trigger: "axis", valueFormatter: formatRatio },
    xAxis: { type: "time", boundaryGap: false, axisLabel: { ...axisText, formatter: "{yyyy}", hideOverlap: true }, axisPointer: monthPointer, axisTick: { show: false }, axisLine: { lineStyle: { color: "#aaa59a" } } },
    yAxis: { type: "value", max: axisMax, axisLabel: { ...axisText, formatter: (value) => ratioFormat.format(value) }, splitLine },
    series: names.map((name, index) => ({
      name,
      type: "line",
      showSymbol: false,
      smooth: false,
      connectNulls: false,
      data: calendarDates.map((date) => [date, valueByKey.get(`${date}|${name}`) ?? null]),
      lineStyle: compact
        ? { width: index === 0 ? 2.2 : 1.2, type: index > 4 ? "dashed" : "solid" }
        : { width: index === leader ? 2.2 : 1.3 },
      endLabel: {
        show: !compact,
        formatter: name,
        color: index === leader ? "#24231f" : "#5f5b54",
        fontFamily: "Inter, sans-serif",
        fontSize: 11,
        offset: [2, labelPositions[index] == null ? 0 : Math.round(labelPositions[index] - lastPositions[index])],
      },
      emphasis: compact
        ? { focus: "series" }
        : { focus: "series", lineStyle: { color: "#114f50", width: 2.4 } },
    })),
  };
}

function forecastOption(rows, district) {
  const selected = rows.filter((row) => row.district === district);
  const actual = selected.filter((row) => row.kind === "Skutečnost");
  const forecast = selected.filter((row) => row.kind === "Predikce");
  const actualWithGaps = materializeMonthlyRows(actual, ["value"]);
  const forecastWithGaps = materializeMonthlyRows(forecast, ["value"]);
  return {
    animationDuration: 450,
    color: [SLATE, SLATE],
    grid: { left: 48, right: 18, top: 30, bottom: 38 },
    legend: { top: 0, left: 0, itemWidth: 16, itemHeight: 3, textStyle: legendText },
    tooltip: { trigger: "axis", valueFormatter: formatRatio },
    xAxis: { type: "time", axisLabel: { ...axisText, formatter: "{yyyy}" }, axisPointer: monthPointer, axisLine: { lineStyle: { color: "#aaa59a" } }, splitLine: { show: false } },
    yAxis: { type: "value", axisLabel: { ...axisText, formatter: (value) => ratioFormat.format(value) }, splitLine },
    series: [
      {
        name: "Skutečnost",
        type: "line",
        showSymbol: false,
        smooth: false,
        connectNulls: false,
        data: actualWithGaps.map((row) => [row.date, row.value]),
        lineStyle: { width: 2.1, color: lineGradient(SLATE, "#aeb6c0") },
      },
      {
        name: "Predikce",
        type: "line",
        showSymbol: false,
        data: forecastWithGaps.map((row) => [row.date, row.value]),
        lineStyle: { width: 2, type: "dashed", color: SLATE },
      },
    ],
  };
}

function PressureAndForecast({ analytics, reduceMotion }) {
  const compact = useMediaQuery("(max-width: 760px)");
  const districts = useMemo(
    () => [...new Set(analytics.forecast.map((row) => row.district))].sort(),
    [analytics.forecast],
  );
  const [selectedDistrict, setSelectedDistrict] = useState(districts[0] ?? "");
  const district = districts.includes(selectedDistrict) ? selectedDistrict : (districts[0] ?? "");
  const pressureOption = useMemo(
    () => motionOption(zsjPressureOption(analytics.zsj_pressure, compact), reduceMotion),
    [analytics.zsj_pressure, compact, reduceMotion],
  );
  const forecastChart = useMemo(
    () => motionOption(forecastOption(analytics.forecast, district), reduceMotion),
    [analytics.forecast, district, reduceMotion],
  );
  return (
    <section className="analysis-section pressure-forecast-section">
      <div className="pressure-forecast-grid">
        <div>
          <AnalysisHeader title="Lokality s nejvyšším tlakem" subtitle={`Oprávnění na jedno parkovací místo v šesti nejzatíženějších lokalitách${compact ? "" : ". Najetím na čáru ji zvýrazníte."}`} />
          <EChart option={pressureOption} className="analysis-chart pressure-chart" ariaLabel="Vývoj tlaku na parkování v šesti nejzatíženějších lokalitách" />
        </div>
        <div>
          <AnalysisHeader title="Výhled tlaku na parkování" subtitle="Dvanáctiměsíční trend podle posledních 24 měsíců">
            <label className="forecast-select">
              <span>Městská část</span>
              <select value={district} onChange={(event) => setSelectedDistrict(event.target.value)}>
                {districts.map((value) => <option value={value} key={value}>{districtLabel(value)}</option>)}
              </select>
            </label>
          </AnalysisHeader>
          <EChart option={forecastChart} className="analysis-chart forecast-chart" ariaLabel={`Dvanáctiměsíční výhled tlaku na parkování pro ${districtLabel(district)}`} />
          <p className="forecast-note"><IconInfoCircle size={14} stroke={1.5} /> Predikce prodlužuje současný trend; nejde o závazný scénář.</p>
        </div>
      </div>
    </section>
  );
}

function withSocialAsResidents(analytics) {
  return {
    ...analytics,
    permits_by_type: mergeSeries(analytics.permits_by_type, "Sociální", "Rezidentní", ["date"]),
    parkers_by_type: mergeSeries(analytics.parkers_by_type, "Sociální", "Rezidenti", ["date"]),
    parker_share_by_district: mergeSeries(analytics.parker_share_by_district, "Sociální", "Rezidenti", ["district"]),
  };
}

export function AnalyticsSections({ analytics: sourceAnalytics, socialAsResidents, onSocialAsResidents }) {
  const reduceMotion = useMediaQuery("(prefers-reduced-motion: reduce)");
  const analytics = useMemo(
    () => (sourceAnalytics && socialAsResidents ? withSocialAsResidents(sourceAnalytics) : sourceAnalytics),
    [sourceAnalytics, socialAsResidents],
  );
  if (!analytics) return null;
  return (
    <div className="analytics-sections">
      <div className="analytics-intro">
        <span>Podrobnější analýzy</span>
        <div className="analytics-intro-notes">
          <p>Období, městská část a typ zóny platí všude; část dne ovlivňuje jen složení parkujících.</p>
          <p>{SOCIAL_NOTE}</p>
        </div>
        <ChartToggle
          label="Sociální oprávnění"
          value={socialAsResidents}
          onChange={onSocialAsResidents}
          options={SOCIAL_OPTIONS}
        />
      </div>
      <TrendBreakdown analytics={analytics} reduceMotion={reduceMotion} />
      <CompositionSection analytics={analytics} reduceMotion={reduceMotion} />
      <ChangeSection analytics={analytics} reduceMotion={reduceMotion} />
      <PressureAndForecast analytics={analytics} reduceMotion={reduceMotion} />
    </div>
  );
}
