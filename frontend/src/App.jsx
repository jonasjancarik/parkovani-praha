import { useEffect, useMemo, useState } from "react";
import {
  IconAdjustmentsHorizontal,
  IconChevronDown,
  IconDownload,
  IconInfoCircle,
} from "@tabler/icons-react";
import { EChart } from "./EChart.jsx";
import { AnalyticsSections } from "./AnalyticsSections.jsx";
import {
  formatMonth,
  hasDrawableObservationAverage,
  materializeMonthlyRows,
  niceAxisMax,
  spreadLabels,
  trailingObservationAverage,
} from "./chartData.js";
import {
  DateRange,
  DistrictSelect,
  MultiSelect,
  SameZonesToggle,
  ToolbarActions,
} from "./ExplorerControls.jsx";
import {
  CoverageNotice,
  ErrorState,
  LoadingState,
  NoResultsState,
} from "./ExplorerStates.jsx";
import {
  defaultFiltersFor,
  initialStateFromSearch,
  monthEnd,
  monthStart,
  queryFor,
  sameSelection,
  stateToSearch,
} from "./explorerState.js";
import {
  TIME_LABELS,
  ZONE_LABELS,
  districtName,
  formatNumber,
  formatRatio,
  isoToCzech,
  numberFormat,
  ratioFormat,
} from "./format.js";
import { Masthead } from "./Masthead.jsx";

const RED = "#e33b32";
const TEAL = "#114f50";
const SLATE = "#667386";
// Height of `.time-chart` in styles.css at widths where the lines carry their own labels.
const TIME_CHART_HEIGHT = 460;
const CHART_BOTTOM = 42;
const RATIO_PANEL_HEIGHT = 84;
const PANEL_GAP = 46;
// Room to the right of the lines, reserved for their labels.
const END_LABEL_WIDTH = 150;
const END_LABEL_HEIGHT = 48;


function lineGradient(start, end) {
  return {
    type: "linear",
    x: 0,
    y: 0,
    x2: 1,
    y2: 0,
    colorStops: [
      { offset: 0, color: start },
      { offset: 0.58, color: end },
      { offset: 1, color: end },
    ],
  };
}

function pct(value) {
  if (value == null) return "—";
  const rounded = Math.round(value * 100);
  return `${rounded > 0 ? "+" : ""}${rounded} %`;
}

function downloadSeriesCsv(series) {
  const header = ["datum", "parkovaci_opravneni", "parkovaci_mista", "opravneni_na_misto"];
  const rows = series.map((row) => [row.date, row.permits, row.spaces, row.permits_per_space]);
  const csv = [header, ...rows].map((row) => row.join(",")).join("\n");
  const url = URL.createObjectURL(new Blob([`\uFEFF${csv}`], { type: "text/csv;charset=utf-8" }));
  const link = document.createElement("a");
  link.href = url;
  link.download = "parkovani-praha-vyvoj.csv";
  link.click();
  URL.revokeObjectURL(url);
}

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

function lastValue(rows, key) {
  for (let index = rows.length - 1; index >= 0; index -= 1) {
    if (Number.isFinite(rows[index][key])) return rows[index][key];
  }
  return null;
}

function highestValue(rows, keys) {
  return rows.reduce((highest, row) => Math.max(
    highest,
    ...keys.map((key) => (Number.isFinite(row[key]) ? row[key] : 0)),
  ), 0);
}

// Vertical shifts that keep the labels at the right end of the lines from covering each other.
function endLabelShifts(rows, axisMax, plotHeight, keys) {
  const positions = keys.map((key) => {
    const value = lastValue(rows, key);
    return value == null ? null : plotHeight * (1 - value / axisMax);
  });
  const placed = spreadLabels(positions, END_LABEL_HEIGHT, plotHeight);
  return Object.fromEntries(keys.map((key, index) => [
    key,
    placed[index] == null ? 0 : Math.round(placed[index] - positions[index]),
  ]));
}

function lineChartOption(
  series,
  compact = false,
  reduceMotion = false,
  metric = "permits-spaces",
  showTrend = false,
) {
  const pressureOnly = metric === "pressure";
  const axisText = { color: "#73736f", fontFamily: "Inter, sans-serif", fontSize: 12 };
  const pointValue = (value) => Array.isArray(value) ? value[1] : value;
  const chartRows = materializeMonthlyRows(
    series,
    ["permits", "spaces", "permits_per_space"],
  );
  const countMax = niceAxisMax(highestValue(chartRows, ["permits", "spaces"]));
  const ratioMax = Math.max(2, Math.ceil(highestValue(chartRows, ["permits_per_space"]) * 2) / 2);
  const top = compact ? 58 : 30;
  const sides = { left: 58, right: compact ? 14 : END_LABEL_WIDTH };
  const countBottom = CHART_BOTTOM + RATIO_PANEL_HEIGHT + PANEL_GAP;
  const labelShift = endLabelShifts(
    chartRows,
    countMax,
    TIME_CHART_HEIGHT - top - countBottom,
    ["permits", "spaces"],
  );
  // The ratio has its own scale, so it gets its own panel under the counts.
  const ratioPanel = pressureOnly ? 0 : 1;

  const timeAxis = {
    type: "time",
    boundaryGap: false,
    axisLine: { lineStyle: { color: "#aaa59a" } },
    axisTick: { show: false },
    axisLabel: { ...axisText, formatter: "{yyyy}", hideOverlap: true },
    axisPointer: { label: { formatter: ({ value }) => formatMonth(value) } },
  };
  const splitLine = { lineStyle: { color: "#e5e0d7", type: "dashed" } };
  const countAxis = {
    type: "value",
    min: 0,
    max: countMax,
    splitNumber: 4,
    axisLabel: { ...axisText, formatter: (value) => numberFormat.format(value) },
    splitLine,
  };
  const ratioAxis = {
    type: "value",
    min: 0,
    max: ratioMax,
    splitNumber: pressureOnly ? 4 : 2,
    axisLabel: { ...axisText, formatter: (value) => ratioFormat.format(value) },
    splitLine,
  };
  const common = {
    type: "line",
    showSymbol: false,
    smooth: false,
    connectNulls: false,
    emphasis: { focus: "series" },
    endLabel: {
      show: !compact,
      formatter: (params) => `${params.seriesName}\n${formatNumber(pointValue(params.value))}`,
      fontFamily: "Newsreader, Georgia, serif",
      fontSize: 14,
      lineHeight: 19,
    },
    tooltip: { valueFormatter: (value) => formatNumber(pointValue(value)) },
  };
  const ratioCommon = {
    ...common,
    xAxisIndex: ratioPanel,
    yAxisIndex: ratioPanel,
    tooltip: { valueFormatter: (value) => formatRatio(pointValue(value)) },
  };
  const lineStyle = (faded, color, width) => ({
    color: lineGradient(faded, color),
    width: showTrend ? width / 2 : width,
    opacity: showTrend ? 0.38 : 1,
  });

  const countSeries = [
    {
      ...common,
      name: "Parkovací oprávnění",
      data: chartRows.map((row) => [row.date, row.permits]),
      endLabel: { ...common.endLabel, color: RED, offset: [0, labelShift.permits] },
      lineStyle: lineStyle("#ed827b", RED, 2.4),
    },
    {
      ...common,
      name: "Parkovací místa",
      data: chartRows.map((row) => [row.date, row.spaces]),
      endLabel: { ...common.endLabel, color: TEAL, offset: [0, labelShift.spaces] },
      lineStyle: lineStyle("#799b98", TEAL, 2.4),
    },
  ];
  const ratioSeries = [{
    ...ratioCommon,
    name: "Oprávnění na místo",
    data: chartRows.map((row) => [row.date, row.permits_per_space]),
    endLabel: {
      ...common.endLabel,
      color: SLATE,
      formatter: (params) => `${params.seriesName}\n${formatRatio(pointValue(params.value))}`,
    },
    lineStyle: lineStyle("#aeb6c0", SLATE, 2.2),
  }];
  if (showTrend) {
    const average = (name, key, color, base) => ({
      ...base,
      name: `Průměr z 12 měření · ${name}`,
      data: trailingObservationAverage(chartRows, key),
      endLabel: { show: false },
      lineStyle: { color, width: 2.4, type: "dashed" },
    });
    countSeries.push(
      average("Parkovací oprávnění", "permits", RED, common),
      average("Parkovací místa", "spaces", TEAL, common),
    );
    ratioSeries.push(average("Oprávnění na místo", "permits_per_space", SLATE, ratioCommon));
  }

  const legendLabels = {
    "Parkovací oprávnění": "Oprávnění",
    "Parkovací místa": "Místa",
    "Oprávnění na místo": "Oprávnění / místo",
  };
  return {
    animationDuration: reduceMotion ? 0 : 650,
    color: pressureOnly ? [SLATE] : [RED, TEAL, SLATE],
    grid: pressureOnly
      ? [{ ...sides, top, bottom: CHART_BOTTOM }]
      : [
          { ...sides, top, bottom: countBottom },
          { ...sides, height: RATIO_PANEL_HEIGHT, bottom: CHART_BOTTOM },
        ],
    legend: compact
      ? {
          show: true,
          top: 12,
          left: 0,
          itemWidth: 18,
          itemHeight: 2,
          data: Object.keys(legendLabels).slice(pressureOnly ? 2 : 0),
          formatter: (name) => legendLabels[name] ?? name,
          textStyle: { color: "#4f4c45", fontFamily: "Inter, sans-serif", fontSize: 11 },
        }
      : { show: false },
    tooltip: {
      trigger: "axis",
      backgroundColor: "#fffdf8",
      borderColor: "#d9d4ca",
      textStyle: { color: "#26251f", fontFamily: "Inter, sans-serif" },
      // Both panels report into one list: counts first, then the ratio.
      formatter: (items) => {
        const rows = [...items].sort((first, second) => first.seriesIndex - second.seriesIndex);
        const lines = rows.map((item) => {
          const format = item.seriesName.includes("Oprávnění na místo") ? formatRatio : formatNumber;
          return `<div style="display:flex;justify-content:space-between;gap:24px"><span>${item.marker}${item.seriesName}</span><strong>${format(pointValue(item.value))}</strong></div>`;
        });
        return `<div style="margin-bottom:6px">${formatMonth(rows[0].axisValue)}</div>${lines.join("")}`;
      },
    },
    axisPointer: { link: [{ xAxisIndex: "all" }] },
    xAxis: pressureOnly
      ? [timeAxis]
      : [{ ...timeAxis, axisLabel: { show: false } }, { ...timeAxis, gridIndex: 1 }],
    yAxis: pressureOnly ? [ratioAxis] : [countAxis, { ...ratioAxis, gridIndex: 1 }],
    series: pressureOnly ? ratioSeries : [...countSeries, ...ratioSeries],
  };
}

function hasDrawableObservationTrend(series, keys) {
  const rows = materializeMonthlyRows(series, keys);
  return keys.some((key) => hasDrawableObservationAverage(rows, key));
}

function ChartToggle({ label, value, options, onChange }) {
  return (
    <div className="chart-toggle" role="group" aria-label={label}>
      {options.map((option) => (
        <button
          key={String(option.value)}
          type="button"
          className={value === option.value ? "active" : ""}
          aria-pressed={value === option.value}
          disabled={option.disabled}
          onClick={() => onChange(option.value)}
        >
          {option.label}
        </button>
      ))}
    </div>
  );
}

function DistrictTable({ districts, month }) {
  const highest = Math.max(...districts.map((row) => row.permits_per_space ?? 0), 0);
  return (
    <section className="district-section" aria-labelledby="district-heading">
      <div className="section-heading">
        <h2 id="district-heading">Porovnání městských částí</h2>
        <p>Oprávnění na 1 parkovací místo, {month}</p>
      </div>
      <table className="district-table">
        <caption className="sr-only">Pět městských částí, kde na jedno parkovací místo připadá nejvíc oprávnění</caption>
        <thead>
          <tr>
            <th scope="col">Městská část</th>
            <td aria-hidden="true" />
            <th scope="col">Oprávnění na místo</th>
            <th scope="col">Oprávnění</th>
            <th scope="col">Parkovací místa</th>
          </tr>
        </thead>
        <tbody>
          {districts.map((row) => (
            <tr key={row.district}>
              <th scope="row">{districtName(row.district)}</th>
              <td className="district-bar" aria-hidden="true">
                <span style={{ width: `${highest ? (row.permits_per_space / highest) * 100 : 0}%` }} />
              </td>
              <td className="district-ratio">{formatRatio(row.permits_per_space)}</td>
              <td>{formatNumber(row.permits)}</td>
              <td>{formatNumber(row.spaces)}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <p className="data-note">
        Pozn.: Zahrnuje aktivní oprávnění a veřejná i neveřejná parkovací místa v regulovaných zónách.
        Do počtů se nezapočítávají oprávnění budov dál než 500 m od nejbližšího úseku zóny (asi 2 %) a souhrnné záznamy bez konkrétního místa, zejména ekologická oprávnění vedená na jedné adrese. Součty jsou proto nižší než ve zdrojových datech TSK.
      </p>
    </section>
  );
}

function ChartView({ payload, metric, onMetric }) {
  const compactChart = useMediaQuery("(max-width: 1100px)");
  const reduceMotion = useMediaQuery("(prefers-reduced-motion: reduce)");
  const [trendRequested, setTrendRequested] = useState(false);
  const trendAvailable = useMemo(
    () => hasDrawableObservationTrend(
      payload.series,
      metric === "pressure"
        ? ["permits_per_space"]
        : ["permits", "spaces", "permits_per_space"],
    ),
    [payload.series, metric],
  );
  const showTrend = trendRequested && trendAvailable;

  const visibleDistricts = useMemo(() => payload.districts.slice(0, 5), [payload.districts]);
  const lineOption = useMemo(
    () => lineChartOption(payload.series, compactChart, reduceMotion, metric, showTrend),
    [payload.series, compactChart, reduceMotion, metric, showTrend],
  );

  return (
    <>
      <section className="chart-section" aria-labelledby="time-heading">
        <div className="section-heading chart-heading">
          <div>
            <h2 id="time-heading">Vývoj v čase</h2>
            <p>{showTrend
              ? "Čárkovaně je průměr z posledních 12 dostupných měření. Přerušení ukazují měsíce bez dat."
              : trendAvailable
                ? "Čára spojuje jednotlivé měsíční hodnoty a přerušuje se tam, kde data chybí."
                : "Pro zobrazení průměru rozšiřte období alespoň na 13 dostupných měření."}</p>
          </div>
          <div className="chart-controls">
            <ChartToggle
              label="Co graf ukazuje"
              value={metric}
              onChange={onMetric}
              options={[
                { value: "permits-spaces", label: "Oprávnění a místa" },
                { value: "pressure", label: "Jen oprávnění na místo" },
              ]}
            />
            <ChartToggle
              label="Zobrazení vývoje"
              value={showTrend}
              onChange={setTrendRequested}
              options={[
                { value: false, label: "Měsíční hodnoty" },
                { value: true, label: "Průměr z 12 měření", disabled: !trendAvailable },
              ]}
            />
          </div>
        </div>
        <EChart
          option={lineOption}
          className="time-chart"
          ariaLabel={metric === "pressure"
            ? "Vývoj počtu parkovacích oprávnění na jedno místo v čase"
            : "Vývoj počtu parkovacích oprávnění a parkovacích míst v čase, pod ním počet oprávnění na jedno místo"}
        />
      </section>
      <DistrictTable districts={visibleDistricts} month={formatMonth(payload.summary?.latest_date)} />
    </>
  );
}

function TableView({ payload }) {
  const rows = useMemo(() => [...payload.series].reverse(), [payload.series]);

  return (
    <section className="data-table-view">
      <header className="table-heading">
        <div>
          <h2>Vývoj v tabulce</h2>
          <p>Nejnovější dostupný měsíc je uvedený jako první.</p>
        </div>
        <button type="button" className="secondary-action" onClick={() => downloadSeriesCsv(payload.series)}>
          <IconDownload size={16} stroke={1.7} aria-hidden="true" />
          Stáhnout CSV
        </button>
      </header>
      <div className="table-scroller">
        <table>
          <caption className="sr-only">Měsíční vývoj parkovacích oprávnění a míst</caption>
          <thead>
            <tr>
              <th>Datum</th>
              <th>Parkovací oprávnění</th>
              <th>Parkovací místa</th>
              <th>Oprávnění na místo</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.date}>
                <td>{isoToCzech(row.date)}</td>
                <td>{formatNumber(row.permits)}</td>
                <td>{formatNumber(row.spaces)}</td>
                <td>{formatRatio(row.permits_per_space)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function InsightPanel({ payload, filters }) {
  const summary = payload.summary;
  const districtText = filters.districts.length
    ? filters.districts.map(districtName).join(", ")
    : "Praha · všechny městské části";

  return (
    <aside className="insight-panel">
      <section>
        <h2>Aktuální výběr</h2>
        <p>{districtText}</p>
        <p>{filters.castDne.map((value) => TIME_LABELS[value] ?? value).join(", ") || "všechny části dne"}</p>
        <p>Typy zón: {filters.zoneTypes.map((value) => ZONE_LABELS[value] ?? value).join(", ") || "všechny"}</p>
        {filters.sameZones ? <p>Jen úseky, které existují po celé vybrané období</p> : null}
        <p className="selection-date">
          {isoToCzech(filters.start)} – {isoToCzech(filters.end)}
        </p>
      </section>
      {summary?.permits != null ? (
        <>
          <section className="insight-metric">
            <span>Parkovací oprávnění</span>
            <div>
              <strong>{formatNumber(summary.permits)}</strong>
              <small>{pct(summary.permits_change)}<br />změna v období</small>
            </div>
          </section>
          <section className="insight-metric ratio">
            <span>Oprávnění na místo</span>
            <div>
              <strong>{formatRatio(summary.permits_per_space)}</strong>
              <small>{pct(summary.ratio_change)}<br />změna v období</small>
            </div>
          </section>
          <section className="insight-note">
            <h2>Poznámka k vývoji</h2>
            <p>
              Ve vybraném období se počet oprávnění změnil o {pct(summary.permits_change)},
              zatímco kapacita parkovacích míst o {pct(summary.spaces_change)}. Na jedno
              dostupné místo nyní připadá v průměru {formatRatio(summary.permits_per_space)} oprávnění.
            </p>
            {filters.sameZones ? (
              <p>
                Úseky, které v některém měsíci vybraného období chybí, nejsou započtené.
                Změny proto nevznikají tím, že se zóny rozšiřovaly.
              </p>
            ) : null}
            <p>
              Do počtů se nezapočítávají oprávnění budov dál než 500 m od nejbližšího úseku zóny (asi 2 %) a souhrnné záznamy bez konkrétního místa, zejména ekologická oprávnění vedená na jedné adrese. Součty jsou proto nižší než ve zdrojových datech TSK.
            </p>
          </section>
        </>
      ) : summary ? (
        <section className="insight-metric ratio">
          <span>Parkovací místa</span>
          <div><strong>{formatNumber(summary.spaces)}</strong></div>
        </section>
      ) : null}
      {summary ? (
        <footer>
          <IconInfoCircle size={16} stroke={1.5} aria-hidden="true" />
          <span>Data k {isoToCzech(summary.latest_date)}</span>
        </footer>
      ) : null}
    </aside>
  );
}

export function App() {
  const initialState = useMemo(() => initialStateFromSearch(window.location.search), []);
  const [view, setView] = useState(initialState.view);
  const [metric, setMetric] = useState(initialState.metric);
  const [request, setRequest] = useState({ payload: null, error: "", loading: true });
  const [retryToken, setRetryToken] = useState(0);
  const [shareStatus, setShareStatus] = useState("");
  const [filtersOpen, setFiltersOpen] = useState(false);
  const { payload, error, loading } = request;
  const [filters, setFilters] = useState(initialState.filters);
  const explorerQuery = useMemo(() => queryFor(filters), [filters]);
  const effectiveFilters = payload
    ? {
        ...filters,
        start: filters.start || payload.options.min_date,
        end: filters.end || payload.options.max_date,
      }
    : filters;
  const hasResults = Boolean(payload?.series.length);
  const defaultFilters = useMemo(() => defaultFiltersFor(payload?.options), [payload?.options]);
  const hasActiveFilters = Boolean(payload) && (
    Boolean(filters.start)
    || Boolean(filters.end)
    || filters.districts.length > 0
    || !sameSelection(filters.castDne, defaultFilters.castDne)
    || !sameSelection(filters.zoneTypes, defaultFilters.zoneTypes)
    || filters.sameZones
  );

  useEffect(() => {
    document.title = "Vývoj a srovnání – Parkování Praha";
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    setRequest((current) => ({ ...current, loading: true }));
    fetch(`/api/explorer?${explorerQuery}`, { signal: controller.signal })
      .then((response) => {
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        return response.json();
      })
      .then((nextPayload) => {
        setRequest({ payload: nextPayload, error: "", loading: false });
      })
      .catch((reason) => {
        if (reason.name !== "AbortError") {
          setRequest((current) => ({
            ...current,
            error: reason.message,
            loading: false,
          }));
        }
      });
    return () => controller.abort();
  }, [explorerQuery, retryToken]);

  useEffect(() => {
    const query = stateToSearch(filters, view, metric);
    window.history.replaceState(null, "", `${window.location.pathname}${query ? `?${query}` : ""}${window.location.hash}`);
  }, [filters, metric, view]);

  const setFilter = (name, value) => setFilters((current) => ({ ...current, [name]: value }));
  const setStart = (value) => setFilters((current) => ({
    ...current,
    start: value,
    end: current.end && value > current.end ? monthEnd(value.slice(0, 7)) : current.end,
  }));
  const setEnd = (value) => setFilters((current) => ({
    ...current,
    start: current.start && value < current.start ? monthStart(value.slice(0, 7)) : current.start,
    end: value,
  }));
  const resetFilters = () => setFilters(defaultFilters);
  const shareSelection = async () => {
    try {
      await navigator.clipboard.writeText(window.location.href);
      setShareStatus("Odkaz je zkopírovaný");
    } catch {
      setShareStatus("Odkaz se nepodařilo zkopírovat");
    }
  };
  useEffect(() => {
    if (!shareStatus) return undefined;
    const timer = window.setTimeout(() => setShareStatus(""), 4000);
    return () => window.clearTimeout(timer);
  }, [shareStatus]);

  const handleTabKeyDown = (event) => {
    if (!event.key.startsWith("Arrow")) return;
    event.preventDefault();
    const views = ["charts", "table"];
    const step = event.key === "ArrowRight" || event.key === "ArrowDown" ? 1 : -1;
    const index = (views.indexOf(view) + step + views.length) % views.length;
    const nextView = views[index];
    document.getElementById(`view-tab-${nextView}`)?.focus();
    setView(nextView);
  };

  return (
    <div className="app-shell">
      <a className="skip-link" href="#explorer">Přejít k vývoji a srovnání</a>
      <Masthead page="explorer" dataDate={payload?.options.max_date} />

      <main id="explorer">
        <section className="explorer-head">
          <div className="explorer-title">
            <h1>Vývoj parkování v Praze</h1>
            <p className="subtitle">Oprávnění, kapacita a tlak v regulovaných zónách</p>
            <p className="explainer">
              Sledujte vývoj počtu vydaných oprávnění, dostupné kapacity parkovacích míst
              a výsledného tlaku. Vyšší hodnota znamená vyšší poptávku po parkování.
            </p>
          </div>
          <div className="view-tabs" role="tablist" aria-label="Zobrazení dat" onKeyDown={handleTabKeyDown}>
            <button id="view-tab-charts" aria-controls="view-panel" tabIndex={view === "charts" ? 0 : -1} type="button" className={view === "charts" ? "active" : ""} onClick={() => setView("charts")} role="tab" aria-selected={view === "charts"}>Grafy</button>
            <button id="view-tab-table" aria-controls="view-panel" tabIndex={view === "table" ? 0 : -1} type="button" className={view === "table" ? "active" : ""} onClick={() => setView("table")} role="tab" aria-selected={view === "table"}>Tabulka</button>
          </div>
        </section>

        {payload && (
          <div className="sticky-toolbar">
            <button
              type="button"
              className="mobile-filter-toggle"
              aria-expanded={filtersOpen}
              aria-controls="explorer-filters"
              onClick={() => setFiltersOpen((value) => !value)}
            >
              <IconAdjustmentsHorizontal size={17} stroke={1.7} aria-hidden="true" />
              Filtry
              <span>{hasActiveFilters ? "Upravený výběr" : "Výchozí výběr"}</span>
              <IconChevronDown size={16} stroke={1.7} aria-hidden="true" />
            </button>
            <section id="explorer-filters" className={`filter-bar${filtersOpen ? " is-open" : ""}`} aria-label="Filtry">
              <DateRange
                start={effectiveFilters.start}
                end={effectiveFilters.end}
                min={payload.options.min_date}
                max={payload.options.max_date}
                onStart={setStart}
                onEnd={setEnd}
              />
              <MultiSelect label="Část dne (parkování)" values={payload.options.cast_dne} selected={filters.castDne} onChange={(values) => setFilter("castDne", values)} display={(value) => TIME_LABELS[value] ?? value} />
              <DistrictSelect values={payload.options.districts} selected={filters.districts} onChange={(values) => setFilter("districts", values)} />
              <MultiSelect label="Typ zóny" values={payload.options.zone_types} selected={filters.zoneTypes} onChange={(values) => setFilter("zoneTypes", values)} display={(value) => ZONE_LABELS[value] ?? value} />
              <SameZonesToggle checked={filters.sameZones} onChange={(value) => setFilter("sameZones", value)} />
            </section>
            <div className="toolbar-utility">
              <ToolbarActions
                loading={loading}
                hasActiveFilters={hasActiveFilters}
                shareStatus={shareStatus}
                onShare={shareSelection}
                onReset={resetFilters}
              />
            </div>
          </div>
        )}

        <div className={`workspace-grid${loading && payload ? " is-refreshing" : ""}`} aria-busy={loading}>
          <div className="workspace-main">
            <div
              id="view-panel"
              className="primary-content"
              role="tabpanel"
              aria-labelledby={`view-tab-${view}`}
            >
            {loading && !payload ? <LoadingState /> : null}
            {error ? <ErrorState hasData={hasResults} onRetry={() => setRetryToken((value) => value + 1)} /> : null}
            {hasResults && payload.coverage?.permits_available === false ? <CoverageNotice /> : null}
            {payload && !hasResults ? <NoResultsState onReset={resetFilters} /> : null}
            {hasResults && view === "charts" ? <ChartView payload={payload} metric={metric} onMetric={setMetric} /> : null}
            {hasResults && view === "table" ? <TableView payload={payload} /> : null}
            </div>
          </div>
          {payload ? <InsightPanel payload={payload} filters={effectiveFilters} /> : <aside className="insight-panel" />}
        </div>
        {hasResults && view === "charts" ? <AnalyticsSections analytics={payload.analytics} /> : null}
      </main>
    </div>
  );
}
