import { useEffect, useMemo, useState } from "react";
import {
  IconAdjustmentsHorizontal,
  IconCalendar,
  IconChevronDown,
  IconDownload,
  IconInfoCircle,
} from "@tabler/icons-react";
import { EChart } from "./EChart.jsx";
import { AnalyticsSections } from "./AnalyticsSections.jsx";
import {
  hasDrawableObservationAverage,
  materializeMonthlyRows,
  trailingObservationAverage,
} from "./chartData.js";
import {
  DateRange,
  DistrictSelect,
  MultiSelect,
  ToolbarActions,
} from "./ExplorerControls.jsx";
import {
  CoverageNotice,
  ErrorState,
  LoadingState,
  NoResultsState,
} from "./ExplorerStates.jsx";
import {
  DEFAULT_FILTERS,
  initialStateFromSearch,
  monthEnd,
  monthStart,
  queryFor,
  selectionIsAll,
  stateToSearch,
} from "./explorerState.js";

const RED = "#e33b32";
const TEAL = "#114f50";
const SLATE = "#667386";
const TIME_LABELS = {
  "Po-Pá": "Pracovní dny",
  "Po-Pá (MPD)": "Pracovní dny · měření",
  "So-Ne": "Víkend",
  noc: "Noc",
};
const ZONE_LABELS = {
  MIX: "Smíšená",
  OST: "Ostatní",
  RES: "Rezidentní",
  VIS: "Návštěvnická",
};


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

function areaGradient(color) {
  return {
    type: "linear",
    x: 0,
    y: 0,
    x2: 0,
    y2: 1,
    colorStops: [
      { offset: 0, color: `${color}16` },
      { offset: 1, color: `${color}00` },
    ],
  };
}

const numberFormat = new Intl.NumberFormat("cs-CZ", { maximumFractionDigits: 0 });
const ratioFormat = new Intl.NumberFormat("cs-CZ", {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});
const dateFormat = new Intl.DateTimeFormat("cs-CZ");

function formatNumber(value) {
  return value == null ? "—" : numberFormat.format(value);
}

function formatRatio(value) {
  return value == null ? "—" : ratioFormat.format(value);
}

function pct(value) {
  if (value == null) return "—";
  const rounded = Math.round(value * 100);
  return `${rounded > 0 ? "+" : ""}${rounded} %`;
}

function isoToCzech(value) {
  if (!value) return "—";
  return dateFormat.format(new Date(`${value}T12:00:00`));
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

function lineChartOption(
  series,
  compact = false,
  reduceMotion = false,
  metric = "permits-spaces",
  showTrend = false,
) {
  const axisText = { color: "#73736f", fontFamily: "Inter, sans-serif", fontSize: 11 };
  const pointValue = (value) => Array.isArray(value) ? value[1] : value;
  const chartRows = materializeMonthlyRows(
    series,
    ["permits", "spaces", "permits_per_space"],
  );
  const common = {
    type: "line",
    showSymbol: false,
    smooth: false,
    connectNulls: false,
    lineStyle: { width: 2.2 },
    emphasis: { focus: "series" },
    endLabel: {
      show: true,
      formatter: (params) => `${params.seriesName}\n${formatNumber(pointValue(params.value))}`,
      fontFamily: "Newsreader, Georgia, serif",
      fontSize: 14,
      lineHeight: 19,
    },
    labelLayout: { moveOverlap: "shiftY" },
  };

  const option = {
    animationDuration: reduceMotion ? 0 : 650,
    color: [RED, TEAL, SLATE],
    grid: compact
      ? { left: 48, right: 14, top: 58, bottom: 42 }
      : { left: 58, right: 122, top: 38, bottom: 42 },
    legend: compact
      ? {
          show: true,
          top: 12,
          left: 0,
          itemWidth: 18,
          itemHeight: 2,
          data: ["Parkovací oprávnění", "Parkovací místa", "Oprávnění na místo"],
          formatter: (name) => ({
            "Parkovací oprávnění": "Oprávnění",
            "Parkovací místa": "Místa",
            "Oprávnění na místo": "Oprávnění / místo",
          })[name] ?? name,
          textStyle: { color: "#4f4c45", fontFamily: "Inter, sans-serif", fontSize: 9 },
        }
      : { show: false },
    tooltip: {
      trigger: "axis",
      backgroundColor: "#fffdf8",
      borderColor: "#d9d4ca",
      textStyle: { color: "#26251f", fontFamily: "Inter, sans-serif" },
      valueFormatter: (value) => formatNumber(pointValue(value)),
    },
    xAxis: {
      type: "time",
      boundaryGap: false,
      axisLine: { lineStyle: { color: "#aaa59a" } },
      axisTick: { show: false },
      axisLabel: {
        ...axisText,
        formatter: (value) => new Date(value).getFullYear(),
        hideOverlap: true,
      },
    },
    yAxis: [
      {
        type: "value",
        min: 0,
        splitNumber: 4,
        axisLabel: {
          ...axisText,
          formatter: (value) => numberFormat.format(value),
        },
        splitLine: { lineStyle: { color: "#e5e0d7", type: "dashed" } },
      },
      {
        type: "value",
        min: 0,
        max: (value) => Math.max(2, Math.ceil(value.max * 2) / 2),
        splitNumber: 4,
        axisLabel: { ...axisText, formatter: (value) => ratioFormat.format(value) },
        splitLine: { show: false },
      },
    ],
    series: [
      {
        ...common,
        name: "Parkovací oprávnění",
        data: chartRows.map((row) => [row.date, row.permits]),
        endLabel: { ...common.endLabel, show: !compact, color: RED },
        lineStyle: { color: lineGradient("#ed827b", RED), width: showTrend ? 1.3 : 2.4, opacity: showTrend ? 0.38 : 1 },
        areaStyle: showTrend ? undefined : { color: areaGradient(RED) },
        tooltip: { valueFormatter: (value) => formatNumber(pointValue(value)) },
      },
      {
        ...common,
        name: "Parkovací místa",
        data: chartRows.map((row) => [row.date, row.spaces]),
        endLabel: { ...common.endLabel, show: !compact, color: TEAL, offset: [0, 14] },
        lineStyle: { color: lineGradient("#799b98", TEAL), width: showTrend ? 1.3 : 2.4, opacity: showTrend ? 0.38 : 1 },
        areaStyle: showTrend ? undefined : { color: areaGradient(TEAL) },
        tooltip: { valueFormatter: (value) => formatNumber(pointValue(value)) },
      },
      {
        ...common,
        name: "Oprávnění na místo",
        yAxisIndex: 1,
        data: chartRows.map((row) => [row.date, row.permits_per_space]),
        endLabel: {
          ...common.endLabel,
          show: !compact,
          color: SLATE,
          offset: [0, -14],
          formatter: (params) => `${params.seriesName}\n${formatRatio(pointValue(params.value))}`,
        },
        lineStyle: { color: lineGradient("#aeb6c0", SLATE), width: showTrend ? 1.1 : 1.8, opacity: showTrend ? 0.38 : 1 },
        tooltip: { valueFormatter: (value) => formatRatio(pointValue(value)) },
      },
    ],
  };
  if (showTrend) {
    option.series.push(
      {
        ...common,
        name: "Průměr z 12 měření · Parkovací oprávnění",
        data: trailingObservationAverage(chartRows, "permits"),
        endLabel: { show: false },
        lineStyle: { color: RED, width: 2.6, type: "dashed" },
        tooltip: { valueFormatter: (value) => formatNumber(pointValue(value)) },
      },
      {
        ...common,
        name: "Průměr z 12 měření · Parkovací místa",
        data: trailingObservationAverage(chartRows, "spaces"),
        endLabel: { show: false },
        lineStyle: { color: TEAL, width: 2.6, type: "dashed" },
        tooltip: { valueFormatter: (value) => formatNumber(pointValue(value)) },
      },
      {
        ...common,
        name: "Průměr z 12 měření · Oprávnění na místo",
        yAxisIndex: 1,
        data: trailingObservationAverage(chartRows, "permits_per_space"),
        endLabel: { show: false },
        lineStyle: { color: SLATE, width: 2.3, type: "dashed" },
        tooltip: { valueFormatter: (value) => formatRatio(pointValue(value)) },
      },
    );
  }
  if (metric === "pressure") {
    option.yAxis = [{ ...option.yAxis[1], position: "left" }];
    option.series = option.series.reduce((pressureSeries, item) => {
      if (item.name.includes("Oprávnění na místo")) {
        pressureSeries.push({ ...item, yAxisIndex: 0 });
      }
      return pressureSeries;
    }, []);
    option.grid = compact
      ? { left: 48, right: 14, top: 58, bottom: 42 }
      : { left: 58, right: 122, top: 38, bottom: 42 };
  }
  return option;
}

function hasDrawableObservationTrend(series, keys) {
  const rows = materializeMonthlyRows(series, keys);
  return keys.some((key) => hasDrawableObservationAverage(rows, key));
}

function districtChartOption(districts, reduceMotion = false) {
  const values = districts.map((row) => row.permits_per_space);
  return {
    animationDuration: reduceMotion ? 0 : 500,
    grid: { left: 76, right: 38, top: 24, bottom: 14 },
    xAxis: {
      type: "value",
      position: "top",
      min: 0,
      max: Math.max(3, ...values.map((value) => Math.ceil(value * 2) / 2)),
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: { color: "#77736a", fontFamily: "Inter, sans-serif", fontSize: 11 },
      splitLine: { show: false },
    },
    yAxis: {
      type: "category",
      inverse: true,
      data: districts.map((row) => row.district.replace(/^P0?/, "Praha ")),
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: { color: "#292821", fontFamily: "Inter, sans-serif", fontSize: 12 },
    },
    tooltip: {
      trigger: "item",
      formatter: ({ name, value }) => `${name}: ${ratioFormat.format(value)}`,
    },
    series: [
      {
        type: "bar",
        data: values,
        barWidth: 2,
        itemStyle: { color: "#aab9b6" },
        z: 1,
      },
      {
        type: "scatter",
        data: districts.map((row, index) => [row.permits_per_space, index]),
        symbolSize: 10,
        itemStyle: { color: TEAL },
        label: {
          show: true,
          position: "right",
          formatter: ({ value }) => ratioFormat.format(value[0]),
          color: TEAL,
          fontFamily: "Newsreader, Georgia, serif",
          fontSize: 13,
        },
        z: 3,
      },
    ],
  };
}

function ChartView({ payload, metric }) {
  const compactChart = useMediaQuery("(max-width: 760px)");
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
  const districtOption = useMemo(
    () => districtChartOption(visibleDistricts, reduceMotion),
    [visibleDistricts, reduceMotion],
  );

  return (
    <>
      <section className="chart-section" aria-labelledby="time-heading">
        <div className="section-heading chart-heading">
          <div>
            <h2 id="time-heading">
              Vývoj v čase <IconInfoCircle size={15} stroke={1.5} aria-label="Nápověda" />
            </h2>
            <p>{showTrend
              ? "Čárkovaně je průměr z posledních 12 dostupných měření. Přerušení ukazují měsíce bez dat."
              : trendAvailable
                ? "Čára spojuje jednotlivé měsíční hodnoty a přerušuje se tam, kde data chybí."
                : "Pro zobrazení průměru rozšiřte období alespoň na 13 dostupných měření."}</p>
          </div>
          <div className="chart-toggle" role="group" aria-label="Zobrazení vývoje">
            <button type="button" className={!showTrend ? "active" : ""} aria-pressed={!showTrend} onClick={() => setTrendRequested(false)}>Měsíční hodnoty</button>
            <button type="button" className={showTrend ? "active" : ""} aria-pressed={showTrend} disabled={!trendAvailable} onClick={() => setTrendRequested(true)}>Průměr z 12 měření</button>
          </div>
        </div>
        <EChart
          option={lineOption}
          className="time-chart"
          ariaLabel={metric === "pressure"
            ? "Vývoj počtu parkovacích oprávnění na jedno místo v čase"
            : "Vývoj počtu parkovacích oprávnění a parkovacích míst v čase"}
        />
      </section>
      <section className="district-section" aria-labelledby="district-heading">
        <div className="section-heading">
          <h2 id="district-heading">Porovnání městských částí</h2>
          <p>Oprávnění na 1 parkovací místo ({payload.summary?.latest_date.slice(0, 7)})</p>
        </div>
        <div className="district-content">
          <EChart
            option={districtOption}
            className="district-chart"
            ariaLabel="Porovnání pěti městských částí s nejvyšším počtem oprávnění na parkovací místo"
          />
          <table>
            <caption className="sr-only">Hodnoty pěti městských částí s nejvyšším tlakem na parkování</caption>
            <thead>
              <tr>
                <th scope="col">Městská část</th>
                <th>Oprávnění</th>
                <th>Parkovací místa</th>
                <th>Oprávnění na místo</th>
              </tr>
            </thead>
            <tbody>
              {visibleDistricts.map((row) => (
                <tr key={row.district}>
                  <th scope="row">{row.district.replace(/^P0?/, "Praha ")}</th>
                  <td>{formatNumber(row.permits)}</td>
                  <td>{formatNumber(row.spaces)}</td>
                  <td>{formatRatio(row.permits_per_space)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <p className="data-note">
          Pozn.: Zahrnuje aktivní oprávnění a veřejná i neveřejná parkovací místa v regulovaných zónách.
        </p>
      </section>
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

function MapView({ onBack }) {
  return (
    <section className="map-empty-state">
      <div>
        <span className="eyebrow">Mapa se připravuje</span>
        <h2>Geografický pohled zatím není součástí průzkumníku</h2>
        <p>
          Data můžete procházet podle městských částí v grafech a tabulce. Mapu
          jednotlivých parkovacích úseků doplníme jako samostatný, plnohodnotný pohled.
        </p>
        <button type="button" className="primary-action" onClick={onBack}>Zpět ke grafům</button>
      </div>
      <div className="map-placeholder" aria-hidden="true">
        <span>Praha</span>
        <i /><i /><i /><i /><i />
      </div>
    </section>
  );
}

function InsightPanel({ payload, filters }) {
  const summary = payload.summary;
  const districtText = filters.districts.length
    ? filters.districts.map((value) => value.replace(/^P0?/, "Praha ")).join(", ")
    : "všechny městské části";

  return (
    <aside className="insight-panel">
      <section>
        <h2>Aktuální výběr</h2>
        <p>Praha · {districtText}</p>
        <p>{filters.castDne.map((value) => TIME_LABELS[value] ?? value).join(", ") || "všechny části dne"}</p>
        <p>Typy zón: {filters.zoneTypes.map((value) => ZONE_LABELS[value] ?? value).join(", ") || "všechny"}</p>
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
  const hasActiveFilters = Boolean(payload) && (
    Boolean(filters.start)
    || Boolean(filters.end)
    || filters.districts.length > 0
    || !selectionIsAll(filters.castDne, payload.options.cast_dne)
    || !selectionIsAll(filters.zoneTypes, payload.options.zone_types)
  );

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
  const resetFilters = () => setFilters({
    ...DEFAULT_FILTERS,
    castDne: payload?.options.cast_dne ?? DEFAULT_FILTERS.castDne,
    zoneTypes: payload?.options.zone_types ?? DEFAULT_FILTERS.zoneTypes,
  });
  const shareSelection = async () => {
    try {
      await navigator.clipboard.writeText(window.location.href);
      setShareStatus("Odkaz je zkopírovaný");
    } catch {
      setShareStatus("Odkaz se nepodařilo zkopírovat");
    }
  };
  const handleTabKeyDown = (event) => {
    if (!event.key.startsWith("Arrow")) return;
    event.preventDefault();
    const views = ["charts", "map", "table"];
    const step = event.key === "ArrowRight" || event.key === "ArrowDown" ? 1 : -1;
    const index = (views.indexOf(view) + step + views.length) % views.length;
    const nextView = views[index];
    document.getElementById(`view-tab-${nextView}`)?.focus();
    setView(nextView);
  };

  return (
    <div className="app-shell">
      <a className="skip-link" href="#explorer">Přejít k datovému průzkumníku</a>
      <header className="masthead">
        <a className="brand" href="#explorer">Parkování Praha</a>
        <nav aria-label="Hlavní navigace">
          <span className="nav-coming-soon" title="Městský přehled připravujeme">Městský přehled <small>Připravujeme</small></span>
          <button className="active" type="button">Datový průzkumník</button>
        </nav>
        <span className="data-date">Data k {isoToCzech(payload?.options.max_date)} <IconCalendar size={17} stroke={1.5} /></span>
      </header>

      <main id="explorer">
        <section className="explorer-head">
          <div>
            <h1>Vývoj parkování v Praze</h1>
            <p>Oprávnění, kapacita a tlak v regulovaných zónách</p>
          </div>
          {view === "charts" ? (
            <label className="metric-select">
              <span>Metrika</span>
              <select value={metric} onChange={(event) => setMetric(event.target.value)}>
                <option value="permits-spaces">Oprávnění a parkovací místa</option>
                <option value="pressure">Oprávnění na místo</option>
              </select>
            </label>
          ) : (
            <div className="view-context">
              <span>Aktivní pohled</span>
              <strong>{view === "table" ? "Tabulková data" : "Geografický výběr"}</strong>
            </div>
          )}
          <p className="explainer">
            Sledujte vývoj počtu vydaných oprávnění, dostupné kapacity parkovacích míst
            a výsledného tlaku. Vyšší hodnota znamená vyšší poptávku po parkování.
          </p>
        </section>

        <div className="view-tabs" role="tablist" aria-label="Zobrazení dat" onKeyDown={handleTabKeyDown}>
          <button id="view-tab-charts" aria-controls="view-panel" tabIndex={view === "charts" ? 0 : -1} type="button" className={view === "charts" ? "active" : ""} onClick={() => setView("charts")} role="tab" aria-selected={view === "charts"}>Grafy</button>
          <button id="view-tab-map" aria-controls="view-panel" tabIndex={view === "map" ? 0 : -1} type="button" className={view === "map" ? "active" : ""} onClick={() => setView("map")} role="tab" aria-selected={view === "map"}>Mapa</button>
          <button id="view-tab-table" aria-controls="view-panel" tabIndex={view === "table" ? 0 : -1} type="button" className={view === "table" ? "active" : ""} onClick={() => setView("table")} role="tab" aria-selected={view === "table"}>Tabulka</button>
        </div>

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
            <section id="explorer-filters" className={`filter-bar${filtersOpen ? " is-open" : ""}`} aria-label="Filtry průzkumníku">
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

        <div className="workspace-grid" aria-busy={loading}>
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
            {payload && !hasResults && view !== "map" ? <NoResultsState onReset={resetFilters} /> : null}
            {hasResults && view === "charts" ? <ChartView payload={payload} metric={metric} /> : null}
            {hasResults && view === "table" ? <TableView payload={payload} /> : null}
            {view === "map" ? <MapView onBack={() => setView("charts")} /> : null}
            </div>
          </div>
          {payload ? <InsightPanel payload={payload} filters={effectiveFilters} /> : <aside className="insight-panel" />}
        </div>
        {hasResults && view === "charts" ? <AnalyticsSections analytics={payload.analytics} /> : null}
      </main>
    </div>
  );
}
