import { useEffect, useMemo, useState } from "react";
import {
  IconCalendar,
  IconChevronDown,
  IconInfoCircle,
  IconSearch,
  IconX,
} from "@tabler/icons-react";
import { EChart } from "./EChart.jsx";
import { AnalyticsSections } from "./AnalyticsSections.jsx";

const RED = "#e33b32";
const TEAL = "#114f50";
const SLATE = "#667386";

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

function pct(value) {
  if (value == null) return "—";
  const rounded = Math.round(value * 100);
  return `${rounded > 0 ? "+" : ""}${rounded} %`;
}

function isoToCzech(value) {
  if (!value) return "—";
  return dateFormat.format(new Date(`${value}T12:00:00`));
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

function queryFor(filters) {
  const params = new URLSearchParams();
  if (filters.start) params.set("start", filters.start);
  if (filters.end) params.set("end", filters.end);
  filters.castDne.forEach((value) => params.append("cast_dne", value));
  filters.districts.forEach((value) => params.append("district", value));
  filters.zoneTypes.forEach((value) => params.append("zone_type", value));
  return params.toString();
}

function MultiSelect({ label, values, selected, onChange, display }) {
  const selectedValues = useMemo(() => new Set(selected), [selected]);
  const toggle = (value) => {
    onChange(
      selectedValues.has(value)
        ? selected.filter((item) => item !== value)
        : [...selected, value],
    );
  };

  return (
    <div className="filter-control">
      <span className="filter-label">{label}</span>
      <details className="multi-select">
        <summary>
          <span className="selection-chips">
            {selected.length === 0 ? (
              <span className="selection-placeholder">Všechny</span>
            ) : (
              selected.slice(0, 4).map((value) => (
                <span className="chip" key={value}>
                  {display?.(value) ?? value}
                  <IconX size={11} stroke={2} aria-hidden="true" />
                </span>
              ))
            )}
          </span>
          <IconChevronDown size={16} stroke={1.8} aria-hidden="true" />
        </summary>
        <div className="select-menu">
          {values.map((value) => (
            <label key={value}>
              <input
                type="checkbox"
                checked={selectedValues.has(value)}
                onChange={() => toggle(value)}
              />
              <span>{display?.(value) ?? value}</span>
            </label>
          ))}
        </div>
      </details>
    </div>
  );
}

function DistrictSelect({ values, selected, onChange }) {
  return (
    <label className="filter-control district-select">
      <span className="filter-label">Městská část</span>
      <select
        value={selected[0] ?? ""}
        onChange={(event) => onChange(event.target.value ? [event.target.value] : [])}
      >
        <option value="">Všechny městské části</option>
        {values.map((value) => (
          <option key={value} value={value}>
            {value.replace(/^P0?/, "Praha ")}
          </option>
        ))}
      </select>
    </label>
  );
}

function DateRange({ start, end, onStart, onEnd }) {
  return (
    <div className="filter-control date-control">
      <span className="filter-label">Období</span>
      <div className="date-fields">
        <IconCalendar size={16} stroke={1.6} aria-hidden="true" />
        <input aria-label="Začátek období" type="date" value={start} onChange={onStart} />
        <span>–</span>
        <input aria-label="Konec období" type="date" value={end} onChange={onEnd} />
      </div>
    </div>
  );
}

function LoadingState() {
  return (
    <div className="loading-state" role="status">
      <span />
      Načítám parkovací data…
    </div>
  );
}

function ErrorState({ message }) {
  return (
    <div className="error-state" role="alert">
      Data se nepodařilo načíst. {message}
    </div>
  );
}

function lineChartOption(series, compact = false, reduceMotion = false, metric = "permits-spaces") {
  const dates = series.map((row) => row.date);
  const axisText = { color: "#73736f", fontFamily: "Inter, sans-serif", fontSize: 11 };
  const common = {
    type: "line",
    showSymbol: false,
    smooth: 0.22,
    lineStyle: { width: 2.2 },
    emphasis: { focus: "series" },
    endLabel: {
      show: true,
      formatter: (params) => `${params.seriesName}\n${numberFormat.format(params.value)}`,
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
      valueFormatter: (value) => numberFormat.format(value),
    },
    xAxis: {
      type: "category",
      boundaryGap: false,
      data: dates,
      axisLine: { lineStyle: { color: "#aaa59a" } },
      axisTick: { show: false },
      axisLabel: {
        ...axisText,
        formatter: (value) => value.slice(0, 4),
        interval: compact ? 23 : 11,
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
        data: series.map((row) => row.permits),
        endLabel: { ...common.endLabel, show: !compact, color: RED },
        lineStyle: { color: lineGradient("#ed827b", RED), width: 2.4 },
        areaStyle: { color: areaGradient(RED) },
      },
      {
        ...common,
        name: "Parkovací místa",
        data: series.map((row) => row.spaces),
        endLabel: { ...common.endLabel, show: !compact, color: TEAL },
        lineStyle: { color: lineGradient("#799b98", TEAL), width: 2.4 },
        areaStyle: { color: areaGradient(TEAL) },
      },
      {
        ...common,
        name: "Oprávnění na místo",
        yAxisIndex: 1,
        data: series.map((row) => row.permits_per_space),
        endLabel: {
          ...common.endLabel,
          show: !compact,
          color: SLATE,
          formatter: (params) => `${params.seriesName}\n${ratioFormat.format(params.value)}`,
        },
        lineStyle: { color: lineGradient("#aeb6c0", SLATE), width: 1.8 },
      },
    ],
  };
  if (metric === "pressure") {
    option.yAxis = [{ ...option.yAxis[1], position: "left" }];
    option.series = [{ ...option.series[2], yAxisIndex: 0 }];
    option.grid = compact
      ? { left: 48, right: 14, top: 58, bottom: 42 }
      : { left: 58, right: 122, top: 38, bottom: 42 };
  }
  return option;
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

  const visibleDistricts = useMemo(() => payload.districts.slice(0, 5), [payload.districts]);
  const lineOption = useMemo(
    () => lineChartOption(payload.series, compactChart, reduceMotion, metric),
    [payload.series, compactChart, reduceMotion, metric],
  );
  const districtOption = useMemo(
    () => districtChartOption(visibleDistricts, reduceMotion),
    [visibleDistricts, reduceMotion],
  );

  return (
    <>
      <section className="chart-section" aria-labelledby="time-heading">
        <h2 id="time-heading">
          Vývoj v čase <IconInfoCircle size={15} stroke={1.5} aria-label="Nápověda" />
        </h2>
        <EChart option={lineOption} className="time-chart" />
      </section>
      <section className="district-section" aria-labelledby="district-heading">
        <div className="section-heading">
          <h2 id="district-heading">Porovnání městských částí</h2>
          <p>Oprávnění na 1 parkovací místo ({payload.summary?.latest_date.slice(0, 7)})</p>
        </div>
        <div className="district-content">
          <EChart option={districtOption} className="district-chart" />
          <table>
            <thead>
              <tr>
                <th>Oprávnění</th>
                <th>Parkovací místa</th>
                <th>Oprávnění na místo</th>
              </tr>
            </thead>
            <tbody>
              {visibleDistricts.map((row) => (
                <tr key={row.district}>
                  <td>{numberFormat.format(row.permits)}</td>
                  <td>{numberFormat.format(row.spaces)}</td>
                  <td>{ratioFormat.format(row.permits_per_space)}</td>
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
  return (
    <section className="data-table-view">
      <h2>Vývoj v tabulce</h2>
      <div className="table-scroller">
        <table>
          <thead>
            <tr>
              <th>Datum</th>
              <th>Parkovací oprávnění</th>
              <th>Parkovací místa</th>
              <th>Oprávnění na místo</th>
            </tr>
          </thead>
          <tbody>
            {payload.series.map((row) => (
              <tr key={row.date}>
                <td>{isoToCzech(row.date)}</td>
                <td>{numberFormat.format(row.permits)}</td>
                <td>{numberFormat.format(row.spaces)}</td>
                <td>{ratioFormat.format(row.permits_per_space)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function MapView() {
  return (
    <section className="map-empty-state">
      <div>
        <span className="eyebrow">Geografický výběr</span>
        <h2>Najděte místo, které chcete prozkoumat</h2>
        <p>
          Zadejte adresu nebo vyberte městskou část. Mapa pak ukáže jednotlivé
          parkovací úseky a jejich tlak.
        </p>
      </div>
      <label className="map-search">
        <span>Adresa v Praze</span>
        <div>
          <input placeholder="Např. Vinohradská 12" />
          <IconSearch size={20} stroke={1.6} aria-hidden="true" />
        </div>
      </label>
    </section>
  );
}

function AddressSearch() {
  return (
    <label className="address-search">
      <input placeholder="Najít adresu" />
      <IconSearch size={18} stroke={1.6} aria-hidden="true" />
    </label>
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
        <p>{filters.castDne.join(", ") || "všechny části dne"}</p>
        <p>Typy zón: {filters.zoneTypes.join(", ")}</p>
        <p className="selection-date">
          {isoToCzech(filters.start)} – {isoToCzech(filters.end)}
        </p>
      </section>
      {summary && (
        <>
          <section className="insight-metric">
            <span>Parkovací oprávnění</span>
            <div>
              <strong>{numberFormat.format(summary.permits)}</strong>
              <small>{pct(summary.permits_change)}<br />změna v období</small>
            </div>
          </section>
          <section className="insight-metric ratio">
            <span>Oprávnění na místo</span>
            <div>
              <strong>{ratioFormat.format(summary.permits_per_space)}</strong>
              <small>{pct(summary.ratio_change)}<br />změna v období</small>
            </div>
          </section>
          <section className="insight-note">
            <h2>Poznámka k vývoji</h2>
            <p>
              Ve vybraném období se počet oprávnění změnil o {pct(summary.permits_change)},
              zatímco kapacita parkovacích míst o {pct(summary.spaces_change)}. Na jedno
              dostupné místo nyní připadá v průměru {ratioFormat.format(summary.permits_per_space)} oprávnění.
            </p>
          </section>
          <footer>
            <IconInfoCircle size={16} stroke={1.5} aria-hidden="true" />
            <span>Data k {isoToCzech(summary.latest_date)}</span>
          </footer>
        </>
      )}
    </aside>
  );
}

export function App() {
  const [view, setView] = useState("charts");
  const [metric, setMetric] = useState("permits-spaces");
  const [request, setRequest] = useState({ payload: null, error: "", loading: true });
  const { payload, error, loading } = request;
  const [filters, setFilters] = useState({
    start: "",
    end: "",
    castDne: ["Po-Pá", "Po-Pá (MPD)", "So-Ne", "noc"],
    districts: [],
    zoneTypes: ["MIX", "OST", "RES", "VIS"],
  });
  const explorerQuery = useMemo(() => queryFor(filters), [filters]);
  const effectiveFilters = payload
    ? {
        ...filters,
        start: filters.start || payload.options.min_date,
        end: filters.end || payload.options.max_date,
      }
    : filters;

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
  }, [explorerQuery]);

  useEffect(() => {
    if (!payload) return;
    const availableTimes = new Set(payload.options.cast_dne);
    const availableZoneTypes = new Set(payload.options.zone_types);
    setFilters((current) => ({
      ...current,
      castDne: current.castDne.filter((value) => availableTimes.has(value)),
      zoneTypes: current.zoneTypes.filter((value) => availableZoneTypes.has(value)),
    }));
  }, [payload]);

  const setFilter = (name, value) => setFilters((current) => ({ ...current, [name]: value }));

  return (
    <div className="app-shell">
      <header className="masthead">
        <a className="brand" href="#explorer">Parkování Praha</a>
        <nav aria-label="Hlavní navigace">
          <button type="button">Městský přehled</button>
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
          <label className="metric-select">
            <span>Metrika</span>
            <select value={metric} onChange={(event) => setMetric(event.target.value)}>
              <option value="permits-spaces">Oprávnění a parkovací místa</option>
              <option value="pressure">Oprávnění na místo</option>
            </select>
          </label>
          <p className="explainer">
            Sledujte vývoj počtu vydaných oprávnění, dostupné kapacity parkovacích míst
            a výsledného tlaku. Vyšší hodnota znamená vyšší poptávku po parkování.
          </p>
        </section>

        <div className="view-tabs" role="tablist" aria-label="Zobrazení dat">
          <button type="button" className={view === "charts" ? "active" : ""} onClick={() => setView("charts")} role="tab" aria-selected={view === "charts"}>Grafy</button>
          <button type="button" className={view === "map" ? "active" : ""} onClick={() => setView("map")} role="tab" aria-selected={view === "map"}>Mapa</button>
          <button type="button" className={view === "table" ? "active" : ""} onClick={() => setView("table")} role="tab" aria-selected={view === "table"}>Tabulka</button>
        </div>

        {payload && (
          <div className="sticky-toolbar">
            <section className="filter-bar" aria-label="Filtry průzkumníku">
              <DateRange
                start={effectiveFilters.start}
                end={effectiveFilters.end}
                onStart={(event) => setFilter("start", event.target.value)}
                onEnd={(event) => setFilter("end", event.target.value)}
              />
              <MultiSelect label="Část dne" values={payload.options.cast_dne} selected={filters.castDne} onChange={(values) => setFilter("castDne", values)} />
              <DistrictSelect values={payload.options.districts} selected={filters.districts} onChange={(values) => setFilter("districts", values)} />
              <MultiSelect label="Typ zóny" values={payload.options.zone_types} selected={filters.zoneTypes} onChange={(values) => setFilter("zoneTypes", values)} />
            </section>
            <div className="toolbar-search">
              <AddressSearch />
            </div>
          </div>
        )}

        <div className="workspace-grid">
          <div className="workspace-main">
            <div className="primary-content">
            {loading && !payload ? <LoadingState /> : null}
            {error ? <ErrorState message={error} /> : null}
            {payload && view === "charts" ? <ChartView payload={payload} metric={metric} /> : null}
            {payload && view === "table" ? <TableView payload={payload} /> : null}
            {view === "map" ? <MapView /> : null}
            </div>
          </div>
          {payload ? <InsightPanel payload={payload} filters={effectiveFilters} /> : <aside className="insight-panel" />}
        </div>
        {payload && view === "charts" ? <AnalyticsSections analytics={payload.analytics} /> : null}
      </main>
    </div>
  );
}
