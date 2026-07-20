import { useEffect, useMemo, useState } from "react";
import {
  IconCalendar,
  IconChevronDown,
  IconInfoCircle,
  IconSearch,
  IconX,
} from "@tabler/icons-react";
import { EChart } from "./EChart.jsx";

const RED = "#e33b32";
const TEAL = "#114f50";
const SLATE = "#667386";

const numberFormat = new Intl.NumberFormat("cs-CZ", { maximumFractionDigits: 0 });
const ratioFormat = new Intl.NumberFormat("cs-CZ", {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});

function pct(value) {
  if (value == null) return "—";
  const rounded = Math.round(value * 100);
  return `${rounded > 0 ? "+" : ""}${rounded} %`;
}

function isoToCzech(value) {
  if (!value) return "—";
  return new Intl.DateTimeFormat("cs-CZ").format(new Date(`${value}T12:00:00`));
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
  const toggle = (value) => {
    onChange(
      selected.includes(value)
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
                checked={selected.includes(value)}
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

function lineChartOption(series) {
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

  return {
    animationDuration: 650,
    color: [RED, TEAL, SLATE],
    grid: { left: 58, right: 122, top: 38, bottom: 42 },
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
      axisLabel: { ...axisText, formatter: (value) => value.slice(0, 4), interval: 11 },
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
        endLabel: { ...common.endLabel, color: RED },
        lineStyle: { color: RED, width: 2.4 },
      },
      {
        ...common,
        name: "Parkovací místa",
        data: series.map((row) => row.spaces),
        endLabel: { ...common.endLabel, color: TEAL },
        lineStyle: { color: TEAL, width: 2.4 },
      },
      {
        ...common,
        name: "Oprávnění na místo",
        yAxisIndex: 1,
        data: series.map((row) => row.permits_per_space),
        endLabel: {
          ...common.endLabel,
          color: SLATE,
          formatter: (params) => `${params.seriesName}\n${ratioFormat.format(params.value)}`,
        },
        lineStyle: { color: SLATE, width: 1.8 },
      },
    ],
  };
}

function districtChartOption(districts) {
  const values = districts.map((row) => row.permits_per_space);
  return {
    animationDuration: 500,
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

function ChartView({ payload }) {
  const lineOption = useMemo(() => lineChartOption(payload.series), [payload.series]);
  const districtOption = useMemo(
    () => districtChartOption(payload.districts),
    [payload.districts],
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
              {payload.districts.map((row) => (
                <tr key={row.district}>
                  <td>{numberFormat.format(row.permits)}</td>
                  <td>{numberFormat.format(row.spaces)}</td>
                  <td>{ratioFormat.format(row.permits_per_space)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
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

function InsightPanel({ payload, filters }) {
  const summary = payload.summary;
  const districtText = filters.districts.length
    ? filters.districts.map((value) => value.replace(/^P0?/, "Praha ")).join(", ")
    : "všechny městské části";

  return (
    <aside className="insight-panel">
      <label className="address-search">
        <input placeholder="Najít adresu" />
        <IconSearch size={18} stroke={1.6} aria-hidden="true" />
      </label>
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
  const [payload, setPayload] = useState(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);
  const [filters, setFilters] = useState({
    start: "",
    end: "",
    castDne: ["Po-Pá", "Po-Pá (MPD)", "So-Ne", "noc"],
    districts: [],
    zoneTypes: ["MIX", "OST", "RES", "VIS"],
  });

  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    fetch(`/api/explorer?${queryFor(filters)}`, { signal: controller.signal })
      .then((response) => {
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        return response.json();
      })
      .then((nextPayload) => {
        setPayload(nextPayload);
        setError("");
        setFilters((current) => ({
          ...current,
          start: current.start || nextPayload.options.min_date,
          end: current.end || nextPayload.options.max_date,
          castDne: current.castDne.filter((value) =>
            nextPayload.options.cast_dne.includes(value),
          ),
          zoneTypes: current.zoneTypes.filter((value) =>
            nextPayload.options.zone_types.includes(value),
          ),
        }));
      })
      .catch((reason) => {
        if (reason.name !== "AbortError") setError(reason.message);
      })
      .finally(() => setLoading(false));
    return () => controller.abort();
  }, [filters.start, filters.end, filters.castDne.join("|"), filters.districts.join("|"), filters.zoneTypes.join("|")]);

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
            <select defaultValue="permits-spaces">
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
          <button className={view === "charts" ? "active" : ""} onClick={() => setView("charts")} role="tab" aria-selected={view === "charts"}>Grafy</button>
          <button className={view === "map" ? "active" : ""} onClick={() => setView("map")} role="tab" aria-selected={view === "map"}>Mapa</button>
          <button className={view === "table" ? "active" : ""} onClick={() => setView("table")} role="tab" aria-selected={view === "table"}>Tabulka</button>
        </div>

        {payload && (
          <section className="filter-bar" aria-label="Filtry průzkumníku">
            <DateRange
              start={filters.start}
              end={filters.end}
              onStart={(event) => setFilter("start", event.target.value)}
              onEnd={(event) => setFilter("end", event.target.value)}
            />
            <MultiSelect label="Část dne" values={payload.options.cast_dne} selected={filters.castDne} onChange={(values) => setFilter("castDne", values)} />
            <MultiSelect label="Městská část" values={payload.options.districts} selected={filters.districts} onChange={(values) => setFilter("districts", values)} display={(value) => value.replace(/^P0?/, "Praha ")} />
            <MultiSelect label="Typ zóny" values={payload.options.zone_types} selected={filters.zoneTypes} onChange={(values) => setFilter("zoneTypes", values)} />
          </section>
        )}

        <div className="content-grid">
          <div className="primary-content">
            {loading && !payload ? <LoadingState /> : null}
            {error ? <ErrorState message={error} /> : null}
            {payload && view === "charts" ? <ChartView payload={payload} /> : null}
            {payload && view === "table" ? <TableView payload={payload} /> : null}
            {view === "map" ? <MapView /> : null}
          </div>
          {payload ? <InsightPanel payload={payload} filters={filters} /> : <aside className="insight-panel" />}
        </div>
      </main>
    </div>
  );
}
