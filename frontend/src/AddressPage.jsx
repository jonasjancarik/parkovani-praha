import { useEffect, useMemo, useState } from "react";
import {
  IconArrowDown,
  IconArrowUp,
  IconDownload,
  IconInfoCircle,
  IconSearch,
  IconShare,
  IconX,
} from "@tabler/icons-react";

import { CapacityChart, ReferenceCharts } from "./AddressCharts.jsx";
import {
  addressStateFromSearch,
  addressStateToSearch,
  fileStem,
  historyRange,
  historySummary,
  monthFromIndex,
  radiusOptions,
  reportQuery,
  sortZones,
  sparklinePath,
  zonesCsv,
  zonesGeoJson,
} from "./addressState.js";
import { formatMonth } from "./chartData.js";
import { ErrorState, LoadingState } from "./ExplorerStates.jsx";
import { useJson, useMediaQuery } from "./hooks.js";
import {
  TIME_LABELS,
  ZONE_LABELS,
  districtName,
  formatDistance,
  formatMonthGenitive,
  formatNumber,
  formatPercent,
  formatRatio,
  formatSigned,
  isoToCzech,
} from "./format.js";
import { Masthead } from "./Masthead.jsx";
import { ZoneMap } from "./ZoneMap.jsx";

const VISIBLE_ZONES = 12;
const SPARK_WIDTH = 112;
const SPARK_HEIGHT = 22;
const ZONE_COLUMNS = [
  { key: "code", label: "Úsek", text: true },
  { key: "distance_m", label: "Vzdálenost", ascending: true },
  { key: "locality", label: "Lokalita", text: true },
  { key: "zone_type", label: "Typ zóny", text: true },
  { key: "spaces", label: "Místa" },
  { key: "history", label: "Vývoj počtu míst" },
  { key: "permits", label: "Oprávnění" },
  { key: "permits_per_space", label: "Oprávnění na místo" },
  { key: "occupancy", label: "Obsazenost" },
  { key: "respect", label: "Respektovanost" },
];

function download(content, type, fileName) {
  const url = URL.createObjectURL(new Blob([content], { type }));
  const link = document.createElement("a");
  link.href = url;
  link.download = fileName;
  link.click();
  URL.revokeObjectURL(url);
}

function pointText(point) {
  const coordinate = new Intl.NumberFormat("cs-CZ", { minimumFractionDigits: 5, maximumFractionDigits: 5 });
  return `Bod na mapě (${coordinate.format(point.lat)}; ${coordinate.format(point.lon)})`;
}

function zoneCountText(count) {
  if (count === 1) return "1 úsek";
  return `${formatNumber(count)} ${count >= 2 && count <= 4 ? "úseky" : "úseků"}`;
}

function SelectionPanel({ report, label, latestDate }) {
  if (!report) {
    return (
      <aside className="insight-panel">
        <section>
          <h2>Vyberte místo</h2>
          <p>Zadejte adresu, nebo klikněte do mapy.</p>
          <p>
            Ukážeme vám, kolik parkovacích míst a oprávnění připadá na zóny placeného stání
            v docházkové vzdálenosti, jak jsou obsazené a jak se jejich kapacita mění.
          </p>
        </section>
        <footer>
          <IconInfoCircle size={16} stroke={1.5} aria-hidden="true" />
          <span>Data k {isoToCzech(latestDate)}</span>
        </footer>
      </aside>
    );
  }

  const { reference, scope, selection, summary, thresholds } = report;
  const place = [districtName(reference.district), reference.locality].filter(Boolean).join(" · ");
  return (
    <aside className="insight-panel">
      <section>
        <h2>Vybrané místo</h2>
        <p className="selection-label">{label}</p>
        <p>
          {reference.inside
            ? `Leží v úseku ${reference.code}`
            : `Nejbližší úsek ${reference.code} je ${formatDistance(reference.distance_m)} daleko`}
          {place ? ` (${place})` : ""}.
        </p>
        <p>
          {summary.zones > 0
            ? `V okruhu ${formatDistance(selection.radius_m)}: ${zoneCountText(summary.zones)} ve stejné městské části.`
            : `V okruhu ${formatDistance(selection.radius_m)} není žádný úsek ze stejné městské části.`}
          {scope.other_district_zones > 0
            ? ` Dalších ${formatNumber(scope.other_district_zones)} patří do jiné městské části, proto je nepočítáme.`
            : ""}
        </p>
      </section>
      {summary.zones > 0 ? (
        <>
          <section className="insight-metric spaces">
            <span>Parkovací místa</span>
            <div>
              <strong>{formatNumber(summary.spaces)}</strong>
              {summary.spaces_change != null ? (
                <small>{formatSigned(summary.spaces_change)}<br />od {formatMonthGenitive(summary.series_start)}</small>
              ) : null}
            </div>
          </section>
          <section className="insight-metric ratio">
            <span>Oprávnění na místo</span>
            <div>
              <strong>{formatRatio(summary.permits_per_space)}</strong>
              <small>{formatNumber(summary.permits)}<br />oprávnění</small>
            </div>
          </section>
          <section className="insight-metric neutral">
            <span>Obsazenost</span>
            <div>
              <strong>{formatPercent(summary.occupancy)}</strong>
              <small>{zoneCountText(summary.high_occupancy_zones)}<br />nad {formatPercent(thresholds.occupancy)}</small>
            </div>
          </section>
          <section className="insight-metric neutral">
            <span>Respektovanost</span>
            <div>
              <strong>{formatPercent(summary.respect)}</strong>
              <small>{zoneCountText(summary.low_respect_zones)}<br />pod {formatPercent(thresholds.respect)}</small>
            </div>
            <p className="metric-note">Podíl aut, která v zóně parkují oprávněně.</p>
          </section>
          <footer>
            <IconInfoCircle size={16} stroke={1.5} aria-hidden="true" />
            <span>Data k {isoToCzech(summary.latest_date)}</span>
          </footer>
        </>
      ) : null}
    </aside>
  );
}

function Sparkline({ history, range }) {
  const summary = historySummary(history);
  if (!summary) return <span aria-hidden="true">—</span>;
  const description = `${formatNumber(summary.first)} → ${formatNumber(summary.last)} míst, ${formatMonth(summary.from)} – ${formatMonth(summary.to)}`;
  return (
    <svg
      className="sparkline"
      width={SPARK_WIDTH}
      height={SPARK_HEIGHT + 4}
      viewBox={`0 -2 ${SPARK_WIDTH} ${SPARK_HEIGHT + 4}`}
      role="img"
      aria-label={description}
    >
      <title>{description}</title>
      <path d={sparklinePath(history, range, SPARK_WIDTH, SPARK_HEIGHT)} />
    </svg>
  );
}

function ZoneTable({ report, label }) {
  const [sort, setSort] = useState({ key: "distance_m", descending: false });
  const [showAll, setShowAll] = useState(false);
  const { reference, thresholds, zones } = report;
  const range = useMemo(() => historyRange(zones), [zones]);
  const sorted = useMemo(() => sortZones(zones, sort.key, sort.descending), [zones, sort]);
  const visible = showAll ? sorted : sorted.slice(0, VISIBLE_ZONES);
  const stem = `${fileStem(label)}-useky`;

  const sortBy = (column) => setSort((current) => (
    current.key === column.key
      ? { key: column.key, descending: !current.descending }
      : { key: column.key, descending: !(column.text || column.ascending) }
  ));

  return (
    <section className="address-section" aria-labelledby="zones-heading">
      <header className="table-heading">
        <div>
          <h2 id="zones-heading">Úseky v okruhu</h2>
          <p>
            Poslední dostupné hodnoty pro každý úsek. Tučně jsou úseky s obsazeností
            nad {formatPercent(thresholds.occupancy)} nebo respektovaností pod {formatPercent(thresholds.respect)}.
            {range ? ` Křivky ukazují počet míst od ${formatMonthGenitive(monthFromIndex(range.first))} do ${formatMonthGenitive(monthFromIndex(range.first + range.months - 1))}.` : ""}
          </p>
        </div>
        <div className="export-actions">
          <button type="button" className="secondary-action" onClick={() => download(`﻿${zonesCsv(sorted)}`, "text/csv;charset=utf-8", `${stem}.csv`)}>
            <IconDownload size={16} stroke={1.7} aria-hidden="true" />
            Stáhnout CSV
          </button>
          <button type="button" className="secondary-action" onClick={() => download(JSON.stringify(zonesGeoJson(report)), "application/geo+json", `${stem}.geojson`)}>
            <IconDownload size={16} stroke={1.7} aria-hidden="true" />
            Stáhnout GeoJSON
          </button>
        </div>
      </header>
      <div className="zone-table-wrap">
        <table className="zone-table">
          <caption className="sr-only">Úseky zón placeného stání ve vybraném okruhu</caption>
          <thead>
            <tr>
              {ZONE_COLUMNS.map((column) => {
                if (column.key === "history") return <th key={column.key} scope="col">{column.label}</th>;
                const active = sort.key === column.key;
                const Arrow = sort.descending ? IconArrowDown : IconArrowUp;
                return (
                  <th
                    key={column.key}
                    scope="col"
                    className={column.text ? "is-text" : ""}
                    aria-sort={active ? (sort.descending ? "descending" : "ascending") : undefined}
                  >
                    <button type="button" onClick={() => sortBy(column)}>
                      {column.label}
                      {active ? <Arrow size={13} stroke={1.8} aria-hidden="true" /> : null}
                    </button>
                  </th>
                );
              })}
            </tr>
          </thead>
          <tbody>
            {visible.map((zone) => (
              <tr key={zone.code}>
                <th scope="row" className="is-text">
                  {zone.code}
                  {zone.is_reference ? <span className="zone-tag">{reference.inside ? "vaše adresa" : "nejbližší"}</span> : null}
                </th>
                <td>{formatDistance(zone.distance_m)}</td>
                <td className="is-text">{zone.locality ?? "—"}</td>
                <td className="is-text">{ZONE_LABELS[zone.zone_type] ?? zone.zone_type ?? "—"}</td>
                <td>{formatNumber(zone.spaces)}</td>
                <td><Sparkline history={zone.history} range={range} /></td>
                <td>{formatNumber(zone.permits)}</td>
                <td>{formatRatio(zone.permits_per_space)}</td>
                <td className={zone.occupancy >= thresholds.occupancy ? "is-flagged" : ""}>{formatPercent(zone.occupancy)}</td>
                <td className={zone.respect != null && zone.respect < thresholds.respect ? "is-flagged" : ""}>{formatPercent(zone.respect)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {zones.length > VISIBLE_ZONES ? (
        <button type="button" className="text-action show-more" onClick={() => setShowAll((value) => !value)}>
          {showAll ? "Zobrazit méně" : `Zobrazit všech ${formatNumber(zones.length)} úseků`}
        </button>
      ) : null}
    </section>
  );
}

function RadiusComparison({ report, onRadius }) {
  const { comparison, selection, thresholds } = report;
  return (
    <section className="address-section" aria-labelledby="comparison-heading">
      <div className="section-heading">
        <h2 id="comparison-heading">Jak se výsledek mění s velikostí okruhu</h2>
        <p>Stejné místo, různá vzdálenost. Kliknutím na okruh ho zobrazíte v mapě.</p>
      </div>
      <div className="zone-table-wrap">
        <table className="zone-table comparison-table">
          <thead>
            <tr>
              <th scope="col" className="is-text">Okruh</th>
              <th scope="col">Úseků</th>
              <th scope="col">Místa</th>
              <th scope="col">Oprávnění na místo</th>
              <th scope="col">Obsazenost</th>
              <th scope="col">Respektovanost</th>
              <th scope="col">Úseky s obsazeností nad {formatPercent(thresholds.occupancy)}</th>
              <th scope="col">Úseky s respektovaností pod {formatPercent(thresholds.respect)}</th>
            </tr>
          </thead>
          <tbody>
            {comparison.map((row) => {
              const current = row.radius_m === selection.radius_m;
              return (
                <tr key={row.radius_m} className={current ? "is-current" : ""}>
                  <th scope="row" className="is-text">
                    <button type="button" onClick={() => onRadius(row.radius_m)} aria-pressed={current}>
                      {formatDistance(row.radius_m)}
                    </button>
                  </th>
                  <td>{formatNumber(row.zones)}</td>
                  <td>{formatNumber(row.spaces)}</td>
                  <td>{formatRatio(row.permits_per_space)}</td>
                  <td>{formatPercent(row.occupancy)}</td>
                  <td>{formatPercent(row.respect)}</td>
                  <td>{formatNumber(row.high_occupancy_zones)}</td>
                  <td>{formatNumber(row.low_respect_zones)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function ReportSections({ report, label, onRadius }) {
  const reduceMotion = useMediaQuery("(prefers-reduced-motion: reduce)");
  const { reference, selection, series, summary } = report;

  if (summary.zones === 0) {
    return (
      <section className="no-results-state" role="status">
        <span className="eyebrow">Bez zón v okolí</span>
        <h2>V okruhu {formatDistance(selection.radius_m)} nejsou žádné zóny placeného stání</h2>
        <p>
          Nejbližší úsek {reference.code} je {formatDistance(reference.distance_m)} daleko.
          Zkuste větší okruh, nebo vyberte jiné místo.
        </p>
      </section>
    );
  }

  return (
    <>
      <section className="address-section" aria-labelledby="capacity-heading">
        <div className="section-heading">
          <h2 id="capacity-heading">Vývoj počtu míst v okruhu</h2>
          <p>
            Součet za měsíce, kdy máme data pro všechny úseky v okruhu
            ({formatMonth(summary.series_start)} – {formatMonth(summary.series_end)}).
            Úsek, který vznikl později, by jinak vypadal jako nová místa.
          </p>
        </div>
        <CapacityChart series={series} reduceMotion={reduceMotion} />
      </section>
      <ZoneTable report={report} label={label} />
      <RadiusComparison report={report} onRadius={onRadius} />
      {reference.in_radius && reference.series.length > 0 ? (
        <section className="address-section" aria-labelledby="reference-heading">
          <div className="section-heading">
            <h2 id="reference-heading">
              {reference.inside ? `Úsek ${reference.code}, ve kterém místo leží` : `Nejbližší úsek ${reference.code}`}
            </h2>
            <p>Vývoj jednoho úseku od prvního měření.</p>
          </div>
          <ReferenceCharts series={reference.series} reduceMotion={reduceMotion} />
        </section>
      ) : null}
    </>
  );
}

export function AddressPage() {
  const initial = useMemo(() => addressStateFromSearch(window.location.search), []);
  const options = useJson("/api/address/options");
  const [point, setPoint] = useState(initial.point);
  const [radiusChoice, setRadiusChoice] = useState(initial.radius);
  const [castChoice, setCastChoice] = useState(initial.castDne);
  const [query, setQuery] = useState(initial.point?.label ?? "");
  const [search, setSearch] = useState({ loading: false, error: "" });
  const [shareStatus, setShareStatus] = useState("");

  const settings = options.data;
  const radii = useMemo(() => (settings ? radiusOptions(settings.radius) : []), [settings]);
  const radius = radii.includes(radiusChoice) ? radiusChoice : settings?.radius.default;
  const castDne = settings?.cast_dne.includes(castChoice) ? castChoice : settings?.default_cast_dne;

  const reportUrl = settings && point ? `/api/address/report?${reportQuery({ point, radius, castDne })}` : null;
  const reportRequest = useJson(reportUrl);
  const report = point ? reportRequest.data : null;
  const reportMatchesPoint = Boolean(report)
    && report.selection.lon.toFixed(6) === point.lon.toFixed(6)
    && report.selection.lat.toFixed(6) === point.lat.toFixed(6);
  const knownLabel = point?.label || (reportMatchesPoint ? report.selection.label : "") || "";
  const label = point ? knownLabel || pointText(point) : "";

  useEffect(() => {
    document.title = "Okolí adresy – Parkování Praha";
  }, []);

  // A place picked on the map gets its address from the report.
  useEffect(() => {
    if (point && !point.label && knownLabel) setQuery(knownLabel);
  }, [point, knownLabel]);

  useEffect(() => {
    if (!settings) return;
    const params = addressStateToSearch(
      { point, label: knownLabel, radius, castDne },
      { radius: settings.radius.default, castDne: settings.default_cast_dne },
    );
    window.history.replaceState(null, "", `${window.location.pathname}?${params}`);
  }, [settings, point, knownLabel, radius, castDne]);

  useEffect(() => {
    if (!shareStatus) return undefined;
    const timer = window.setTimeout(() => setShareStatus(""), 4000);
    return () => window.clearTimeout(timer);
  }, [shareStatus]);

  const submitSearch = async (event) => {
    event.preventDefault();
    const text = query.trim();
    if (text.length < 3) {
      setSearch({ loading: false, error: "Zadejte ulici a číslo domu, například Štefánikova 12." });
      return;
    }
    setSearch({ loading: true, error: "" });
    try {
      const response = await fetch(`/api/address/search?${new URLSearchParams({ q: text })}`);
      if (response.status === 404) {
        setSearch({ loading: false, error: "Tuhle adresu jsme v Praze nenašli. Zkuste přidat číslo domu nebo čtvrť, případně vyberte místo kliknutím do mapy." });
        return;
      }
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const found = await response.json();
      setPoint(found);
      setQuery(found.label);
      setSearch({ loading: false, error: "" });
    } catch {
      setSearch({ loading: false, error: "Hledání adres teď nefunguje. Místo můžete vybrat kliknutím do mapy." });
    }
  };
  const pickOnMap = (lon, lat) => {
    setPoint({ lon, lat, label: "" });
    setQuery("");
    setSearch({ loading: false, error: "" });
  };
  const clearSelection = () => {
    setPoint(null);
    setQuery("");
    setSearch({ loading: false, error: "" });
  };
  const shareLink = async () => {
    try {
      await navigator.clipboard.writeText(window.location.href);
      setShareStatus("Odkaz je zkopírovaný");
    } catch {
      setShareStatus("Odkaz se nepodařilo zkopírovat");
    }
  };

  const loading = reportRequest.loading || search.loading;
  return (
    <div className="app-shell">
      <a className="skip-link" href="#address">Přejít k hledání adresy</a>
      <Masthead page="address" dataDate={settings?.latest_date} />

      <main id="address">
        <section className="explorer-head">
          <div className="explorer-title">
            <h1>Parkování v okolí adresy</h1>
            <p className="subtitle">Místa, oprávnění a obsazenost zón v docházkové vzdálenosti</p>
            <p className="explainer">
              Vyberte adresu nebo bod na mapě a vzdálenost, kterou jste ochotni dojít.
              Sečteme zóny placeného stání ve stejné městské části, které do okruhu zasahují.
            </p>
          </div>
        </section>

        {options.loading && !settings ? <LoadingState /> : null}
        {options.error ? <ErrorState hasData={false} onRetry={options.retry} /> : null}

        {settings ? (
          <>
            <form className="address-toolbar" onSubmit={submitSearch}>
              <label className="filter-control address-field">
                <span className="filter-label">Adresa v Praze</span>
                <span className="address-input">
                  <input
                    type="search"
                    value={query}
                    onChange={(event) => setQuery(event.target.value)}
                    placeholder={settings.search_available ? "Například Štefánikova 12" : "Hledání adres není dostupné, klikněte do mapy"}
                    disabled={!settings.search_available}
                    maxLength={120}
                    autoComplete="street-address"
                  />
                  <button type="submit" className="primary-action" disabled={!settings.search_available || search.loading}>
                    <IconSearch size={16} stroke={1.8} aria-hidden="true" />
                    Najít
                  </button>
                </span>
              </label>
              <label className="filter-control district-select">
                <span className="filter-label">Okruh</span>
                <select value={radius} onChange={(event) => setRadiusChoice(Number(event.target.value))}>
                  {radii.map((value) => <option key={value} value={value}>{formatDistance(value)}</option>)}
                </select>
              </label>
              <label className="filter-control district-select">
                <span className="filter-label">Část dne (obsazenost)</span>
                <select value={castDne} onChange={(event) => setCastChoice(event.target.value)}>
                  {settings.cast_dne.map((value) => <option key={value} value={value}>{TIME_LABELS[value] ?? value}</option>)}
                </select>
              </label>
              <div className="toolbar-actions">
                <div className="toolbar-buttons">
                  <button type="button" className="icon-action" onClick={shareLink} disabled={!point}>
                    <IconShare size={16} stroke={1.7} aria-hidden="true" />
                    Sdílet odkaz
                  </button>
                  <button type="button" className="icon-action" onClick={clearSelection} disabled={!point}>
                    <IconX size={16} stroke={1.7} aria-hidden="true" />
                    Vymazat
                  </button>
                </div>
                <span className={`toolbar-status${loading ? " is-loading" : ""}`} role="status" aria-live="polite">
                  {search.loading ? "Hledám adresu…" : reportRequest.loading ? "Počítám okolí…" : shareStatus}
                </span>
              </div>
              {search.error ? <p className="address-error" role="alert">{search.error}</p> : null}
            </form>

            <div className={`workspace-grid address-grid${reportRequest.loading && report ? " is-refreshing" : ""}`} aria-busy={reportRequest.loading}>
              <div className="workspace-main">
                <div className="primary-content">
                  {reportRequest.error ? <ErrorState hasData={Boolean(report)} onRetry={reportRequest.retry} /> : null}
                  <div className="map-frame">
                    <ZoneMap
                      point={point}
                      radius={radius}
                      geometry={reportMatchesPoint ? report.geometry : null}
                      zones={reportMatchesPoint ? report.zones : null}
                      bounds={settings.bounds}
                      onPick={pickOnMap}
                    />
                  </div>
                  <p className="data-note">
                    Kliknutím do mapy vyberete jiné místo. Tmavě ohraničený je úsek, ve kterém místo leží nebo který je mu nejblíž.
                  </p>
                </div>
              </div>
              <SelectionPanel report={report} label={label} latestDate={settings.latest_date} />
            </div>

            {report ? (
              <div className={`address-sections${reportRequest.loading ? " is-refreshing" : ""}`}>
                <ReportSections report={report} label={label} onRadius={setRadiusChoice} />
              </div>
            ) : null}
          </>
        ) : null}
      </main>
    </div>
  );
}
