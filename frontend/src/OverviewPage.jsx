import { useEffect, useMemo, useState } from "react";
import { IconArrowRight, IconSearch } from "@tabler/icons-react";

import { addressStateToSearch } from "./addressState.js";
import { formatMonth, materializeMonthlyRows } from "./chartData.js";
import { EChart } from "./EChart.jsx";
import { ErrorState, LoadingState } from "./ExplorerStates.jsx";
import {
  districtName,
  formatMonthGenitive,
  formatNumber,
  formatPercent,
  formatRatio,
  isoToCzech,
  ratioFormat,
} from "./format.js";
import { useJson, useMediaQuery } from "./hooks.js";
import { Masthead } from "./Masthead.jsx";
import {
  biggestChanges,
  districtLocative,
  explorerHref,
  formatPercentChange,
  formatRatioChange,
  longRunTrend,
  ratioChangeClause,
  shareOf,
  yearChangeClause,
} from "./overviewState.js";

const SLATE = "#667386";
// Red and slate stay reserved for permits and for permits per space, so breakdowns use other hues.
const PARKER_COLORS = ["#114f50", "#9a684f", "#8a866c", "#b48b87", "#5d8a8b", "#c2a878"];
const PERIOD_LABELS = { den: "Ve dne", noc: "V noci" };
const TREND_HEADINGS = {
  up: "Na jedno místo připadá čím dál víc oprávnění",
  down: "Na jedno místo připadá čím dál méně oprávnění",
  flat: "Na jedno místo připadá zhruba stejně oprávnění jako dřív",
};

function ratioChartOption(series, compact, reduceMotion) {
  const rows = materializeMonthlyRows(series, ["permits_per_space"]);
  const values = series.map((row) => row.permits_per_space);
  const axisText = { color: "#73736f", fontFamily: "Inter, sans-serif", fontSize: 12 };
  const pointValue = (value) => (Array.isArray(value) ? value[1] : value);
  return {
    animationDuration: reduceMotion ? 0 : 650,
    grid: { left: 46, right: compact ? 14 : 64, top: 18, bottom: 34 },
    tooltip: {
      trigger: "axis",
      backgroundColor: "#fffdf8",
      borderColor: "#d9d4ca",
      textStyle: { color: "#26251f", fontFamily: "Inter, sans-serif" },
      valueFormatter: (value) => formatRatio(pointValue(value)),
    },
    xAxis: {
      type: "time",
      boundaryGap: false,
      axisLine: { lineStyle: { color: "#aaa59a" } },
      axisTick: { show: false },
      axisLabel: { ...axisText, formatter: "{yyyy}", hideOverlap: true },
      axisPointer: { label: { formatter: ({ value }) => formatMonth(value) } },
    },
    yAxis: {
      type: "value",
      // The ratio moves within a few tenths, so the scale follows the data instead of starting at zero.
      min: Math.floor(Math.min(...values) * 10 - 0.5) / 10,
      max: Math.ceil(Math.max(...values) * 10 + 0.5) / 10,
      interval: 0.1,
      axisLabel: { ...axisText, formatter: (value) => ratioFormat.format(value) },
      splitLine: { lineStyle: { color: "#e5e0d7", type: "dashed" } },
    },
    series: [{
      name: "Oprávnění na místo",
      type: "line",
      showSymbol: false,
      connectNulls: false,
      data: rows.map((row) => [row.date, row.permits_per_space]),
      lineStyle: { color: SLATE, width: 2.4 },
      itemStyle: { color: SLATE },
      endLabel: {
        show: !compact,
        formatter: (params) => formatRatio(pointValue(params.value)),
        color: SLATE,
        fontFamily: "Newsreader, Georgia, serif",
        fontSize: 17,
      },
    }],
  };
}

function Figures({ city }) {
  const sinceLastYear = city.compared_with ? "za rok" : "";
  return (
    <dl className="overview-figures">
      <div className="permits">
        <dt>Parkovací oprávnění</dt>
        <dd>
          <strong>{formatNumber(city.permits)}</strong>
          {city.permits_change != null ? <small>{formatPercentChange(city.permits_change)} {sinceLastYear}</small> : null}
        </dd>
      </div>
      <div className="spaces">
        <dt>Parkovací místa</dt>
        <dd>
          <strong>{formatNumber(city.spaces)}</strong>
          {city.spaces_change != null ? <small>{formatPercentChange(city.spaces_change)} {sinceLastYear}</small> : null}
        </dd>
      </div>
      <div className="ratio">
        <dt>Oprávnění na jedno místo</dt>
        <dd>
          <strong>{formatRatio(city.permits_per_space)}</strong>
          {city.permits_per_space_year_ago != null ? <small>před rokem {formatRatio(city.permits_per_space_year_ago)}</small> : null}
        </dd>
      </div>
    </dl>
  );
}

function TrendSection({ overview }) {
  const compact = useMediaQuery("(max-width: 760px)");
  const reduceMotion = useMediaQuery("(prefers-reduced-motion: reduce)");
  const option = useMemo(
    () => ratioChartOption(overview.trend.series, compact, reduceMotion),
    [overview.trend.series, compact, reduceMotion],
  );
  const { city, trend: { series, zones, spaces, share_of_spaces: share } } = overview;
  const since = formatMonthGenitive(series[0].date);
  const trend = longRunTrend(series);
  const first = series[0].permits_per_space;
  const last = series.at(-1).permits_per_space;
  return (
    <section className="overview-section" aria-labelledby="trend-heading">
      <div className="overview-text">
        <h2 id="trend-heading">{TREND_HEADINGS[trend] ?? "Kolik oprávnění připadá na jedno místo"}</h2>
        <p>
          V úsecích, které existují od {since}, počet oprávnění na jedno místo
          {" "}{ratioChangeClause(first, last)}.
          {city.compared_with
            ? ` Za celou Prahu za poslední rok ${yearChangeClause("oprávnění", city.permits_change)}, ${yearChangeClause("parkovacích míst", city.spaces_change)}.`
            : ""}
        </p>
        <p>
          Graf sleduje jen úseky s daty v každém měsíci, aby vývoj nezkreslovalo rozšiřování
          zón. Je jich {formatNumber(zones)} a mají {formatNumber(spaces)} míst
          {share != null ? `, tedy ${formatPercent(share)} dnešní kapacity` : ""}. Proto končí
          na {formatRatio(last)}, zatímco za celou Prahu je to {formatRatio(city.permits_per_space)}.
        </p>
        <p>Čára se přerušuje v měsících, pro které data chybí.</p>
        <a
          className="overview-link"
          href={explorerHref({ metric: "pressure", start: `${series[0].date.slice(0, 7)}-01`, sameZones: true })}
        >
          Prozkoumat vývoj podrobně <IconArrowRight size={15} stroke={1.7} aria-hidden="true" />
        </a>
      </div>
      <EChart
        option={option}
        className="overview-chart"
        ariaLabel={`Vývoj počtu parkovacích oprávnění na jedno místo od ${since} v úsecích, které existují celou dobu`}
      />
    </section>
  );
}

function DistrictSection({ overview }) {
  const { districts } = overview;
  const highest = districts[0];
  const lowest = districts.at(-1);
  const { rise, fall } = biggestChanges(districts);
  const longest = Math.max(...districts.map((row) => row.permits_per_space), 0);
  return (
    <section className="overview-section" aria-labelledby="districts-heading">
      <div className="overview-text">
        <h2 id="districts-heading">Kde je nejtěsněji</h2>
        <p>
          Nejvíc oprávnění na jedno místo je v {districtLocative(highest.district)} ({formatRatio(highest.permits_per_space)}),
          nejméně v {districtLocative(lowest.district)} ({formatRatio(lowest.permits_per_space)}).
        </p>
        {rise || fall ? (
          <p>
            {rise ? `Za poslední rok poměr nejvíc vzrostl v ${districtLocative(rise.district)} (${formatRatioChange(rise.ratio_change)})` : null}
            {rise && fall ? " a nejvíc klesl " : null}
            {!rise && fall ? "Za poslední rok poměr nejvíc klesl " : null}
            {fall ? `v ${districtLocative(fall.district)} (${formatRatioChange(fall.ratio_change)})` : null}.
          </p>
        ) : null}
        <p>Kliknutím na městskou část otevřete její podrobný vývoj.</p>
      </div>
      <div className="overview-table-wrap">
        <table className="district-table overview-districts">
          <caption className="sr-only">Městské části seřazené podle počtu oprávnění na jedno parkovací místo</caption>
          <thead>
            <tr>
              <th scope="col">Městská část</th>
              <td aria-hidden="true" />
              <th scope="col">Oprávnění na místo</th>
              <th scope="col">Změna za rok</th>
              <th scope="col">Oprávnění</th>
              <th scope="col">Parkovací místa</th>
            </tr>
          </thead>
          <tbody>
            {districts.map((row) => (
              <tr key={row.district}>
                <th scope="row"><a href={explorerHref({ district: row.district })}>{districtName(row.district)}</a></th>
                <td className="district-bar" aria-hidden="true">
                  <span style={{ width: `${longest ? (row.permits_per_space / longest) * 100 : 0}%` }} />
                </td>
                <td className="district-ratio">{formatRatio(row.permits_per_space)}</td>
                <td>{formatRatioChange(row.ratio_change)}</td>
                <td>{formatNumber(row.permits)}</td>
                <td>{formatNumber(row.spaces)}</td>
              </tr>
            ))}
          </tbody>
        </table>
        <p className="data-note">
          Údaje za {formatMonth(overview.latest_date)}. Městské části, které mají v zónách méně
          než {formatNumber(overview.min_ranked_spaces)} parkovacích míst, v tabulce nejsou.
        </p>
      </div>
    </section>
  );
}

function ParkerSection({ parkers }) {
  const day = parkers.find((row) => row.period === "den");
  const night = parkers.find((row) => row.period === "noc");
  const names = (day ?? night).groups.map((group) => group.series);
  const colorOf = (name) => PARKER_COLORS[names.indexOf(name) % PARKER_COLORS.length];
  const dayResidents = shareOf(day, "Rezidenti");
  const dayVisitors = shareOf(day, "Návštěvníci");
  const nightResidents = shareOf(night, "Rezidenti");
  return (
    <section className="overview-section" aria-labelledby="parkers-heading">
      <div className="overview-text">
        <h2 id="parkers-heading">Kdo v zónách parkuje</h2>
        {dayResidents != null && dayVisitors != null ? (
          <p>
            Ve dne patří rezidentům {formatPercent(dayResidents)} zaparkovaných aut
            a návštěvníkům {formatPercent(dayVisitors)}.
            {nightResidents != null ? ` V noci je rezidentských aut ${formatPercent(nightResidents)}.` : ""}
          </p>
        ) : null}
        <p>Podíly vycházejí z měření obsazenosti za {formatMonth((day ?? night).date)}.</p>
      </div>
      <div className="parker-mix">
        {parkers.map((period) => (
          <div key={period.period} className="parker-row">
            <span className="parker-period">{PERIOD_LABELS[period.period] ?? period.period}</span>
            <div className="parker-bar" aria-hidden="true">
              {names.map((name) => (
                <span key={name} style={{ flexGrow: shareOf(period, name) ?? 0, background: colorOf(name) }} />
              ))}
            </div>
          </div>
        ))}
        <table className="parker-table">
          <caption className="sr-only">Podíl zaparkovaných aut podle toho, komu patří</caption>
          <thead>
            <tr>
              <th scope="col">Kdo parkuje</th>
              {parkers.map((period) => (
                <th key={period.period} scope="col">{PERIOD_LABELS[period.period] ?? period.period}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {names.map((name) => (
              <tr key={name}>
                <th scope="row"><i style={{ background: colorOf(name) }} aria-hidden="true" />{name}</th>
                {parkers.map((period) => (
                  <td key={period.period}>{formatPercent(shareOf(period, name))}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function AddressSection() {
  const options = useJson("/api/address/options");
  const [query, setQuery] = useState("");
  const [search, setSearch] = useState({ loading: false, error: "" });
  const searchAvailable = Boolean(options.data?.search_available);

  const submit = async (event) => {
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
        setSearch({ loading: false, error: "Tuhle adresu jsme v Praze nenašli. Zkuste přidat číslo domu nebo čtvrť, případně vyberte místo na mapě." });
        return;
      }
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const point = await response.json();
      window.location.assign(`./?${addressStateToSearch({ point, label: point.label }, {})}`);
    } catch {
      setSearch({ loading: false, error: "Adresu se nepodařilo vyhledat. Zkuste to znovu, nebo vyberte místo na mapě." });
    }
  };

  return (
    <section className="overview-section" aria-labelledby="address-heading">
      <div className="overview-text">
        <h2 id="address-heading">A co u vás?</h2>
        <p>
          Pro kteroukoli adresu ukážeme, kolik parkovacích míst a oprávnění je
          v docházkové vzdálenosti a jak bývají místa obsazená.
        </p>
      </div>
      <div className="overview-address">
        {searchAvailable ? (
          <form onSubmit={submit}>
            <label className="filter-label" htmlFor="overview-address">Adresa v Praze</label>
            <div className="address-input">
              <input
                id="overview-address"
                type="search"
                value={query}
                onChange={(event) => setQuery(event.target.value)}
                placeholder="Například Štefánikova 12"
                autoComplete="street-address"
              />
              <button type="submit" className="primary-action" disabled={search.loading}>
                <IconSearch size={16} stroke={1.7} aria-hidden="true" />
                {search.loading ? "Hledám…" : "Zobrazit okolí"}
              </button>
            </div>
            {search.error ? <p className="address-error" role="alert">{search.error}</p> : null}
          </form>
        ) : null}
        <a className="overview-link" href="./?page=address">
          {searchAvailable ? "Nebo vyberte místo na mapě" : "Vybrat místo na mapě"}
          <IconArrowRight size={15} stroke={1.7} aria-hidden="true" />
        </a>
      </div>
    </section>
  );
}

function DataSection({ latestDate, downloadedOn }) {
  return (
    <section className="overview-section" aria-labelledby="data-heading">
      <div className="overview-text">
        <h2 id="data-heading">O datech</h2>
      </div>
      <div className="overview-prose">
        <p>
          Data poskytuje Technická správa komunikací přes svůj{" "}
          <a href="https://zps.tsk-praha.cz/">portál zón placeného stání</a>.{" "}
          {downloadedOn
            ? `Náš poslední export z portálu je z ${isoToCzech(downloadedOn)}, nejnovější měsíc v datech je ${formatMonth(latestDate)}.`
            : `Nejnovější měsíc v datech je ${formatMonth(latestDate)}.`}
        </p>
        <p>
          Oprávnění na jedno místo říká, kolik platných parkovacích oprávnění připadá na jedno
          parkovací místo v zónách. Hodnota nad 1 znamená, že oprávnění je víc než míst.
          Neznamená to, že se auta nevejdou, protože všechna neparkují ve stejnou chvíli,
          ale čím je číslo vyšší, tím hůř se místo hledá.
        </p>
        <p>
          Do počtů se nezapočítávají oprávnění budov dál než 500 m od nejbližšího úseku zóny
          (asi 2 %) a souhrnné záznamy bez konkrétního místa, zejména ekologická oprávnění
          vedená na jedné adrese. Součty jsou proto nižší než ve zdrojových datech TSK.
        </p>
      </div>
    </section>
  );
}

export function OverviewPage() {
  const request = useJson("/api/overview");
  const overview = request.data;
  const ready = Boolean(overview?.city);

  useEffect(() => {
    document.title = "Parkování Praha";
  }, []);

  return (
    <div className="app-shell">
      <a className="skip-link" href="#overview">Přejít k přehledu</a>
      <Masthead page="overview" dataDate={overview?.latest_date} />

      <main id="overview" className="overview">
        <section className="explorer-head overview-head">
          <div className="explorer-title">
            <h1>Parkování v pražských zónách</h1>
            <p className="subtitle">Kolik je oprávnění, kolik míst a kde je nejtěsněji</p>
            {ready ? (
              <p className="explainer">
                V zónách placeného stání je {formatNumber(overview.city.spaces)} parkovacích míst
                a platí pro ně {formatNumber(overview.city.permits)} parkovacích oprávnění.
                Údaje za {formatMonth(overview.latest_date)}.
              </p>
            ) : null}
          </div>
        </section>

        {request.loading && !overview ? <LoadingState /> : null}
        {request.error ? <ErrorState hasData={ready} onRetry={request.retry} /> : null}
        {overview && !ready ? (
          <p className="explainer" role="status">Data o parkovacích oprávněních teď nejsou k dispozici.</p>
        ) : null}
        {ready ? (
          <>
            <Figures city={overview.city} />
            {overview.trend ? <TrendSection overview={overview} /> : null}
            {overview.districts.length > 0 ? <DistrictSection overview={overview} /> : null}
            {overview.parkers.length > 0 ? <ParkerSection parkers={overview.parkers} /> : null}
            <AddressSection />
            <DataSection latestDate={overview.latest_date} downloadedOn={overview.downloaded_on} />
          </>
        ) : null}
      </main>
    </div>
  );
}
