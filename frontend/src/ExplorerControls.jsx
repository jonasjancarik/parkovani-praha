import { useMemo } from "react";
import {
  IconCalendar,
  IconChevronDown,
  IconRefresh,
  IconShare,
} from "@tabler/icons-react";

import { monthEnd, monthStart } from "./explorerState.js";


export function MultiSelect({ label, values, selected, onChange, display }) {
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
          <span className="selection-chips" aria-label={`${label}: ${selected.length ? `${selected.length} vybráno` : "všechny"}`}>
            {selected.length === 0 ? (
              <span className="selection-placeholder">Všechny</span>
            ) : selected.slice(0, 3).map((value) => (
              <span className="chip" key={value}>{display?.(value) ?? value}</span>
            ))}
            {selected.length > 3 ? <span className="chip-overflow">+{selected.length - 3}</span> : null}
          </span>
          <IconChevronDown size={16} stroke={1.8} aria-hidden="true" />
        </summary>
        <div className="select-menu">
          <div className="select-menu-actions">
            <button type="button" onClick={() => onChange(values)}>Vybrat vše</button>
            <button type="button" onClick={() => onChange([])}>Zrušit výběr</button>
          </div>
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

export function DistrictSelect({ values, selected, onChange }) {
  return (
    <label className="filter-control district-select">
      <span className="filter-label">Městská část</span>
      <select
        value={selected[0] ?? ""}
        onChange={(event) => onChange(event.target.value ? [event.target.value] : [])}
      >
        <option value="">Všechny městské části</option>
        {values.map((value) => (
          <option key={value} value={value}>{value.replace(/^P0?/, "Praha ")}</option>
        ))}
      </select>
    </label>
  );
}

export function DateRange({ start, end, min, max, onStart, onEnd }) {
  return (
    <div className="filter-control date-control">
      <span className="filter-label">Období po měsících</span>
      <div className="date-fields">
        <IconCalendar size={16} stroke={1.6} aria-hidden="true" />
        <input
          aria-label="Začátek období"
          type="month"
          value={start?.slice(0, 7)}
          min={min?.slice(0, 7)}
          max={(end || max)?.slice(0, 7)}
          onChange={(event) => onStart(monthStart(event.target.value))}
        />
        <span>–</span>
        <input
          aria-label="Konec období"
          type="month"
          value={end?.slice(0, 7)}
          min={(start || min)?.slice(0, 7)}
          max={max?.slice(0, 7)}
          onChange={(event) => onEnd(monthEnd(event.target.value))}
        />
      </div>
    </div>
  );
}

export function ToolbarActions({ loading, hasActiveFilters, shareStatus, onShare, onReset }) {
  return (
    <div className="toolbar-actions">
      <div className="toolbar-buttons">
        <button type="button" className="icon-action" onClick={onShare}>
          <IconShare size={16} stroke={1.7} aria-hidden="true" />
          Sdílet výběr
        </button>
        <button type="button" className="icon-action" onClick={onReset} disabled={!hasActiveFilters}>
          <IconRefresh size={16} stroke={1.7} aria-hidden="true" />
          Obnovit
        </button>
      </div>
      <span className={`toolbar-status${loading ? " is-loading" : ""}`} role="status" aria-live="polite">
        {loading ? "Aktualizuji data…" : shareStatus}
      </span>
    </div>
  );
}
