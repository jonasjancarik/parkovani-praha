import { useEffect, useMemo, useRef } from "react";
import {
  IconCalendar,
  IconChevronDown,
  IconRefresh,
  IconShare,
} from "@tabler/icons-react";

import { formatMonth, monthsBetween } from "./chartData.js";
import { monthEnd, monthStart } from "./explorerState.js";


export function MultiSelect({ label, values, selected, onChange, display }) {
  const menuRef = useRef(null);
  const selectedValues = useMemo(() => new Set(selected), [selected]);

  useEffect(() => {
    const closeOnOutsidePress = (event) => {
      const menu = menuRef.current;
      if (menu?.open && !menu.contains(event.target)) menu.open = false;
    };
    const closeOnEscape = (event) => {
      const menu = menuRef.current;
      if (event.key !== "Escape" || !menu?.open) return;
      const hadFocus = menu.contains(document.activeElement);
      menu.open = false;
      if (hadFocus) menu.querySelector("summary")?.focus();
    };
    document.addEventListener("pointerdown", closeOnOutsidePress);
    document.addEventListener("keydown", closeOnEscape);
    return () => {
      document.removeEventListener("pointerdown", closeOnOutsidePress);
      document.removeEventListener("keydown", closeOnEscape);
    };
  }, []);
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
      <details className="multi-select" ref={menuRef}>
        <summary>
          <span className="selection-chips" aria-label={`${label}: ${selected.length ? `${selected.length} vybráno` : "všechny"}`}>
            {selected.length === 0 ? (
              <span className="selection-placeholder">Všechny</span>
            ) : selected.slice(0, 2).map((value) => (
              <span className="chip" key={value}>{display?.(value) ?? value}</span>
            ))}
            {selected.length > 2 ? <span className="chip-overflow">+{selected.length - 2}</span> : null}
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
  const months = useMemo(() => monthsBetween(min, max), [min, max]);
  const startMonth = start?.slice(0, 7);
  const endMonth = end?.slice(0, 7);
  return (
    <div className="filter-control date-control">
      <span className="filter-label">Období</span>
      <div className="date-fields">
        <IconCalendar size={16} stroke={1.6} aria-hidden="true" />
        <select
          aria-label="Začátek období"
          value={startMonth}
          onChange={(event) => onStart(monthStart(event.target.value))}
        >
          {months.filter((month) => month <= endMonth).map((month) => (
            <option key={month} value={month}>{formatMonth(month)}</option>
          ))}
        </select>
        <span>–</span>
        <select
          aria-label="Konec období"
          value={endMonth}
          onChange={(event) => onEnd(monthEnd(event.target.value))}
        >
          {months.filter((month) => month >= startMonth).map((month) => (
            <option key={month} value={month}>{formatMonth(month)}</option>
          ))}
        </select>
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

export function SameZonesToggle({ checked, onChange }) {
  return (
    <div className="filter-control">
      <span className="filter-label" id="same-zones-label">Rozšiřování zón</span>
      <label className="same-zones-toggle">
        <input
          type="checkbox"
          checked={checked}
          aria-describedby="same-zones-label"
          onChange={(event) => onChange(event.target.checked)}
        />
        Jen úseky, které existují celé období
      </label>
    </div>
  );
}
