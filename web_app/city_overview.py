from __future__ import annotations

import pandas as pd

try:
    from .explorer_analytics import PARKER_TYPES, _district_pressure
    from .explorer_data import (
        ExplorerFilters,
        _filter_rows,
        _unique_zone_months,
        build_explorer_payload,
    )
except ImportError:  # Streamlit runs modules from web_app as top-level imports.
    from explorer_analytics import PARKER_TYPES, _district_pressure
    from explorer_data import (
        ExplorerFilters,
        _filter_rows,
        _unique_zone_months,
        build_explorer_payload,
    )


# Districts with only a handful of paid-parking spaces would lead or close the
# ranking with ratios that say nothing about the district as a whole.
MIN_RANKED_SPACES = 1000
PARKER_PERIODS = ("den", "noc")


def _same_month_a_year_earlier(dates: pd.Series, latest: pd.Timestamp) -> pd.Timestamp | None:
    target = latest.to_period("M") - 12
    matches = dates[dates.dt.to_period("M") == target]
    return matches.max() if not matches.empty else None


def _relative_change(current: float | None, previous: float | None) -> float | None:
    if current is None or previous is None or not previous:
        return None
    return (current - previous) / previous


def _city(series: list[dict]) -> dict | None:
    if not series:
        return None
    dates = pd.to_datetime(pd.Series([row["date"] for row in series]))
    latest = series[-1]
    earlier_date = _same_month_a_year_earlier(dates, dates.iloc[-1])
    earlier = (
        next(row for row in series if row["date"] == earlier_date.date().isoformat())
        if earlier_date is not None else None
    )
    ratio = latest["permits_per_space"]
    earlier_ratio = earlier["permits_per_space"] if earlier else None
    return {
        "permits": latest["permits"],
        "spaces": latest["spaces"],
        "permits_per_space": ratio,
        "compared_with": earlier["date"] if earlier else None,
        "permits_change": _relative_change(latest["permits"], earlier and earlier["permits"]),
        "spaces_change": _relative_change(latest["spaces"], earlier and earlier["spaces"]),
        "permits_per_space_year_ago": earlier_ratio,
        "ratio_change": (
            round(ratio - earlier_ratio, 3)
            if ratio is not None and earlier_ratio is not None else None
        ),
    }


def _districts(base: pd.DataFrame) -> list[dict]:
    pressure = _district_pressure(base)
    if pressure.empty:
        return []
    latest_date = pressure["date"].max()
    latest = pressure[
        (pressure["date"] == latest_date)
        & (pressure["parkovacich_mist_v_zps"] >= MIN_RANKED_SPACES)
    ].sort_values("value", ascending=False)
    earlier_date = _same_month_a_year_earlier(pressure["date"], latest_date)
    earlier = (
        pressure[pressure["date"] == earlier_date].set_index("mestska_cast")["value"]
        if earlier_date is not None else pd.Series(dtype=float)
    )
    rows = []
    for row in latest.itertuples(index=False):
        previous = earlier.get(row.mestska_cast)
        rows.append(
            {
                "district": row.mestska_cast,
                "permits": round(float(row.POP_CELKEM)),
                "spaces": round(float(row.parkovacich_mist_v_zps)),
                "permits_per_space": round(float(row.value), 3),
                "ratio_change": (
                    round(float(row.value) - float(previous), 3)
                    if previous is not None and pd.notna(previous) else None
                ),
            }
        )
    return rows


def _parkers(scoped: pd.DataFrame) -> list[dict]:
    """Who is parked in the zones by day and by night in the latest measured month."""
    columns = [column for column in PARKER_TYPES.values() if column in scoped.columns]
    if scoped.empty or not columns:
        return []
    label_by_column = {column: label for label, column in PARKER_TYPES.items()}
    periods = []
    for period in PARKER_PERIODS:
        rows = scoped[scoped["cast_dne"] == period]
        rows = rows[rows[columns].notna().any(axis=1)]
        if rows.empty:
            continue
        month = rows[rows["date"] == rows["date"].max()]
        totals = month.groupby("kod_useku")[columns].mean().sum()
        total = float(totals.sum())
        if total <= 0:
            continue
        periods.append(
            {
                "period": period,
                "date": month["date"].max().date().isoformat(),
                "groups": [
                    {
                        "series": label_by_column[column],
                        "value": round(float(value)),
                        "share": float(value) / total,
                    }
                    for column, value in totals.sort_values(ascending=False).items()
                ],
            }
        )
    return periods


def build_city_overview(df: pd.DataFrame, downloaded_on: str | None = None) -> dict:
    """Citywide figures for the overview page, using the explorer's default selection."""
    filters = ExplorerFilters()
    explorer = build_explorer_payload(df, filters)
    series = [row for row in explorer["series"] if row["permits_per_space"] is not None]
    scoped = _filter_rows(df, filters, apply_cast_dne=False)
    return {
        "latest_date": series[-1]["date"] if series else None,
        "downloaded_on": downloaded_on,
        "city": _city(series),
        "series": series,
        "districts": _districts(_unique_zone_months(scoped)),
        "min_ranked_spaces": MIN_RANKED_SPACES,
        "parkers": _parkers(scoped),
    }
