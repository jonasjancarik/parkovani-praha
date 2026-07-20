from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

try:
    from .explorer_data import ExplorerFilters, _filter_rows
except ImportError:  # Streamlit runs modules from web_app as top-level imports.
    from explorer_data import ExplorerFilters, _filter_rows


PERMIT_TYPES = {
    "Rezidentní": "pop_rezidentska",
    "Vlastnická": "pop_vlastnicka",
    "Abonentní": "pop_abonentska",
    "Přenosná": "pop_prenosna",
    "Carsharing": "pop_carsharing",
    "Ekologická": "pop_ekologicka",
    "Ostatní": "pop_ostatni",
    "Sociální": "pop_socialni",
}

PARKER_TYPES = {
    "Rezidenti": "rezidentska",
    "Vlastníci": "vlastnicka",
    "Abonenti": "abonentska",
    "Návštěvníci": "navstevnici",
    "Ostatní": "ostatni",
    "Sociální": "socialni",
}


@dataclass(frozen=True)
class ForecastSettings:
    history_months: int = 24
    horizon_months: int = 12


def _available_columns(df: pd.DataFrame, columns: list[str]) -> list[str]:
    return [column for column in columns if column in df.columns]


def _zone_month_values(scoped: pd.DataFrame, measures: list[str]) -> pd.DataFrame:
    metadata = [
        "kod_useku",
        "date",
        "mestska_cast",
        "kod_zsj",
        "naz_zsj",
        "typ_zony",
    ]
    metadata = _available_columns(scoped, metadata)
    measures = _available_columns(scoped, measures)
    if scoped.empty or not measures:
        return pd.DataFrame(columns=metadata + measures)

    aggregations = {column: "first" for column in metadata if column not in {"kod_useku", "date"}}
    aggregations.update({column: "mean" for column in measures})
    return (
        scoped[metadata + measures]
        .groupby(["kod_useku", "date"], as_index=False, dropna=False)
        .agg(aggregations)
    )


def _long_time_series(
    base: pd.DataFrame,
    measures: dict[str, str],
) -> list[dict]:
    columns = _available_columns(base, list(measures.values()))
    if base.empty or not columns:
        return []
    grouped = base.groupby("date", as_index=False)[columns].sum().sort_values("date")
    label_by_column = {column: label for label, column in measures.items()}
    rows: list[dict] = []
    for row in grouped.itertuples(index=False):
        row_dict = row._asdict()
        date_value = row_dict["date"].date().isoformat()
        for column in columns:
            rows.append(
                {
                    "date": date_value,
                    "series": label_by_column[column],
                    "value": round(float(row_dict[column])),
                }
            )
    return rows


def _zone_mix(base: pd.DataFrame) -> list[dict]:
    if base.empty:
        return []
    mix = (
        base.groupby(["typ_zony", "kod_useku"], as_index=False)[
            "parkovacich_mist_v_zps"
        ]
        .mean()
        .groupby("typ_zony", as_index=False)["parkovacich_mist_v_zps"]
        .sum()
        .sort_values("parkovacich_mist_v_zps", ascending=False)
    )
    total = float(mix["parkovacich_mist_v_zps"].sum())
    return [
        {
            "zone_type": row.typ_zony,
            "spaces": round(float(row.parkovacich_mist_v_zps)),
            "share": float(row.parkovacich_mist_v_zps) / total if total else 0,
        }
        for row in mix.itertuples(index=False)
    ]


def _parker_share_by_district(base: pd.DataFrame) -> list[dict]:
    columns = _available_columns(base, list(PARKER_TYPES.values()))
    if base.empty or not columns:
        return []
    by_zone = (
        base.groupby(["mestska_cast", "kod_useku"], as_index=False)[columns]
        .mean()
        .groupby("mestska_cast", as_index=False)[columns]
        .sum()
    )
    label_by_column = {column: label for label, column in PARKER_TYPES.items()}
    rows: list[dict] = []
    for row in by_zone.itertuples(index=False):
        row_dict = row._asdict()
        total = sum(float(row_dict[column]) for column in columns)
        if total <= 0:
            continue
        for column in columns:
            value = float(row_dict[column])
            rows.append(
                {
                    "district": row_dict["mestska_cast"],
                    "series": label_by_column[column],
                    "value": round(value),
                    "share": value / total,
                }
            )
    return rows


def _zsj_months(base: pd.DataFrame) -> pd.DataFrame:
    if base.empty:
        return base
    grouped = (
        base.groupby(["date", "kod_zsj", "naz_zsj", "mestska_cast"], as_index=False)[
            ["POP_CELKEM", "parkovacich_mist_v_zps"]
        ]
        .sum()
        .sort_values("date")
    )
    grouped = grouped[grouped["parkovacich_mist_v_zps"] > 0].copy()
    grouped["permits_per_space"] = (
        grouped["POP_CELKEM"] / grouped["parkovacich_mist_v_zps"]
    )
    return grouped


def _zsj_changes(zsj_months: pd.DataFrame) -> list[dict]:
    rows: list[dict] = []
    if zsj_months.empty:
        return rows
    for (code, name, district), group in zsj_months.groupby(
        ["kod_zsj", "naz_zsj", "mestska_cast"], dropna=False
    ):
        group = group.sort_values("date")
        positive = group[group["POP_CELKEM"] > 0]
        if len(positive) < 2:
            continue
        first = positive.iloc[0]
        latest = positive.iloc[-1]
        days = (latest["date"] - first["date"]).days
        if days <= 0:
            continue
        annual_change = (
            (float(latest["POP_CELKEM"]) - float(first["POP_CELKEM"]))
            / days
            * 365
        )
        rows.append(
            {
                "code": str(code),
                "name": str(name),
                "district": str(district),
                "start_permits": round(float(first["POP_CELKEM"])),
                "end_permits": round(float(latest["POP_CELKEM"])),
                "annual_change": round(annual_change, 2),
                "pressure_change": round(
                    float(latest["permits_per_space"])
                    - float(first["permits_per_space"]),
                    3,
                ),
            }
        )
    return rows


def _zsj_pressure_series(zsj_months: pd.DataFrame, limit: int = 6) -> list[dict]:
    if zsj_months.empty:
        return []
    latest_date = zsj_months["date"].max()
    leaders = (
        zsj_months[zsj_months["date"] == latest_date]
        .sort_values("permits_per_space", ascending=False)
        .head(limit)["kod_zsj"]
        .tolist()
    )
    selected = zsj_months[zsj_months["kod_zsj"].isin(leaders)].copy()
    selected["label"] = selected["naz_zsj"].fillna(selected["kod_zsj"].astype(str))
    return [
        {
            "date": row.date.date().isoformat(),
            "code": str(row.kod_zsj),
            "name": str(row.label),
            "district": str(row.mestska_cast),
            "value": round(float(row.permits_per_space), 3),
        }
        for row in selected.sort_values(["label", "date"]).itertuples(index=False)
    ]


def _district_pressure(base: pd.DataFrame) -> pd.DataFrame:
    if base.empty:
        return base
    grouped = (
        base.groupby(["date", "mestska_cast"], as_index=False)[
            ["POP_CELKEM", "parkovacich_mist_v_zps"]
        ]
        .sum()
        .sort_values("date")
    )
    grouped = grouped[grouped["parkovacich_mist_v_zps"] > 0].copy()
    grouped["value"] = grouped["POP_CELKEM"] / grouped["parkovacich_mist_v_zps"]
    return grouped


def _forecast(
    district_pressure: pd.DataFrame,
    settings: ForecastSettings,
) -> list[dict]:
    rows: list[dict] = []
    if district_pressure.empty:
        return rows
    for district, group in district_pressure.groupby("mestska_cast"):
        group = group.sort_values("date")
        for row in group.itertuples(index=False):
            rows.append(
                {
                    "district": str(district),
                    "date": row.date.date().isoformat(),
                    "kind": "Skutečnost",
                    "value": round(float(row.value), 3),
                }
            )
        window = group.tail(settings.history_months)
        if len(window) < 2:
            continue
        x_values = np.arange(len(window))
        slope, intercept = np.polyfit(x_values, window["value"].to_numpy(), 1)
        residuals = window["value"].to_numpy() - (slope * x_values + intercept)
        spread = float(residuals.std(ddof=1)) if len(residuals) > 1 else 0
        future_dates = pd.date_range(
            group["date"].max() + pd.offsets.MonthEnd(1),
            periods=settings.horizon_months,
            freq="ME",
        )
        for index, future_date in enumerate(future_dates, start=len(window)):
            predicted = float(slope * index + intercept)
            rows.append(
                {
                    "district": str(district),
                    "date": future_date.date().isoformat(),
                    "kind": "Predikce",
                    "value": round(predicted, 3),
                    "lower": round(predicted - spread, 3),
                    "upper": round(predicted + spread, 3),
                }
            )
    return rows


def build_overview_analytics(
    df: pd.DataFrame,
    filters: ExplorerFilters,
    forecast_settings: ForecastSettings = ForecastSettings(),
) -> dict:
    scoped = _filter_rows(df, filters)
    numeric_columns = list(PERMIT_TYPES.values()) + list(PARKER_TYPES.values()) + [
        "POP_CELKEM",
        "parkovacich_mist_v_zps",
    ]
    base = _zone_month_values(scoped, numeric_columns)
    zsj_months = _zsj_months(base)
    district_pressure = _district_pressure(base)
    return {
        "zone_mix": _zone_mix(base),
        "parker_share_by_district": _parker_share_by_district(base),
        "spaces_by_zone": _spaces_by_zone(base),
        "permits_by_type": _long_time_series(base, PERMIT_TYPES),
        "parkers_by_type": _long_time_series(base, PARKER_TYPES),
        "zsj_changes": _zsj_changes(zsj_months),
        "zsj_pressure": _zsj_pressure_series(zsj_months),
        "forecast": _forecast(district_pressure, forecast_settings),
    }


def _spaces_by_zone(base: pd.DataFrame) -> list[dict]:
    if base.empty:
        return []
    grouped = (
        base.groupby(["date", "typ_zony"], as_index=False)["parkovacich_mist_v_zps"]
        .sum()
        .sort_values("date")
    )
    return [
        {
            "date": row.date.date().isoformat(),
            "series": str(row.typ_zony),
            "value": round(float(row.parkovacich_mist_v_zps)),
        }
        for row in grouped.itertuples(index=False)
    ]
