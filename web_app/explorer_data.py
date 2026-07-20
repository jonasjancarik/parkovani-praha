from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Iterable

import pandas as pd


DEFAULT_ZONE_TYPES = ("MIX", "OST", "RES", "VIS")


@dataclass(frozen=True)
class ExplorerFilters:
    start: date | None = None
    end: date | None = None
    cast_dne: tuple[str, ...] = ()
    districts: tuple[str, ...] = ()
    zone_types: tuple[str, ...] = DEFAULT_ZONE_TYPES


def _string_values(values: Iterable[str] | None) -> tuple[str, ...]:
    return tuple(value for value in (values or ()) if value)


def explorer_options(df: pd.DataFrame) -> dict:
    return {
        "min_date": df["date"].min().date().isoformat(),
        "max_date": df["date"].max().date().isoformat(),
        "cast_dne": sorted(df["cast_dne"].dropna().unique().tolist()),
        "districts": sorted(df["mestska_cast"].dropna().unique().tolist()),
        "zone_types": sorted(df["typ_zony"].dropna().unique().tolist()),
    }


def _filter_rows(df: pd.DataFrame, filters: ExplorerFilters) -> pd.DataFrame:
    mask = pd.Series(True, index=df.index)
    if filters.start:
        mask &= df["date"] >= pd.Timestamp(filters.start)
    if filters.end:
        mask &= df["date"] <= pd.Timestamp(filters.end)
    if filters.cast_dne:
        mask &= df["cast_dne"].isin(filters.cast_dne)
    if filters.districts:
        mask &= df["mestska_cast"].isin(filters.districts)
    if filters.zone_types:
        mask &= df["typ_zony"].isin(filters.zone_types)
    return df.loc[mask]


def _unique_zone_months(scoped: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "kod_useku",
        "date",
        "mestska_cast",
        "typ_zony",
        "POP_CELKEM",
        "parkovacich_mist_v_zps",
    ]
    return (
        scoped.loc[:, columns]
        .sort_values(["kod_useku", "date"])
        .drop_duplicates(subset=["kod_useku", "date"], keep="first")
    )


def _safe_change(current: float, initial: float) -> float | None:
    if not initial:
        return None
    return (current - initial) / initial


def build_explorer_payload(
    df: pd.DataFrame,
    filters: ExplorerFilters,
    district_limit: int = 8,
) -> dict:
    scoped = _filter_rows(df, filters)
    options = explorer_options(df)
    if scoped.empty:
        return {
            "options": options,
            "filters": filters_to_dict(filters, options),
            "series": [],
            "districts": [],
            "summary": None,
        }

    base = _unique_zone_months(scoped)
    series = (
        base.groupby("date", as_index=False)[
            ["POP_CELKEM", "parkovacich_mist_v_zps"]
        ]
        .sum()
        .sort_values("date")
    )
    series = series[series["parkovacich_mist_v_zps"] > 0].copy()
    series["opravneni_na_misto"] = (
        series["POP_CELKEM"] / series["parkovacich_mist_v_zps"]
    )

    latest_date = base["date"].max()
    latest = base[base["date"] == latest_date]
    districts = (
        latest.groupby("mestska_cast", as_index=False)[
            ["POP_CELKEM", "parkovacich_mist_v_zps"]
        ]
        .sum()
    )
    districts = districts[districts["parkovacich_mist_v_zps"] > 0].copy()
    districts["opravneni_na_misto"] = (
        districts["POP_CELKEM"] / districts["parkovacich_mist_v_zps"]
    )
    districts = districts.sort_values("opravneni_na_misto", ascending=False).head(
        district_limit
    )

    first = series.iloc[0]
    current = series.iloc[-1]
    current_permits = float(current["POP_CELKEM"])
    current_spaces = float(current["parkovacich_mist_v_zps"])
    current_ratio = float(current["opravneni_na_misto"])

    return {
        "options": options,
        "filters": filters_to_dict(filters, options),
        "series": [
            {
                "date": row.date.date().isoformat(),
                "permits": round(float(row.POP_CELKEM)),
                "spaces": round(float(row.parkovacich_mist_v_zps)),
                "permits_per_space": round(float(row.opravneni_na_misto), 3),
            }
            for row in series.itertuples(index=False)
        ],
        "districts": [
            {
                "district": row.mestska_cast,
                "permits": round(float(row.POP_CELKEM)),
                "spaces": round(float(row.parkovacich_mist_v_zps)),
                "permits_per_space": round(float(row.opravneni_na_misto), 3),
            }
            for row in districts.itertuples(index=False)
        ],
        "summary": {
            "latest_date": current["date"].date().isoformat(),
            "permits": round(current_permits),
            "spaces": round(current_spaces),
            "permits_per_space": round(current_ratio, 3),
            "permits_change": _safe_change(
                current_permits, float(first["POP_CELKEM"])
            ),
            "spaces_change": _safe_change(
                current_spaces, float(first["parkovacich_mist_v_zps"])
            ),
            "ratio_change": _safe_change(
                current_ratio, float(first["opravneni_na_misto"])
            ),
        },
    }


def filters_to_dict(filters: ExplorerFilters, options: dict) -> dict:
    return {
        "start": (filters.start.isoformat() if filters.start else options["min_date"]),
        "end": (filters.end.isoformat() if filters.end else options["max_date"]),
        "cast_dne": list(filters.cast_dne),
        "districts": list(filters.districts),
        "zone_types": list(filters.zone_types),
    }


def make_filters(
    start: date | None,
    end: date | None,
    cast_dne: list[str] | None,
    districts: list[str] | None,
    zone_types: list[str] | None,
) -> ExplorerFilters:
    return ExplorerFilters(
        start=start,
        end=end,
        cast_dne=_string_values(cast_dne),
        districts=_string_values(districts),
        zone_types=_string_values(zone_types) or DEFAULT_ZONE_TYPES,
    )
