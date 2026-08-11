from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Iterable

import pandas as pd


DEFAULT_ZONE_TYPES = ("MIX", "OST", "RES", "VIS")
INVARIANT_CAST_PRIORITY = {
    "den": 0,
    "Po-Pá": 1,
    "Po-Pá (MPD)": 2,
    "So-Ne": 3,
    "noc": 4,
}


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


def _filter_rows(
    df: pd.DataFrame,
    filters: ExplorerFilters,
    *,
    apply_cast_dne: bool = True,
) -> pd.DataFrame:
    mask = pd.Series(True, index=df.index)
    if filters.start:
        mask &= df["date"] >= pd.Timestamp(filters.start)
    if filters.end:
        mask &= df["date"] <= pd.Timestamp(filters.end)
    if apply_cast_dne and filters.cast_dne:
        mask &= df["cast_dne"].isin(filters.cast_dne)
    if filters.districts:
        mask &= df["mestska_cast"].isin(filters.districts)
    if filters.zone_types:
        mask &= df["typ_zony"].isin(filters.zone_types)
    return df.loc[mask]


def canonical_zone_month_rows(scoped: pd.DataFrame) -> pd.DataFrame:
    """Select one stable row for measures that do not vary by time of day.

    Historical exports do not contain every ``cast_dne`` category for every zone.
    Filtering those rows before calculating permits or capacity creates artificial
    jumps when source coverage changes. Prefer the canonical daytime row when it
    exists and fall back deterministically to any available observation.
    """
    if scoped.empty:
        return scoped.copy()
    ranked = scoped.assign(
        _cast_rank=scoped["cast_dne"].map(INVARIANT_CAST_PRIORITY).fillna(99)
    )
    return (
        ranked.sort_values(["kod_useku", "date", "_cast_rank", "cast_dne"])
        .drop_duplicates(subset=["kod_useku", "date"], keep="first")
        .drop(columns="_cast_rank")
    )


def _unique_zone_months(scoped: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "kod_useku",
        "date",
        "mestska_cast",
        "typ_zony",
        "POP_CELKEM",
        "parkovacich_mist_v_zps",
    ]
    return canonical_zone_month_rows(scoped).loc[:, columns]


def _safe_change(current: float, initial: float) -> float | None:
    if pd.isna(current) or pd.isna(initial) or not initial:
        return None
    return (current - initial) / initial


def _rounded_or_none(value: object, digits: int = 0) -> int | float | None:
    if pd.isna(value):
        return None
    number = float(value)
    return round(number) if digits == 0 else round(number, digits)


def build_explorer_payload(
    df: pd.DataFrame,
    filters: ExplorerFilters,
    district_limit: int = 5,
) -> dict:
    scoped = _filter_rows(df, filters, apply_cast_dne=False)
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
        .sum(min_count=1)
        .sort_values("date")
    )
    series = series[series["parkovacich_mist_v_zps"] > 0].copy()
    series["POP_CELKEM"] = pd.to_numeric(series["POP_CELKEM"], errors="coerce")
    series["opravneni_na_misto"] = series["POP_CELKEM"].div(
        series["parkovacich_mist_v_zps"]
    )

    latest_date = base["date"].max()
    latest = base[base["date"] == latest_date]
    districts = (
        latest.groupby("mestska_cast", as_index=False)[
            ["POP_CELKEM", "parkovacich_mist_v_zps"]
        ]
        .sum(min_count=1)
    )
    districts = districts[districts["parkovacich_mist_v_zps"] > 0].copy()
    districts["opravneni_na_misto"] = (
        districts["POP_CELKEM"] / districts["parkovacich_mist_v_zps"]
    )
    districts = districts.sort_values("opravneni_na_misto", ascending=False).head(
        district_limit
    )

    first = series.iloc[0]
    permit_series = series.loc[series["POP_CELKEM"].notna() & (series["POP_CELKEM"] > 0)]
    ratio_series = series.loc[
        series["opravneni_na_misto"].notna()
        & (series["opravneni_na_misto"] > 0)
    ]
    current = series.iloc[-1]
    current_permits = current["POP_CELKEM"]
    current_spaces = float(current["parkovacich_mist_v_zps"])
    current_ratio = current["opravneni_na_misto"]
    first_permits = permit_series.iloc[0] if not permit_series.empty else None
    first_ratio = ratio_series.iloc[0] if not ratio_series.empty else None

    return {
        "options": options,
        "filters": filters_to_dict(filters, options),
        "series": [
            {
                "date": row.date.date().isoformat(),
                "permits": _rounded_or_none(row.POP_CELKEM),
                "spaces": round(float(row.parkovacich_mist_v_zps)),
                "permits_per_space": _rounded_or_none(row.opravneni_na_misto, 3),
            }
            for row in series.itertuples(index=False)
        ],
        "districts": [
            {
                "district": row.mestska_cast,
                "permits": _rounded_or_none(row.POP_CELKEM),
                "spaces": round(float(row.parkovacich_mist_v_zps)),
                "permits_per_space": _rounded_or_none(row.opravneni_na_misto, 3),
            }
            for row in districts.itertuples(index=False)
        ],
        "summary": {
            "latest_date": current["date"].date().isoformat(),
            "permits": _rounded_or_none(current_permits),
            "spaces": round(current_spaces),
            "permits_per_space": _rounded_or_none(current_ratio, 3),
            "permits_change": _safe_change(
                current_permits,
                first_permits["POP_CELKEM"] if first_permits is not None else float("nan"),
            ) if first_permits is not None else None,
            "spaces_change": _safe_change(
                current_spaces, float(first["parkovacich_mist_v_zps"])
            ),
            "ratio_change": _safe_change(
                current_ratio,
                first_ratio["opravneni_na_misto"] if first_ratio is not None else float("nan"),
            ) if first_ratio is not None else None,
        },
        "coverage": {
            "permits_available": not permit_series.empty,
            "permits_start": (
                permit_series.iloc[0]["date"].date().isoformat()
                if not permit_series.empty else None
            ),
            "permits_end": (
                permit_series.iloc[-1]["date"].date().isoformat()
                if not permit_series.empty else None
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
