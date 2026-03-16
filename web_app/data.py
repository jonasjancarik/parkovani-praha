from typing import Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from config import PARKING_PATH
from src.parking_cleanup import apply_temporary_capacity_regime_cleanup


def load_parking_data() -> pd.DataFrame:
    df = pd.read_csv(PARKING_PATH)
    df["date"] = pd.to_datetime(df["date"])
    df = apply_temporary_capacity_regime_cleanup(
        df,
        code_col="kod_useku",
        date_col="date",
        capacity_cols=["parkovacich_mist_v_zps", "parkovacich_mist_celkem"],
    )
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    return df


def apply_filters(
    df: pd.DataFrame,
    date_range: Tuple[pd.Timestamp, pd.Timestamp],
    cast_dne: List[str],
    mestska_cast: Optional[str],
    naz_zsj: Optional[str],
    typ_zony: List[str],
) -> pd.DataFrame:
    start_date, end_date = date_range
    mask = (df["date"] >= start_date) & (df["date"] <= end_date)
    if cast_dne:
        mask &= df["cast_dne"].isin(cast_dne)
    if mestska_cast and mestska_cast != "All":
        mask &= df["mestska_cast"] == mestska_cast
    if naz_zsj and naz_zsj != "All":
        mask &= df["naz_zsj"] == naz_zsj
    if typ_zony:
        mask &= df["typ_zony"].isin(typ_zony)
    return df.loc[mask]


def permits_base(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "kod_useku",
        "kod_zsj",
        "naz_zsj",
        "mestska_cast",
        "date",
        "POP_CELKEM",
        "parkovacich_mist_v_zps",
    ]
    base = df[cols].drop_duplicates(subset=["kod_useku", "date"])
    return base


def zone_rows_for_cast_dne(
    df: pd.DataFrame,
    zone_codes: Sequence[str],
    cast_dne: Optional[str],
) -> pd.DataFrame:
    scoped = df[df["kod_useku"].isin(zone_codes)].copy()
    if scoped.empty:
        return scoped

    if cast_dne:
        scoped["_cast_rank"] = (scoped["cast_dne"] != cast_dne).astype(int)
        sort_cols = ["kod_useku", "date", "_cast_rank", "cast_dne"]
    else:
        sort_cols = ["kod_useku", "date", "cast_dne"]

    scoped = scoped.sort_values(sort_cols).drop_duplicates(
        subset=["kod_useku", "date"],
        keep="first",
    )
    return scoped.drop(columns="_cast_rank", errors="ignore")


def zone_capacity_history(
    df: pd.DataFrame,
    zone_codes: Sequence[str],
    cast_dne: Optional[str],
    value_col: str = "parkovacich_mist_v_zps",
    gap_rel_tolerance: float = 0.10,
    gap_abs_tolerance: float = 3.0,
) -> pd.DataFrame:
    scoped = zone_rows_for_cast_dne(df, zone_codes, cast_dne)
    if scoped.empty:
        return scoped

    filled_groups = []
    month_end = pd.offsets.MonthEnd()
    meta_cols = [col for col in scoped.columns if col not in {"date", value_col}]

    for zone_code, group in scoped.groupby("kod_useku"):
        group = (
            group.sort_values("date")
            .drop_duplicates(subset=["date"], keep="last")
            .set_index("date")
        )
        full_index = pd.date_range(group.index.min(), group.index.max(), freq=month_end)
        group = group.reindex(full_index)
        group.index.name = "date"
        group["kod_useku"] = zone_code

        for col in meta_cols:
            if col == "kod_useku":
                continue
            group[col] = group[col].ffill().bfill()

        prev_vals = group[value_col].ffill()
        next_vals = group[value_col].bfill()
        tolerance = np.maximum(
            gap_abs_tolerance,
            np.maximum(prev_vals.abs(), next_vals.abs()) * gap_rel_tolerance,
        )
        gap_fill_mask = (
            group[value_col].isna()
            & prev_vals.notna()
            & next_vals.notna()
            & ((prev_vals - next_vals).abs() <= tolerance)
        )
        if gap_fill_mask.any():
            group.loc[gap_fill_mask, value_col] = (
                (prev_vals + next_vals) / 2
            ).round()[gap_fill_mask]

        group = group[group[value_col].notna()].reset_index()
        filled_groups.append(group)

    filled = pd.concat(filled_groups, ignore_index=True)
    coverage = filled.groupby("kod_useku")["date"].agg(["min", "max"])
    if coverage.empty:
        return filled

    common_start = coverage["min"].max()
    common_end = coverage["max"].min()
    if common_start <= common_end:
        filled = filled[
            (filled["date"] >= common_start) & (filled["date"] <= common_end)
        ].copy()

    return filled


def radius_spaces_series(
    df: pd.DataFrame,
    zone_codes: Sequence[str],
    cast_dne: Optional[str],
) -> pd.DataFrame:
    scoped = zone_capacity_history(df, zone_codes, cast_dne)
    if scoped.empty:
        return scoped

    return (
        scoped.groupby("date")[["parkovacich_mist_v_zps"]]
        .sum()
        .reset_index()
        .sort_values("date")
    )


def radius_latest_snapshot(
    df: pd.DataFrame,
    zone_codes: Sequence[str],
    cast_dne: Optional[str],
) -> pd.DataFrame:
    scoped = zone_rows_for_cast_dne(df, zone_codes, cast_dne)
    if scoped.empty:
        return scoped

    cols = [
        "kod_useku",
        "naz_zsj",
        "mestska_cast",
        "typ_zony",
        "parkovacich_mist_v_zps",
    ]
    return (
        scoped.sort_values(["kod_useku", "date"])
        .drop_duplicates(subset=["kod_useku"], keep="last")
        .loc[:, cols]
        .copy()
    )


def zsj_pop_per_space_series(
    df: pd.DataFrame,
    date_range: Tuple[pd.Timestamp, pd.Timestamp],
    mestska_cast: Optional[str],
    naz_zsj: Optional[str],
) -> pd.DataFrame:
    start_date, end_date = date_range
    mask = (df["date"] >= start_date) & (df["date"] <= end_date)
    if mestska_cast and mestska_cast != "All":
        mask &= df["mestska_cast"] == mestska_cast
    if naz_zsj and naz_zsj != "All":
        mask &= df["naz_zsj"] == naz_zsj
    scoped = df.loc[mask]
    if scoped.empty:
        return scoped

    grouped = (
        scoped.groupby(["date", "kod_zsj", "naz_zsj"])[
            ["POP_CELKEM", "parkovacich_mist_v_zps"]
        ]
        .sum()
        .reset_index()
    )
    grouped = grouped[grouped["parkovacich_mist_v_zps"] > 0]
    grouped["pop_per_space"] = (
        grouped["POP_CELKEM"] / grouped["parkovacich_mist_v_zps"]
    )
    return grouped


def area_pop_per_space_series(
    df: pd.DataFrame,
    date_range: Tuple[pd.Timestamp, pd.Timestamp],
    mestska_cast: Optional[str],
    typ_zony: Optional[List[str]],
    origin_max: Optional[pd.Timestamp] = None,
) -> pd.DataFrame:
    start_date, end_date = date_range
    cols = [
        "kod_useku",
        "mestska_cast",
        "typ_zony",
        "date",
        "POP_CELKEM",
        "parkovacich_mist_v_zps",
    ]
    base = df[cols].drop_duplicates(subset=["kod_useku", "date"])
    mask = (base["date"] >= start_date) & (base["date"] <= end_date)
    if mestska_cast and mestska_cast != "All":
        mask &= base["mestska_cast"] == mestska_cast
    if typ_zony:
        mask &= base["typ_zony"].isin(typ_zony)
    scoped = base.loc[mask]
    if origin_max is not None and not scoped.empty:
        origin = scoped.groupby("kod_useku")["date"].min()
        allowed = origin[origin <= origin_max].index
        scoped = scoped[scoped["kod_useku"].isin(allowed)]
    if scoped.empty:
        return scoped
    grouped = (
        scoped.groupby(["date", "mestska_cast"])[
            ["POP_CELKEM", "parkovacich_mist_v_zps"]
        ]
        .sum()
        .reset_index()
    )
    grouped = grouped[grouped["parkovacich_mist_v_zps"] > 0]
    grouped["pop_per_space"] = (
        grouped["POP_CELKEM"] / grouped["parkovacich_mist_v_zps"]
    )
    return grouped


def build_date_range(
    df: pd.DataFrame,
    date_input: Tuple[object, object],
) -> Tuple[pd.Timestamp, pd.Timestamp]:
    if isinstance(date_input, tuple):
        start, end = date_input
    else:
        start = end = date_input
    return pd.Timestamp(start), pd.Timestamp(end)
