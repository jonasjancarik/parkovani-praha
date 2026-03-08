from typing import Iterable, List, Optional, Tuple

import pandas as pd

from config import PARKING_PATH


def load_parking_data() -> pd.DataFrame:
    df = pd.read_csv(PARKING_PATH)
    df["date"] = pd.to_datetime(df["date"])
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
