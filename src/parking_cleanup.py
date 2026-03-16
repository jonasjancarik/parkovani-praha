from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class TemporaryRegime:
    start_date: pd.Timestamp
    end_date: pd.Timestamp
    original_value: float
    replacement_value: float
    months: int


def _build_regimes(series: pd.Series) -> list[tuple[int, int, float]]:
    if series.empty:
        return []

    values = series.tolist()
    regimes: list[tuple[int, int, float]] = []
    start_idx = 0

    for idx in range(1, len(values) + 1):
        if idx == len(values) or values[idx] != values[start_idx]:
            regimes.append((start_idx, idx - 1, float(values[start_idx])))
            start_idx = idx

    return regimes


def find_temporary_capacity_regimes(
    series: pd.Series,
    *,
    max_temp_months: int = 24,
    reversion_rel_tolerance: float = 0.10,
    reversion_abs_tolerance: float = 3.0,
    min_jump_ratio: float = 0.50,
    min_jump_abs: float = 20.0,
) -> list[TemporaryRegime]:
    clean = series.dropna().sort_index()
    if len(clean) < 3:
        return []

    regimes = _build_regimes(clean)
    detected: list[TemporaryRegime] = []

    for idx in range(1, len(regimes) - 1):
        prev_start, prev_end, prev_value = regimes[idx - 1]
        curr_start, curr_end, curr_value = regimes[idx]
        next_start, next_end, next_value = regimes[idx + 1]

        baseline = float(np.median([prev_value, next_value]))
        baseline_tolerance = max(
            reversion_abs_tolerance,
            abs(baseline) * reversion_rel_tolerance,
        )
        if abs(prev_value - next_value) > baseline_tolerance:
            continue

        if curr_value <= baseline:
            continue

        jump_abs = curr_value - baseline
        jump_ratio = jump_abs / max(abs(baseline), 1.0)
        if jump_abs < min_jump_abs or jump_ratio < min_jump_ratio:
            continue

        months = curr_end - curr_start + 1
        if months > max_temp_months:
            continue

        replacement = float(round(baseline))
        detected.append(
            TemporaryRegime(
                start_date=clean.index[curr_start],
                end_date=clean.index[curr_end],
                original_value=curr_value,
                replacement_value=replacement,
                months=months,
            )
        )

    return detected


def apply_temporary_capacity_regime_cleanup(
    df: pd.DataFrame,
    *,
    code_col: str,
    date_col: str,
    capacity_cols: Iterable[str],
    max_temp_months: int = 24,
    reversion_rel_tolerance: float = 0.10,
    reversion_abs_tolerance: float = 3.0,
    min_jump_ratio: float = 0.50,
    min_jump_abs: float = 20.0,
) -> pd.DataFrame:
    df_out = df.copy()

    for column in capacity_cols:
        if column not in df_out.columns:
            continue

        monthly = (
            df_out[[code_col, date_col, column]]
            .dropna(subset=[column])
            .groupby([code_col, date_col])[column]
            .median()
            .reset_index()
            .sort_values([code_col, date_col])
        )

        if monthly.empty:
            continue

        for code, group in monthly.groupby(code_col):
            series = group.set_index(date_col)[column]
            regimes = find_temporary_capacity_regimes(
                series,
                max_temp_months=max_temp_months,
                reversion_rel_tolerance=reversion_rel_tolerance,
                reversion_abs_tolerance=reversion_abs_tolerance,
                min_jump_ratio=min_jump_ratio,
                min_jump_abs=min_jump_abs,
            )
            for regime in regimes:
                mask = (
                    (df_out[code_col] == code)
                    & (df_out[date_col] >= regime.start_date)
                    & (df_out[date_col] <= regime.end_date)
                )
                df_out.loc[mask, column] = regime.replacement_value

    return df_out
