from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
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


@dataclass(frozen=True)
class VerifiedCapacityCorrection:
    code: str
    start_date: pd.Timestamp
    end_date: pd.Timestamp
    months: int
    original_zps: float
    corrected_zps: float
    original_total: float
    corrected_total: float
    zps_jump_ratio: float
    total_jump_ratio: float
    occupied_before: float
    occupied_during: float
    occupied_after: float
    confirmed_at: pd.Timestamp


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


def apply_verified_capacity_regime_cleanup(
    df: pd.DataFrame,
    *,
    code_col: str,
    date_col: str,
    zps_capacity_col: str,
    total_capacity_col: str,
    occupancy_col: str,
    occupancy_scale: float = 100.0,
    category_col: str | None = None,
    context_months: int = 3,
    max_temp_months: int = 24,
    reversion_rel_tolerance: float = 0.10,
    reversion_abs_tolerance: float = 3.0,
    min_jump_ratio: float = 0.50,
    min_jump_abs: float = 20.0,
    paired_ratio_tolerance: float = 0.06,
    cast_agreement_rel_tolerance: float = 0.01,
    cast_agreement_abs_tolerance: float = 1.0,
    occupied_rel_tolerance: float = 0.20,
    occupied_abs_tolerance: float = 3.0,
    as_of: pd.Timestamp | str | None = None,
) -> tuple[pd.DataFrame, list[VerifiedCapacityCorrection]]:
    """Correct only strongly corroborated temporary capacity denominator errors.

    A correction requires contiguous stable context on both sides, matching jumps
    in both capacity measures, agreement across all available time variants, a
    stable implied occupied-vehicle numerator, and one stable zone category
    across the full context. The numerator is preserved when occupancy is
    rebased onto corrected capacity.
    """
    required = {
        code_col,
        date_col,
        zps_capacity_col,
        total_capacity_col,
        occupancy_col,
    }
    if category_col is not None:
        required.add(category_col)
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(
            "Cannot verify capacity regimes; missing columns: "
            + ", ".join(sorted(missing))
        )

    result = df.copy()
    result[date_col] = pd.to_datetime(result[date_col])
    numeric_columns = [zps_capacity_col, total_capacity_col, occupancy_col]
    numeric = result[numeric_columns].apply(pd.to_numeric, errors="coerce")
    raw_occupied = (
        numeric[zps_capacity_col]
        * numeric[occupancy_col]
        / occupancy_scale
    )
    working = result[[code_col, date_col]].copy()
    working[numeric_columns] = numeric
    working["_occupied"] = raw_occupied
    if category_col is not None:
        working[category_col] = result[category_col]

    corrections: list[VerifiedCapacityCorrection] = []
    analysis_cutoff = pd.Timestamp(as_of) if as_of is not None else None
    for code, rows in working.groupby(code_col, sort=False, dropna=False):
        if analysis_cutoff is not None:
            rows = rows[rows[date_col] <= analysis_cutoff]
        monthly = (
            rows.groupby(date_col, as_index=True)[
                [zps_capacity_col, total_capacity_col, "_occupied"]
            ]
            .median()
            .sort_index()
        )
        if len(monthly) < (context_months * 2 + 1):
            continue

        zps_series = monthly[zps_capacity_col].dropna()
        regimes = _build_regimes(zps_series)
        for regime_index in range(1, len(regimes) - 1):
            start_idx, end_idx, regime_zps = regimes[regime_index]
            start_date = zps_series.index[start_idx]
            end_date = zps_series.index[end_idx]
            expected_regime_index = pd.date_range(start_date, end_date, freq="ME")
            months = len(expected_regime_index)
            if months > max_temp_months or months != end_idx - start_idx + 1:
                continue

            before_index = pd.date_range(
                start_date - pd.offsets.MonthEnd(context_months),
                periods=context_months,
                freq="ME",
            )
            after_index = pd.date_range(
                end_date + pd.offsets.MonthEnd(1),
                periods=context_months,
                freq="ME",
            )
            required_index = before_index.append(expected_regime_index).append(
                after_index
            )
            window = monthly.reindex(required_index)
            if window[[zps_capacity_col, total_capacity_col, "_occupied"]].isna().any().any():
                continue

            before = window.loc[before_index]
            during = window.loc[expected_regime_index]
            after = window.loc[after_index]
            before_zps = float(before[zps_capacity_col].median())
            after_zps = float(after[zps_capacity_col].median())
            baseline_zps = float(np.median([before_zps, after_zps]))
            zps_tolerance = max(
                reversion_abs_tolerance,
                abs(baseline_zps) * reversion_rel_tolerance,
            )
            if (
                _spread(before[zps_capacity_col]) > zps_tolerance
                or _spread(after[zps_capacity_col]) > zps_tolerance
                or abs(before_zps - after_zps) > zps_tolerance
            ):
                continue
            zps_jump = regime_zps - baseline_zps
            zps_jump_abs = abs(zps_jump)
            zps_jump_ratio = zps_jump / max(abs(baseline_zps), 1.0)
            if zps_jump_abs < min_jump_abs or abs(zps_jump_ratio) < min_jump_ratio:
                continue

            before_total = float(before[total_capacity_col].median())
            after_total = float(after[total_capacity_col].median())
            baseline_total = float(np.median([before_total, after_total]))
            regime_total = float(during[total_capacity_col].median())
            total_tolerance = max(
                reversion_abs_tolerance,
                abs(baseline_total) * reversion_rel_tolerance,
            )
            if (
                _spread(before[total_capacity_col]) > total_tolerance
                or _spread(during[total_capacity_col]) > total_tolerance
                or _spread(after[total_capacity_col]) > total_tolerance
                or abs(before_total - after_total) > total_tolerance
            ):
                continue
            total_jump_ratio = (regime_total - baseline_total) / max(
                abs(baseline_total), 1.0
            )
            zps_factor = regime_zps / max(abs(baseline_zps), 1.0)
            total_factor = regime_total / max(abs(baseline_total), 1.0)
            paired_factor_difference = abs(zps_factor - total_factor) / max(
                abs(zps_factor), abs(total_factor), 1.0
            )
            if paired_factor_difference > paired_ratio_tolerance:
                continue

            relevant_rows = rows[rows[date_col].isin(required_index)]
            if not _variants_agree(
                relevant_rows,
                date_col=date_col,
                value_columns=[zps_capacity_col, total_capacity_col],
                relative_tolerance=cast_agreement_rel_tolerance,
                absolute_tolerance=cast_agreement_abs_tolerance,
            ):
                continue
            if category_col is not None:
                categories = relevant_rows[category_col].dropna()
                if categories.nunique() > 1:
                    continue

            occupied_before = float(before["_occupied"].median())
            occupied_during = float(during["_occupied"].median())
            occupied_after = float(after["_occupied"].median())
            occupied_context = pd.concat(
                [before["_occupied"], after["_occupied"]], ignore_index=True
            )
            occupied_baseline = float(occupied_context.median())
            occupied_mad = float(
                np.median(np.abs(occupied_context - occupied_baseline))
            )
            occupied_tolerance = max(
                occupied_abs_tolerance,
                abs(occupied_baseline) * occupied_rel_tolerance,
                3 * 1.4826 * occupied_mad,
            )
            if abs(occupied_during - occupied_baseline) > occupied_tolerance:
                continue

            corrected_zps = float(round(baseline_zps))
            corrected_total = float(round(baseline_total))
            regime_mask = (result[code_col] == code) & result[date_col].isin(
                expected_regime_index
            )
            rebased_occupancy = (
                raw_occupied.loc[regime_mask]
                .div(corrected_zps)
                .mul(occupancy_scale)
            )
            if (
                rebased_occupancy.isna().any()
                or (rebased_occupancy < 0).any()
                or (rebased_occupancy > occupancy_scale).any()
            ):
                continue

            result.loc[regime_mask, zps_capacity_col] = corrected_zps
            result.loc[regime_mask, total_capacity_col] = corrected_total
            result.loc[regime_mask, occupancy_col] = rebased_occupancy
            corrections.append(
                VerifiedCapacityCorrection(
                    code=str(code),
                    start_date=pd.Timestamp(start_date),
                    end_date=pd.Timestamp(end_date),
                    months=months,
                    original_zps=float(regime_zps),
                    corrected_zps=corrected_zps,
                    original_total=regime_total,
                    corrected_total=corrected_total,
                    zps_jump_ratio=round(float(zps_jump_ratio), 6),
                    total_jump_ratio=round(float(total_jump_ratio), 6),
                    occupied_before=round(occupied_before, 6),
                    occupied_during=round(occupied_during, 6),
                    occupied_after=round(occupied_after, 6),
                    confirmed_at=pd.Timestamp(after_index[-1]),
                )
            )

    return result, corrections


def _spread(series: pd.Series) -> float:
    return float(series.max() - series.min())


def _variants_agree(
    rows: pd.DataFrame,
    *,
    date_col: str,
    value_columns: Iterable[str],
    relative_tolerance: float,
    absolute_tolerance: float,
) -> bool:
    for column in value_columns:
        grouped = rows.groupby(date_col)[column]
        spread = grouped.max() - grouped.min()
        median = grouped.median().abs()
        tolerance = np.maximum(absolute_tolerance, median * relative_tolerance)
        if (spread > tolerance).any():
            return False
    return True


CONFIRMED_CORRECTION_COLUMNS = [
    "kod_useku",
    "od",
    "do",
    "puvodni_mist_v_zps",
    "mist_v_zps",
    "mist_celkem",
    "duvod",
]


def load_confirmed_capacity_corrections(path: Path | str) -> pd.DataFrame:
    """Read the hand-reviewed capacity corrections; months are inclusive."""
    corrections = pd.read_csv(path, dtype={"kod_useku": str, "od": str, "do": str})
    missing = set(CONFIRMED_CORRECTION_COLUMNS).difference(corrections.columns)
    if missing:
        raise ValueError(
            "Confirmed capacity corrections lack columns: " + ", ".join(sorted(missing))
        )
    corrections["start_date"] = pd.to_datetime(corrections["od"] + "-01") + pd.offsets.MonthEnd(0)
    corrections["end_date"] = pd.to_datetime(corrections["do"] + "-01") + pd.offsets.MonthEnd(0)
    if (corrections["end_date"] < corrections["start_date"]).any():
        raise ValueError("Confirmed capacity correction ends before it starts")
    numeric = ["puvodni_mist_v_zps", "mist_v_zps", "mist_celkem"]
    if corrections[numeric].isna().any().any() or (corrections[numeric] <= 0).any().any():
        raise ValueError("Confirmed capacity corrections need positive capacities")
    return corrections


def apply_confirmed_capacity_corrections(
    df: pd.DataFrame,
    corrections: pd.DataFrame,
    *,
    code_col: str,
    date_col: str,
    zps_capacity_col: str,
    total_capacity_col: str,
    occupancy_col: str,
    match_rel_tolerance: float = 0.10,
    match_abs_tolerance: float = 3.0,
) -> tuple[pd.DataFrame, list[dict]]:
    """Replace capacities that a person confirmed to be wrong in the source.

    Only rows that still carry the reviewed wrong value are changed, so a
    correction stops applying once the source is fixed. Occupancy is rebased so
    that the number of parked cars stays what the source reported.
    """
    result = df.copy()
    dates = pd.to_datetime(result[date_col])
    zps = pd.to_numeric(result[zps_capacity_col], errors="coerce")
    occupancy = pd.to_numeric(result[occupancy_col], errors="coerce")
    # Rebased occupancy is fractional; capacities keep the type the source has.
    result[occupancy_col] = result[occupancy_col].astype(float)
    records = []
    for correction in corrections.itertuples():
        in_period = (
            (result[code_col] == correction.kod_useku)
            & (dates >= correction.start_date)
            & (dates <= correction.end_date)
        )
        tolerance = max(
            match_abs_tolerance, correction.puvodni_mist_v_zps * match_rel_tolerance
        )
        matches = in_period & ((zps - correction.puvodni_mist_v_zps).abs() <= tolerance)
        result.loc[matches, occupancy_col] = (
            occupancy[matches] * zps[matches] / correction.mist_v_zps
        )
        for column, value in (
            (zps_capacity_col, correction.mist_v_zps),
            (total_capacity_col, correction.mist_celkem),
        ):
            result.loc[matches, column] = result[column].dtype.type(value)
        records.append(
            {
                "code": correction.kod_useku,
                "start_date": correction.start_date.date().isoformat(),
                "end_date": correction.end_date.date().isoformat(),
                "original_zps": float(correction.puvodni_mist_v_zps),
                "corrected_zps": float(correction.mist_v_zps),
                "corrected_total": float(correction.mist_celkem),
                "reason": correction.duvod,
                "rows_corrected": int(matches.sum()),
                # Rows of the period that no longer carry the reviewed wrong value.
                "rows_left_unchanged": int((in_period & ~matches).sum()),
            }
        )
    return result, records
