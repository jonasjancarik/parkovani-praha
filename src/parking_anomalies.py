from __future__ import annotations

from dataclasses import asdict, dataclass
import re
import warnings
from typing import Iterable, Sequence

import numpy as np
import pandas as pd


RAW_OCCUPANCY_COLUMNS = ("Obs", "Resp")
RAW_COMPOSITION_COLUMNS = (
    "R1",
    "R2",
    "R3",
    "R4",
    "R",
    "V",
    "A",
    "P",
    "C",
    "E",
    "O",
    "S",
    "VI",
    "NR",
    "FR",
)


@dataclass(frozen=True)
class AnomalyDetectionConfig:
    """Conservative settings for flagging, not rewriting, monthly observations."""

    window_months: int = 12
    min_observations: int = 12
    sigma_threshold: float = 6.0
    minimum_absolute_deviation: float = 20.0

    def __post_init__(self) -> None:
        if self.window_months < 3:
            raise ValueError("window_months must be at least 3")
        if not 2 <= self.min_observations <= self.window_months:
            raise ValueError("min_observations must be between 2 and window_months")


@dataclass(frozen=True)
class ObservationNormalizationReport:
    input_rows: int
    output_rows: int
    excluded_cross_district_rows: int
    exact_duplicate_rows: int


def normalize_parking_observations(
    df: pd.DataFrame,
    *,
    code_column: str = "CODE",
    date_column: str = "date",
    cast_column: str = "cast_dne",
    filename_column: str = "filename",
) -> tuple[pd.DataFrame, ObservationNormalizationReport]:
    """Enforce one trustworthy source row per zone, time variant, and month."""
    key = [code_column, date_column, cast_column]
    required = {*key, filename_column}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(
            "Cannot normalize parking observations; missing columns: "
            + ", ".join(sorted(missing))
        )

    work = df.copy()
    work[code_column] = work[code_column].map(
        lambda value: value.upper() if isinstance(value, str) else value
    )
    input_rows = len(work)
    code_district = work[code_column].map(_district_number)
    file_district = work[filename_column].map(_district_number)
    district_match = code_district.notna() & code_district.eq(file_district)
    group_has_match = district_match.groupby(
        [work[column] for column in key], dropna=False
    ).transform("any")
    cross_district = group_has_match & ~district_match
    work = work.loc[~cross_district].copy()

    before_exact_dedup = len(work)
    work = work.drop_duplicates()
    exact_duplicate_rows = before_exact_dedup - len(work)

    conflicting = work.duplicated(key, keep=False)
    if conflicting.any():
        conflicts = (
            work.loc[conflicting, key]
            .drop_duplicates()
            .sort_values(key)
            .head(20)
            .to_dict(orient="records")
        )
        raise ValueError(
            "Conflicting parking observations remain at zone × time variant × "
            f"month grain: {conflicts}"
        )

    report = ObservationNormalizationReport(
        input_rows=input_rows,
        output_rows=len(work),
        excluded_cross_district_rows=int(cross_district.sum()),
        exact_duplicate_rows=exact_duplicate_rows,
    )
    return work, report


def recompute_derived_parking_measures(df: pd.DataFrame) -> pd.DataFrame:
    """Rebuild additive fields after any primitive parking measure changes."""
    required = {
        "navstevnici_platici",
        "navstevnici_neplatici",
        "prenosna",
        "rezidentska",
        "vlastnicka",
        "abonentska",
        "carsharing",
        "ekologicka",
        "ostatni",
        "socialni",
        "volna_mista",
    }
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(
            "Cannot recompute derived parking measures; missing columns: "
            + ", ".join(sorted(missing))
        )

    result = df.copy()
    result["navstevnici"] = (
        result["navstevnici_platici"]
        + result["navstevnici_neplatici"]
        + result["prenosna"]
    )
    result["soucet_vsech_typu"] = (
        result["rezidentska"]
        + result["vlastnicka"]
        + result["abonentska"]
        + result["carsharing"]
        + result["ekologicka"]
        + result["ostatni"]
        + result["socialni"]
        + result["navstevnici"]
        + result["volna_mista"]
    )
    return result


def validate_processed_parking_data(
    df: pd.DataFrame,
    *,
    tolerance: float = 1e-8,
) -> dict[str, int]:
    """Fail on structural corruption and report ambiguous source contradictions."""
    logical_key = ["kod_useku", "date", "cast_dne"]
    duplicate_rows = int(df.duplicated(logical_key, keep=False).sum())
    invalid_capacity_rows = int(
        (
            pd.to_numeric(df["parkovacich_mist_v_zps"], errors="coerce")
            > pd.to_numeric(df["parkovacich_mist_celkem"], errors="coerce")
        ).sum()
    )
    capacity_conflicts = (
        df.groupby(["kod_useku", "date"])[
            ["parkovacich_mist_v_zps", "parkovacich_mist_celkem"]
        ]
        .nunique(dropna=True)
        .gt(1)
        .any(axis=1)
    )
    capacity_conflict_groups = int(capacity_conflicts.sum())

    expected_visitors = (
        df["navstevnici_platici"]
        + df["navstevnici_neplatici"]
        + df["prenosna"]
    )
    expected_total = (
        df["rezidentska"]
        + df["vlastnicka"]
        + df["abonentska"]
        + df["prenosna"]
        + df["carsharing"]
        + df["ekologicka"]
        + df["ostatni"]
        + df["socialni"]
        + df["navstevnici_platici"]
        + df["navstevnici_neplatici"]
        + df["volna_mista"]
    )
    visitor_mismatches = int(
        (~np.isclose(
            df["navstevnici"],
            expected_visitors,
            atol=tolerance,
            rtol=0,
            equal_nan=True,
        )).sum()
    )
    total_mismatches = int(
        (~np.isclose(
            df["soucet_vsech_typu"],
            expected_total,
            atol=tolerance,
            rtol=0,
            equal_nan=True,
        )).sum()
    )

    summary = {
        "duplicate_logical_rows": duplicate_rows,
        "capacity_conflict_groups": capacity_conflict_groups,
        "zps_above_total_rows": invalid_capacity_rows,
        "visitor_identity_mismatches": visitor_mismatches,
        "total_identity_mismatches": total_mismatches,
    }
    reported_only = {"zps_above_total_rows"}
    failed = {
        name: count
        for name, count in summary.items()
        if count and name not in reported_only
    }
    if failed:
        raise ValueError(f"Processed parking data failed quality gates: {failed}")
    return summary


def detect_monthly_anomaly_candidates(
    df: pd.DataFrame,
    *,
    value_columns: Iterable[str] = RAW_COMPOSITION_COLUMNS,
    group_columns: Sequence[str] = ("CODE", "cast_dne"),
    date_column: str = "date",
    config: AnomalyDetectionConfig = AnomalyDetectionConfig(),
) -> pd.DataFrame:
    """Flag observations against the preceding calendar-aware Hampel window.

    The detector deliberately does not mutate values. Observations must already
    be unique at the logical grain; normalization is kept separate so conflicts
    cannot be hidden inside a median.
    """
    columns = [column for column in value_columns if column in df.columns]
    required = {*group_columns, date_column, *columns}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(
            "Cannot detect parking anomalies; missing columns: "
            + ", ".join(sorted(missing))
        )
    if not columns or df.empty:
        return _empty_candidates(group_columns, date_column)

    work = df[[*group_columns, date_column, *columns]].copy()
    work[date_column] = pd.to_datetime(work[date_column]).dt.to_period("M")
    for column in columns:
        work[column] = pd.to_numeric(work[column], errors="coerce")

    logical_key = [*group_columns, date_column]
    duplicates = work.duplicated(logical_key, keep=False)
    if duplicates.any():
        examples = (
            work.loc[duplicates, logical_key]
            .drop_duplicates()
            .head(20)
            .astype(str)
            .to_dict(orient="records")
        )
        raise ValueError(
            "Anomaly detection requires unique zone × time variant × month rows: "
            f"{examples}"
        )

    monthly = work.sort_values([*group_columns, date_column])

    candidate_frames: list[pd.DataFrame] = []
    grouped = monthly.groupby(list(group_columns), dropna=False, sort=False)
    for keys, group in grouped:
        key_values = keys if isinstance(keys, tuple) else (keys,)
        group = group.set_index(date_column).sort_index()
        full_index = pd.period_range(group.index.min(), group.index.max(), freq="M")
        values = group[columns].reindex(full_index).to_numpy(dtype=float)
        padded = np.pad(
            values,
            ((config.window_months, 0), (0, 0)),
            mode="constant",
            constant_values=np.nan,
        )
        windows = np.lib.stride_tricks.sliding_window_view(
            padded,
            window_shape=config.window_months,
            axis=0,
        )[:-1]
        observation_counts = np.count_nonzero(~np.isnan(windows), axis=-1)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=RuntimeWarning)
            baselines = np.nanmedian(windows, axis=-1)
            median_abs_deviation = np.nanmedian(
                np.abs(windows - baselines[..., np.newaxis]),
                axis=-1,
            )

        absolute_deviation = np.abs(values - baselines)
        robust_sigma = 1.4826 * median_abs_deviation
        threshold = np.fmax(
            config.sigma_threshold * robust_sigma,
            config.minimum_absolute_deviation,
        )
        candidate_mask = (
            (observation_counts >= config.min_observations)
            & ~np.isnan(values)
            & ~np.isnan(baselines)
            & (absolute_deviation > threshold)
        )
        row_indexes, column_indexes = np.nonzero(candidate_mask)
        if not len(row_indexes):
            continue

        frame = pd.DataFrame(
            {
                date_column: full_index[row_indexes].to_timestamp("M"),
                "column": np.asarray(columns, dtype=object)[column_indexes],
                "observed": values[row_indexes, column_indexes],
                "baseline": baselines[row_indexes, column_indexes],
                "absolute_deviation": absolute_deviation[
                    row_indexes, column_indexes
                ],
                "robust_sigma": robust_sigma[row_indexes, column_indexes],
                "threshold": threshold[row_indexes, column_indexes],
                "local_observations": observation_counts[
                    row_indexes, column_indexes
                ],
            }
        )
        for column, value in zip(group_columns, key_values, strict=True):
            frame[column] = value
        candidate_frames.append(frame)

    if not candidate_frames:
        return _empty_candidates(group_columns, date_column)

    candidates = pd.concat(candidate_frames, ignore_index=True)
    denominator = candidates["baseline"].abs().replace(0, np.nan)
    candidates["relative_deviation"] = candidates["absolute_deviation"].div(
        denominator
    )
    return candidates[
        [
            *group_columns,
            date_column,
            "column",
            "observed",
            "baseline",
            "absolute_deviation",
            "relative_deviation",
            "robust_sigma",
            "threshold",
            "local_observations",
        ]
    ].sort_values("absolute_deviation", ascending=False, ignore_index=True)


def summarize_value_changes(
    before: pd.DataFrame,
    after: pd.DataFrame,
    *,
    value_columns: Iterable[str],
    key_columns: Sequence[str],
    example_limit: int = 20,
) -> dict:
    """Summarize deterministic corrections applied to aligned dataframes."""
    if not before.index.equals(after.index):
        raise ValueError("before and after dataframes must have aligned indexes")

    summary: dict[str, dict] = {}
    for column in value_columns:
        original = pd.to_numeric(before[column], errors="coerce")
        corrected = pd.to_numeric(after[column], errors="coerce")
        difference = corrected - original
        changed = difference.abs().gt(1e-9) & ~(original.isna() & corrected.isna())
        changed_rows = after.loc[changed, [*key_columns, column]].copy()
        changed_rows["original"] = original.loc[changed]
        changed_rows["corrected"] = corrected.loc[changed]
        changed_rows["absolute_change"] = difference.loc[changed].abs()
        changed_rows = changed_rows.sort_values(
            "absolute_change", ascending=False
        ).head(example_limit)
        examples = _records_for_json(changed_rows.drop(columns=column))
        unique_keys = (
            after.loc[changed, list(key_columns)].drop_duplicates().shape[0]
            if changed.any()
            else 0
        )
        summary[column] = {
            "changed_rows": int(changed.sum()),
            "changed_keys": int(unique_keys),
            "sum_absolute_change": round(float(difference.loc[changed].abs().sum()), 6),
            "max_absolute_change": (
                round(float(difference.loc[changed].abs().max()), 6)
                if changed.any()
                else 0.0
            ),
            "examples": examples,
        }
    return summary


def build_anomaly_audit(
    df: pd.DataFrame,
    candidates: pd.DataFrame,
    *,
    configs: dict[str, AnomalyDetectionConfig],
    capacity_corrections: dict,
    group_columns: Sequence[str] = ("kod_useku", "cast_dne"),
    date_column: str = "date",
    example_limit: int = 100,
) -> dict:
    logical_key = [*group_columns, date_column]
    duplicates = df.duplicated(logical_key, keep=False)
    duplicate_rows = df.loc[duplicates, logical_key]
    duplicate_groups = duplicate_rows.drop_duplicates().shape[0]

    by_column = {}
    if not candidates.empty:
        for column, group in candidates.groupby("column", sort=True):
            finite_relative = group["relative_deviation"].replace(
                [np.inf, -np.inf], np.nan
            )
            by_column[column] = {
                "candidates": int(len(group)),
                "median_absolute_deviation": round(
                    float(group["absolute_deviation"].median()), 6
                ),
                "max_absolute_deviation": round(
                    float(group["absolute_deviation"].max()), 6
                ),
                "median_relative_deviation": (
                    round(float(finite_relative.median()), 6)
                    if finite_relative.notna().any()
                    else None
                ),
            }

    examples = _records_for_json(candidates.head(example_limit))
    return {
        "algorithm": {
            "name": "trailing calendar-aware Hampel candidate detector",
            "mode": "report-only",
            "grouping": [*group_columns, "calendar month"],
            "configs": {
                name: asdict(config) for name, config in configs.items()
            },
        },
        "rows": int(len(df)),
        "duplicate_logical_rows": int(duplicates.sum()),
        "duplicate_logical_groups": int(duplicate_groups),
        "capacity_corrections": capacity_corrections,
        "observation_candidates": {
            "total": int(len(candidates)),
            "by_column": by_column,
            "examples": examples,
        },
    }


def _empty_candidates(
    group_columns: Sequence[str], date_column: str
) -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            *group_columns,
            date_column,
            "column",
            "observed",
            "baseline",
            "absolute_deviation",
            "relative_deviation",
            "robust_sigma",
            "threshold",
            "local_observations",
        ]
    )


def _district_number(value: object) -> int | None:
    if not isinstance(value, str):
        return None
    match = re.match(r"^P0*(\d+)", value)
    return int(match.group(1)) if match else None


def _records_for_json(df: pd.DataFrame) -> list[dict]:
    records = []
    for record in df.to_dict(orient="records"):
        cleaned = {}
        for key, value in record.items():
            if isinstance(value, pd.Timestamp):
                cleaned[key] = value.date().isoformat()
            elif isinstance(value, np.integer):
                cleaned[key] = int(value)
            elif isinstance(value, np.floating):
                cleaned[key] = None if np.isnan(value) else round(float(value), 6)
            elif pd.isna(value):
                cleaned[key] = None
            else:
                cleaned[key] = value
        records.append(cleaned)
    return records
