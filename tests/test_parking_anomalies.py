from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.parking_anomalies import (
    AnomalyDetectionConfig,
    detect_monthly_anomaly_candidates,
    normalize_parking_observations,
    recompute_derived_parking_measures,
    validate_processed_parking_data,
)
from src.parking_cleanup import apply_verified_capacity_regime_cleanup


def test_normalization_prefers_the_zone_owning_district_and_deduplicates() -> None:
    rows = [
        {
            "CODE": "p2-0509",
            "date": "2024-07-31",
            "cast_dne": "den",
            "filename": "P02-OB_202407D_NA.json",
            "Obs": 44.0,
        },
        {
            "CODE": "P2-0509",
            "date": "2024-07-31",
            "cast_dne": "den",
            "filename": "P10-OB_202407D_NA.json",
            "Obs": None,
        },
        {
            "CODE": "P7-9068",
            "date": "2023-06-30",
            "cast_dne": "Po-Pá (MPD)",
            "filename": "P07-OB_202306P_NA.json",
            "Obs": 23.0,
        },
        {
            "CODE": "P7-9068",
            "date": "2023-06-30",
            "cast_dne": "Po-Pá (MPD)",
            "filename": "P07-OB_202306P_NA.json",
            "Obs": 23.0,
        },
    ]

    normalized, report = normalize_parking_observations(pd.DataFrame(rows))

    assert len(normalized) == 2
    assert normalized.loc[normalized["CODE"] == "P2-0509", "Obs"].item() == 44.0
    assert report.excluded_cross_district_rows == 1
    assert report.exact_duplicate_rows == 1


def test_normalization_rejects_conflicting_rows_at_the_logical_grain() -> None:
    rows = [
        {
            "CODE": "P7-9068",
            "date": "2023-06-30",
            "cast_dne": "den",
            "filename": "P07-OB_202306D_NA.json",
            "Obs": value,
        }
        for value in (23.0, 55.0)
    ]

    with pytest.raises(ValueError, match="Conflicting parking observations"):
        normalize_parking_observations(pd.DataFrame(rows))


def test_anomaly_detection_keeps_time_variants_independent() -> None:
    dates = pd.date_range("2024-01-31", periods=13, freq="ME")
    rows = []
    for cast_dne, final_value in (("den", 80.0), ("noc", 80.0)):
        baseline = 10.0 if cast_dne == "den" else 80.0
        for index, date in enumerate(dates):
            rows.append(
                {
                    "CODE": "P1-0001",
                    "date": date,
                    "cast_dne": cast_dne,
                    "Obs": final_value if index == 12 else baseline,
                }
            )
    frame = pd.DataFrame(rows)
    original = frame.copy(deep=True)

    candidates = detect_monthly_anomaly_candidates(
        frame,
        value_columns=["Obs"],
        config=AnomalyDetectionConfig(minimum_absolute_deviation=25.0),
    )

    assert candidates[["CODE", "cast_dne", "column"]].to_dict("records") == [
        {"CODE": "P1-0001", "cast_dne": "den", "column": "Obs"}
    ]
    pd.testing.assert_frame_equal(frame, original)


def test_anomaly_detection_requires_consecutive_calendar_support() -> None:
    dates = list(pd.date_range("2024-01-31", periods=12, freq="ME"))
    dates.pop(5)
    dates.append(pd.Timestamp("2025-01-31"))
    frame = pd.DataFrame(
        {
            "CODE": "P1-0001",
            "date": dates,
            "cast_dne": "den",
            "Obs": [10.0] * 11 + [90.0],
        }
    )

    candidates = detect_monthly_anomaly_candidates(
        frame,
        value_columns=["Obs"],
        config=AnomalyDetectionConfig(minimum_absolute_deviation=25.0),
    )

    assert candidates.empty


def test_anomaly_detection_zero_mad_still_respects_domain_floor() -> None:
    dates = pd.date_range("2024-01-31", periods=13, freq="ME")
    frame = pd.DataFrame(
        {
            "CODE": "P1-0001",
            "date": dates,
            "cast_dne": "den",
            "Obs": [10.0] * 12 + [30.0],
        }
    )

    candidates = detect_monthly_anomaly_candidates(
        frame,
        value_columns=["Obs"],
        config=AnomalyDetectionConfig(minimum_absolute_deviation=25.0),
    )

    assert candidates.empty


def test_derived_measures_count_each_primitive_once() -> None:
    frame = pd.DataFrame(
        {
            "rezidentska": [1.0],
            "vlastnicka": [2.0],
            "abonentska": [3.0],
            "prenosna": [4.0],
            "carsharing": [5.0],
            "ekologicka": [6.0],
            "ostatni": [7.0],
            "socialni": [8.0],
            "navstevnici_platici": [9.0],
            "navstevnici_neplatici": [10.0],
            "volna_mista": [11.0],
        }
    )

    result = recompute_derived_parking_measures(frame)

    assert result["navstevnici"].item() == 23.0
    assert result["soucet_vsech_typu"].item() == sum(range(1, 12))


def test_processed_quality_gate_checks_all_core_invariants() -> None:
    primitives = pd.DataFrame(
        {
            "rezidentska": [1.0], "vlastnicka": [2.0], "abonentska": [3.0],
            "prenosna": [4.0], "carsharing": [5.0], "ekologicka": [6.0],
            "ostatni": [7.0], "socialni": [8.0], "navstevnici_platici": [9.0],
            "navstevnici_neplatici": [10.0], "volna_mista": [11.0],
        }
    )
    frame = recompute_derived_parking_measures(primitives).assign(
        kod_useku="P1-0001",
        date=pd.Timestamp("2025-01-31"),
        cast_dne="den",
        parkovacich_mist_v_zps=20.0,
        parkovacich_mist_celkem=25.0,
    )

    assert set(validate_processed_parking_data(frame).values()) == {0}
    hierarchy_issue = frame.copy()
    hierarchy_issue["parkovacich_mist_v_zps"] = 30.0
    assert validate_processed_parking_data(hierarchy_issue)[
        "zps_above_total_rows"
    ] == 1

    broken_total = frame.copy()
    broken_total["soucet_vsech_typu"] = 0.0
    with pytest.raises(ValueError, match="total_identity_mismatches"):
        validate_processed_parking_data(broken_total)


def capacity_regime_frame(
    *,
    missing_regime_month: bool = False,
    category_transition: bool = False,
) -> pd.DataFrame:
    dates = pd.date_range("2020-11-30", periods=28, freq="ME")
    rows = []
    for index, date in enumerate(dates):
        if 3 <= index < 25:
            zps, total, occupied = 245.0, 250.0, 42.0
        elif index < 3:
            zps, total, occupied = 65.0, 70.0, 39.0
        else:
            zps, total, occupied = 66.0, 70.0, 46.0
        if missing_regime_month and index == 10:
            continue
        rows.append(
            {
                "CODE": "P5-1410",
                "date": date,
                "cast_dne": "den",
                "CATEGORY": (
                    "MIX" if category_transition and index < 3 else "RES"
                ),
                "PS_ZPS": zps,
                "CELKEM_PS": total,
                "Obs": occupied / zps * 100,
            }
        )
    return pd.DataFrame(rows)


def test_verified_capacity_cleanup_preserves_occupied_numerator() -> None:
    frame = capacity_regime_frame()

    cleaned, corrections = apply_verified_capacity_regime_cleanup(
        frame,
        code_col="CODE",
        date_col="date",
        zps_capacity_col="PS_ZPS",
        total_capacity_col="CELKEM_PS",
        occupancy_col="Obs",
        category_col="CATEGORY",
    )

    assert len(corrections) == 1
    assert corrections[0].months == 22
    regime = cleaned[
        cleaned["date"].between("2021-02-28", "2022-11-30")
    ]
    assert regime["PS_ZPS"].eq(66.0).all()
    assert regime["CELKEM_PS"].eq(70.0).all()
    assert (regime["PS_ZPS"] * regime["Obs"] / 100).round(6).eq(42.0).all()


def test_verified_capacity_cleanup_rejects_calendar_gaps() -> None:
    frame = capacity_regime_frame(missing_regime_month=True)

    cleaned, corrections = apply_verified_capacity_regime_cleanup(
        frame,
        code_col="CODE",
        date_col="date",
        zps_capacity_col="PS_ZPS",
        total_capacity_col="CELKEM_PS",
        occupancy_col="Obs",
        category_col="CATEGORY",
    )

    assert corrections == []
    pd.testing.assert_frame_equal(cleaned.reset_index(drop=True), frame.reset_index(drop=True))


def test_verified_capacity_cleanup_rejects_zone_category_transitions() -> None:
    frame = capacity_regime_frame(category_transition=True)

    cleaned, corrections = apply_verified_capacity_regime_cleanup(
        frame,
        code_col="CODE",
        date_col="date",
        zps_capacity_col="PS_ZPS",
        total_capacity_col="CELKEM_PS",
        occupancy_col="Obs",
        category_col="CATEGORY",
    )

    assert corrections == []
    pd.testing.assert_frame_equal(cleaned.reset_index(drop=True), frame.reset_index(drop=True))


def test_verified_capacity_cleanup_rejects_unstable_paired_capacity() -> None:
    frame = capacity_regime_frame()
    regime = frame["date"].between("2021-02-28", "2022-11-30")
    frame.loc[frame[regime].index[5], "CELKEM_PS"] = 500.0

    cleaned, corrections = apply_verified_capacity_regime_cleanup(
        frame,
        code_col="CODE",
        date_col="date",
        zps_capacity_col="PS_ZPS",
        total_capacity_col="CELKEM_PS",
        occupancy_col="Obs",
        category_col="CATEGORY",
    )

    assert corrections == []
    pd.testing.assert_frame_equal(cleaned.reset_index(drop=True), frame.reset_index(drop=True))


def test_verified_capacity_cleanup_rejects_implausible_rebased_occupancy() -> None:
    frame = capacity_regime_frame()
    regime = frame["date"].between("2021-02-28", "2022-11-30")
    frame.loc[~regime, "Obs"] = 65.0 / frame.loc[~regime, "PS_ZPS"] * 100
    frame.loc[regime, "Obs"] = 70.0 / frame.loc[regime, "PS_ZPS"] * 100

    cleaned, corrections = apply_verified_capacity_regime_cleanup(
        frame,
        code_col="CODE",
        date_col="date",
        zps_capacity_col="PS_ZPS",
        total_capacity_col="CELKEM_PS",
        occupancy_col="Obs",
        category_col="CATEGORY",
    )

    assert corrections == []
    pd.testing.assert_frame_equal(cleaned.reset_index(drop=True), frame.reset_index(drop=True))


def test_verified_capacity_cleanup_does_not_use_future_after_as_of() -> None:
    frame = capacity_regime_frame()

    cleaned, corrections = apply_verified_capacity_regime_cleanup(
        frame,
        code_col="CODE",
        date_col="date",
        zps_capacity_col="PS_ZPS",
        total_capacity_col="CELKEM_PS",
        occupancy_col="Obs",
        category_col="CATEGORY",
        as_of="2022-12-31",
    )

    assert corrections == []
    pd.testing.assert_series_equal(cleaned["PS_ZPS"], frame["PS_ZPS"])


def test_verified_capacity_cleanup_is_value_idempotent() -> None:
    frame = capacity_regime_frame()
    cleaned, _ = apply_verified_capacity_regime_cleanup(
        frame,
        code_col="CODE",
        date_col="date",
        zps_capacity_col="PS_ZPS",
        total_capacity_col="CELKEM_PS",
        occupancy_col="Obs",
        category_col="CATEGORY",
    )

    cleaned_again, _ = apply_verified_capacity_regime_cleanup(
        cleaned,
        code_col="CODE",
        date_col="date",
        zps_capacity_col="PS_ZPS",
        total_capacity_col="CELKEM_PS",
        occupancy_col="Obs",
        category_col="CATEGORY",
    )

    pd.testing.assert_frame_equal(cleaned_again, cleaned)
