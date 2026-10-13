import sys
from pathlib import Path

import pandas as pd
from shapely.geometry import box

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.capacity_steps import (  # noqa: E402
    CARS_CHANGED,
    LIKELY_ERROR,
    NO_SHAPE,
    REDRAWN,
    REDRAWN_MISMATCH,
    classify_steps,
    compare_shapes,
    find_capacity_steps,
    monthly_capacity,
)


def zone_rows(code: str, capacities: list[float], cars: list[float], start: str = "2024-01-31") -> list[dict]:
    months = pd.date_range(start, periods=len(capacities), freq="ME")
    return [
        {
            "kod_useku": code,
            "date": month,
            "cast_dne": cast,
            "parkovacich_mist_v_zps": capacity,
            "parkovacich_mist_celkem": capacity + 5,
            "obsazenost": parked / capacity,
        }
        for month, capacity, parked in zip(months, capacities, cars)
        for cast in ("den", "noc")
    ]


def steps_for(rows: list[dict]) -> pd.DataFrame:
    return find_capacity_steps(monthly_capacity(pd.DataFrame(rows)))


def test_temporary_jump_is_found_in_both_directions_with_stable_cars() -> None:
    steps = steps_for(zone_rows("P5-1", [65, 65, 66, 245, 245, 245, 66, 66], [36, 38, 37, 36, 40, 41, 44, 45]))

    assert steps[["capacity_before", "capacity_after"]].values.tolist() == [[66, 245], [245, 66]]
    up, down = steps.to_dict("records")
    assert up["date"] == pd.Timestamp("2024-04-30")
    assert (up["months_before"], up["months_after"]) == (3, 3)
    assert up["cars_stable"] and up["starts_series"]
    assert up["reverts_on"] == pd.Timestamp("2024-07-31")
    assert not up["returns_to_earlier"]
    assert down["returns_to_earlier"] and pd.isna(down["reverts_on"])


def test_small_or_gradual_changes_are_not_jumps() -> None:
    rows = (
        zone_rows("P1-SMALL", [10, 10, 25, 25], [5, 5, 5, 5])  # +150 % but only 15 spaces
        + zone_rows("P1-MILD", [100, 100, 130, 130], [50, 50, 50, 50])  # 30 spaces but only +30 %
    )
    assert steps_for(rows).empty


def test_a_drop_counts_the_same_as_the_matching_rise() -> None:
    steps = steps_for(zone_rows("P1-DROP", [100, 100, 60, 60], [40, 40, 20, 20]))

    assert len(steps) == 1
    assert not steps.iloc[0]["cars_stable"]


def test_shapes_compare_by_overlap_and_area() -> None:
    assert compare_shapes(box(0, 0, 10, 10), box(0, 0, 10, 10)) == (1.0, 1.0)
    overlap, area = compare_shapes(box(0, 0, 10, 10), box(0, 0, 20, 10))
    assert (round(overlap, 2), area) == (0.5, 2.0)
    assert compare_shapes(None, box(0, 0, 1, 1)) == (None, None)


def test_verdict_combines_shape_and_cars() -> None:
    base = {
        "date": pd.Timestamp("2024-04-30"),
        "previous_date": pd.Timestamp("2024-03-31"),
        "capacity_before": 50.0,
        "capacity_after": 100.0,
        "cars_before": 30.0,
        "cars_after": 30.0,
        "cars_stable": True,
        "months_before": 12,
        "months_after": 12,
        "starts_series": False,
        "reverts_on": pd.NaT,
        "returns_to_earlier": False,
    }
    steps = pd.DataFrame(
        [
            base | {"code": "P1-ERR", "shape_overlap": 1.0, "area_ratio": 1.0},
            base | {"code": "P1-LOW", "shape_overlap": 1.0, "area_ratio": 1.0, "cars_before": 70.0, "cars_after": 72.0},
            base | {"code": "P1-CARS", "shape_overlap": 1.0, "area_ratio": 1.0, "cars_after": 70.0, "cars_stable": False},
            base | {"code": "P2-NEW", "shape_overlap": 0.5, "area_ratio": 2.0},
            base | {"code": "P2-ODD", "shape_overlap": 0.3, "area_ratio": 0.4},
            base | {"code": "P3-NONE", "shape_overlap": float("nan"), "area_ratio": float("nan")},
        ]
    )

    result = classify_steps(steps).set_index("code")

    assert result["verdict"].to_dict() == {
        "P1-ERR": LIKELY_ERROR,
        "P1-LOW": LIKELY_ERROR,
        "P1-CARS": CARS_CHANGED,
        "P2-NEW": REDRAWN,
        "P2-ODD": REDRAWN_MISMATCH,
        "P3-NONE": NO_SHAPE,
    }
    assert result.loc["P1-ERR", "suspect_value"] == "nelze určit"
    # 70 cars on 50 spaces cannot be right, so the earlier capacity is the wrong one.
    assert result.loc["P1-LOW", "suspect_value"] == "před skokem (obsazenost 140%)"
    assert result.loc["P2-NEW", "redrawn_in_district"] == 2
    assert result.loc["P1-ERR", "redrawn_in_district"] == 0


def reviewed(rows: list[dict], overlap: float = 1.0) -> pd.DataFrame:
    steps = steps_for(rows)
    return classify_steps(steps.assign(shape_overlap=overlap, area_ratio=1.0))


def test_plan_corrects_a_value_that_returned() -> None:
    from src.capacity_steps import plan_corrections

    rows = zone_rows("P5-1", [65, 65, 66, 245, 245, 245, 66, 66], [36, 38, 37, 36, 40, 41, 44, 45])

    plan = plan_corrections(reviewed(rows))

    assert len(plan) == 1
    correction = plan.iloc[0]
    assert (correction["start_date"], correction["end_date"]) == (
        pd.Timestamp("2024-04-30"),
        pd.Timestamp("2024-06-30"),
    )
    assert (correction["wrong_zps"], correction["right_zps"]) == (245, 66)
    # A redrawn zone is a real change, whatever the numbers do.
    assert plan_corrections(reviewed(rows, overlap=0.5)).empty


def test_plan_corrects_only_impossible_occupancy() -> None:
    from src.capacity_steps import plan_corrections

    rows = (
        # 26 cars on 11 spaces cannot be right; 34 spaces afterwards can.
        zone_rows("P6-LOW", [11, 34, 34, 34], [26, 28, 27, 28])
        # 53 cars on 39 spaces is 136 %, which the data show often enough.
        + zone_rows("P2-FULL", [64, 64, 39, 39], [53, 53, 53, 54])
        # Both sides are plausible, so nothing says which one is wrong.
        + zone_rows("P4-OPEN", [100, 100, 158, 158], [90, 92, 91, 93])
    )

    plan = plan_corrections(reviewed(rows))

    assert plan["code"].tolist() == ["P6-LOW"]
    correction = plan.iloc[0]
    assert (correction["start_date"], correction["end_date"]) == (
        pd.Timestamp("2024-01-31"),
        pd.Timestamp("2024-01-31"),
    )
    assert (correction["wrong_zps"], correction["right_zps"]) == (11, 34)


def test_correction_keeps_the_number_of_parked_cars() -> None:
    from src.parking_cleanup import apply_capacity_corrections

    corrections = pd.DataFrame(
        [
            {
                "code": "P5-1",
                "start_date": pd.Timestamp("2024-02-29"),
                "end_date": pd.Timestamp("2024-03-31"),
                "wrong_zps": 245.0,
                "right_zps": 66.0,
                "right_total": 70.0,
                "reason": "zkouška",
            }
        ]
    )
    source = pd.DataFrame(
        {
            "CODE": ["P5-1", "P5-1", "P5-1", "P5-1", "P5-2"],
            "date": pd.to_datetime(["2024-01-31", "2024-02-29", "2024-03-31", "2024-04-30", "2024-02-29"]),
            # March does not carry the wrong value, so it is left alone.
            "PS_ZPS": [65, 245, 66, 66, 245],
            "CELKEM_PS": [70, 250, 70, 70, 250],
            "Obs": [60, 12, 50, 55, 12],
        }
    )

    corrected, records = apply_capacity_corrections(
        source,
        corrections,
        code_col="CODE",
        date_col="date",
        zps_capacity_col="PS_ZPS",
        total_capacity_col="CELKEM_PS",
        occupancy_col="Obs",
    )

    assert corrected["PS_ZPS"].tolist() == [65, 66, 66, 66, 245]
    assert corrected["PS_ZPS"].dtype == source["PS_ZPS"].dtype
    assert corrected["CELKEM_PS"].tolist() == [70, 70, 70, 70, 250]
    # 12 % of 245 spaces is 29.4 cars, which is 44.5 % of 66 spaces.
    assert round(corrected.loc[1, "Obs"], 1) == 44.5
    assert corrected.loc[[0, 2, 3, 4], "Obs"].tolist() == [60, 50, 55, 12]
    assert records[0]["rows_corrected"] == 1
