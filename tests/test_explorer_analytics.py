from datetime import date
import sys
from pathlib import Path

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from web_app.explorer_analytics import ForecastSettings, _forecast, build_overview_analytics
from web_app.explorer_data import ExplorerFilters


def analytics_sample() -> pd.DataFrame:
    rows = []
    for month, permit_growth in [("2025-01-31", 0), ("2025-02-28", 10), ("2025-03-31", 20)]:
        for zone, district, zsj, zone_type, base_permits in [
            ("A", "P01", "Z1", "RES", 100),
            ("B", "P02", "Z2", "MIX", 80),
        ]:
            for cast_dne in ["den", "noc"]:
                rows.append(
                    {
                        "kod_useku": zone,
                        "date": month,
                        "mestska_cast": district,
                        "kod_zsj": zsj,
                        "naz_zsj": f"Oblast {zsj}",
                        "typ_zony": zone_type,
                        "cast_dne": cast_dne,
                        "POP_CELKEM": base_permits + permit_growth,
                        "parkovacich_mist_v_zps": 100,
                        "pop_rezidentska": base_permits + permit_growth,
                        "pop_vlastnicka": 0,
                        "pop_abonentska": 0,
                        "pop_prenosna": 0,
                        "pop_carsharing": 0,
                        "pop_ekologicka": 0,
                        "pop_ostatni": 0,
                        "pop_socialni": 0,
                        "rezidentska": 50,
                        "vlastnicka": 5,
                        "abonentska": 10,
                        "navstevnici": 20 if cast_dne == "den" else 10,
                        "ostatni": 2,
                        "socialni": 1,
                    }
                )
    frame = pd.DataFrame(rows)
    frame["date"] = pd.to_datetime(frame["date"])
    return frame


def test_overview_analytics_covers_all_original_questions() -> None:
    payload = build_overview_analytics(
        analytics_sample(),
        ExplorerFilters(cast_dne=("den", "noc"), zone_types=("RES", "MIX")),
    )

    assert set(payload) == {
        "zone_mix",
        "parker_share_by_district",
        "spaces_by_zone",
        "permits_by_type",
        "parkers_by_type",
        "zsj_changes",
        "zsj_pressure",
        "forecast",
    }
    assert {row["zone_type"] for row in payload["zone_mix"]} == {"RES", "MIX"}
    assert len(payload["zsj_changes"]) == 2
    assert {row["kind"] for row in payload["forecast"]} == {"Skutečnost", "Predikce"}


def test_overview_analytics_respects_district_filter() -> None:
    payload = build_overview_analytics(
        analytics_sample(),
        ExplorerFilters(
            start=date(2025, 1, 1),
            end=date(2025, 3, 31),
            districts=("P01",),
            zone_types=("RES", "MIX"),
        ),
    )

    assert {row["district"] for row in payload["parker_share_by_district"]} == {"P01"}
    assert {row["district"] for row in payload["forecast"]} == {"P01"}


def test_invariant_analytics_ignore_uneven_time_of_day_coverage() -> None:
    frame = analytics_sample()
    frame = frame[
        ~(
            (frame["kod_useku"] == "B")
            & (frame["cast_dne"] == "noc")
            & (frame["date"] == pd.Timestamp("2025-01-31"))
        )
    ]

    payload = build_overview_analytics(
        frame,
        ExplorerFilters(cast_dne=("noc",), zone_types=("RES", "MIX")),
    )

    january_spaces = [
        row["value"] for row in payload["spaces_by_zone"]
        if row["date"] == "2025-01-31"
    ]
    january_parkers = [
        row["value"] for row in payload["parkers_by_type"]
        if row["date"] == "2025-01-31" and row["series"] == "Rezidenti"
    ]
    assert sum(january_spaces) == 200
    assert january_parkers == [50]


def test_forecast_uses_elapsed_months_when_history_has_gaps() -> None:
    pressure = pd.DataFrame(
        [
            {"date": pd.Timestamp("2025-01-31"), "mestska_cast": "P01", "value": 1.0},
            {"date": pd.Timestamp("2025-03-31"), "mestska_cast": "P01", "value": 3.0},
        ]
    )

    rows = _forecast(pressure, ForecastSettings(history_months=24, horizon_months=1))
    forecast = next(row for row in rows if row["kind"] == "Predikce")

    assert forecast["date"] == "2025-04-30"
    assert forecast["value"] == 4.0
