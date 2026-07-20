from datetime import date
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from web_app.explorer_data import ExplorerFilters, build_explorer_payload


def sample_data() -> pd.DataFrame:
    rows = []
    for cast_dne in ["den", "noc"]:
        rows.extend(
            [
                {
                    "kod_useku": "P1-A",
                    "date": "2025-01-31",
                    "mestska_cast": "P01",
                    "typ_zony": "RES",
                    "cast_dne": cast_dne,
                    "POP_CELKEM": 120,
                    "parkovacich_mist_v_zps": 100,
                },
                {
                    "kod_useku": "P1-A",
                    "date": "2025-02-28",
                    "mestska_cast": "P01",
                    "typ_zony": "RES",
                    "cast_dne": cast_dne,
                    "POP_CELKEM": 130,
                    "parkovacich_mist_v_zps": 100,
                },
                {
                    "kod_useku": "P2-A",
                    "date": "2025-02-28",
                    "mestska_cast": "P02",
                    "typ_zony": "MIX",
                    "cast_dne": cast_dne,
                    "POP_CELKEM": 90,
                    "parkovacich_mist_v_zps": 100,
                },
            ]
        )
    frame = pd.DataFrame(rows)
    frame["date"] = pd.to_datetime(frame["date"])
    return frame


def test_explorer_deduplicates_time_of_day_rows() -> None:
    payload = build_explorer_payload(
        sample_data(),
        ExplorerFilters(
            start=date(2025, 1, 1),
            end=date(2025, 2, 28),
            cast_dne=("den", "noc"),
            zone_types=("RES", "MIX"),
        ),
    )

    assert payload["series"][-1] == {
        "date": "2025-02-28",
        "permits": 220,
        "spaces": 200,
        "permits_per_space": 1.1,
    }
    assert [row["district"] for row in payload["districts"]] == ["P01", "P02"]


def test_explorer_applies_district_filter() -> None:
    payload = build_explorer_payload(
        sample_data(),
        ExplorerFilters(districts=("P01",), zone_types=("RES", "MIX")),
    )

    assert payload["summary"]["permits"] == 130
    assert payload["summary"]["permits_per_space"] == pytest.approx(1.3)
    assert payload["districts"][0]["district"] == "P01"


def test_explorer_change_uses_first_nonzero_permit_month() -> None:
    frame = sample_data()
    frame.loc[frame["date"] == "2025-01-31", "POP_CELKEM"] = 0

    payload = build_explorer_payload(
        frame,
        ExplorerFilters(zone_types=("RES", "MIX")),
    )

    assert payload["summary"]["permits_change"] == 0
    assert payload["summary"]["ratio_change"] == 0
