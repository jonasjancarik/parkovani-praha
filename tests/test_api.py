import sys
from pathlib import Path

import pandas as pd
from fastapi import Response

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from web_app import api


def test_health_checks_loaded_parking_data(monkeypatch) -> None:
    frame = pd.DataFrame(
        [
            {
                "date": pd.Timestamp("2026-06-30"),
                "kod_useku": "P1-A",
                "POP_CELKEM": 10,
                "parkovacich_mist_v_zps": 8,
            }
        ]
    )
    monkeypatch.setattr(api, "parking_data", lambda: frame)
    response = Response()

    assert api.health(response) == {
        "status": "ok",
        "rows": 1,
        "latest_date": "2026-06-30",
    }
    assert response.status_code == 200


def test_health_rejects_incomplete_parking_data(monkeypatch) -> None:
    monkeypatch.setattr(api, "parking_data", lambda: pd.DataFrame({"date": []}))
    response = Response()

    assert api.health(response) == {
        "status": "error",
        "detail": "parking data are incomplete",
    }
    assert response.status_code == 503


def test_overview_reports_the_loaded_parking_data(monkeypatch) -> None:
    frame = pd.DataFrame(
        [
            {
                "date": pd.Timestamp(month),
                "kod_useku": "P1-A",
                "mestska_cast": "P01",
                "typ_zony": "RES",
                "cast_dne": "den",
                "POP_CELKEM": permits,
                "parkovacich_mist_v_zps": 1000,
            }
            for month, permits in [("2025-06-30", 1200), ("2026-06-30", 1500)]
        ]
    )
    monkeypatch.setattr(api, "parking_data", lambda: frame)
    api.city_overview_payload.cache_clear()

    payload = api.city_overview()
    api.city_overview_payload.cache_clear()

    assert payload["latest_date"] == "2026-06-30"
    assert payload["city"]["permits_per_space"] == 1.5
    assert payload["city"]["compared_with"] == "2025-06-30"
