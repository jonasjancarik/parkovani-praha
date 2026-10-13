import sys
from pathlib import Path

import pandas as pd
import pytest
from fastapi import HTTPException
from shapely.geometry import box

ROOT_DIR = Path(__file__).resolve().parents[1]
WEB_APP_DIR = ROOT_DIR / "web_app"
for path in (ROOT_DIR, WEB_APP_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

import geo  # noqa: E402
from address_report import build_address_report  # noqa: E402
from geo import Zone, ZoneIndex, project_geometry  # noqa: E402
from web_app import api  # noqa: E402

LON, LAT = 14.4000, 50.0800
# Roughly 70 m of longitude at Prague's latitude.
STEP = 0.001


def make_zone(code: str, offset: int) -> Zone:
    west = LON + offset * STEP
    geometry = box(west, LAT - 0.0002, west + 0.0005, LAT + 0.0002)
    return Zone(code=code, geometry=geometry, geometry_metric=project_geometry(geometry))


def make_rows(code: str, district: str, months: list[str], spaces: list[int], **extra) -> list[dict]:
    return [
        {
            "kod_useku": code,
            "naz_zsj": f"Lokalita {district}",
            "mestska_cast": district,
            "typ_zony": "RES",
            "cast_dne": "den",
            "date": pd.Timestamp(month),
            "POP_CELKEM": 30,
            "parkovacich_mist_v_zps": value,
            "obsazenost": 0.9,
            "respektovanost": 0.75,
            **extra,
        }
        for month, value in zip(months, spaces)
    ]


@pytest.fixture
def zone_index() -> ZoneIndex:
    return ZoneIndex(
        {
            "P1-A": make_zone("P1-A", 0),
            "P1-B": make_zone("P1-B", 2),
            "P2-C": make_zone("P2-C", 3),
            "P1-FAR": make_zone("P1-FAR", 40),
        }
    )


@pytest.fixture
def data() -> pd.DataFrame:
    months = ["2026-05-31", "2026-06-30", "2026-07-31", "2026-08-31"]
    rows = (
        make_rows("P1-A", "P01", months, [20, 20, 20, 18])
        # Starts a month later and has a one-month gap with unchanged capacity.
        + make_rows("P1-B", "P01", ["2026-06-30", "2026-08-31"], [10, 10])
        + make_rows("P2-C", "P02", months, [50, 50, 50, 50])
        + make_rows("P1-FAR", "P01", months, [99, 99, 99, 99])
    )
    return pd.DataFrame(rows)


def report(data, zone_index, **overrides):
    params = {"lon": LON + 0.0002, "lat": LAT, "radius_m": 300, "cast_dne": "den", "label": "Test 1"}
    return build_address_report(data, zone_index, {}, **(params | overrides))


def test_report_counts_only_zones_from_the_same_district(data, zone_index) -> None:
    payload = report(data, zone_index)

    assert payload["reference"]["code"] == "P1-A"
    assert payload["reference"]["inside"] is True
    assert payload["reference"]["district"] == "P01"
    assert [zone["code"] for zone in payload["zones"]] == ["P1-A", "P1-B"]
    assert payload["scope"]["other_district_zones"] == 1
    assert len(payload["geometry"]["features"]) == 2
    assert payload["summary"] | {"latest_date": None} == {
        "zones": 2,
        "spaces": 28,
        "permits": 60,
        "permits_per_space": 2.14,
        "occupancy": 0.9,
        "respect": 0.75,
        "high_occupancy_zones": 2,
        "low_respect_zones": 2,
        "spaces_change": -2,
        "series_start": "2026-06-30",
        "series_end": "2026-08-31",
        "latest_date": None,
    }


def test_total_covers_shared_months_and_zone_history_keeps_its_own(data, zone_index) -> None:
    payload = report(data, zone_index)

    # May is left out because P1-B has no data yet; its July gap is filled.
    assert payload["series"] == [
        {"date": "2026-06-30", "spaces": 30},
        {"date": "2026-07-31", "spaces": 30},
        {"date": "2026-08-31", "spaces": 28},
    ]
    histories = {zone["code"]: zone["history"] for zone in payload["zones"]}
    assert histories["P1-A"] == {"start": "2026-05-31", "values": [20, 20, 20, 18]}
    assert histories["P1-B"] == {"start": "2026-06-30", "values": [10, 10, 10]}


def test_comparison_lists_standard_radii_and_the_chosen_one(data, zone_index) -> None:
    payload = report(data, zone_index, radius_m=200)

    assert [row["radius_m"] for row in payload["comparison"]] == [100, 200, 300, 500, 800, 1000, 1500]
    by_radius = {row["radius_m"]: row for row in payload["comparison"]}
    assert (by_radius[100]["zones"], by_radius[100]["spaces"]) == (1, 18)
    assert (by_radius[300]["zones"], by_radius[300]["spaces"]) == (2, 28)


def test_point_without_zones_nearby_still_names_the_closest_zone(data, zone_index) -> None:
    payload = report(data, zone_index, lon=LON - 0.02, radius_m=100)

    assert payload["zones"] == []
    assert payload["series"] == []
    assert payload["summary"]["zones"] == 0
    assert payload["reference"]["code"] == "P1-A"
    assert payload["reference"]["inside"] is False
    assert payload["reference"]["in_radius"] is False
    assert payload["reference"]["distance_m"] > 1000


def test_latest_zone_files_include_every_part_of_the_day(tmp_path, monkeypatch) -> None:
    def write(name: str, codes: list[str]) -> None:
        features = [
            {"type": "Feature", "properties": {"CODE": code}, "geometry": box(0, 0, 1, 1).__geo_interface__}
            for code in codes
        ]
        (tmp_path / name).write_text(pd.Series({"features": features}).to_json())

    write("P01-OB_202607D_NA.json", ["P1-OLD"])
    write("P01-OB_202608D_NA.json", ["P1-A", "P1-B"])
    write("P01-OB_202608N_NA.json", ["P1-A", "P1-NIGHT"])
    monkeypatch.setattr(geo, "ZONES_PATH", tmp_path)

    codes = [feature["properties"]["CODE"] for feature in geo.latest_zone_features()]

    assert codes == ["P1-A", "P1-B", "P1-NIGHT"]


def test_report_endpoint_falls_back_to_default_part_of_day(data, zone_index, monkeypatch) -> None:
    monkeypatch.setattr(api, "parking_data", lambda: data)
    monkeypatch.setattr(api, "zone_index", lambda: zone_index)
    monkeypatch.setattr(api, "zsj_mapping", lambda: {})
    api.address_report_payload.cache_clear()

    payload = api.address_report(lon=LON + 0.0002, lat=LAT, radius=300, cast_dne="neznámá", label="Test 1")

    assert payload["selection"] == {
        "label": "Test 1",
        "lon": LON + 0.0002,
        "lat": LAT,
        "radius_m": 300,
        "cast_dne": "den",
    }
    api.address_report_payload.cache_clear()


def test_search_reports_missing_configuration_and_unknown_address(data, monkeypatch) -> None:
    monkeypatch.setattr(api, "parking_data", lambda: data)
    monkeypatch.delenv("MAPY_CZ_API_KEY", raising=False)
    with pytest.raises(HTTPException) as missing_key:
        api.address_search(q="Štefánikova 12")
    assert missing_key.value.status_code == 503

    monkeypatch.setenv("MAPY_CZ_API_KEY", "test")
    monkeypatch.setattr(api, "search_address", lambda query: None)
    with pytest.raises(HTTPException) as unknown:
        api.address_search(q="Neexistující 999")
    assert unknown.value.status_code == 404
