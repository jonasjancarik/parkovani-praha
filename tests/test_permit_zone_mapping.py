import json
import sys
from pathlib import Path

import geopandas as gpd
from shapely.geometry import Point, Polygon

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.permit_zone_mapping import (
    OUTPUT_PERMIT_COLUMNS,
    aggregate_mapped_permits,
    load_month_buildings,
    map_buildings_to_active_zones,
)


def building_frame(point: Point) -> gpd.GeoDataFrame:
    row = {
        "building_code": 1,
        "POP_CELKEM": 3,
        "coordinate_fallback": False,
        "geometry": point,
        **{column: 0 for column in OUTPUT_PERMIT_COLUMNS if column != "POP_CELKEM"},
    }
    row["pop_rezidentska"] = 3
    return gpd.GeoDataFrame([row], geometry="geometry", crs="EPSG:4326")


def test_nearest_tie_is_resolved_to_one_deterministic_zone() -> None:
    buildings = building_frame(Point(14.0, 50.0))
    zones = gpd.GeoDataFrame(
        [
            {"usek_code": "B", "geometry": Polygon([(14.0, 50.0), (14.01, 50.0), (14.01, 50.01), (14.0, 50.01)])},
            {"usek_code": "A", "geometry": Polygon([(13.99, 50.0), (14.0, 50.0), (14.0, 50.01), (13.99, 50.01)])},
        ],
        geometry="geometry",
        crs="EPSG:4326",
    )

    mapping, tie_expansions = map_buildings_to_active_zones(buildings, zones)

    assert tie_expansions == 1
    assert mapping[["building_code", "usek_code"]].to_dict("records") == [
        {"building_code": 1, "usek_code": "A"}
    ]


def test_aggregation_reconciles_all_permit_columns() -> None:
    buildings = building_frame(Point(14.0, 50.0))
    zones = gpd.GeoDataFrame(
        [{"usek_code": "P1-A", "geometry": Point(14.0, 50.0).buffer(0.001)}],
        geometry="geometry",
        crs="EPSG:4326",
    )
    mapping, _ = map_buildings_to_active_zones(buildings, zones)

    candidate = aggregate_mapped_permits(mapping, "202606")

    assert candidate.loc[0, "date"] == "2026-06-30"
    assert candidate.loc[0, "kod_useku"] == "P1-A"
    assert candidate[OUTPUT_PERMIT_COLUMNS].sum().to_dict() == buildings[OUTPUT_PERMIT_COLUMNS].sum().to_dict()


def test_missing_current_coordinate_uses_latest_prior_value(tmp_path: Path) -> None:
    buildings_dir = tmp_path / "buildings"
    buildings_dir.mkdir()
    properties = {
        "CODE": 1,
        "XSUM": 3,
        "GraphData": "[3,0,0,0,0,0,0,0]",
        "GraphLegend": "[R,V,A,P,C,E,O,S]",
    }
    prior = {"features": [{"properties": properties, "geometry": {"type": "Point", "coordinates": [14.0, 50.0]}}]}
    current = {"features": [{"properties": properties, "geometry": None}]}
    (buildings_dir / "PO_202605M_TR.json").write_text(json.dumps(prior))
    (buildings_dir / "PO_202606M_TR.json").write_text(json.dumps(current))

    buildings = load_month_buildings(buildings_dir, "202606")

    assert buildings.loc[0, "coordinate_fallback"]
    assert buildings.loc[0, "geometry"].equals(Point(14.0, 50.0))
