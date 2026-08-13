import json
import sys
from pathlib import Path

import geopandas as gpd
import pytest
from shapely.geometry import MultiPolygon, Point, Polygon

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.permit_zone_mapping import (
    OUTPUT_PERMIT_COLUMNS,
    aggregate_mapped_permits,
    audit_history,
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


def test_intersection_tie_is_recorded_and_resolved_deterministically() -> None:
    buildings = building_frame(Point(14.0, 50.0))
    zones = gpd.GeoDataFrame(
        [
            {
                "usek_code": "B",
                "geometry": Polygon(
                    [(14.0, 50.0), (14.01, 50.0), (14.01, 50.01), (14.0, 50.01)]
                ),
            },
            {
                "usek_code": "A",
                "geometry": Polygon(
                    [(13.99, 50.0), (14.0, 50.0), (14.0, 50.01), (13.99, 50.01)]
                ),
            },
        ],
        geometry="geometry",
        crs="EPSG:4326",
    )

    mapping, tie_expansions = map_buildings_to_active_zones(buildings, zones)

    assert tie_expansions == 1
    assert mapping[["building_code", "usek_code"]].to_dict("records") == [
        {"building_code": 1, "usek_code": "A"}
    ]
    assert mapping.loc[0, "mapping_method"] == "polygon_intersection"
    assert bool(mapping.loc[0, "spatial_tie"])
    assert mapping.loc[0, "tie_kind"] == "overlapping_polygons"


def test_nearest_fallback_is_labeled() -> None:
    buildings = building_frame(Point(14.02, 50.02))
    zones = gpd.GeoDataFrame(
        [
            {
                "usek_code": "A",
                "geometry": Polygon(
                    [(14.0, 50.0), (14.01, 50.0), (14.01, 50.01), (14.0, 50.01)]
                ),
            }
        ],
        geometry="geometry",
        crs="EPSG:4326",
    )

    mapping, tie_expansions = map_buildings_to_active_zones(buildings, zones)

    assert tie_expansions == 0
    assert mapping.loc[0, "mapping_method"] == "nearest_active_zone"
    assert mapping.loc[0, "distance_m"] > 0
    assert not bool(mapping.loc[0, "spatial_tie"])


def test_real_june_2026_building_moves_from_legacy_to_containing_zone() -> None:
    """Regression from PO_202606M_TR and P01-OB_202606D_NA source rows."""

    buildings = building_frame(Point(14.420315569546352, 50.09068591128229))
    buildings["building_code"] = 21692823
    buildings["POP_CELKEM"] = 6
    buildings["pop_rezidentska"] = 3
    buildings["pop_vlastnicka"] = 1
    buildings["pop_socialni"] = 2
    current_zone = MultiPolygon(
        [
            Polygon(
                [
                    (14.420374151132997, 50.09105737300209),
                    (14.420194990031115, 50.09091847660889),
                    (14.420092113153968, 50.09088653332448),
                    (14.420249113226305, 50.090509236095514),
                    (14.420344991972451, 50.09028907522778),
                    (14.42051109777613, 50.09025671630034),
                    (14.42068509167687, 50.090549470810814),
                    (14.420478667928933, 50.0905991092009),
                    (14.420630965566957, 50.09072547994333),
                    (14.420644970034449, 50.09100182737214),
                    (14.420374151132997, 50.09105737300209),
                ]
            ),
            Polygon(
                [
                    (14.420374151132997, 50.09105737300209),
                    (14.420657836930774, 50.09123218685843),
                    (14.42065519182881, 50.091463505624915),
                    (14.420561918607833, 50.0916594189361),
                    (14.420289435747547, 50.09178873132428),
                    (14.420080406865182, 50.09177316414961),
                    (14.420142737767403, 50.091124429873716),
                    (14.420374151132997, 50.09105737300209),
                ]
            ),
        ]
    )
    legacy_zone = Polygon(
        [
            (14.421495419030753, 50.09046550537962),
            (14.421277757346035, 50.0906585121891),
            (14.421071180536051, 50.09081635837702),
            (14.420630965566792, 50.09072547994342),
            (14.420478667928991, 50.09059910920093),
            (14.420685091676747, 50.090549470810764),
            (14.420952801965832, 50.0905140609659),
            (14.420982454941285, 50.090234690545174),
            (14.421263309087552, 50.0902093751966),
            (14.421495419030753, 50.09046550537962),
        ]
    )
    zones = gpd.GeoDataFrame(
        [
            {"usek_code": "P1-0223", "geometry": current_zone},
            {"usek_code": "P1-0233", "geometry": legacy_zone},
        ],
        geometry="geometry",
        crs="EPSG:4326",
    )

    mapping, _ = map_buildings_to_active_zones(buildings, zones)

    legacy_assignment = "P1-0233"
    assert mapping.loc[0, "usek_code"] != legacy_assignment
    assert mapping.loc[0, "usek_code"] == "P1-0223"
    assert mapping.loc[0, "mapping_method"] == "polygon_intersection"


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
    assert candidate[OUTPUT_PERMIT_COLUMNS].sum().to_dict() == (
        buildings[OUTPUT_PERMIT_COLUMNS].sum().to_dict()
    )


def test_missing_current_coordinate_uses_latest_prior_value(tmp_path: Path) -> None:
    buildings_dir = tmp_path / "buildings"
    buildings_dir.mkdir()
    properties = {
        "CODE": 1,
        "XSUM": 3,
        "GraphData": "[3,0,0,0,0,0,0,0]",
        "GraphLegend": "[R,V,A,P,C,E,O,S]",
    }
    prior = {
        "features": [
            {
                "properties": properties,
                "geometry": {"type": "Point", "coordinates": [14.0, 50.0]},
            }
        ]
    }
    current = {"features": [{"properties": properties, "geometry": None}]}
    (buildings_dir / "PO_202605M_TR.json").write_text(json.dumps(prior))
    (buildings_dir / "PO_202606M_TR.json").write_text(json.dumps(current))

    buildings = load_month_buildings(buildings_dir, "202606")

    assert buildings.loc[0, "coordinate_fallback"]
    assert buildings.loc[0, "coordinate_source_month"] == "202605"
    assert buildings.loc[0, "geometry"].equals(Point(14.0, 50.0))


def _write_building_export(path: Path, *, code: int = 1, permits: int = 3) -> None:
    payload = {
        "features": [
            {
                "properties": {
                    "CODE": code,
                    "XSUM": permits,
                    "GraphData": f"[{permits},0,0,0,0,0,0,0]",
                    "GraphLegend": "[R,V,A,P,C,E,O,S]",
                },
                "geometry": {"type": "Point", "coordinates": [14.0, 50.0]},
            }
        ]
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_zone_export(path: Path, code: str) -> None:
    payload = {
        "features": [
            {
                "properties": {"CODE": code},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [13.99, 49.99],
                            [14.01, 49.99],
                            [14.01, 50.01],
                            [13.99, 50.01],
                            [13.99, 49.99],
                        ]
                    ],
                },
            }
        ]
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_history_audit_records_coverage_changes_and_hashed_inputs(tmp_path: Path) -> None:
    buildings_dir = tmp_path / "buildings"
    zones_dir = tmp_path / "zones"
    buildings_dir.mkdir()
    zones_dir.mkdir()
    for month in ("202605", "202606", "202607"):
        _write_building_export(buildings_dir / f"PO_{month}M_TR.json")
    _write_zone_export(zones_dir / "P01-OB_202605D_NA.json", "P1-A")
    _write_zone_export(zones_dir / "P01-OB_202606D_NA.json", "P1-B")

    parking_csv = tmp_path / "parking.csv"
    parking_csv.write_text(
        "date,kod_useku\n2026-05-31,P1-A\n2026-06-30,P1-B\n",
        encoding="utf-8",
    )
    legacy_mapping_csv = tmp_path / "legacy.csv"
    legacy_mapping_csv.write_text(
        "building_code,usek_code\n1,P1-A\n",
        encoding="utf-8",
    )

    result = audit_history(
        buildings_dir=buildings_dir,
        zones_dir=zones_dir,
        parking_csv=parking_csv,
        legacy_mapping_csv=legacy_mapping_csv,
    )

    assert result.candidate["POP_CELKEM"].sum() == 6
    assert result.mapping["month"].tolist() == ["202605", "202606"]
    assert result.mapping["mapping_method"].tolist() == [
        "polygon_intersection",
        "polygon_intersection",
    ]
    assert result.report["coverage"]["unaudited_months"] == [
        {"month": "202607", "reason": "missing_parking_observations"}
    ]
    june_report = result.report["monthly_reports"][1]
    assert june_report["compared_with_month"] == "202605"
    assert june_report["changed_previous_assignments"] == 1
    assert not result.report["structural_gates"]["complete_source_coverage"]
    assert result.report["release_decision"] == {
        "ready": False,
        "blocking_reasons": [
            "spatial_review_not_approved",
            "coverage_decision_not_approved",
        ],
    }
    assert len(result.report["inputs"]["parking"]["sha256"]) == 64


def test_history_audit_rejects_an_explicit_empty_month_range(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="No building export months"):
        audit_history(
            buildings_dir=tmp_path / "buildings",
            zones_dir=tmp_path / "zones",
            parking_csv=tmp_path / "parking.csv",
            months=[],
        )
