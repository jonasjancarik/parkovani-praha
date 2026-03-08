import sys
import unittest
from pathlib import Path

import pandas as pd
from shapely.geometry import Point, box

WEB_APP_DIR = Path(__file__).resolve().parents[1] / "web_app"
if str(WEB_APP_DIR) not in sys.path:
    sys.path.insert(0, str(WEB_APP_DIR))

from data import radius_latest_snapshot, radius_spaces_series
from geo import Zone, ZoneIndex, project_geometry


class AddressRadiusTests(unittest.TestCase):
    def test_zone_index_supports_shapely2_indices_and_radius_lookup(self):
        zone_a_geom = box(14.4200, 50.0800, 14.4210, 50.0810)
        zone_b_geom = box(14.4230, 50.0800, 14.4240, 50.0810)
        zones = {
            "A": Zone(
                code="A",
                geometry=zone_a_geom,
                geometry_metric=project_geometry(zone_a_geom),
            ),
            "B": Zone(
                code="B",
                geometry=zone_b_geom,
                geometry_metric=project_geometry(zone_b_geom),
            ),
        }
        index = ZoneIndex(zones)
        point = Point(14.4205, 50.0805)

        zone, match_type = index.find_zone(point)

        self.assertEqual(zone.code, "A")
        self.assertEqual(match_type, "inside")

        point_metric = project_geometry(point)
        radius_to_b = int(zones["B"].geometry_metric.distance(point_metric))

        near_codes = [zone.code for zone, _ in index.find_zones_within_radius(point, 50)]
        wider_codes = [
            zone.code
            for zone, _ in index.find_zones_within_radius(point, radius_to_b + 1)
        ]

        self.assertEqual(near_codes, ["A"])
        self.assertEqual(wider_codes, ["A", "B"])

    def test_radius_helpers_prefer_selected_cast_dne_and_fallback(self):
        df = pd.DataFrame(
            [
                {
                    "kod_useku": "A",
                    "date": "2025-01-31",
                    "cast_dne": "den",
                    "parkovacich_mist_v_zps": 10,
                    "naz_zsj": "ZSJ A",
                    "mestska_cast": "P01",
                    "typ_zony": "RES",
                },
                {
                    "kod_useku": "A",
                    "date": "2025-01-31",
                    "cast_dne": "noc",
                    "parkovacich_mist_v_zps": 99,
                    "naz_zsj": "ZSJ A",
                    "mestska_cast": "P01",
                    "typ_zony": "RES",
                },
                {
                    "kod_useku": "B",
                    "date": "2025-01-31",
                    "cast_dne": "noc",
                    "parkovacich_mist_v_zps": 20,
                    "naz_zsj": "ZSJ B",
                    "mestska_cast": "P02",
                    "typ_zony": "MIX",
                },
                {
                    "kod_useku": "A",
                    "date": "2025-02-28",
                    "cast_dne": "den",
                    "parkovacich_mist_v_zps": 11,
                    "naz_zsj": "ZSJ A",
                    "mestska_cast": "P01",
                    "typ_zony": "RES",
                },
                {
                    "kod_useku": "B",
                    "date": "2025-02-28",
                    "cast_dne": "den",
                    "parkovacich_mist_v_zps": 21,
                    "naz_zsj": "ZSJ B",
                    "mestska_cast": "P02",
                    "typ_zony": "MIX",
                },
            ]
        )
        df["date"] = pd.to_datetime(df["date"])

        series = radius_spaces_series(df, ["A", "B"], "den")
        snapshot = radius_latest_snapshot(df, ["A", "B"], "den")

        self.assertEqual(series["parkovacich_mist_v_zps"].tolist(), [30, 32])
        self.assertEqual(snapshot["kod_useku"].tolist(), ["A", "B"])
        self.assertEqual(snapshot["parkovacich_mist_v_zps"].tolist(), [11, 21])


if __name__ == "__main__":
    unittest.main()
