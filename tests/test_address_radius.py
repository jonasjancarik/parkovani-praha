import sys
import unittest
from pathlib import Path

import pandas as pd
from shapely.geometry import Point, box

WEB_APP_DIR = Path(__file__).resolve().parents[1] / "web_app"
if str(WEB_APP_DIR) not in sys.path:
    sys.path.insert(0, str(WEB_APP_DIR))
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from data import radius_latest_snapshot, radius_spaces_series, zone_capacity_history
from geo import Zone, ZoneIndex, project_geometry
from src.parking_cleanup import apply_temporary_capacity_regime_cleanup
from address_exports import build_zone_hits_geojson
from address_logic import (
    address_query_params_to_selection_seed,
    build_radius_policy_metrics,
    build_radius_scenario_row,
    build_policy_pressure_ranking,
    filter_zone_hits_to_data_scope,
    filter_zone_hits_to_same_area,
    format_geocode_result_label,
)


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

    def test_radius_zone_hits_are_limited_to_same_mestska_cast(self):
        zone_a_geom = box(14.4200, 50.0800, 14.4210, 50.0810)
        zone_b_geom = box(14.4230, 50.0800, 14.4240, 50.0810)
        zone_c_geom = box(14.4250, 50.0800, 14.4260, 50.0810)
        zone_a = Zone("A", zone_a_geom, project_geometry(zone_a_geom))
        zone_b = Zone("B", zone_b_geom, project_geometry(zone_b_geom))
        zone_c = Zone("C", zone_c_geom, project_geometry(zone_c_geom))
        df = pd.DataFrame(
            [
                {"kod_useku": "A", "mestska_cast": "P05"},
                {"kod_useku": "B", "mestska_cast": "P01"},
                {"kod_useku": "C", "mestska_cast": "P05"},
            ]
        )
        zsj_mapping = {
            "A": {"mestska_cast": "P05"},
            "B": {"mestska_cast": "P01"},
            "C": {"mestska_cast": "P05"},
        }
        zone_hits = [(zone_a, 0.0), (zone_b, 120.0), (zone_c, 240.0)]

        filtered_hits, reference_area, excluded_count = filter_zone_hits_to_same_area(
            df,
            zsj_mapping,
            zone_hits,
            "A",
        )

        self.assertEqual(reference_area, "P05")
        self.assertEqual(excluded_count, 1)
        self.assertEqual([zone.code for zone, _ in filtered_hits], ["A", "C"])

    def test_radius_zone_hits_are_limited_to_current_data_scope(self):
        zone_a_geom = box(14.4200, 50.0800, 14.4210, 50.0810)
        zone_b_geom = box(14.4230, 50.0800, 14.4240, 50.0810)
        zone_a = Zone("A", zone_a_geom, project_geometry(zone_a_geom))
        zone_b = Zone("B", zone_b_geom, project_geometry(zone_b_geom))
        df = pd.DataFrame([{"kod_useku": "A"}])
        zone_hits = [(zone_a, 0.0), (zone_b, 120.0)]

        filtered_hits, excluded_count, reference_in_scope = filter_zone_hits_to_data_scope(
            df,
            zone_hits,
            "A",
        )

        self.assertEqual([zone.code for zone, _ in filtered_hits], ["A"])
        self.assertEqual(excluded_count, 1)
        self.assertTrue(reference_in_scope)

    def test_mapy_generic_address_label_uses_specific_name(self):
        result = {
            "label": "Adresa",
            "name": "Vodičkova 1/1",
            "location": "Praha 2 - Nové Město, Česko",
        }

        label = format_geocode_result_label(result, "Vodičkova 1, Praha")

        self.assertEqual(label, "Vodičkova 1/1, Praha 2 - Nové Město")

    def test_radius_policy_metrics_surface_pressure_indicators(self):
        snapshot = pd.DataFrame(
            [
                {
                    "kod_useku": "A",
                    "parkovacich_mist_v_zps": 10,
                    "POP_CELKEM": 15,
                    "obsazenost": 0.90,
                    "respektovanost": 0.75,
                },
                {
                    "kod_useku": "B",
                    "parkovacich_mist_v_zps": 30,
                    "POP_CELKEM": 15,
                    "obsazenost": 0.60,
                    "respektovanost": 0.95,
                },
            ]
        )

        metrics = build_radius_policy_metrics(snapshot)

        self.assertAlmostEqual(metrics["permits_per_space"], 0.75)
        self.assertAlmostEqual(metrics["occupancy"], 0.675)
        self.assertAlmostEqual(metrics["respect"], 0.90)
        self.assertEqual(metrics["high_occupancy_zones"], 1)
        self.assertEqual(metrics["low_respect_zones"], 1)

    def test_address_query_params_parse_shareable_selection_seed(self):
        seed = address_query_params_to_selection_seed(
            {
                "address_label": "Vodičkova 1",
                "address_lon": "14.422100",
                "address_lat": "50.081100",
                "address_radius": "900",
                "address_cast": "noc",
                "address_source": "address",
            },
            ["den", "noc"],
        )

        self.assertEqual(seed["label"], "Vodičkova 1")
        self.assertEqual(seed["lon"], 14.4221)
        self.assertEqual(seed["lat"], 50.0811)
        self.assertEqual(seed["radius_m"], 900)
        self.assertEqual(seed["cast_dne"], "noc")

    def test_radius_scenario_row_summarizes_comparison_metrics(self):
        zone_a_geom = box(14.4200, 50.0800, 14.4210, 50.0810)
        zone_a = Zone("A", zone_a_geom, project_geometry(zone_a_geom))
        df = pd.DataFrame(
            [
                {
                    "kod_useku": "A",
                    "date": "2025-01-31",
                    "cast_dne": "den",
                    "parkovacich_mist_v_zps": 10,
                    "POP_CELKEM": 12,
                    "obsazenost": 0.80,
                    "respektovanost": 0.90,
                    "naz_zsj": "ZSJ A",
                    "mestska_cast": "P05",
                    "typ_zony": "RES",
                },
                {
                    "kod_useku": "A",
                    "date": "2025-02-28",
                    "cast_dne": "den",
                    "parkovacich_mist_v_zps": 8,
                    "POP_CELKEM": 12,
                    "obsazenost": 0.95,
                    "respektovanost": 0.70,
                    "naz_zsj": "ZSJ A",
                    "mestska_cast": "P05",
                    "typ_zony": "RES",
                },
            ]
        )
        df["date"] = pd.to_datetime(df["date"])

        row = build_radius_scenario_row(df, [(zone_a, 0.0)], "den", 500)

        self.assertEqual(row["radius_m"], 500)
        self.assertEqual(row["pocet_useku"], 1)
        self.assertEqual(row["mista_v_zps"], 8)
        self.assertEqual(row["zmena_mist"], -2)
        self.assertAlmostEqual(row["opravneni_na_misto"], 1.5)
        self.assertEqual(row["useky_vysoka_obsazenost"], 1)
        self.assertEqual(row["useky_nizka_respektovanost"], 1)

    def test_policy_pressure_ranking_orders_highest_pressure_first(self):
        df = pd.DataFrame(
            [
                {
                    "kod_useku": "A",
                    "date": "2025-02-28",
                    "cast_dne": "den",
                    "parkovacich_mist_v_zps": 10,
                    "POP_CELKEM": 25,
                    "obsazenost": 0.90,
                    "respektovanost": 0.60,
                    "naz_zsj": "ZSJ A",
                    "mestska_cast": "P05",
                    "typ_zony": "RES",
                },
                {
                    "kod_useku": "B",
                    "date": "2025-02-28",
                    "cast_dne": "den",
                    "parkovacich_mist_v_zps": 20,
                    "POP_CELKEM": 10,
                    "obsazenost": 0.50,
                    "respektovanost": 0.95,
                    "naz_zsj": "ZSJ B",
                    "mestska_cast": "P05",
                    "typ_zony": "MIX",
                },
            ]
        )
        df["date"] = pd.to_datetime(df["date"])

        ranking = build_policy_pressure_ranking(df, "den", limit=2)

        self.assertEqual(ranking["kod_useku"].tolist(), ["A", "B"])
        self.assertGreater(ranking["tlakove_skore"].iloc[0], ranking["tlakove_skore"].iloc[1])

    def test_zone_hits_geojson_exports_zone_geometry_and_snapshot_metrics(self):
        zone_a_geom = box(14.4200, 50.0800, 14.4210, 50.0810)
        zone_a = Zone("A", zone_a_geom, project_geometry(zone_a_geom))
        snapshot = pd.DataFrame(
            [
                {
                    "kod_useku": "A",
                    "date": pd.Timestamp("2025-02-28"),
                    "parkovacich_mist_v_zps": 8,
                    "POP_CELKEM": 12,
                }
            ]
        )

        geojson = build_zone_hits_geojson(
            snapshot,
            [(zone_a, 12.4)],
            {"label": "Test", "radius_m": 500, "cast_dne": "den", "zone_code": "A"},
        )

        self.assertIn('"FeatureCollection"', geojson)
        self.assertIn('"kod_useku": "A"', geojson)
        self.assertIn('"vzdalenost_m": 12', geojson)
        self.assertIn('"referencni_usek": true', geojson)

    def test_temporary_capacity_regime_cleanup_reverts_transient_spike(self):
        df = pd.DataFrame(
            [
                {
                    "kod_useku": "P5-1410",
                    "date": "2021-01-31",
                    "cast_dne": "den",
                    "parkovacich_mist_v_zps": 65,
                    "parkovacich_mist_celkem": 70,
                },
                {
                    "kod_useku": "P5-1410",
                    "date": "2021-02-28",
                    "cast_dne": "den",
                    "parkovacich_mist_v_zps": 245,
                    "parkovacich_mist_celkem": 250,
                },
                {
                    "kod_useku": "P5-1410",
                    "date": "2021-02-28",
                    "cast_dne": "Po-Pá (MPD)",
                    "parkovacich_mist_v_zps": 245,
                    "parkovacich_mist_celkem": 250,
                },
                {
                    "kod_useku": "P5-1410",
                    "date": "2022-11-30",
                    "cast_dne": "den",
                    "parkovacich_mist_v_zps": 245,
                    "parkovacich_mist_celkem": 250,
                },
                {
                    "kod_useku": "P5-1410",
                    "date": "2022-12-31",
                    "cast_dne": "den",
                    "parkovacich_mist_v_zps": 66,
                    "parkovacich_mist_celkem": 70,
                },
            ]
        )
        df["date"] = pd.to_datetime(df["date"])

        cleaned = apply_temporary_capacity_regime_cleanup(
            df,
            code_col="kod_useku",
            date_col="date",
            capacity_cols=["parkovacich_mist_v_zps", "parkovacich_mist_celkem"],
        )

        spike_rows = cleaned[cleaned["date"].isin(pd.to_datetime(["2021-02-28", "2022-11-30"]))]
        self.assertEqual(spike_rows["parkovacich_mist_v_zps"].tolist(), [66.0, 66.0, 66.0])
        self.assertEqual(spike_rows["parkovacich_mist_celkem"].tolist(), [70.0, 70.0, 70.0])

    def test_radius_series_fills_internal_zone_gaps_with_stable_capacity(self):
        rows = [
            {
                "kod_useku": "A",
                "date": "2019-04-30",
                "cast_dne": "den",
                "parkovacich_mist_v_zps": 19,
                "naz_zsj": "ZSJ A",
                "mestska_cast": "P05",
                "typ_zony": "RES",
            },
            {
                "kod_useku": "A",
                "date": "2020-05-31",
                "cast_dne": "den",
                "parkovacich_mist_v_zps": 19,
                "naz_zsj": "ZSJ A",
                "mestska_cast": "P05",
                "typ_zony": "RES",
            },
        ]
        for date in pd.date_range("2019-04-30", "2020-05-31", freq="ME"):
            rows.append(
                {
                    "kod_useku": "B",
                    "date": date,
                    "cast_dne": "den",
                    "parkovacich_mist_v_zps": 261,
                    "naz_zsj": "ZSJ B",
                    "mestska_cast": "P05",
                    "typ_zony": "MIX",
                }
            )

        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["date"])

        history = zone_capacity_history(df, ["A", "B"], "den")
        series = radius_spaces_series(df, ["A", "B"], "den")

        may_2019 = history[(history["kod_useku"] == "A") & (history["date"] == "2019-05-31")]
        self.assertEqual(may_2019["parkovacich_mist_v_zps"].iloc[0], 19)

        may_2019_total = series.loc[series["date"] == "2019-05-31", "parkovacich_mist_v_zps"].iloc[0]
        self.assertEqual(may_2019_total, 280)

    def test_zone_capacity_history_trims_to_common_coverage_window(self):
        df = pd.DataFrame(
            [
                {
                    "kod_useku": "A",
                    "date": "2019-04-30",
                    "cast_dne": "den",
                    "parkovacich_mist_v_zps": 19,
                    "naz_zsj": "ZSJ A",
                    "mestska_cast": "P05",
                    "typ_zony": "RES",
                },
                {
                    "kod_useku": "A",
                    "date": "2019-05-31",
                    "cast_dne": "den",
                    "parkovacich_mist_v_zps": 19,
                    "naz_zsj": "ZSJ A",
                    "mestska_cast": "P05",
                    "typ_zony": "RES",
                },
                {
                    "kod_useku": "B",
                    "date": "2019-05-31",
                    "cast_dne": "den",
                    "parkovacich_mist_v_zps": 30,
                    "naz_zsj": "ZSJ B",
                    "mestska_cast": "P05",
                    "typ_zony": "MIX",
                },
                {
                    "kod_useku": "B",
                    "date": "2019-06-30",
                    "cast_dne": "den",
                    "parkovacich_mist_v_zps": 30,
                    "naz_zsj": "ZSJ B",
                    "mestska_cast": "P05",
                    "typ_zony": "MIX",
                },
            ]
        )
        df["date"] = pd.to_datetime(df["date"])

        history = zone_capacity_history(df, ["A", "B"], "den")

        self.assertEqual(history["date"].min(), pd.Timestamp("2019-05-31"))


if __name__ == "__main__":
    unittest.main()
