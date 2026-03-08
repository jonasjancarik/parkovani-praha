import json
import logging
import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from pyproj import Transformer
import requests
from shapely.geometry import Point, shape
from shapely.ops import transform
from shapely.strtree import STRtree

from config import ZONE_FILE_RE, ZONES_PATH, ZONES_TO_ZSJ_PATH

WGS84 = "EPSG:4326"
PRAGUE_METRIC_CRS = "EPSG:5514"
_TO_METRIC = Transformer.from_crs(WGS84, PRAGUE_METRIC_CRS, always_xy=True)


@dataclass(frozen=True)
class Zone:
    code: str
    geometry: Any
    geometry_metric: Any


def project_geometry(geometry: Any) -> Any:
    return transform(_TO_METRIC.transform, geometry)


class ZoneIndex:
    def __init__(self, zones: Dict[str, Zone]) -> None:
        self._zones = zones
        self._zone_list = list(zones.values())
        self._geoms = [zone.geometry for zone in self._zone_list]
        self._geoms_metric = [zone.geometry_metric for zone in self._zone_list]
        self._tree = STRtree(self._geoms) if self._geoms else None
        self._metric_tree = STRtree(self._geoms_metric) if self._geoms_metric else None

    def _zone_from_index(self, index: int) -> Zone:
        return self._zone_list[int(index)]

    def find_zone(self, point: Point) -> Tuple[Optional[Zone], str]:
        if not self._tree:
            return None, "none"

        candidates = self._tree.query(point)
        for index in candidates:
            zone = self._zone_from_index(index)
            if zone.geometry.covers(point):
                return zone, "inside"

        nearest_index = self._tree.nearest(point)
        if nearest_index is None:
            return None, "none"

        return self._zone_from_index(nearest_index), "nearest"

    def find_zones_within_radius(
        self,
        point: Point,
        radius_m: float,
    ) -> List[Tuple[Zone, float]]:
        if not self._metric_tree:
            return []

        point_metric = project_geometry(point)
        search_area = point_metric.buffer(radius_m)
        candidates = self._metric_tree.query(search_area)
        matches: List[Tuple[Zone, float]] = []

        for index in candidates:
            zone = self._zone_from_index(index)
            distance_m = float(zone.geometry_metric.distance(point_metric))
            if distance_m <= radius_m:
                matches.append((zone, distance_m))

        matches.sort(key=lambda item: (item[1], item[0].code))
        return matches


def geocode_with_mapy_cz(query: str) -> Optional[Dict[str, Any]]:
    api_key = os.getenv("MAPY_CZ_API_KEY")
    if not api_key:
        return None

    try:
        for endpoint in ["geocode", "suggest"]:
            data = _mapy_geocode_request(
                endpoint,
                (
                    ("query", query),
                    ("limit", 15),
                    ("locality", "Praha"),
                    ("type", "regional.address"),
                    ("apikey", api_key),
                ),
            )
            if data.get("items"):
                result = data["items"][0]
                result["endpoint"] = endpoint
                return result
        return None
    except Exception as exc:
        logging.error("Geocoding failed for '%s': %s", query, exc)
        return None


@lru_cache(maxsize=256)
def _mapy_geocode_request(endpoint: str, params_key: Tuple[Tuple[str, Any], ...]) -> Dict[str, Any]:
    params = dict(params_key)
    response = requests.get(f"https://api.mapy.cz/v1/{endpoint}", params=params, timeout=10)
    response.raise_for_status()
    return response.json()


def reverse_geocode_with_mapy_cz(lon: float, lat: float) -> Optional[Dict[str, Any]]:
    api_key = os.getenv("MAPY_CZ_API_KEY")
    if not api_key:
        return None

    try:
        data = _mapy_geocode_request(
            "rgeocode",
            (
                ("lon", round(lon, 6)),
                ("lat", round(lat, 6)),
                ("apikey", api_key),
            ),
        )
        if data.get("items"):
            result = data["items"][0]
            result["endpoint"] = "rgeocode"
            return result
        return None
    except Exception as exc:
        logging.error("Reverse geocoding failed for '%s,%s': %s", lon, lat, exc)
        return None


def extract_lon_lat(item: Dict[str, Any]) -> Optional[Tuple[float, float]]:
    position = item.get("position") or item.get("location") or item.get("gps")
    if isinstance(position, dict):
        lon = position.get("lon") or position.get("x")
        lat = position.get("lat") or position.get("y")
        if lon is not None and lat is not None:
            return float(lon), float(lat)
    if isinstance(position, (list, tuple)) and len(position) >= 2:
        return float(position[0]), float(position[1])
    return None


def load_latest_zone_files() -> Dict[str, str]:
    latest: Dict[str, Tuple[int, str]] = {}
    for file_name in os.listdir(ZONES_PATH):
        match = ZONE_FILE_RE.match(file_name)
        if not match:
            continue
        district, yyyymm = match.groups()
        yyyymm_int = int(yyyymm)
        if district not in latest or yyyymm_int > latest[district][0]:
            latest[district] = (yyyymm_int, str(ZONES_PATH / file_name))
    return {district: path for district, (_, path) in latest.items()}


def load_zone_index() -> ZoneIndex:
    files = load_latest_zone_files()
    zones: Dict[str, Zone] = {}
    for path in files.values():
        try:
            with open(path, encoding="utf-8") as handle:
                data = json.load(handle)
        except Exception as exc:
            logging.warning("Skipping %s: %s", path, exc)
            continue

        for feature in data.get("features", []):
            props = feature.get("properties", {})
            code = props.get("CODE")
            geom = feature.get("geometry")
            if not code or not geom:
                continue
            if code in zones:
                continue
            try:
                geometry = shape(geom)
                zones[code] = Zone(
                    code=code,
                    geometry=geometry,
                    geometry_metric=project_geometry(geometry),
                )
            except Exception as exc:
                logging.debug("Invalid geometry for %s: %s", code, exc)

    logging.info("Loaded %s zones from %s files", len(zones), len(files))
    return ZoneIndex(zones)


def load_zsj_mapping() -> Dict[str, Dict[str, Any]]:
    if not ZONES_TO_ZSJ_PATH.exists():
        return {}
    df = pd.read_csv(ZONES_TO_ZSJ_PATH)
    df = df.sort_values("overlap", ascending=False).drop_duplicates("code")
    return df.set_index("code")[["kod_zsj", "naz_zsj", "mestska_cast"]].to_dict("index")
