import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import pandas as pd
import requests
from shapely.geometry import Point, shape
from shapely.strtree import STRtree

from config import ZONE_FILE_RE, ZONES_PATH, ZONES_TO_ZSJ_PATH


@dataclass(frozen=True)
class Zone:
    code: str
    geometry: Any


class ZoneIndex:
    def __init__(self, zones: Dict[str, Zone]) -> None:
        self._zones = zones
        self._geoms = [zone.geometry for zone in zones.values()]
        self._geom_by_id = {id(zone.geometry): zone for zone in zones.values()}
        self._tree = STRtree(self._geoms) if self._geoms else None

    def find_zone(self, point: Point) -> Tuple[Optional[Zone], str]:
        if not self._tree:
            return None, "none"

        candidates = self._tree.query(point)
        for geom in candidates:
            if geom.covers(point):
                return self._geom_by_id[id(geom)], "inside"

        nearest_geom = self._tree.nearest(point)
        if nearest_geom is None:
            return None, "none"

        return self._geom_by_id[id(nearest_geom)], "nearest"


def geocode_with_mapy_cz(query: str) -> Optional[Dict[str, Any]]:
    api_key = os.getenv("MAPY_CZ_API_KEY")
    if not api_key:
        return None

    params = {
        "query": query,
        "limit": 15,
        "locality": "Praha",
        "type": "regional.address",
        "apikey": api_key,
    }

    try:
        for endpoint in ["geocode", "suggest"]:
            response = requests.get(
                f"https://api.mapy.cz/v1/{endpoint}", params=params, timeout=10
            )
            response.raise_for_status()
            data = response.json()
            if data.get("items"):
                result = data["items"][0]
                result["endpoint"] = endpoint
                return result
        return None
    except Exception as exc:
        logging.error("Geocoding failed for '%s': %s", query, exc)
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
                zones[code] = Zone(code=code, geometry=shape(geom))
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
