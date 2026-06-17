from __future__ import annotations

import json
import re
from typing import Any, Optional

import numpy as np
import pandas as pd
from shapely.geometry import mapping

from address_logic import build_zone_hits_table


def export_file_stem(address_result: Optional[dict]) -> str:
    label = (address_result or {}).get("label") or "address-radius"
    normalized = re.sub(r"[^0-9A-Za-z]+", "-", str(label).strip()).strip("-").lower()
    if not normalized:
        normalized = "address-radius"
    return normalized[:60]


def _json_safe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (np.integer, np.floating)):
        value = value.item()
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()
    if isinstance(value, float) and pd.isna(value):
        return None
    if pd.isna(value):
        return None
    return value


def build_zone_hits_csv(snapshot: pd.DataFrame, zone_hits) -> bytes:
    table = build_zone_hits_table(snapshot, zone_hits)
    return table.to_csv(index=False).encode("utf-8-sig")


def build_zone_hits_geojson(
    snapshot: pd.DataFrame,
    zone_hits,
    address_result: Optional[dict],
) -> str:
    snapshot_lookup = {}
    if not snapshot.empty:
        snapshot_lookup = snapshot.set_index("kod_useku").to_dict("index")

    features = []
    for zone, distance_m in zone_hits:
        properties = {
            "kod_useku": zone.code,
            "vzdalenost_m": round(float(distance_m)),
            "referencni_usek": bool(
                address_result and zone.code == address_result.get("zone_code")
            ),
        }
        for key, value in snapshot_lookup.get(zone.code, {}).items():
            properties[key] = _json_safe(value)

        features.append(
            {
                "type": "Feature",
                "geometry": mapping(zone.geometry),
                "properties": properties,
            }
        )

    collection = {
        "type": "FeatureCollection",
        "properties": {
            "label": (address_result or {}).get("label"),
            "radius_m": (address_result or {}).get("radius_m"),
            "cast_dne": (address_result or {}).get("cast_dne"),
        },
        "features": features,
    }
    return json.dumps(collection, ensure_ascii=False)
