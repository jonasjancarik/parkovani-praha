from __future__ import annotations

import os
from typing import Optional

import pandas as pd
from shapely.geometry import Point, mapping

from address_logic import (
    DEFAULT_RADIUS_M,
    LOW_RESPECT_THRESHOLD,
    MAX_RADIUS_M,
    MIN_RADIUS_M,
    OCCUPANCY_PRESSURE_THRESHOLD,
    build_radius_policy_metrics,
    build_radius_scope_for_point,
    clamp_radius,
    format_geocode_result_label,
    safe_total,
)
from constants import POP_MEASURES
from data import (
    clip_to_common_period,
    radius_latest_snapshot,
    zone_filled_history,
    zone_rows_for_cast_dne,
)
from geo import (
    extract_lon_lat,
    geocode_with_mapy_cz,
    project_geometry,
    reverse_geocode_with_mapy_cz,
)

DEFAULT_CAST_DNE = "den"
RADIUS_STEP_M = 100
COMPARISON_RADII = (100, 300, 500, 800, 1000, 1500)
# Rough box around Prague; points outside cannot have a zone nearby.
LON_RANGE = (14.15, 14.80)
LAT_RANGE = (49.90, 50.25)
SPACES = "parkovacich_mist_v_zps"
# Enough for capacity history and the latest snapshot; the full rows are only
# needed for the detail of the zone the address belongs to.
RADIUS_COLUMNS = [
    "kod_useku",
    "naz_zsj",
    "mestska_cast",
    "typ_zony",
    "cast_dne",
    "date",
    "POP_CELKEM",
    SPACES,
    "obsazenost",
    "respektovanost",
]
PARKER_GROUPS = ["rezidentska", "vlastnicka", "abonentska", "navstevnici", "volna_mista"]
PERMIT_TYPES = list(POP_MEASURES.values())


def _number(value: object, digits: int = 0) -> int | float | None:
    if value is None or pd.isna(value):
        return None
    number = float(value)
    return int(round(number)) if digits == 0 else round(number, digits)


def _iso(value: object) -> Optional[str]:
    if value is None or pd.isna(value):
        return None
    return pd.Timestamp(value).date().isoformat()


def _text(value: object) -> Optional[str]:
    if value is None or pd.isna(value):
        return None
    return str(value)


def address_options(data: pd.DataFrame) -> dict:
    cast_values = sorted(data["cast_dne"].dropna().unique().tolist())
    default_cast = DEFAULT_CAST_DNE if DEFAULT_CAST_DNE in cast_values else None
    if default_cast is None and cast_values:
        default_cast = cast_values[0]
    return {
        "cast_dne": cast_values,
        "default_cast_dne": default_cast,
        "radius": {
            "min": MIN_RADIUS_M,
            "max": MAX_RADIUS_M,
            "step": RADIUS_STEP_M,
            "default": DEFAULT_RADIUS_M,
        },
        "bounds": {"lon": list(LON_RANGE), "lat": list(LAT_RANGE)},
        "latest_date": _iso(data["date"].max()),
        "search_available": bool(os.getenv("MAPY_CZ_API_KEY")),
    }


def search_address(query: str) -> Optional[dict]:
    result = geocode_with_mapy_cz(query, raise_errors=True)
    if not result:
        return None
    coords = extract_lon_lat(result)
    if not coords:
        return None
    return {
        "label": format_geocode_result_label(result, query),
        "lon": coords[0],
        "lat": coords[1],
    }


def point_label(lon: float, lat: float) -> Optional[str]:
    """Name a point picked on the map; None when no address is known for it."""
    result = reverse_geocode_with_mapy_cz(lon, lat)
    return format_geocode_result_label(result, "") or None


def _zone_rows(hits, snapshot: pd.DataFrame, history: pd.DataFrame, reference_code: str) -> list[dict]:
    """One row per zone; `history` holds each zone's own months, gaps as None."""
    latest = snapshot.set_index("kod_useku").to_dict("index") if not snapshot.empty else {}
    histories = (
        {code: group.sort_values("date") for code, group in history.groupby("kod_useku")}
        if not history.empty
        else {}
    )
    rows = []
    for zone, distance_m in hits:
        row = latest.get(zone.code, {})
        spaces = _number(row.get(SPACES))
        permits = _number(row.get("POP_CELKEM"))
        zone_history = histories.get(zone.code)
        history = None
        if zone_history is not None:
            values = zone_history.set_index("date")[SPACES]
            months = pd.date_range(values.index.min(), values.index.max(), freq="ME")
            history = {
                "start": _iso(months[0]),
                "values": [_number(value) for value in values.reindex(months)],
            }
        rows.append(
            {
                "code": zone.code,
                "is_reference": zone.code == reference_code,
                "distance_m": int(round(distance_m)),
                "locality": _text(row.get("naz_zsj")),
                "district": _text(row.get("mestska_cast")),
                "zone_type": _text(row.get("typ_zony")),
                "spaces": spaces,
                "permits": permits,
                "permits_per_space": (
                    round(permits / spaces, 2) if permits is not None and spaces else None
                ),
                "occupancy": _number(row.get("obsazenost"), 3),
                "respect": _number(row.get("respektovanost"), 3),
                "history": history,
            }
        )
    return rows


def _reference_series(data: pd.DataFrame, code: str, cast_dne: Optional[str]) -> list[dict]:
    rows = zone_rows_for_cast_dne(data, [code], cast_dne)
    if rows.empty:
        return []
    series = []
    for row in rows.sort_values("date").to_dict("records"):
        series.append(
            {
                "date": _iso(row["date"]),
                "permits": _number(row.get("POP_CELKEM")),
                "spaces": _number(row.get(SPACES)),
                "occupancy": _number(row.get("obsazenost"), 3),
                "respect": _number(row.get("respektovanost"), 3),
                "parkers": {key: _number(row.get(key), 1) for key in PARKER_GROUPS},
                "permit_types": {key: _number(row.get(key)) for key in PERMIT_TYPES},
            }
        )
    return series


def build_address_report(
    data: pd.DataFrame,
    zone_index,
    zsj_mapping,
    *,
    lon: float,
    lat: float,
    radius_m: int,
    cast_dne: Optional[str],
    label: Optional[str] = None,
) -> Optional[dict]:
    point = Point(lon, lat)
    reference, match_type = zone_index.find_zone(point)
    if reference is None:
        return None

    radius_m = clamp_radius(radius_m)
    radii = sorted({*COMPARISON_RADII, radius_m})
    widest_hits = zone_index.find_zones_within_radius(point, max(radii))
    nearby_codes = {zone.code for zone, _ in widest_hits} | {reference.code}
    nearby_full = data[data["kod_useku"].isin(nearby_codes)]
    nearby = nearby_full[[column for column in RADIUS_COLUMNS if column in nearby_full.columns]]

    scopes = {
        radius: build_radius_scope_for_point(
            nearby, zsj_mapping, zone_index, lon, lat, radius, reference.code
        )
        for radius in radii
    }
    scope = scopes[radius_m]
    hits = scope.zone_hits
    codes = [zone.code for zone, _ in hits]

    # Gap filling is the slow part and does not depend on the radius, so it runs
    # once for the widest circle and each radius only picks its zones from it.
    widest_codes = [zone.code for zone, _ in scopes[max(radii)].zone_hits]
    filled = zone_filled_history(nearby, widest_codes, cast_dne)
    latest = radius_latest_snapshot(nearby, widest_codes, cast_dne)

    snapshot = latest[latest["kod_useku"].isin(codes)] if not latest.empty else latest
    history = filled[filled["kod_useku"].isin(codes)] if not filled.empty else filled
    metrics = build_radius_policy_metrics(snapshot)

    # The total only covers months with data for every zone in the circle;
    # otherwise a zone that appears later would look like added capacity.
    series = []
    if not history.empty:
        totals = clip_to_common_period(history).groupby("date")[SPACES].sum().sort_index()
        series = [{"date": _iso(date), "spaces": _number(value)} for date, value in totals.items()]

    comparison = []
    for radius in radii:
        radius_hits = scopes[radius].zone_hits
        radius_snapshot = latest[latest["kod_useku"].isin([zone.code for zone, _ in radius_hits])]
        radius_metrics = build_radius_policy_metrics(radius_snapshot)
        comparison.append(
            {
                "radius_m": radius,
                "zones": len(radius_hits),
                "spaces": _number(safe_total(radius_snapshot[SPACES])),
                "permits_per_space": _number(radius_metrics["permits_per_space"], 2),
                "occupancy": _number(radius_metrics["occupancy"], 3),
                "respect": _number(radius_metrics["respect"], 3),
                "high_occupancy_zones": radius_metrics["high_occupancy_zones"],
                "low_respect_zones": radius_metrics["low_respect_zones"],
            }
        )

    reference_meta = zsj_mapping.get(reference.code, {})
    reference_distance = reference.geometry_metric.distance(project_geometry(point))
    features = [
        {
            "type": "Feature",
            "geometry": mapping(zone.geometry),
            "properties": {"code": zone.code, "is_reference": zone.code == reference.code},
        }
        for zone, _ in hits
    ]

    return {
        "selection": {
            "label": label,
            "lon": lon,
            "lat": lat,
            "radius_m": radius_m,
            "cast_dne": cast_dne,
        },
        "reference": {
            "code": reference.code,
            "inside": match_type == "inside",
            "distance_m": int(round(reference_distance)),
            "district": scope.reference_area or _text(reference_meta.get("mestska_cast")),
            "locality": _text(reference_meta.get("naz_zsj")),
            "in_radius": reference.code in codes,
            "series": _reference_series(nearby_full, reference.code, cast_dne),
        },
        "scope": {
            "other_district_zones": scope.area_excluded_count,
            "zones_without_data": scope.data_scope_excluded_count,
        },
        "summary": {
            "zones": len(hits),
            "spaces": _number(safe_total(snapshot[SPACES])) if not snapshot.empty else 0,
            "permits": _number(safe_total(snapshot["POP_CELKEM"])) if not snapshot.empty else 0,
            "permits_per_space": _number(metrics["permits_per_space"], 2),
            "occupancy": _number(metrics["occupancy"], 3),
            "respect": _number(metrics["respect"], 3),
            "high_occupancy_zones": metrics["high_occupancy_zones"],
            "low_respect_zones": metrics["low_respect_zones"],
            "spaces_change": (series[-1]["spaces"] - series[0]["spaces"]) if series else None,
            "series_start": series[0]["date"] if series else None,
            "series_end": series[-1]["date"] if series else None,
            "latest_date": _iso(snapshot["date"].max()) if not snapshot.empty else None,
        },
        "thresholds": {
            "occupancy": OCCUPANCY_PRESSURE_THRESHOLD,
            "respect": LOW_RESPECT_THRESHOLD,
        },
        "series": series,
        "zones": _zone_rows(hits, snapshot, history, reference.code),
        "geometry": {"type": "FeatureCollection", "features": features},
        "comparison": comparison,
    }
