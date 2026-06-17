from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence

import numpy as np
import pandas as pd
from shapely.geometry import Point

from data import radius_latest_snapshot, radius_spaces_series

GENERIC_GEOCODE_LABELS = {"Adresa"}
OCCUPANCY_PRESSURE_THRESHOLD = 0.85
LOW_RESPECT_THRESHOLD = 0.80
MIN_RADIUS_M = 100
MAX_RADIUS_M = 1500
DEFAULT_RADIUS_M = 500
ADDRESS_QUERY_PARAM_KEYS = {
    "address_label",
    "address_lon",
    "address_lat",
    "address_radius",
    "address_cast",
    "address_source",
}


@dataclass
class RadiusScope:
    zone_hits: list
    reference_area: Optional[str]
    area_excluded_count: int
    data_scope_excluded_count: int
    reference_in_scope: bool


def format_int(value: float) -> str:
    return f"{value:,.0f}".replace(",", " ")


def format_signed_int(value: float) -> str:
    if value > 0:
        return f"+{format_int(value)}"
    if value < 0:
        return f"-{format_int(abs(value))}"
    return "0"


def safe_total(series: pd.Series) -> float:
    if series.empty:
        return 0.0
    numeric = pd.to_numeric(series, errors="coerce")
    return float(numeric.fillna(0).sum())


def format_ratio(value: Optional[float]) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"{value:.2f}"


def format_pct(value: Optional[float]) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"{value:.0%}"


def format_point_label(lon: float, lat: float) -> str:
    return f"Bod z mapy ({lat:.5f}, {lon:.5f})"


def format_geocode_result_label(
    result: Optional[dict],
    fallback: str,
) -> str:
    if not result:
        return fallback

    label = result.get("label")
    if label and label not in GENERIC_GEOCODE_LABELS:
        return str(label)

    name = result.get("name")
    if name:
        location = result.get("location")
        if location:
            locality = str(location).split(",")[0].strip()
            if locality and locality not in str(name):
                return f"{name}, {locality}"
        return str(name)

    return fallback


def clamp_radius(radius_m: object) -> int:
    try:
        radius = int(radius_m)
    except (TypeError, ValueError):
        return DEFAULT_RADIUS_M
    return max(MIN_RADIUS_M, min(MAX_RADIUS_M, radius))


def address_result_to_query_params(result: Optional[dict]) -> dict[str, str]:
    if not result:
        return {}
    return {
        "address_label": str(result.get("label") or ""),
        "address_lon": f"{float(result['lon']):.6f}",
        "address_lat": f"{float(result['lat']):.6f}",
        "address_radius": str(clamp_radius(result.get("radius_m"))),
        "address_cast": str(result.get("cast_dne") or ""),
        "address_source": str(result.get("source") or "address"),
    }


def address_query_params_to_selection_seed(
    params: dict,
    cast_dne_values: Sequence[str],
) -> Optional[dict]:
    lon = params.get("address_lon")
    lat = params.get("address_lat")
    if lon is None or lat is None:
        return None

    try:
        lon_value = float(lon)
        lat_value = float(lat)
    except (TypeError, ValueError):
        return None

    cast_dne = params.get("address_cast")
    if cast_dne not in cast_dne_values:
        cast_dne = cast_dne_values[0] if cast_dne_values else None
    if not cast_dne:
        return None

    return {
        "label": params.get("address_label") or format_point_label(lon_value, lat_value),
        "lon": lon_value,
        "lat": lat_value,
        "radius_m": clamp_radius(params.get("address_radius")),
        "cast_dne": cast_dne,
        "source": params.get("address_source") or "shared-link",
    }


def address_selection_signature(result: Optional[dict]) -> Optional[str]:
    if not result:
        return None
    return "|".join(
        [
            f"{float(result['lon']):.6f}",
            f"{float(result['lat']):.6f}",
            str(clamp_radius(result.get("radius_m"))),
            str(result.get("cast_dne") or ""),
            str(result.get("label") or ""),
            str(result.get("source") or ""),
        ]
    )


def build_zone_hits_table(
    snapshot: pd.DataFrame,
    zone_hits,
) -> pd.DataFrame:
    distances = {zone.code: distance_m for zone, distance_m in zone_hits}
    table = snapshot.copy()
    table["vzdalenost_m"] = (
        table["kod_useku"].map(distances).fillna(0).round().astype(int)
    )
    if {"POP_CELKEM", "parkovacich_mist_v_zps"}.issubset(table.columns):
        spaces = pd.to_numeric(table["parkovacich_mist_v_zps"], errors="coerce")
        permits = pd.to_numeric(table["POP_CELKEM"], errors="coerce")
        table["opravneni_na_misto"] = np.where(spaces > 0, permits / spaces, np.nan)
    table = table.rename(
        columns={
            "kod_useku": "Úsek",
            "naz_zsj": "ZSJ",
            "mestska_cast": "MČ",
            "typ_zony": "Typ zóny",
            "POP_CELKEM": "Oprávnění",
            "parkovacich_mist_v_zps": "Místa v ZPS",
            "opravneni_na_misto": "Oprávnění / místo",
            "obsazenost": "Obsazenost",
            "respektovanost": "Respektovanost",
            "vzdalenost_m": "Vzdálenost (m)",
        }
    )
    return table.sort_values(["Vzdálenost (m)", "Úsek"]).reset_index(drop=True)


def build_zone_area_lookup(
    data: pd.DataFrame,
    zsj_mapping,
    zone_codes,
) -> dict[str, str]:
    codes = list(dict.fromkeys(zone_codes))
    if not codes:
        return {}

    lookup = (
        data.loc[data["kod_useku"].isin(codes), ["kod_useku", "mestska_cast"]]
        .dropna(subset=["mestska_cast"])
        .drop_duplicates(subset=["kod_useku"])
        .set_index("kod_useku")["mestska_cast"]
        .to_dict()
    )
    for code in codes:
        mapped_area = zsj_mapping.get(code, {}).get("mestska_cast")
        if mapped_area:
            lookup[code] = mapped_area
    return lookup


def filter_zone_hits_to_same_area(
    data: pd.DataFrame,
    zsj_mapping,
    zone_hits,
    reference_zone_code: str,
) -> tuple[list, Optional[str], int]:
    zone_codes = [zone.code for zone, _ in zone_hits] + [reference_zone_code]
    area_lookup = build_zone_area_lookup(data, zsj_mapping, zone_codes)
    reference_area = area_lookup.get(reference_zone_code)
    if not reference_area:
        return zone_hits, None, 0

    filtered_hits = [
        (zone, distance_m)
        for zone, distance_m in zone_hits
        if area_lookup.get(zone.code) == reference_area
    ]
    excluded_count = len(zone_hits) - len(filtered_hits)
    return filtered_hits, reference_area, excluded_count


def filter_zone_hits_to_data_scope(
    data: pd.DataFrame,
    zone_hits,
    reference_zone_code: str,
) -> tuple[list, int, bool]:
    available_codes = set(data["kod_useku"].dropna().unique())
    filtered_hits = [
        (zone, distance_m)
        for zone, distance_m in zone_hits
        if zone.code in available_codes
    ]
    excluded_count = len(zone_hits) - len(filtered_hits)
    return filtered_hits, excluded_count, reference_zone_code in available_codes


def build_radius_scope(
    data: pd.DataFrame,
    zsj_mapping,
    raw_zone_hits,
    reference_zone_code: str,
) -> RadiusScope:
    scoped_zone_hits, data_scope_excluded_count, reference_in_scope = (
        filter_zone_hits_to_data_scope(data, raw_zone_hits, reference_zone_code)
    )
    zone_hits, reference_area, area_excluded_count = filter_zone_hits_to_same_area(
        data,
        zsj_mapping,
        scoped_zone_hits,
        reference_zone_code,
    )
    return RadiusScope(
        zone_hits=zone_hits,
        reference_area=reference_area,
        area_excluded_count=area_excluded_count,
        data_scope_excluded_count=data_scope_excluded_count,
        reference_in_scope=reference_in_scope,
    )


def build_radius_scope_for_point(
    data: pd.DataFrame,
    zsj_mapping,
    zone_index,
    lon: float,
    lat: float,
    radius_m: int,
    reference_zone_code: str,
) -> RadiusScope:
    point = Point(lon, lat)
    raw_zone_hits = zone_index.find_zones_within_radius(point, radius_m)
    return build_radius_scope(data, zsj_mapping, raw_zone_hits, reference_zone_code)


def weighted_average(
    df: pd.DataFrame,
    value_col: str,
    weight_col: str,
) -> Optional[float]:
    if value_col not in df.columns or weight_col not in df.columns:
        return None

    values = pd.to_numeric(df[value_col], errors="coerce")
    weights = pd.to_numeric(df[weight_col], errors="coerce")
    mask = values.notna() & weights.notna() & (weights > 0)
    if not mask.any():
        return None

    return float((values[mask] * weights[mask]).sum() / weights[mask].sum())


def build_radius_policy_metrics(snapshot: pd.DataFrame) -> dict:
    spaces = safe_total(snapshot.get("parkovacich_mist_v_zps", pd.Series(dtype=float)))
    permits = safe_total(snapshot.get("POP_CELKEM", pd.Series(dtype=float)))
    permits_per_space = permits / spaces if spaces > 0 else None

    occupancy = weighted_average(
        snapshot,
        "obsazenost",
        "parkovacich_mist_v_zps",
    )
    respect = weighted_average(
        snapshot,
        "respektovanost",
        "parkovacich_mist_v_zps",
    )

    high_occupancy_zones = 0
    if "obsazenost" in snapshot.columns:
        occupancy_values = pd.to_numeric(snapshot["obsazenost"], errors="coerce")
        high_occupancy_zones = int(
            (occupancy_values >= OCCUPANCY_PRESSURE_THRESHOLD).sum()
        )

    low_respect_zones = 0
    if "respektovanost" in snapshot.columns:
        respect_values = pd.to_numeric(snapshot["respektovanost"], errors="coerce")
        low_respect_zones = int((respect_values < LOW_RESPECT_THRESHOLD).sum())

    return {
        "permits_per_space": permits_per_space,
        "occupancy": occupancy,
        "respect": respect,
        "high_occupancy_zones": high_occupancy_zones,
        "low_respect_zones": low_respect_zones,
    }


def build_radius_scenario_row(
    data: pd.DataFrame,
    zone_hits,
    cast_dne: str,
    radius_m: int,
) -> dict:
    zone_codes = [zone.code for zone, _ in zone_hits]
    series = radius_spaces_series(data, zone_codes, cast_dne)
    snapshot = radius_latest_snapshot(data, zone_codes, cast_dne)
    metrics = build_radius_policy_metrics(snapshot)

    latest_spaces = 0.0
    spaces_delta = 0.0
    if not series.empty:
        latest_spaces = safe_total(series.iloc[[-1]]["parkovacich_mist_v_zps"])
        oldest_spaces = safe_total(series.iloc[[0]]["parkovacich_mist_v_zps"])
        spaces_delta = latest_spaces - oldest_spaces

    return {
        "radius_m": int(radius_m),
        "pocet_useku": len(zone_hits),
        "mista_v_zps": latest_spaces,
        "zmena_mist": spaces_delta,
        "opravneni_na_misto": metrics["permits_per_space"],
        "obsazenost": metrics["occupancy"],
        "respektovanost": metrics["respect"],
        "useky_vysoka_obsazenost": metrics["high_occupancy_zones"],
        "useky_nizka_respektovanost": metrics["low_respect_zones"],
    }


def build_policy_pressure_ranking(
    data: pd.DataFrame,
    cast_dne: Optional[str],
    limit: int = 15,
) -> pd.DataFrame:
    zone_codes = sorted(data["kod_useku"].dropna().unique())
    snapshot = radius_latest_snapshot(data, zone_codes, cast_dne)
    if snapshot.empty:
        return snapshot

    spaces = pd.to_numeric(snapshot["parkovacich_mist_v_zps"], errors="coerce")
    permits = pd.to_numeric(snapshot.get("POP_CELKEM"), errors="coerce")
    occupancy = pd.to_numeric(snapshot.get("obsazenost"), errors="coerce")
    respect = pd.to_numeric(snapshot.get("respektovanost"), errors="coerce")

    snapshot = snapshot.copy()
    snapshot["opravneni_na_misto"] = np.where(spaces > 0, permits / spaces, np.nan)
    snapshot["nedostatek_respektu"] = 1 - respect
    snapshot["tlakove_skore"] = (
        snapshot["opravneni_na_misto"].fillna(0)
        + occupancy.fillna(0)
        + snapshot["nedostatek_respektu"].fillna(0)
    )
    snapshot = snapshot.sort_values(
        ["tlakove_skore", "opravneni_na_misto", "obsazenost"],
        ascending=[False, False, False],
    )
    return snapshot.head(limit).reset_index(drop=True)
