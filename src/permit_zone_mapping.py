"""Create auditable monthly permit-to-zone mapping candidates.

The legacy pipeline joins every month through one all-history mapping CSV. This
module maps one month against zones active in that month and only writes dry-run
artifacts under ``/tmp``; promotion remains an explicit review step.
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely import make_valid
from shapely.geometry import Point, shape


WGS84 = "EPSG:4326"
PRAGUE_METRIC_CRS = "EPSG:5514"
PERMIT_COLUMNS = {
    "R": "pop_rezidentska",
    "V": "pop_vlastnicka",
    "A": "pop_abonentska",
    "P": "pop_prenosna",
    "C": "pop_carsharing",
    "E": "pop_ekologicka",
    "O": "pop_ostatni",
    "S": "pop_socialni",
}
OUTPUT_PERMIT_COLUMNS = ["POP_CELKEM", *PERMIT_COLUMNS.values()]
ZONE_FILE_RE = re.compile(r"^(P\d{2})-OB_(\d{6})([DNPWSX])_NA\.json$")
ZONE_VARIANT_PRIORITY = {code: index for index, code in enumerate("DNWXPS")}


@dataclass(frozen=True)
class MappingAudit:
    candidate: pd.DataFrame
    mapping: pd.DataFrame
    report: dict


def _permit_values(properties: dict) -> dict[str, int]:
    legends = str(properties.get("GraphLegend", "")).strip("[]").split(",")
    values = str(properties.get("GraphData", "")).strip("[]").split(",")
    parsed = {column: 0 for column in PERMIT_COLUMNS.values()}
    for legend, value in zip(legends, values):
        column = PERMIT_COLUMNS.get(legend)
        if column:
            parsed[column] = int(float(value))
    parsed["POP_CELKEM"] = int(float(properties.get("XSUM", sum(parsed.values()))))
    return parsed


def _building_file(buildings_dir: Path, month: str) -> Path:
    path = buildings_dir / f"PO_{month}M_TR.json"
    if not path.exists():
        raise FileNotFoundError(f"Building export is missing for {month}: {path}")
    return path


def _point_from_feature(feature: dict) -> Point | None:
    geometry = feature.get("geometry")
    if not geometry or geometry.get("type") != "Point" or not geometry.get("coordinates"):
        return None
    return Point(geometry["coordinates"])


def load_month_buildings(buildings_dir: Path, month: str) -> gpd.GeoDataFrame:
    current_path = _building_file(buildings_dir, month)
    with current_path.open(encoding="utf-8") as handle:
        features = json.load(handle).get("features", [])

    rows: list[dict] = []
    for feature in features:
        properties = feature.get("properties", {})
        code = int(properties.get("CODE") or 0)
        if code <= 0:
            continue
        rows.append(
            {
                "building_code": code,
                **_permit_values(properties),
                "geometry": _point_from_feature(feature),
                "coordinate_fallback": False,
            }
        )
    buildings = gpd.GeoDataFrame(rows, geometry="geometry", crs=WGS84)
    if buildings.empty:
        raise ValueError(f"Building export contains no usable rows for {month}")
    if buildings["building_code"].duplicated().any():
        raise ValueError(f"Building export contains duplicate codes for {month}")

    missing = set(buildings.loc[buildings.geometry.isna(), "building_code"])
    if missing:
        fallbacks: dict[int, Point] = {}
        for path in sorted(buildings_dir.glob("PO_*M_TR.json"), reverse=True):
            if path.name >= current_path.name:
                continue
            with path.open(encoding="utf-8") as handle:
                prior_features = json.load(handle).get("features", [])
            for feature in prior_features:
                code = int(feature.get("properties", {}).get("CODE") or 0)
                if code in missing and code not in fallbacks:
                    point = _point_from_feature(feature)
                    if point is not None:
                        fallbacks[code] = point
            if missing.issubset(fallbacks):
                break
        for code, point in fallbacks.items():
            index = buildings.index[buildings["building_code"] == code][0]
            buildings.at[index, "geometry"] = point
            buildings.at[index, "coordinate_fallback"] = True

    unusable = buildings[buildings.geometry.isna() & (buildings["POP_CELKEM"] > 0)]
    if not unusable.empty:
        codes = unusable["building_code"].astype(str).tolist()[:10]
        raise ValueError(f"Nonzero-permit buildings have no usable coordinates: {', '.join(codes)}")
    return buildings[buildings.geometry.notna()].copy()


def active_zone_codes(parking_csv: Path, month: str) -> set[str]:
    rows = pd.read_csv(parking_csv, usecols=["date", "kod_useku"], dtype="string")
    date_prefix = f"{month[:4]}-{month[4:]}"
    return set(
        rows.loc[rows["date"].str.startswith(date_prefix, na=False), "kod_useku"]
        .dropna()
    )


def load_month_zones(zones_dir: Path, month: str, active_codes: set[str]) -> gpd.GeoDataFrame:
    candidates: list[tuple[int, str, Path]] = []
    for path in zones_dir.glob("*_NA.json"):
        match = ZONE_FILE_RE.match(path.name)
        if match and match.group(2) == month:
            district, _, variant = match.groups()
            candidates.append((ZONE_VARIANT_PRIORITY.get(variant, 99), district, path))
    if not candidates:
        raise FileNotFoundError(f"No zone geometry exports found for {month}")

    rows_by_code: dict[str, dict] = {}
    for _, _, path in sorted(candidates):
        with path.open(encoding="utf-8") as handle:
            features = json.load(handle).get("features", [])
        for feature in features:
            code = str(feature.get("properties", {}).get("CODE") or "")
            if code not in active_codes or code in rows_by_code or not feature.get("geometry"):
                continue
            geometry = make_valid(shape(feature["geometry"]))
            if not geometry.is_empty:
                rows_by_code[code] = {"usek_code": code, "geometry": geometry}

    missing = sorted(active_codes.difference(rows_by_code))
    if missing:
        raise ValueError(f"Active zones lack same-month geometry ({len(missing)}): {', '.join(missing[:10])}")
    return gpd.GeoDataFrame(list(rows_by_code.values()), geometry="geometry", crs=WGS84)


def map_buildings_to_active_zones(
    buildings: gpd.GeoDataFrame,
    zones: gpd.GeoDataFrame,
) -> tuple[pd.DataFrame, int]:
    building_metric = buildings.to_crs(PRAGUE_METRIC_CRS)
    zone_metric = zones.to_crs(PRAGUE_METRIC_CRS)
    joined = gpd.sjoin_nearest(
        building_metric,
        zone_metric[["usek_code", "geometry"]],
        how="left",
        distance_col="distance_m",
    )
    tie_expansions = len(joined) - len(building_metric)
    joined = (
        joined.sort_values(["building_code", "distance_m", "usek_code"])
        .drop_duplicates("building_code", keep="first")
        .drop(columns=["index_right", "geometry"], errors="ignore")
    )
    if len(joined) != len(buildings) or joined["building_code"].duplicated().any():
        raise ValueError("Mapping did not produce exactly one target per building")
    if joined["usek_code"].isna().any():
        raise ValueError("Mapping left buildings without a target zone")
    return pd.DataFrame(joined), tie_expansions


def aggregate_mapped_permits(mapping: pd.DataFrame, month: str) -> pd.DataFrame:
    candidate = (
        mapping.groupby("usek_code", as_index=False)[OUTPUT_PERMIT_COLUMNS]
        .sum()
        .rename(columns={"usek_code": "kod_useku"})
    )
    candidate.insert(0, "date", pd.Period(month, freq="M").end_time.date().isoformat())
    return candidate


def _totals(rows: pd.DataFrame) -> dict[str, int]:
    return {column: int(rows[column].sum()) for column in OUTPUT_PERMIT_COLUMNS}


def _distance_scenarios(mapping: pd.DataFrame) -> dict[str, dict[str, int | float]]:
    total_permits = max(int(mapping["POP_CELKEM"].sum()), 1)
    total_buildings = max(len(mapping), 1)
    scenarios: dict[str, dict[str, int | float]] = {}
    for threshold in (100, 500, 1000):
        distant = mapping[mapping["distance_m"] > threshold]
        permits = int(distant["POP_CELKEM"].sum())
        scenarios[f"over_{threshold}m"] = {
            "buildings": len(distant),
            "building_share": round(len(distant) / total_buildings, 4),
            "permits": permits,
            "permit_share": round(permits / total_permits, 4),
        }
    return scenarios


def audit_month(
    *,
    month: str,
    buildings_dir: Path,
    zones_dir: Path,
    parking_csv: Path,
    legacy_mapping_csv: Path | None = None,
) -> MappingAudit:
    buildings = load_month_buildings(buildings_dir, month)
    active_codes = active_zone_codes(parking_csv, month)
    zones = load_month_zones(zones_dir, month, active_codes)
    mapping, tie_expansions = map_buildings_to_active_zones(buildings, zones)
    candidate = aggregate_mapped_permits(mapping, month)
    source_totals = _totals(buildings)
    candidate_totals = _totals(candidate)
    inactive_targets = sorted(set(mapping["usek_code"]).difference(active_codes))

    changed_assignments = None
    legacy_shared_buildings = None
    if legacy_mapping_csv and legacy_mapping_csv.exists():
        legacy = pd.read_csv(legacy_mapping_csv).drop_duplicates("building_code")
        comparison = mapping[["building_code", "usek_code"]].merge(
            legacy,
            on="building_code",
            how="inner",
            suffixes=("_candidate", "_legacy"),
        )
        changed_assignments = int(
            (comparison["usek_code_candidate"] != comparison["usek_code_legacy"]).sum()
        )
        legacy_shared_buildings = len(comparison)

    report = {
        "month": month,
        "source_buildings": len(buildings),
        "active_zones": len(active_codes),
        "geometry_zones": len(zones),
        "coordinate_fallbacks": int(buildings["coordinate_fallback"].sum()),
        "tie_expansions": tie_expansions,
        "mapped_buildings": len(mapping),
        "inactive_targets": inactive_targets,
        "source_totals": source_totals,
        "candidate_totals": candidate_totals,
        "reconciled": source_totals == candidate_totals,
        "changed_legacy_assignments": changed_assignments,
        "legacy_shared_buildings": legacy_shared_buildings,
        "legacy_change_share": (
            round(changed_assignments / legacy_shared_buildings, 4)
            if changed_assignments is not None and legacy_shared_buildings else None
        ),
        "max_distance_m": round(float(mapping["distance_m"].max()), 2),
        "distance_scenarios": _distance_scenarios(mapping),
    }
    return MappingAudit(candidate=candidate, mapping=mapping, report=report)


def _tmp_only(path: Path) -> None:
    if not path.resolve().is_relative_to(Path("/tmp").resolve()):
        raise ValueError(f"Dry-run output must stay under /tmp: {path}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["audit"])
    parser.add_argument("--month", required=True, help="Month in YYYYMM form")
    parser.add_argument("--buildings-dir", type=Path, default=Path("data/downloaded/buildings"))
    parser.add_argument("--zones-dir", type=Path, default=Path("data/downloaded/parked_cars"))
    parser.add_argument("--parking-csv", type=Path, default=Path("data/processed/data_parking.csv"))
    parser.add_argument("--legacy-mapping-csv", type=Path, default=Path("data/buildings_useky_mapping.csv"))
    parser.add_argument("--candidate-output", type=Path, required=True)
    parser.add_argument("--report-output", type=Path, required=True)
    parser.add_argument("--dry-run", action="store_true", required=True)
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args()

    _tmp_only(args.candidate_output)
    _tmp_only(args.report_output)
    result = audit_month(
        month=args.month,
        buildings_dir=args.buildings_dir,
        zones_dir=args.zones_dir,
        parking_csv=args.parking_csv,
        legacy_mapping_csv=args.legacy_mapping_csv,
    )
    args.candidate_output.parent.mkdir(parents=True, exist_ok=True)
    args.report_output.parent.mkdir(parents=True, exist_ok=True)
    result.candidate.to_csv(args.candidate_output, index=False)
    args.report_output.write_text(
        json.dumps(result.report, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(json.dumps(result.report, indent=2, ensure_ascii=False))
    if args.strict and (not result.report["reconciled"] or result.report["inactive_targets"]):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
