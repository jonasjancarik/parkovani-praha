"""Create auditable monthly permit-to-zone mapping candidates.

The legacy pipeline joins every month through one all-history mapping CSV. This
module maps buildings against zones active in the same month and only writes
dry-run artifacts under ``/tmp``; promotion remains an explicit review step.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

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
BUILDING_FILE_RE = re.compile(r"^PO_(\d{6})M_TR\.json$")
ALGORITHM_VERSION = "3"


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
                "coordinate_source_month": month,
            }
        )
    buildings = gpd.GeoDataFrame(rows, geometry="geometry", crs=WGS84)
    if buildings.empty:
        raise ValueError(f"Building export contains no usable rows for {month}")
    if buildings["building_code"].duplicated().any():
        raise ValueError(f"Building export contains duplicate codes for {month}")

    missing = set(buildings.loc[buildings.geometry.isna(), "building_code"])
    if missing:
        fallbacks: dict[int, tuple[Point, str]] = {}
        for path in sorted(buildings_dir.glob("PO_*M_TR.json"), reverse=True):
            if path.name >= current_path.name:
                continue
            match = BUILDING_FILE_RE.match(path.name)
            if not match:
                continue
            source_month = match.group(1)
            with path.open(encoding="utf-8") as handle:
                prior_features = json.load(handle).get("features", [])
            for feature in prior_features:
                code = int(feature.get("properties", {}).get("CODE") or 0)
                if code in missing and code not in fallbacks:
                    point = _point_from_feature(feature)
                    if point is not None:
                        fallbacks[code] = (point, source_month)
            if missing.issubset(fallbacks):
                break
        for code, (point, source_month) in fallbacks.items():
            index = buildings.index[buildings["building_code"] == code][0]
            buildings.at[index, "geometry"] = point
            buildings.at[index, "coordinate_fallback"] = True
            buildings.at[index, "coordinate_source_month"] = source_month

    unusable = buildings[buildings.geometry.isna() & (buildings["POP_CELKEM"] > 0)]
    if not unusable.empty:
        codes = unusable["building_code"].astype(str).tolist()[:10]
        raise ValueError(f"Nonzero-permit buildings have no usable coordinates: {', '.join(codes)}")
    return buildings[buildings.geometry.notna()].copy()


def active_zone_codes(parking_csv: Path, month: str) -> set[str]:
    return active_zone_codes_by_month(parking_csv).get(month, set())


def active_zone_codes_by_month(parking_csv: Path) -> dict[str, set[str]]:
    """Read the large parking CSV once and return active zones by month."""

    active: defaultdict[str, set[str]] = defaultdict(set)
    for rows in pd.read_csv(
        parking_csv,
        usecols=["date", "kod_useku"],
        dtype="string",
        chunksize=250_000,
    ):
        rows = rows.assign(month=rows["date"].str.replace("-", "", regex=False).str[:6])
        rows = rows.dropna(subset=["month", "kod_useku"])
        for row_month, codes in rows.groupby("month")["kod_useku"]:
            active[str(row_month)].update(codes.astype(str))
    return dict(active)


def _zone_geometry_paths(zones_dir: Path, month: str) -> list[Path]:
    candidates: list[tuple[int, str, Path]] = []
    for path in zones_dir.glob("*_NA.json"):
        match = ZONE_FILE_RE.match(path.name)
        if match and match.group(2) == month:
            district, _, variant = match.groups()
            candidates.append((ZONE_VARIANT_PRIORITY.get(variant, 99), district, path))
    return [path for _, _, path in sorted(candidates)]


def load_month_zones(zones_dir: Path, month: str, active_codes: set[str]) -> gpd.GeoDataFrame:
    candidates = _zone_geometry_paths(zones_dir, month)
    if not candidates:
        raise FileNotFoundError(f"No zone geometry exports found for {month}")

    rows_by_code: dict[str, dict] = {}
    for path in candidates:
        with path.open(encoding="utf-8") as handle:
            features = json.load(handle).get("features", [])
        for feature in features:
            code = str(feature.get("properties", {}).get("CODE") or "").strip().upper()
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
    """Map each building once, preferring intersection over nearest fallback.

    Both overlapping polygons and equally near fallback polygons are retained
    long enough to mark a spatial tie. The lexicographically smallest zone code
    is then selected so repeated runs are deterministic.
    """

    building_metric = buildings.to_crs(PRAGUE_METRIC_CRS)
    zone_metric = zones.to_crs(PRAGUE_METRIC_CRS)
    zone_targets = zone_metric[["usek_code", "geometry"]]

    intersecting = gpd.sjoin(
        building_metric,
        zone_targets,
        how="inner",
        predicate="intersects",
    )
    intersecting["distance_m"] = 0.0
    intersecting["mapping_method"] = "polygon_intersection"
    intersecting["tie_kind"] = ""

    matched_codes = set(intersecting["building_code"])
    unmatched = building_metric.loc[
        ~building_metric["building_code"].isin(matched_codes)
    ]
    nearest = gpd.sjoin_nearest(
        unmatched,
        zone_metric[["usek_code", "geometry"]],
        how="left",
        distance_col="distance_m",
    )
    nearest["mapping_method"] = "nearest_active_zone"
    nearest["tie_kind"] = ""

    matches = pd.concat([intersecting, nearest], ignore_index=True)
    candidate_counts = matches.groupby("building_code")["usek_code"].transform("size")
    matches["spatial_tie"] = candidate_counts.gt(1)
    matches.loc[
        matches["spatial_tie"] & matches["mapping_method"].eq("polygon_intersection"),
        "tie_kind",
    ] = "overlapping_polygons"
    matches.loc[
        matches["spatial_tie"] & matches["mapping_method"].eq("nearest_active_zone"),
        "tie_kind",
    ] = "equidistant_nearest"
    tie_expansions = len(matches) - len(building_metric)
    selected = (
        matches.sort_values(["building_code", "distance_m", "usek_code"])
        .drop_duplicates("building_code", keep="first")
        .drop(columns=["index_right", "geometry"], errors="ignore")
        .reset_index(drop=True)
    )
    if len(selected) != len(buildings) or selected["building_code"].duplicated().any():
        raise ValueError("Mapping did not produce exactly one target per building")
    if selected["usek_code"].isna().any():
        raise ValueError("Mapping left buildings without a target zone")
    return pd.DataFrame(selected), tie_expansions


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
    active_codes_for_month: set[str] | None = None,
    max_distance_m: float | None = None,
) -> MappingAudit:
    buildings = load_month_buildings(buildings_dir, month)
    active_codes = (
        active_codes_for_month
        if active_codes_for_month is not None
        else active_zone_codes(parking_csv, month)
    )
    if not active_codes:
        raise ValueError(f"Parking data contain no active zones for {month}")
    zones = load_month_zones(zones_dir, month, active_codes)
    mapping, tie_expansions = map_buildings_to_active_zones(buildings, zones)
    # Buildings farther from every active zone than the approved limit are left
    # out of the candidate on purpose; their permits are reported, never lost silently.
    if max_distance_m is None:
        mapping["excluded_by_distance"] = False
    else:
        mapping["excluded_by_distance"] = mapping["distance_m"].gt(max_distance_m)
    excluded = mapping[mapping["excluded_by_distance"]]
    candidate = aggregate_mapped_permits(mapping[~mapping["excluded_by_distance"]], month)
    source_totals = _totals(buildings)
    candidate_totals = _totals(candidate)
    excluded_totals = _totals(excluded)
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
        "coordinate_fallback_source_months": {
            str(source_month): int(count)
            for source_month, count in (
                buildings.loc[
                    buildings["coordinate_fallback"],
                    "coordinate_source_month",
                ]
                .value_counts()
                .sort_index()
                .items()
            )
        },
        "polygon_intersections": int(
            mapping["mapping_method"].eq("polygon_intersection").sum()
        ),
        "nearest_fallbacks": int(
            mapping["mapping_method"].eq("nearest_active_zone").sum()
        ),
        "tie_buildings": int(mapping["spatial_tie"].sum()),
        "tie_expansions": tie_expansions,
        "mapped_buildings": len(mapping),
        "inactive_targets": inactive_targets,
        "source_totals": source_totals,
        "candidate_totals": candidate_totals,
        "excluded_by_distance_buildings": len(excluded),
        "excluded_by_distance_totals": excluded_totals,
        "reconciled": source_totals
        == {k: candidate_totals[k] + excluded_totals[k] for k in source_totals},
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


def available_building_months(buildings_dir: Path) -> list[str]:
    months = []
    for path in buildings_dir.glob("PO_*M_TR.json"):
        match = BUILDING_FILE_RE.match(path.name)
        if match:
            months.append(match.group(1))
    return sorted(set(months))


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _file_record(path: Path) -> dict[str, str | int]:
    return {
        "path": path.as_posix(),
        "bytes": path.stat().st_size,
        "sha256": _sha256_file(path),
    }


def _is_next_month(previous: str, current: str) -> bool:
    return pd.Period(current, freq="M") == pd.Period(previous, freq="M") + 1


def _add_previous_month_comparison(
    mapping: pd.DataFrame,
    previous_mapping: pd.DataFrame | None,
) -> tuple[pd.DataFrame, dict[str, int | float | None]]:
    mapping = mapping.copy()
    if previous_mapping is None:
        mapping["previous_usek_code"] = pd.NA
        mapping["changed_from_previous_month"] = pd.NA
        return mapping, {
            "previous_shared_buildings": None,
            "changed_previous_assignments": None,
            "previous_change_share": None,
        }

    previous_targets = previous_mapping.set_index("building_code")["usek_code"]
    mapping["previous_usek_code"] = mapping["building_code"].map(previous_targets)
    shared = mapping["previous_usek_code"].notna()
    mapping["changed_from_previous_month"] = pd.NA
    mapping.loc[shared, "changed_from_previous_month"] = (
        mapping.loc[shared, "usek_code"]
        != mapping.loc[shared, "previous_usek_code"]
    )
    shared_count = int(shared.sum())
    changed_count = int(mapping.loc[shared, "changed_from_previous_month"].sum())
    return mapping, {
        "previous_shared_buildings": shared_count,
        "changed_previous_assignments": changed_count,
        "previous_change_share": (
            round(changed_count / shared_count, 4) if shared_count else None
        ),
    }


def audit_history(
    *,
    buildings_dir: Path,
    zones_dir: Path,
    parking_csv: Path,
    legacy_mapping_csv: Path | None = None,
    months: Iterable[str] | None = None,
    max_distance_m: float | None = None,
) -> MappingAudit:
    """Audit all requested building months and return one draft release manifest."""

    requested_source = (
        available_building_months(buildings_dir) if months is None else months
    )
    requested_months = sorted(set(requested_source))
    if not requested_months:
        raise ValueError("No building export months were found")
    active_by_month = active_zone_codes_by_month(parking_csv)

    candidates: list[pd.DataFrame] = []
    mappings: list[pd.DataFrame] = []
    monthly_reports: list[dict] = []
    unaudited_months: list[dict[str, str]] = []
    monthly_inputs: list[dict] = []
    previous_month: str | None = None
    previous_mapping: pd.DataFrame | None = None

    for month in requested_months:
        building_path = _building_file(buildings_dir, month)
        geometry_paths = _zone_geometry_paths(zones_dir, month)
        monthly_inputs.append(
            {
                "month": month,
                "building_export": _file_record(building_path),
                "zone_geometry_exports": [
                    _file_record(path) for path in geometry_paths
                ],
                "coordinate_fallback_exports": [],
            }
        )
        if not active_by_month.get(month):
            unaudited_months.append(
                {"month": month, "reason": "missing_parking_observations"}
            )
            previous_month = None
            previous_mapping = None
            continue
        if not geometry_paths:
            unaudited_months.append(
                {"month": month, "reason": "missing_zone_geometry"}
            )
            previous_month = None
            previous_mapping = None
            continue

        result = audit_month(
            month=month,
            buildings_dir=buildings_dir,
            zones_dir=zones_dir,
            parking_csv=parking_csv,
            legacy_mapping_csv=legacy_mapping_csv,
            active_codes_for_month=active_by_month[month],
            max_distance_m=max_distance_m,
        )
        monthly_inputs[-1]["coordinate_fallback_exports"] = [
            _file_record(_building_file(buildings_dir, source_month))
            for source_month in result.report["coordinate_fallback_source_months"]
            if source_month != month
        ]
        comparable_previous = (
            previous_mapping
            if previous_month is not None and _is_next_month(previous_month, month)
            else None
        )
        mapping, comparison = _add_previous_month_comparison(
            result.mapping,
            comparable_previous,
        )
        mapping.insert(0, "month", month)
        report = {
            **result.report,
            "compared_with_month": previous_month if comparable_previous is not None else None,
            **comparison,
        }
        candidates.append(result.candidate)
        mappings.append(mapping)
        monthly_reports.append(report)
        previous_month = month
        previous_mapping = result.mapping

    if candidates:
        candidate = pd.concat(candidates, ignore_index=True)
        mapping = pd.concat(mappings, ignore_index=True)
    else:
        candidate = pd.DataFrame(columns=["date", "kod_useku", *OUTPUT_PERMIT_COLUMNS])
        mapping = pd.DataFrame(
            columns=[
                "month",
                "building_code",
                "usek_code",
                "mapping_method",
                "distance_m",
                "coordinate_fallback",
                "coordinate_source_month",
                "spatial_tie",
                "tie_kind",
                "previous_usek_code",
                "changed_from_previous_month",
                "excluded_by_distance",
                *OUTPUT_PERMIT_COLUMNS,
            ]
        )

    duplicate_candidate_keys = int(candidate.duplicated(["date", "kod_useku"]).sum())
    structural_gates = {
        "all_buildings_mapped_once": all(
            report["mapped_buildings"] == report["source_buildings"]
            for report in monthly_reports
        ),
        "all_targets_active": all(
            not report["inactive_targets"] for report in monthly_reports
        ),
        "all_monthly_totals_reconciled": all(
            report["reconciled"] for report in monthly_reports
        ),
        "candidate_zone_month_keys_unique": duplicate_candidate_keys == 0,
        "complete_source_coverage": not unaudited_months,
    }
    spatial_review = {
        "status": "required",
        "approved_by": None,
        "max_distance_m": max_distance_m,
        "excluded_by_distance_buildings": sum(
            report["excluded_by_distance_buildings"] for report in monthly_reports
        ),
        "excluded_by_distance_permits": sum(
            report["excluded_by_distance_totals"]["POP_CELKEM"]
            for report in monthly_reports
        ),
        "nearest_fallback_assignments": sum(
            report["nearest_fallbacks"] for report in monthly_reports
        ),
        "tie_assignments": sum(report["tie_buildings"] for report in monthly_reports),
        "coordinate_fallback_assignments": sum(
            report["coordinate_fallbacks"] for report in monthly_reports
        ),
        "over_100m_assignments": sum(
            report["distance_scenarios"]["over_100m"]["buildings"]
            for report in monthly_reports
        ),
        "over_500m_assignments": sum(
            report["distance_scenarios"]["over_500m"]["buildings"]
            for report in monthly_reports
        ),
        "over_1000m_assignments": sum(
            report["distance_scenarios"]["over_1000m"]["buildings"]
            for report in monthly_reports
        ),
    }
    blocking_reasons = [
        "spatial_review_not_approved",
    ]
    if unaudited_months:
        blocking_reasons.append("coverage_decision_not_approved")
    structural_failures = [
        gate
        for gate, passed in structural_gates.items()
        if gate != "complete_source_coverage" and not passed
    ]
    if structural_failures:
        blocking_reasons.append("structural_quality_gate_failed")

    report = {
        "schema_version": 1,
        "manifest_status": "draft_review_required",
        "algorithm": {
            "name": "monthly_active_zone_spatial_mapping",
            "version": ALGORITHM_VERSION,
            "metric_crs": PRAGUE_METRIC_CRS,
            "method_order": ["polygon_intersection", "nearest_active_zone"],
            "tie_break": "lexicographically_smallest_zone_code",
            "max_distance_m": max_distance_m,
        },
        "coverage": {
            "requested_months": requested_months,
            "audited_months": [report["month"] for report in monthly_reports],
            "unaudited_months": unaudited_months,
        },
        "inputs": {
            "parking": _file_record(parking_csv),
            "legacy_mapping": (
                _file_record(legacy_mapping_csv)
                if legacy_mapping_csv and legacy_mapping_csv.exists()
                else None
            ),
            "months": monthly_inputs,
        },
        "outputs": {
            "candidate_rows": len(candidate),
            "mapping_rows": len(mapping),
            "duplicate_candidate_zone_month_keys": duplicate_candidate_keys,
        },
        "structural_gates": structural_gates,
        "spatial_review": spatial_review,
        "monthly_reports": monthly_reports,
        "release_decision": {
            "ready": False,
            "blocking_reasons": blocking_reasons,
        },
    }
    return MappingAudit(candidate=candidate, mapping=mapping, report=report)


def _tmp_only(path: Path) -> None:
    if not path.resolve().is_relative_to(Path("/tmp").resolve()):
        raise ValueError(f"Dry-run output must stay under /tmp: {path}")


def _write_csv(rows: pd.DataFrame, path: Path) -> dict[str, str | int]:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows.to_csv(path, index=False)
    record = _file_record(path)
    record["rows"] = len(rows)
    return record


def _write_json_with_hash(payload: dict, path: Path) -> dict[str, str | int]:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    record = _file_record(path)
    checksum_path = Path(f"{path}.sha256")
    checksum_path.write_text(
        f"{record['sha256']}  {path.name}\n",
        encoding="utf-8",
    )
    return record


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["audit", "audit-history"])
    parser.add_argument("--month", help="Month in YYYYMM form")
    parser.add_argument("--start-month", help="First history month in YYYYMM form")
    parser.add_argument("--end-month", help="Last history month in YYYYMM form")
    parser.add_argument("--buildings-dir", type=Path, default=Path("data/downloaded/buildings"))
    parser.add_argument("--zones-dir", type=Path, default=Path("data/downloaded/parked_cars"))
    parser.add_argument(
        "--parking-csv",
        type=Path,
        default=Path("data/processed/data_parking.csv"),
    )
    parser.add_argument(
        "--legacy-mapping-csv",
        type=Path,
        default=Path("data/buildings_useky_mapping.csv"),
    )
    parser.add_argument(
        "--max-distance-m",
        type=float,
        help="Leave out buildings farther than this from every active zone",
    )
    parser.add_argument("--candidate-output", type=Path, required=True)
    parser.add_argument("--mapping-output", type=Path)
    parser.add_argument("--report-output", type=Path)
    parser.add_argument("--manifest-output", type=Path)
    parser.add_argument("--dry-run", action="store_true", required=True)
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args()

    _tmp_only(args.candidate_output)
    if args.mapping_output:
        _tmp_only(args.mapping_output)

    if args.command == "audit":
        if not args.month or not args.report_output:
            parser.error("audit requires --month and --report-output")
        _tmp_only(args.report_output)
        result = audit_month(
            month=args.month,
            buildings_dir=args.buildings_dir,
            zones_dir=args.zones_dir,
            parking_csv=args.parking_csv,
            legacy_mapping_csv=args.legacy_mapping_csv,
            max_distance_m=args.max_distance_m,
        )
        result.report["algorithm_version"] = ALGORITHM_VERSION
        result.report["outputs"] = {
            "candidate": _write_csv(result.candidate, args.candidate_output),
            "mapping": (
                _write_csv(result.mapping, args.mapping_output)
                if args.mapping_output else None
            ),
        }
        _write_json_with_hash(result.report, args.report_output)
        print(json.dumps(result.report, indent=2, ensure_ascii=False))
        if args.strict and (
            not result.report["reconciled"] or result.report["inactive_targets"]
        ):
            raise SystemExit(1)
        return

    if not args.mapping_output or not args.manifest_output:
        parser.error("audit-history requires --mapping-output and --manifest-output")
    _tmp_only(args.manifest_output)
    months = available_building_months(args.buildings_dir)
    if args.start_month:
        months = [month for month in months if month >= args.start_month]
    if args.end_month:
        months = [month for month in months if month <= args.end_month]
    result = audit_history(
        buildings_dir=args.buildings_dir,
        zones_dir=args.zones_dir,
        parking_csv=args.parking_csv,
        legacy_mapping_csv=args.legacy_mapping_csv,
        months=months,
        max_distance_m=args.max_distance_m,
    )
    result.report["outputs"].update(
        {
            "candidate": _write_csv(result.candidate, args.candidate_output),
            "mapping": _write_csv(result.mapping, args.mapping_output),
        }
    )
    _write_json_with_hash(result.report, args.manifest_output)
    print(json.dumps(result.report["release_decision"], indent=2, ensure_ascii=False))
    if args.strict and not all(result.report["structural_gates"].values()):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
