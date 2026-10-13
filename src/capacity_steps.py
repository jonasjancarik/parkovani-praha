"""List large jumps in zone capacity and say whether each looks like a source error.

Two independent signals are compared for every jump: the number of parked cars
(capacity times occupancy) and the shape of the zone in the monthly exports. A
capacity that multiplies while the shape and the number of cars stay the same is
a wrong denominator; a jump that comes with a new shape is a redrawn zone.

The list is for manual review. Nothing here changes the processed data.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd
from pyproj import Transformer
from shapely import make_valid
from shapely.geometry import shape
from shapely.ops import transform

from src.permit_zone_mapping import PRAGUE_METRIC_CRS, ZONE_FILE_RE

LIKELY_ERROR = "pravděpodobná chyba kapacity"
REDRAWN = "úsek překreslen, kapacita odpovídá ploše"
REDRAWN_MISMATCH = "úsek překreslen, kapacita se změnila jinak než plocha"
CARS_CHANGED = "tvar stejný, změnil se i počet aut"
NO_SHAPE = "tvar úseku chybí"
VERDICT_ORDER = [LIKELY_ERROR, CARS_CHANGED, REDRAWN_MISMATCH, NO_SHAPE, REDRAWN]

_TO_METRIC = Transformer.from_crs("EPSG:4326", PRAGUE_METRIC_CRS, always_xy=True)


@dataclass(frozen=True)
class StepSettings:
    # A jump counts when the larger value exceeds the smaller by this share and count.
    min_jump_ratio: float = 0.50
    min_jump_abs: float = 20.0
    # Observations on each side used for the typical number of parked cars.
    context_observations: int = 3
    cars_rel_tolerance: float = 0.20
    cars_abs_tolerance: float = 3.0
    # Capacity within this distance of a level still belongs to that level.
    level_rel_tolerance: float = 0.10
    level_abs_tolerance: float = 3.0
    # Exports redraw a zone either not at all or substantially; small differences are noise.
    same_shape_overlap: float = 0.90
    # Area only roughly tracks the number of spaces, so only a clear disagreement counts.
    area_match_tolerance: float = 0.50
    # Occupancy above this means the capacity on that side is too low to be true.
    overfull_occupancy: float = 1.20


def monthly_capacity(
    df: pd.DataFrame,
    *,
    code_col: str = "kod_useku",
    date_col: str = "date",
    capacity_col: str = "parkovacich_mist_v_zps",
    occupancy_col: str = "obsazenost",
    occupancy_scale: float = 1.0,
) -> pd.DataFrame:
    """One row per zone and month: capacity and the implied number of parked cars."""
    rows = pd.DataFrame(
        {
            "code": df[code_col],
            "date": pd.to_datetime(df[date_col]),
            "capacity": pd.to_numeric(df[capacity_col], errors="coerce"),
        }
    )
    occupancy = pd.to_numeric(df[occupancy_col], errors="coerce") / occupancy_scale
    rows["cars"] = rows["capacity"] * occupancy
    return (
        rows.groupby(["code", "date"], as_index=False)[["capacity", "cars"]]
        .median()
        .sort_values(["code", "date"])
        .reset_index(drop=True)
    )


def _near(value: float, level: float, settings: StepSettings) -> bool:
    tolerance = max(settings.level_abs_tolerance, abs(level) * settings.level_rel_tolerance)
    return abs(value - level) <= tolerance


def _median(values: np.ndarray) -> float:
    known = values[~np.isnan(values)]
    return float(np.median(known)) if len(known) else float("nan")


def find_capacity_steps(monthly: pd.DataFrame, settings: StepSettings = StepSettings()) -> pd.DataFrame:
    """Every large jump between two consecutive observations of a zone."""
    steps = []
    for code, zone in monthly.dropna(subset=["capacity"]).groupby("code", sort=True):
        dates = zone["date"].to_numpy()
        capacity = zone["capacity"].to_numpy(dtype=float)
        cars = zone["cars"].to_numpy(dtype=float)
        for index in range(1, len(capacity)):
            before, after = capacity[index - 1], capacity[index]
            low, high = sorted((before, after))
            if high - low < settings.min_jump_abs:
                continue
            if high / max(low, 1.0) - 1 < settings.min_jump_ratio:
                continue

            start = index - 1
            while start > 0 and _near(capacity[start - 1], before, settings):
                start -= 1
            end = index
            while end + 1 < len(capacity) and _near(capacity[end + 1], after, settings):
                end += 1

            context = settings.context_observations
            cars_before = _median(cars[max(start, index - context):index])
            cars_after = _median(cars[index:min(end + 1, index + context)])
            cars_tolerance = max(
                settings.cars_abs_tolerance, abs(cars_before) * settings.cars_rel_tolerance
            )
            next_index = end + 1
            steps.append(
                {
                    "code": code,
                    "date": pd.Timestamp(dates[index]),
                    "previous_date": pd.Timestamp(dates[index - 1]),
                    "capacity_before": before,
                    "capacity_after": after,
                    "cars_before": cars_before,
                    "cars_after": cars_after,
                    "cars_stable": bool(abs(cars_after - cars_before) <= cars_tolerance),
                    "months_before": index - start,
                    "months_after": end - index + 1,
                    "starts_series": start == 0,
                    # The new value later gives way to the old one again.
                    "reverts_on": (
                        pd.Timestamp(dates[next_index])
                        if next_index < len(capacity) and _near(capacity[next_index], before, settings)
                        else pd.NaT
                    ),
                    # This jump is itself the way back to a value seen before the old one.
                    "returns_to_earlier": bool(
                        start > 0 and _near(capacity[start - 1], after, settings)
                    ),
                }
            )
    return pd.DataFrame(steps)


@lru_cache(maxsize=4)
def load_month_shapes(zones_dir: Path, month: str) -> dict:
    """Zone shapes of one month in metres, merged across parts of the day."""
    shapes = {}
    for path in sorted(zones_dir.iterdir()):
        match = ZONE_FILE_RE.match(path.name)
        if not match or match.group(2) != month:
            continue
        with open(path, encoding="utf-8") as handle:
            features = json.load(handle).get("features", [])
        for feature in features:
            code = feature.get("properties", {}).get("CODE")
            if not code or code in shapes or not feature.get("geometry"):
                continue
            try:
                shapes[code] = make_valid(transform(_TO_METRIC.transform, shape(feature["geometry"])))
            except Exception:
                continue
    return shapes


def compare_shapes(before, after) -> tuple[float | None, float | None]:
    """Shared share of the two shapes (1 = identical) and how the area changed."""
    if before is None or after is None or before.area == 0:
        return None, None
    union = before.union(after).area
    overlap = before.intersection(after).area / union if union else None
    return overlap, after.area / before.area


def add_shape_evidence(steps: pd.DataFrame, zones_dir: Path) -> pd.DataFrame:
    result = steps.copy()
    overlaps, areas = [], []
    # Sorted by month so that the small cache of loaded months is reused.
    for row in result.sort_values("date").itertuples():
        before = load_month_shapes(zones_dir, row.previous_date.strftime("%Y%m")).get(row.code)
        after = load_month_shapes(zones_dir, row.date.strftime("%Y%m")).get(row.code)
        overlap, area = compare_shapes(before, after)
        overlaps.append((row.Index, overlap))
        areas.append((row.Index, area))
    result["shape_overlap"] = pd.Series(dict(overlaps), dtype=float)
    result["area_ratio"] = pd.Series(dict(areas), dtype=float)
    return result


def classify_steps(steps: pd.DataFrame, settings: StepSettings = StepSettings()) -> pd.DataFrame:
    result = steps.copy()
    verdicts, suspects = [], []
    for row in result.itertuples():
        capacity_ratio = row.capacity_after / max(row.capacity_before, 1.0)
        if pd.isna(row.shape_overlap):
            verdict = NO_SHAPE
        elif row.shape_overlap < settings.same_shape_overlap:
            matches = abs(row.area_ratio / capacity_ratio - 1) <= settings.area_match_tolerance
            verdict = REDRAWN if matches else REDRAWN_MISMATCH
        else:
            verdict = LIKELY_ERROR if row.cars_stable else CARS_CHANGED
        verdicts.append(verdict)

        suspect = ""
        if verdict == LIKELY_ERROR or (verdict == NO_SHAPE and row.cars_stable):
            occupancy_before = row.cars_before / max(row.capacity_before, 1.0)
            occupancy_after = row.cars_after / max(row.capacity_after, 1.0)
            if pd.notna(row.reverts_on):
                suspect = f"po skoku (dočasná hodnota, vrací se {row.reverts_on.date().isoformat()})"
            elif row.returns_to_earlier:
                suspect = "před skokem (návrat k dřívější hodnotě)"
            elif occupancy_before > settings.overfull_occupancy >= occupancy_after:
                suspect = f"před skokem (obsazenost {occupancy_before:.0%})"
            elif occupancy_after > settings.overfull_occupancy >= occupancy_before:
                suspect = f"po skoku (obsazenost {occupancy_after:.0%})"
            elif row.starts_series and row.months_before <= 2:
                suspect = "před skokem (první měsíce měření)"
            else:
                suspect = "nelze určit"
        suspects.append(suspect)
    result["verdict"] = verdicts
    result["suspect_value"] = suspects

    # Many zones of one district redrawn in the same month point to a new survey.
    result["district"] = result["code"].str.split("-").str[0]
    redrawn = result["verdict"].isin([REDRAWN, REDRAWN_MISMATCH])
    result["redrawn_in_district"] = (
        redrawn.groupby([result["district"], result["date"]]).transform("sum").astype(int)
    )
    order = {verdict: position for position, verdict in enumerate(VERDICT_ORDER)}
    result["_order"] = result["verdict"].map(order)
    result["_jump"] = (result["capacity_after"] - result["capacity_before"]).abs()
    return (
        result.sort_values(["_order", "_jump"], ascending=[True, False])
        .drop(columns=["_order", "_jump"])
        .reset_index(drop=True)
    )


def review_table(steps: pd.DataFrame) -> pd.DataFrame:
    """The list as it is saved: Czech headings, rounded values."""
    occupancy_before = steps["cars_before"] / steps["capacity_before"].clip(lower=1)
    occupancy_after = steps["cars_after"] / steps["capacity_after"].clip(lower=1)
    return pd.DataFrame(
        {
            "usek": steps["code"],
            "mesic_skoku": steps["date"].dt.strftime("%Y-%m"),
            "verdikt": steps["verdict"],
            "podezrela_hodnota": steps["suspect_value"],
            "kapacita_pred": steps["capacity_before"].round().astype("Int64"),
            "kapacita_po": steps["capacity_after"].round().astype("Int64"),
            "aut_pred": steps["cars_before"].round().astype("Int64"),
            "aut_po": steps["cars_after"].round().astype("Int64"),
            "obsazenost_pred": occupancy_before.round(2),
            "obsazenost_po": occupancy_after.round(2),
            "mereni_na_hodnote_pred": steps["months_before"],
            "mereni_na_hodnote_po": steps["months_after"],
            "prekryv_tvaru": steps["shape_overlap"].round(3),
            "pomer_plochy": steps["area_ratio"].round(2),
            "prekresleno_useku_v_mc": steps["redrawn_in_district"],
        }
    )


def build_review(parking: pd.DataFrame, zones_dir: Path, settings: StepSettings = StepSettings()) -> pd.DataFrame:
    steps = find_capacity_steps(monthly_capacity(parking), settings)
    if steps.empty:
        return steps
    return classify_steps(add_shape_evidence(steps, zones_dir), settings)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parking", type=Path, default=Path("data/processed/data_parking.csv"))
    parser.add_argument("--zones", type=Path, default=Path("data/downloaded/parked_cars"))
    parser.add_argument(
        "--output", type=Path, default=Path("data/processed/data_parking_capacity_steps.csv")
    )
    args = parser.parse_args()

    parking = pd.read_csv(
        args.parking,
        usecols=["kod_useku", "date", "parkovacich_mist_v_zps", "obsazenost"],
        low_memory=False,
    )
    steps = build_review(parking, args.zones)
    table = review_table(steps)
    table.to_csv(args.output, index=False)
    print(f"{len(table)} capacity jumps in {table['usek'].nunique()} zones -> {args.output}")
    print(table["verdikt"].value_counts().to_string())


if __name__ == "__main__":
    main()
