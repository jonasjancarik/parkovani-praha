from __future__ import annotations

import sys
from datetime import date
from functools import lru_cache
from pathlib import Path

import pandas as pd
from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.staticfiles import StaticFiles

WEB_APP_DIR = Path(__file__).resolve().parent
if str(WEB_APP_DIR) not in sys.path:
    sys.path.insert(0, str(WEB_APP_DIR))

from address_logic import DEFAULT_RADIUS_M, MAX_RADIUS_M, MIN_RADIUS_M  # noqa: E402
from address_report import (  # noqa: E402
    LAT_RANGE,
    LON_RANGE,
    address_options,
    build_address_report,
    point_label,
    search_address,
)
from data import load_parking_data  # noqa: E402
from explorer_analytics import build_overview_analytics  # noqa: E402
from explorer_data import build_explorer_payload, make_filters  # noqa: E402
from geo import GeocodingError, load_zone_index, load_zsj_mapping  # noqa: E402


app = FastAPI(title="Parkování Praha API", version="0.1.0")


@lru_cache(maxsize=1)
def parking_data() -> pd.DataFrame:
    return load_parking_data()


@app.get("/api/health")
def health(response: Response) -> dict[str, str | int]:
    try:
        data = parking_data()
        required = {
            "date",
            "kod_useku",
            "POP_CELKEM",
            "parkovacich_mist_v_zps",
        }
        missing = required.difference(data.columns)
        if data.empty or missing:
            response.status_code = 503
            return {"status": "error", "detail": "parking data are incomplete"}
        return {
            "status": "ok",
            "rows": len(data),
            "latest_date": data["date"].max().date().isoformat(),
        }
    except Exception:
        response.status_code = 503
        return {"status": "error", "detail": "parking data are unavailable"}


@lru_cache(maxsize=16)
def explorer_payload(filters) -> dict:
    data = parking_data()
    payload = build_explorer_payload(data, filters)
    payload["analytics"] = build_overview_analytics(data, filters)
    return payload


@app.get("/api/explorer")
def explorer(
    start: date | None = None,
    end: date | None = None,
    cast_dne: list[str] | None = Query(default=None),
    district: list[str] | None = Query(default=None),
    zone_type: list[str] | None = Query(default=None),
) -> dict:
    filters = make_filters(start, end, cast_dne, district, zone_type)
    return explorer_payload(filters)


@lru_cache(maxsize=1)
def zone_index():
    return load_zone_index()


@lru_cache(maxsize=1)
def zsj_mapping() -> dict:
    return load_zsj_mapping()


@app.get("/api/address/options")
def address_report_options() -> dict:
    return address_options(parking_data())


@app.get("/api/address/search")
def address_search(q: str = Query(min_length=3, max_length=120)) -> dict:
    if not address_options(parking_data())["search_available"]:
        raise HTTPException(status_code=503, detail="address search is not configured")
    try:
        result = search_address(q.strip())
    except GeocodingError:
        raise HTTPException(status_code=502, detail="address search failed") from None
    if not result:
        raise HTTPException(status_code=404, detail="address not found")
    return result


@lru_cache(maxsize=64)
def address_report_payload(
    lon: float, lat: float, radius: int, cast_dne: str | None, label: str | None
) -> dict | None:
    return build_address_report(
        parking_data(),
        zone_index(),
        zsj_mapping(),
        lon=lon,
        lat=lat,
        radius_m=radius,
        cast_dne=cast_dne,
        label=label or point_label(lon, lat),
    )


@app.get("/api/address/report")
def address_report(
    lon: float = Query(ge=LON_RANGE[0], le=LON_RANGE[1]),
    lat: float = Query(ge=LAT_RANGE[0], le=LAT_RANGE[1]),
    radius: int = Query(default=DEFAULT_RADIUS_M, ge=MIN_RADIUS_M, le=MAX_RADIUS_M),
    cast_dne: str | None = None,
    label: str | None = Query(default=None, max_length=160),
) -> dict:
    options = address_options(parking_data())
    if cast_dne not in options["cast_dne"]:
        cast_dne = options["default_cast_dne"]
    payload = address_report_payload(round(lon, 6), round(lat, 6), radius, cast_dne, label)
    if payload is None:
        raise HTTPException(status_code=404, detail="no parking zone found")
    return payload


FRONTEND_DIST = WEB_APP_DIR.parent / "frontend" / "dist"
if FRONTEND_DIST.is_dir():
    app.mount("/", StaticFiles(directory=FRONTEND_DIST, html=True), name="frontend")
