from __future__ import annotations

import sys
from datetime import date
from functools import lru_cache
from pathlib import Path

import pandas as pd
from fastapi import FastAPI, Query
from fastapi.staticfiles import StaticFiles

WEB_APP_DIR = Path(__file__).resolve().parent
if str(WEB_APP_DIR) not in sys.path:
    sys.path.insert(0, str(WEB_APP_DIR))

from data import load_parking_data  # noqa: E402
from explorer_analytics import build_overview_analytics  # noqa: E402
from explorer_data import build_explorer_payload, make_filters  # noqa: E402


app = FastAPI(title="Parkování Praha API", version="0.1.0")


@lru_cache(maxsize=1)
def parking_data() -> pd.DataFrame:
    return load_parking_data()


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/explorer")
def explorer(
    start: date | None = None,
    end: date | None = None,
    cast_dne: list[str] | None = Query(default=None),
    district: list[str] | None = Query(default=None),
    zone_type: list[str] | None = Query(default=None),
) -> dict:
    filters = make_filters(start, end, cast_dne, district, zone_type)
    data = parking_data()
    payload = build_explorer_payload(data, filters)
    payload["analytics"] = build_overview_analytics(data, filters)
    return payload


FRONTEND_DIST = WEB_APP_DIR.parent / "frontend" / "dist"
if FRONTEND_DIST.is_dir():
    app.mount("/", StaticFiles(directory=FRONTEND_DIST, html=True), name="frontend")
