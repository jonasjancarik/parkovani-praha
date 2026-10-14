import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from web_app.city_overview import build_city_overview


def overview_sample(months=("2025-03-31", "2026-02-28", "2026-03-31")) -> pd.DataFrame:
    rows = []
    for index, month in enumerate(months):
        for zone, district, spaces, permits in [
            ("A", "P01", 1000, 1500 + 100 * index),
            ("B", "P02", 2000, 2000),
            ("C", "P13", 50, 10),
        ]:
            for cast_dne, residents, visitors in [("den", 60, 40), ("noc", 90, 10)]:
                rows.append(
                    {
                        "kod_useku": zone,
                        "date": month,
                        "mestska_cast": district,
                        "typ_zony": "RES",
                        "cast_dne": cast_dne,
                        "POP_CELKEM": permits,
                        "parkovacich_mist_v_zps": spaces,
                        "rezidentska": residents,
                        "navstevnici": visitors,
                    }
                )
    frame = pd.DataFrame(rows)
    frame["date"] = pd.to_datetime(frame["date"])
    return frame


def test_city_figures_compare_with_the_same_month_a_year_earlier() -> None:
    payload = build_city_overview(overview_sample())

    assert payload["latest_date"] == "2026-03-31"
    city = payload["city"]
    assert (city["permits"], city["spaces"]) == (3710, 3050)
    assert city["compared_with"] == "2025-03-31"
    assert city["permits_change"] == pytest.approx(200 / 3510)
    assert city["spaces_change"] == 0
    assert city["ratio_change"] == pytest.approx(3710 / 3050 - 3510 / 3050, abs=0.001)


def test_city_figures_have_no_change_without_a_month_to_compare() -> None:
    payload = build_city_overview(overview_sample(months=("2026-02-28", "2026-03-31")))

    assert payload["city"]["compared_with"] is None
    assert payload["city"]["permits_change"] is None
    assert payload["city"]["ratio_change"] is None
    assert payload["districts"][0]["ratio_change"] is None


def test_district_ranking_leaves_out_districts_with_few_spaces() -> None:
    districts = build_city_overview(overview_sample())["districts"]

    assert [row["district"] for row in districts] == ["P01", "P02"]
    assert districts[0]["permits_per_space"] == 1.7
    assert districts[0]["ratio_change"] == pytest.approx(0.2)
    assert districts[1]["ratio_change"] == 0


def test_parker_shares_are_reported_for_day_and_night() -> None:
    parkers = {row["period"]: row for row in build_city_overview(overview_sample())["parkers"]}

    assert set(parkers) == {"den", "noc"}
    day = {group["series"]: group["share"] for group in parkers["den"]["groups"]}
    night = {group["series"]: group["share"] for group in parkers["noc"]["groups"]}
    assert day == {"Rezidenti": pytest.approx(0.6), "Návštěvníci": pytest.approx(0.4)}
    assert night["Rezidenti"] == pytest.approx(0.9)


def test_overview_carries_the_download_date() -> None:
    assert build_city_overview(overview_sample(), "2026-10-13")["downloaded_on"] == "2026-10-13"
    assert build_city_overview(overview_sample())["downloaded_on"] is None
