import pandas as pd
import pytest
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from join import preprocess_data


def parking_rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"kod_useku": "P1-A", "date": "2026-06-30", "cast_dne": "den"},
            {"kod_useku": "P1-A", "date": "2026-06-30", "cast_dne": "noc"},
        ]
    )


def test_permit_join_accepts_one_zone_month_for_multiple_observations() -> None:
    permits = pd.DataFrame(
        [{"kod_useku": "P1-A", "date": "2026-06-30", "POP_CELKEM": 12}]
    )

    joined = preprocess_data(parking_rows(), permits)

    assert joined["POP_CELKEM"].tolist() == [12, 12]


def test_permit_join_rejects_inactive_zone_targets() -> None:
    permits = pd.DataFrame(
        [{"kod_useku": "P1-OLD", "date": "2026-06-30", "POP_CELKEM": 12}]
    )

    with pytest.raises(ValueError, match="inactive zone-month"):
        preprocess_data(parking_rows(), permits)


def test_permit_join_rejects_duplicate_zone_months() -> None:
    permits = pd.DataFrame(
        [
            {"kod_useku": "P1-A", "date": "2026-06-30", "POP_CELKEM": 12},
            {"kod_useku": "P1-A", "date": "2026-06-30", "POP_CELKEM": 3},
        ]
    )

    with pytest.raises(ValueError, match="duplicate zone-month"):
        preprocess_data(parking_rows(), permits)
