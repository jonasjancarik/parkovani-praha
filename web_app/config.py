import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
PARKING_PATH = DATA_DIR / "processed" / "data_parking_and_permits.csv"
ZONES_PATH = DATA_DIR / "downloaded" / "parked_cars"
ZONES_TO_ZSJ_PATH = DATA_DIR / "useky_zsj_mapping.csv"

ZONE_FILE_RE = re.compile(r"^(P\d{2})-OB_(\d{6})[A-Z]_NA\.json$")
