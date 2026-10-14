import re
import sys
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / ".env")

DATA_DIR = PROJECT_ROOT / "data"
PARKING_PATH = DATA_DIR / "processed" / "data_parking_and_permits.csv"
ZONES_PATH = DATA_DIR / "downloaded" / "parked_cars"
# Written by download.py at the end of every run.
LAST_DOWNLOAD_PATH = DATA_DIR / "downloaded" / "last_download.json"
ZONES_SNAPSHOT_PATH = DATA_DIR / "processed" / "zones.geojson"
ZONES_TO_ZSJ_PATH = DATA_DIR / "useky_zsj_mapping.csv"

ZONE_FILE_RE = re.compile(r"^(P\d{2})-OB_(\d{6})[A-Z]_NA\.json$")
