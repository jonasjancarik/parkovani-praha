import requests
import os
from dotenv import load_dotenv

load_dotenv()

DOWNLOAD_FILES = True

districts = [
    "P01",
    "P02",
    "P03",
    "P04",
    "P05",
    "P06",
    "P07",
    "P08",
    "P09",
    "P10",
    "P13",
    "P16",
    "P18",
    "P22",
]
filenames = ["OR_202311D_NJ", "OR_202311N_NJ"]

if DOWNLOAD_FILES:

    USER = os.getenv("TSK_USERNAME")
    PASSWORD = os.getenv("TSK_PASSWORD")

    url_template = "https://zps.tsk-praha.cz/puzzle/genmaps/DISTRICT/FILENAME.json"

    if not os.path.exists("data"):
        os.makedirs("data/downloaded")

    for district in districts:
        for filename in filenames:
            url = url_template.replace("DISTRICT", district).replace("FILENAME", filename)
            r = requests.get(url, auth=(USER, PASSWORD))
            with open(f"./data/{district}-{filename}.json", "w", encoding="utf-8") as f:
                f.write(r.text)

