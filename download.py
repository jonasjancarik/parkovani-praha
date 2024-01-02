import requests
import os
from dotenv import load_dotenv

load_dotenv()
USER = os.getenv("TSK_USERNAME")
PASSWORD = os.getenv("TSK_PASSWORD")

YEARS = [2023]
START_MONTH = 11
END_MONTH = 11
INCLUDE_QUARTERLY = False
INCLUDE_SECTIONS = False  # úseky

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

filename_templates = [
    "OB_YYYYMMD_NA.json",
    "OB_YYYYMMD_NJ.json",
    "OB_YYYYMMD_QA.json",
    "OB_YYYYMMD_QJ.json",
    "OB_YYYYMMN_NA.json",
    "OB_YYYYMMN_NJ.json",
    "OB_YYYYMMN_QA.json",
    "OB_YYYYMMN_QJ.json",
    "OB_YYYYMMP_NA.json",
    "OB_YYYYMMP_NJ.json",
    "OB_YYYYMMS_QA.json",
    "OB_YYYYMMS_QJ.json",
    "OB_YYYYMMW_NA.json",
    "OB_YYYYMMW_NJ.json",
    "OB_YYYYMMW_QA.json",
    "OB_YYYYMMW_QJ.json",
    "OB_YYYYMMX_NA.json",
    "OB_YYYYMMX_NJ.json",
    "OB_YYYYMMX_QA.json",
    "OB_YYYYMMX_QJ.json",
]

if not INCLUDE_QUARTERLY:
    filename_templates = [
        filename
        for filename in filename_templates
        if not '_Q' in filename
    ]

if not INCLUDE_SECTIONS:
    filename_templates = [
        filename
        for filename in filename_templates
        if not 'J.json' in filename
    ]

url_template = "https://zps.tsk-praha.cz/puzzle/genmaps/DISTRICT/FILENAME"

if not os.path.exists("data"):
    os.makedirs("data/downloaded")

total_files = len(districts) * len(filename_templates) * ( END_MONTH - START_MONTH + 1 ) * len(YEARS)
counter = 0

for district_index, district in enumerate(districts):
    for filename_index, filename in enumerate(filename_templates):
        for year in YEARS:
            for month in range(START_MONTH, END_MONTH + 1):
                filename = filename.replace("YYYY", str(year)).replace(
                    "MM", str(month).zfill(2)
                )
                url = url_template.replace("DISTRICT", district).replace(
                    "FILENAME", filename
                )
                r = requests.get(url, auth=(USER, PASSWORD))
                # check response status code
                if r.status_code != 200:
                    print(f"Error while downloading {district}-{filename}")
                    continue
                with open(
                    f"./data/downloaded/{district}-{filename}.json",
                    "w",
                    encoding="utf-8",
                ) as f:
                    f.write(r.text)

                counter += 1
                print(f"Progress: {counter}/{total_files}")
