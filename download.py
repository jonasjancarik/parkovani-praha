import requests
import os
from dotenv import load_dotenv
import datetime

load_dotenv()
USER = os.getenv("TSK_USERNAME")
PASSWORD = os.getenv("TSK_PASSWORD")

TYPE_OF_DATA = "PARKING_SPACES"  # one of PARKED_CARS, PARKING_PERMITS, PARKING_SPACES

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

if TYPE_OF_DATA == "PARKED_CARS":
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
            filename for filename in filename_templates if "_Q" not in filename
        ]

    if not INCLUDE_SECTIONS:
        filename_templates = [
            filename for filename in filename_templates if "J.json" not in filename
        ]

    url_template = "https://zps.tsk-praha.cz/puzzle/genmaps/DISTRICT/FILENAME"

    if not os.path.exists("data"):
        os.makedirs("data/downloaded")

    # load list of files to skip from data/skip.txt
    skip = []
    if os.path.exists("data/skip.txt"):
        with open("data/skip.txt", "r", encoding="utf-8") as f:
            skip = f.read().splitlines()

    # get list of already downloaded files
    downloaded_files = os.listdir("data/downloaded")

    # generate list of filenames
    filenames = []

    for district_index, district in enumerate(districts):
        for filename_index, filename_template in enumerate(filename_templates):
            for year in YEARS:
                for month in range(START_MONTH, END_MONTH + 1):
                    filename = filename_template.replace("YYYY", str(year)).replace(
                        "MM", str(month).zfill(2)
                    )
                    filename_complete = f"{district}-{filename}"
                    if (
                        filename_complete not in skip
                        and filename_complete not in downloaded_files
                    ):
                        filenames.append(filename_complete)

    total_files = len(filenames)

    for counter, filename in enumerate(filenames):
        district = filename.split("-")[0]
        filename_server = filename.split("-")[1]
        url = url_template.replace("DISTRICT", district).replace(
            "FILENAME", filename_server
        )
        r = requests.get(url, auth=(USER, PASSWORD), timeout=60)
        counter += 1
        # check response status code
        if r.status_code != 200:
            print(f"Error while downloading {district}-{filename}")
            # add to skip list
            skip.append(f"{filename}")
            # save skip list
            with open("data/skip.txt", "w", encoding="utf-8") as f:
                f.write("\n".join(skip))
            continue
        with open(f"data/downloaded/{filename}", "w", encoding="utf-8") as f:
            f.write(r.text)

        print(f"Progress: {counter}/{total_files}")

    print("Done")

elif TYPE_OF_DATA == "PARKING_PERMITS":
    index_url = "https://zps.tsk-praha.cz/geodata/stats/POP/"

    # create directory if it does not exist
    if not os.path.exists("data/downloaded/permits"):
        os.makedirs("data/downloaded/permits")
        downloaded_files = []

    else:
        # get list of already downloaded files
        downloaded_files = os.listdir("data/downloaded/permits")

    # generate list of filenames
    filenames = []

    filename_template = "P00_YYYYMMPOP.tsv"

    START_YEAR = 2019
    START_MONTH = "4"

    # loop through months and years until we reach current month

    current_year = datetime.datetime.now().year
    current_month = datetime.datetime.now().month
    for year in range(START_YEAR, current_year + 1):
        for month in range(1, 13):
            if year == START_YEAR and month < int(START_MONTH):
                continue
            if year == current_year and month > current_month:
                continue
            filename = filename_template.replace("YYYY", str(year)).replace(
                "MM", str(month).zfill(2)
            )
            if filename not in downloaded_files:
                filenames.append(filename)

    print(f"Downloading {len(filenames)} files")

    for counter, filename in enumerate(filenames):
        url = index_url + filename
        r = requests.get(url, auth=(USER, PASSWORD), timeout=60)
        counter += 1
        # check response status code
        if r.status_code != 200:
            print(f"Error while downloading {filename}")
            continue

        with open(f"data/downloaded/permits/{filename}", "w", encoding="utf-8") as f:
            f.write(r.text)

        print(f"Progress: {counter}/{len(filenames)}", end="\r")

    print("\nDone")


if TYPE_OF_DATA == "PARKING_SPACES":
    # the URLs look like this: https://zps.tsk-praha.cz/geodata/stats/PM/P00-202401PM2.tsv
    # let's cycle through the years and months and download the data
    # the data is in the form of a tab-separated file

    # create directory if it does not exist
    if not os.path.exists("data/downloaded/spaces"):
        os.makedirs("data/downloaded/spaces")
        downloaded_files = []
    else:
        # get list of already downloaded files
        downloaded_files = os.listdir("data/downloaded/spaces")

    # generate list of filenames
    filenames = []

    filename_template = "P00-YYYYMMPM2.tsv"

    START_YEAR = 2018
    START_MONTH = 1
    current_year = datetime.datetime.now().year
    current_month = datetime.datetime.now().month

    # loop through months and years until we reach current month
    for year in range(START_YEAR, current_year + 1):
        for month in range(1, 13):
            if year == START_YEAR and month < int(START_MONTH):
                continue
            if year == current_year and month > current_month:
                continue
            filename = filename_template.replace("YYYY", str(year)).replace(
                "MM", str(month).zfill(2)
            )
            if filename not in downloaded_files:
                filenames.append(filename)

    print(f"Downloading {len(filenames)} files")

    for counter, filename in enumerate(filenames):
        url = "https://zps.tsk-praha.cz/geodata/stats/PM/" + filename
        r = requests.get(url, auth=(USER, PASSWORD), timeout=60)
        counter += 1
        # check response status code
        if r.status_code != 200:
            print(f"Error while downloading {filename}")
            continue

        with open(f"data/downloaded/spaces/{filename}", "w", encoding="utf-8") as f:
            f.write(r.text)

        print(f"Progress: {counter}/{len(filenames)}", end="\r")

    print("\nDone")
