import requests
import os
from dotenv import load_dotenv
import datetime
import sys
import argparse

load_dotenv()
USER = os.getenv("TSK_USERNAME")
PASSWORD = os.getenv("TSK_PASSWORD")

# load cli argument for type of data to download
# if not provided, default to PARKING
# if provided, check if it is one of the valid options
# if not, print error message and exit

parser = argparse.ArgumentParser(description="Download data for parking in Prague.")
parser.add_argument(
    "type_of_data",
    choices=["PARKING", "PARKING_PERMITS", "PARKING_SPACES", "HOUSES"],
    help="Type of data to download",
)
parser.add_argument(
    "--start_year",
    type=int,
    default=2018,
    help="Year to start downloading data from",
)
parser.add_argument(
    "--end_year",
    type=int,
    default=datetime.datetime.now().year,
    help="Year to end downloading data from",
)
parser.add_argument(
    "--include_quarterly",
    action="store_true",
    help="Include quarterly data",
)
parser.add_argument(
    "--include-sections",
    action="store_true",
    help="Include section data (úseky)",
)

args = parser.parse_args()

TYPE_OF_DATA = args.type_of_data

# build list of years to download data for
# if start_year is greater than end_year, print error message and exit
# if start_year is greater than current year, print error message and exit
# if end_year is greater than current year, set it to current year

if args.start_year > args.end_year:
    print("Start year must be less than or equal to end year")
    sys.exit()
if args.start_year > datetime.datetime.now().year:
    print("Start year must be less than or equal to current year")
    sys.exit()

if args.end_year > datetime.datetime.now().year:
    args.end_year = datetime.datetime.now().year

YEARS = list(range(args.start_year, args.end_year + 1))

# set start month to 1 (hardcoded)
START_MONTH = 1

# set end month to 12, unless it's the current year, then set it to the current month
if args.end_year == datetime.datetime.now().year:
    END_MONTH = datetime.datetime.now().month
else:
    END_MONTH = 12

INCLUDE_QUARTERLY = args.include_quarterly
INCLUDE_SECTIONS = args.include_sections

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


def session_setup(session):
    session.auth = (USER, PASSWORD)
    session.timeout = 60
    return session


if TYPE_OF_DATA == "PARKING":
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
            filename for filename in filename_templates if "A.json" not in filename
        ]

    url_template = "https://zps.tsk-praha.cz/puzzle/genmaps/DISTRICT/FILENAME"

    if not os.path.exists("data/downloaded/parked_cars"):
        os.makedirs("data/downloaded/parked_cars")

    # load list of files to skip from data/skip.txt
    skip = []
    if os.path.exists("data/skip.txt"):
        with open("data/skip.txt", "r", encoding="utf-8") as f:
            skip = f.read().splitlines()

    # get list of already downloaded files
    downloaded_files = os.listdir("data/downloaded/parked_cars")

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

    # create a session
    with requests.Session() as s:
        s = session_setup(s)

        for counter, filename in enumerate(filenames):
            district = filename.split("-")[0]
            filename_server = filename.split("-")[1]
            url = url_template.replace("DISTRICT", district).replace(
                "FILENAME", filename_server
            )
            counter += 1
            # make a head request to check if the file exists
            r = s.head(url)
            # check response status code
            if r.status_code != 200:
                print(f"Error while downloading {filename}")

                # Check if the year and month are the previous month or newer
                year = int(filename_server.split("_")[1][:4])
                month = int(filename_server.split("_")[1][4:6])

                # build date object
                date = datetime.datetime(year, month, 1)

                # Check if the date is in the previous month or newer
                if date > datetime.datetime.now() - datetime.timedelta(days=30):
                    # If the year and month are current or future, don't add to skip list
                    print(
                        "-> File missing, but it's too new so it might be added later."
                    )
                    continue
                else:
                    # Add to skip list
                    skip.append(f"{filename}")
                    # Save skip list
                    with open("data/skip.txt", "w", encoding="utf-8") as f:
                        f.write("\n".join(skip))
                    continue
            else:
                r = s.get(url)
            with open(
                f"data/downloaded/parked_cars/{filename}", "w", encoding="utf-8"
            ) as f:
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

    with requests.Session() as s:
        s = session_setup(s)

        for counter, filename in enumerate(filenames):
            url = index_url + filename
            counter += 1
            r = s.head(url)
            # check response status code
            if r.status_code != 200:
                print(f"Error while downloading {filename}")
                continue
            else:
                r = s.get(url)

            with open(
                f"data/downloaded/permits/{filename}", "w", encoding="utf-8"
            ) as f:
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

    with requests.Session() as s:
        s = session_setup(s)

        for counter, filename in enumerate(filenames):
            url = "https://zps.tsk-praha.cz/geodata/stats/PM/" + filename
            counter += 1
            r = s.head(url)
            # check response status code
            if r.status_code != 200:
                print(f"Error while downloading {filename}")
                continue
            else:
                r = s.get(url)

            with open(f"data/downloaded/spaces/{filename}", "w", encoding="utf-8") as f:
                f.write(r.text)

            print(f"Progress: {counter}/{len(filenames)}", end="\r")

    print("\nDone")

if TYPE_OF_DATA == "HOUSES":
    # the URLs look like this: https://zps.tsk-praha.cz/puzzle/genmaps/PO_201801M_TR.json
    # let's cycle through the years and months and download the data
    # the data is in the form of a JSON file

    # create directory if it does not exist
    if not os.path.exists("data/downloaded/houses"):
        os.makedirs("data/downloaded/houses")
        downloaded_files = []
    else:
        # get list of already downloaded files
        downloaded_files = os.listdir("data/downloaded/houses")

    # generate list of filenames
    filenames = []

    filename_template = "PO_YYYYMMM_TR.json"

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
                "MMM", str(month).zfill(2) + "M"
            )
            if filename not in downloaded_files:
                filenames.append(filename)

    print(f"Downloading {len(filenames)} files")

    with requests.Session() as s:
        s = session_setup(s)

        for counter, filename in enumerate(filenames):
            url = "https://zps.tsk-praha.cz/puzzle/genmaps/" + filename
            counter += 1
            r = s.head(url)
            # check response status code
            if r.status_code != 200:
                print(f"Error while downloading {filename}")  # todo: add to skip list
                continue
            else:
                r = s.get(url)

            with open(f"data/downloaded/houses/{filename}", "w", encoding="utf-8") as f:
                f.write(r.text)

            print(f"Progress: {counter}/{len(filenames)}", end="\r")
