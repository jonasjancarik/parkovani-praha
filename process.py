import os
import pandas as pd
import json
import sys
from src import mapping, utils
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up basic configuration for logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),  # default to INFO
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def parse_arguments():
    valid_args = {
        "PARKING": process_parked_cars,
        "PERMITS": process_permits,
        "SPACES": process_spaces,
        "PERMITS_SPACES": process_permits_and_spaces,
        "ALL": lambda: [
            process_parked_cars(),
            process_permits(),
            process_spaces(),
            process_permits_and_spaces(),
        ],
        "USEKY_NA_ZSJ": mapping.map_useky_to_zsj,
        "DOMY_NA_USEKY": mapping.map_houses_to_useky,
    }

    arg = sys.argv[1].upper() if len(sys.argv) > 1 else "ALL"
    if arg in valid_args:
        return valid_args[arg]
    elif arg in ["-H", "--HELP"]:
        display_help(valid_args.keys())
        sys.exit()
    else:
        logging.info(f"Invalid argument: {arg}")
        display_help(valid_args.keys())
        sys.exit(1)


def display_help(valid_args):
    logging.info("Usage: python process.py [TYPE_OF_DATA]")
    logging.info("\nTYPE_OF_DATA can be one of the following:")
    for arg in valid_args:
        logging.info(f"- {arg}")


def process_parked_cars():
    def process_json_data(data):
        areas = []
        for feature in data["features"]:
            area = {
                key: value
                for key, value in feature["properties"].items()
                if key
                not in [
                    "GraphData",
                    "GraphLegend",
                    "GraphData2",
                    "GraphLegend2",
                    "fill",
                    "stroke",
                    "stroke-width",
                    "opacity",
                    "r",
                ]
            }

            area = parse_graph_data(feature, area)
            if not any(area[key] == "NaN" for key in area):
                areas.append(area)
            else:
                logging.debug(
                    f"Skipping area {area.get('NAZ_ZSJ', area.get('CODE'))} due to missing data"
                )

        logging.debug(f"-> {len(areas)}/{len(data['features'])} zones had data")
        if len(areas) == 0:
            logging.debug("-> no areas in the file had data")
        return areas

    def parse_graph_data(feature, area):
        updated_area = area.copy()
        for label_suffix in ["", "2"]:
            graph_data_key = f"GraphData{label_suffix}"
            graph_legend_key = f"GraphLegend{label_suffix}"
            graph_data = (
                feature["properties"]
                .get(graph_data_key, "")
                .replace("[", "")
                .replace("]", "")
                .split(",")
            )
            graph_legend = (
                feature["properties"]
                .get(graph_legend_key, "")
                .replace("[", "")
                .replace("]", "")
                .split(",")
            )

            for i, datapoint in enumerate(graph_data):
                try:
                    updated_area[graph_legend[i]] = int(datapoint)
                except ValueError:
                    updated_area[graph_legend[i]] = datapoint
        return updated_area

    def enrich_dataframe(df, file, zones_to_areas_df):
        # parse time period code
        time_period = {
            "D_": "den",
            "N_": "noc",
            "P_": "Po-Pá (MPD)",
            "S_": "So-Ne (MPD)",
            "W_": "Po-Pá",
            "X_": "So-Ne",
        }
        df["část dne"] = next(
            (time_period[key] for key in time_period if key in file), "Unknown"
        )

        # add area (ZSJ)
        # assign the ZSJ code to each zone based on the zones_to_areas_df; join the two dataframes on 'code'
        df = df.merge(zones_to_areas_df, on="CODE")

        # remove column 'overlap'
        df = df.drop(columns=["overlap"])

        # add column with the source file name
        df["filename"] = file

        # add date column
        # filenames look like P01-OB_202311D_NA.json, where 202311 is the year and month
        # - extract the year and month from the filename and use the last day of the month
        year_and_month = file.split("_")[1][:6]
        year = year_and_month[:4]
        month = year_and_month[4:]
        date = pd.to_datetime(f"{year}-{month}-01") + pd.offsets.MonthEnd(0)
        df["date"] = date

        # add frekvence column, wich is monthly or quarterly, based on whether the filename contains _N for monthly or _Q for quarterly
        if "_N" in file:
            df["frekvence"] = "měsíční"
        if "_Q" in file:
            df["frekvence"] = "čtvrtletní"

        return df

    def rename_and_calculate_columns(df):
        new_column_names = {
            "CELKEM_PS": "parkovacich_mist_celkem",
            "PS_ZPS": "parkovacich_mist_v_zps",
            "R1": "rezidenti_do_125m",
            "R2": "rezidenti_od_126m_do_500m",
            "R3": "rezidenti_od_501m_do_2000m",
            "R4": "rezidenti_nad_2000m",
            "R": "rezidentska",
            "V": "vlastnicka",
            "A": "abonentska",
            "P": "prenosna",
            "C": "carsharing",
            "E": "ekologicka",
            "O": "ostatni",
            "S": "socialni",
            "VI": "navstevnici_platici",
            "NR": "navstevnici_neplatici",
            "FR": "volna_mista",
            "Resp": "respektovanost",
            "Obs": "obsazenost",
            "ResPct": "rezidenti_do_500m",
        }

        new_column_names["CODE"] = "kod_useku"
        new_column_names["CATEGORY"] = "kategorie"

        df.rename(columns=new_column_names, inplace=True)

        # calculate navstevnici: navstevnici_platici + navstevnici_neplatici
        df["navstevnici"] = (
            df["navstevnici_platici"] + df["navstevnici_neplatici"] + df["prenosna"]
        )

        # calculate kontrola - RT_R + RT_V + RT_A + RT_P + RT_C + RT_E + RT_O + RT_S + R_VIS + NR + Free
        df["soucet_vsech_typu"] = (
            df["rezidentska"]
            + df["vlastnicka"]
            + df["abonentska"]
            + df["prenosna"]
            + df["carsharing"]
            + df["ekologicka"]
            + df["ostatni"]
            + df["socialni"]
            + df["navstevnici"]
            + df["volna_mista"]
        )

        # divide percentage values by 100
        percent_columns = list(
            set(df.columns)
            - set(
                [
                    "filename",
                    "mestska_cast",
                    "část dne",
                    "kod_useku",
                    "kategorie",
                    "parkovacich_mist_celkem",
                    "parkovacich_mist_v_zps",
                    "date",
                    "frekvence",
                    "kod_zsj",
                    "naz_zsj",
                    "code",
                ]
            )
        )

        df[percent_columns] = df[percent_columns] / 100

        # affix _pct to percentage columns
        df.rename(columns={col: col + "_pct" for col in percent_columns}, inplace=True)

        # prepare a list of percentage columns to calculate absolute values for
        percent_columns_with_affix = [col + "_pct" for col in percent_columns]

        # rename percent_columns var to absolute_columns for clarity
        absolute_columns = percent_columns

        # calculate absolute values for percentage columns by multiplying by parkovacich_mist_celkem
        df[absolute_columns] = df[percent_columns_with_affix].multiply(
            df["parkovacich_mist_celkem"], axis="index"
        )

        df["mestska_cast"] = utils.add_leading_zero_to_district(df, "mestska_cast")

        return df

    def save_to_csv(df, processed_dir):
        if not os.path.exists(processed_dir):
            os.makedirs(processed_dir)
        output_file = os.path.join(processed_dir, "data_parking.csv")
        df.to_csv(output_file, index=False, encoding="utf-8")
        logging.info("Data saved to:", output_file)

    ####################################################
    # main part of the parked cars processing function #
    ####################################################

    # Directory paths
    data_dir = "data/downloaded/parked_cars"
    processed_dir = "data/processed"
    # create processed directory if it does not exist
    if not os.path.exists(processed_dir):
        os.makedirs(processed_dir)

    # File filtering
    # - we need zone (useky) data, meaning files ending with A.json

    files = [file for file in os.listdir(data_dir) if file.endswith("A.json")]

    # load mapping of zones to areas
    try:
        zones_to_areas_df = pd.read_csv("data/useky_zsj_mapping.csv")
    except Exception:  # todo: better exception handling
        logging.info("Run `process.py useky_na_zsj` first.")
        sys.exit(1)

    # make CODE column uppercase, we'll be using it to merge the dfs
    zones_to_areas_df["CODE"] = zones_to_areas_df["code"]

    # Processing

    parked_cars_all_df = pd.DataFrame()

    # loop through files (districts)
    for counter, file in enumerate(files, start=1):
        file_path = os.path.join(data_dir, file)

        logging.debug(f"Processing {file_path}")

        try:
            with open(file_path, encoding="utf-8") as f:
                data = json.load(f)
        except json.decoder.JSONDecodeError:
            logging.debug(f"Error while loading {file} (potentially missing data)")
            continue

        parked_cars_district = process_json_data(data)
        parked_cars_district_df = pd.DataFrame(parked_cars_district)

        # enrich the data (unless empty)
        if not parked_cars_district_df.empty:
            parked_cars_district_df = enrich_dataframe(
                parked_cars_district_df, file, zones_to_areas_df
            )
        else:
            logging.debug(f"Skipping file {file} due to missing data")
            continue

        # append this zone's data to the main dataframe
        parked_cars_all_df = pd.concat(
            [parked_cars_all_df, parked_cars_district_df], ignore_index=True
        )

        if os.getenv("LOG_LEVEL", "INFO").upper() == "DEBUG":
            logging.info(f"Processed {counter} out of {len(files)} files")
        else:
            print(f"\rProcessed {counter} out of {len(files)} files", end="")

    # rename columns to more readable names
    parked_cars_all_df = rename_and_calculate_columns(parked_cars_all_df)

    # export to CSV
    logging.info("Saving CSV...")
    save_to_csv(parked_cars_all_df, processed_dir)


def process_permits():
    logging.info("Processing parking permit data...")

    df = utils.load_files_to_df("permits")

    # now drop rows where Oblast == 'Enum'
    df = df[df["Oblast"] != "Enum"]

    df["Oblast"] = utils.add_leading_zero_to_district(df, "Oblast")

    # identify Oblast values that contain dots - those are subdistricts
    subdistricts = df[df["Oblast"].str.contains(r"\.")]["Oblast"].unique()

    # identify districts with subdistricts - this means removing the parts after the dot
    districts_with_subdistricts = {
        subdistrict_name.split(".")[0] for subdistrict_name in subdistricts
    }

    # in the df, append .0 to the Oblast values that are districts with subdistricts
    df.loc[df["Oblast"].isin(districts_with_subdistricts), "Oblast"] = (
        df.loc[df["Oblast"].isin(districts_with_subdistricts), "Oblast"] + ".0"
    )

    # now extract the year and month from the season column and create a new column with the date. The last six characters of the season column are the year and month, e.g. 201910; make it the first of the month
    df["date"] = pd.to_datetime(df["Season"].str[-6:] + "01")

    # create a new column "parent district" - only do this for rows where Oblast contains a dot
    df["parent district"] = df["Oblast"].apply(
        lambda x: x.split(".")[0] if "." in x else None
    )

    # now for all Oblast values that contain dots, sum the values for the subdistricts of the same district to a new row which where Oblast will be the main district code (i.e. without the dot part). This should be done for each date

    # now, for each date and parent district group, sum the values and create a new row which where Oblast will be the parent district value
    df_grouped = df.groupby(["date", "parent district"]).sum().reset_index()

    # Replace the Oblast values in df_grouped with the parent district values
    df_grouped["Oblast"] = df_grouped["parent district"]

    # Set the parent district column to Null in df_grouped
    df_grouped["parent district"] = None

    # Append df_grouped back to df
    df_final = pd.concat([df, df_grouped], ignore_index=True)

    # sort by date and oblast
    df_final.sort_values(["date", "Oblast"], inplace=True)

    # drop teh season column
    df_final.drop(columns=["Season"], inplace=True)

    # reorder the columns so that date is first
    columns = df_final.columns.tolist()

    # move date to the beginning
    columns.insert(0, columns.pop(columns.index("date")))

    df_final = df_final[columns]

    # save to csv
    df_final.to_csv("data/processed/data_parking_permits.csv", index=False)

    logging.info('Data saved to "data/processed/data_parking_permits.csv"')


def process_spaces():
    logging.info("Processing parking spaces data...")

    df = utils.load_files_to_df("spaces")

    # turn Season into date. Season is YYYYMM, so add 01 to the end to make it YYYYMMDD and then convert to date
    df["date"] = pd.to_datetime(df["Season"].astype(str) + "01")

    # drop the Season column
    df.drop(columns=["Season"], inplace=True)

    # save to csv
    df.to_csv("data/processed/data_parking_spaces.csv", index=False)


def process_permits_and_spaces():
    """
    Produces a combined file with permits and spaces data.
    """

    # load permits data from processed CSV
    df_permits = pd.read_csv("data/processed/data_parking_permits.csv")

    # load spaces data
    df_spaces = pd.read_csv("data/processed/data_parking_spaces.csv")

    # rename MC in spaces data to Oblast
    df_spaces.rename(columns={"MC": "Oblast"}, inplace=True)

    # merge permits and spaces data
    df = pd.merge(df_permits, df_spaces, on=["date", "Oblast"], how="outer")

    # drop rows where Oblast contains a dot
    df = df[~df["Oblast"].str.contains(r"\.")]

    # save to csv
    df.to_csv("data/processed/data_parking_permits_and_spaces.csv", index=False)

    logging.info('Data saved to "data/processed/data_parking_permits_and_spaces.csv"')


if __name__ == "__main__":
    process_function = parse_arguments()
    process_function()
