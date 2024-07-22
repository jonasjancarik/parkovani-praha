import os
import pandas as pd
import json
import sys
from src import mapping, utils
import logging
from dotenv import load_dotenv
import zipfile

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
        "PERMITS_ZONES": process_permits_from_houses,
        "ALL": lambda: [  # todo: unclear that this won't process mapping zones and houses to areas
            process_parked_cars(),
            process_permits(),
            process_spaces(),
            process_permits_and_spaces(),
            process_permits_from_houses(),
        ],
        "USEKY_NA_ZSJ": mapping.map_zones_to_areas,
        "DOMY_NA_USEKY": mapping.map_houses_to_zones,
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


def process_json_data(data):
    areas = []

    for feature in data["features"]:
        # Filtering properties
        area = {
            key: value
            for key, value in feature["properties"].items()
            if key
            not in {
                "GraphData",
                "GraphLegend",
                "GraphData2",
                "GraphLegend2",
                "fill",
                "stroke",
                "stroke-width",
                "opacity",
                "r",
            }
        }

        # Parsing graph data
        nan_found = False

        # count how many keys there are which start with GraphData in data['features'][0]['properties']
        graphdata_key_count = len(
            data["features"][0]["properties"].keys() & {"GraphData", "GraphData2"}
        )

        if graphdata_key_count == 1:
            label_suffixes = [""]
        elif graphdata_key_count == 2:
            label_suffixes = ["", "2"]
        else:
            sys.exit(
                "Error: The number of keys starting with GraphData in the JSON file is neither 1 nor 2."
            )  # todo: handle this better

        for label_suffix in label_suffixes:
            graph_data_key = f"GraphData{label_suffix}"
            graph_legend_key = f"GraphLegend{label_suffix}"
            graph_data = (
                feature["properties"].get(graph_data_key, "").strip("[]").split(",")
            )
            graph_legend = (
                feature["properties"].get(graph_legend_key, "").strip("[]").split(",")
            )

            for legend, datapoint in zip(graph_legend, graph_data):
                try:
                    area[legend] = int(datapoint)
                except ValueError:
                    area[legend] = datapoint
                    if datapoint == "NaN":
                        nan_found = True
                        break  # No need to parse further if NaN is found

            if nan_found:
                break  # Exit outer loop as well if NaN is found

        if not nan_found:
            areas.append(area)

    if len(areas):
        logging.debug(f"-> {len(areas)}/{len(data['features'])} zones had data")
    else:
        logging.debug("-> no areas in the file had data")

    return areas


def process_parked_cars():
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
        df["cast_dne"] = next(
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
        df["date"] = utils.get_date_from_filename(file)

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
            "CODE": "kod_useku",
            "CATEGORY": "typ_zony",
        }

        df.rename(columns=new_column_names, inplace=True)

        # calculate navstevnici: navstevnici_platici + navstevnici_neplatici
        df["navstevnici"] = (
            df["navstevnici_platici"] + df["navstevnici_neplatici"] + df["prenosna"]
        )

        # calculate sum of all types - RT_R + RT_V + RT_A + RT_P + RT_C + RT_E + RT_O + RT_S + R_VIS + NR + Free
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
                    "cast_dne",  # todo: make consistent with other column names
                    "kod_useku",
                    "typ_zony",
                    "parkovacich_mist_celkem",
                    "parkovacich_mist_v_zps",
                    "date",
                    "kod_zsj",
                    "naz_zsj",
                    "code",
                    "obsazenost",
                    "respektovanost",
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
            df["parkovacich_mist_v_zps"],
            axis="index",
        )

        # now drop all the _pct columns as we won't need them anymore
        df.drop(columns=percent_columns_with_affix, inplace=True)

        # process numbers for obsazenost and respektovanost, which are also percentages (but we'll keep them as percentages)
        df["obsazenost"] = df["obsazenost"] / 100
        df["respektovanost"] = df["respektovanost"] / 100

        # as a check, print the value of obsazenost for the row where kod useku is P1-0165 and filename is P01-OB_202001D_NA.json
        print(
            df[
                (df["kod_useku"] == "P1-0165")
                & (df["filename"] == "P01-OB_202001D_NA.json")
            ]["obsazenost"]
        )

        # add leading zero to district
        df["mestska_cast"] = utils.add_leading_zero_to_district(df, "mestska_cast")

        column_order = [
            "kod_useku",
            "kod_zsj",
            "naz_zsj",
            "mestska_cast",
            "typ_zony",
            "cast_dne",
            "date",
            "filename",
            "parkovacich_mist_celkem",
            "parkovacich_mist_v_zps",
            "rezidenti_do_500m",
            "rezidenti_do_125m",
            "rezidenti_od_126m_do_500m",
            "rezidenti_od_501m_do_2000m",
            "rezidenti_nad_2000m",
            "obsazenost",
            "respektovanost",
            "abonentska",
            "carsharing",
            "ekologicka",
            "navstevnici",
            "navstevnici_neplatici",
            "navstevnici_platici",
            "ostatni",
            "prenosna",
            "rezidentska",
            "socialni",
            "vlastnicka",
            "soucet_vsech_typu",
            "volna_mista",
        ]

        df = df[column_order]

        return df

    def save_to_csv(df, processed_dir):
        if not os.path.exists(processed_dir):
            os.makedirs(processed_dir)
        output_file = os.path.join(processed_dir, "data_parking.csv")
        df.to_csv(output_file, index=False, encoding="utf-8")
        logging.info(f"Data saved to: {output_file}")

        # compress the file with zipfile
        try:
            logging.info("Compressing data...")
            with zipfile.ZipFile(
                output_file + ".zip", "w", zipfile.ZIP_DEFLATED
            ) as zipf:
                zipf.write(output_file, os.path.basename(output_file))
            logging.info(f"Data compressed to: {output_file}.zip")
        except Exception as e:
            logging.error(f"Error while compressing the file: {e}")

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
    if "CODE" not in zones_to_areas_df.columns:
        zones_to_areas_df.rename(columns={"code": "CODE"}, inplace=True)

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

    if os.getenv("LOG_LEVEL", "INFO").upper() != "DEBUG":
        print("")  # print newline after the progress counter

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
    df_final.to_csv("data/processed/data_permits.csv", index=False)

    logging.info('Data saved to "data/processed/data_permits.csv"')


def process_spaces():
    logging.info("Processing parking spaces data...")

    df = utils.load_files_to_df("spaces")

    # turn Season into date. Season is YYYYMM, so add 01 to the end to make it YYYYMMDD and then convert to date
    df["date"] = pd.to_datetime(df["Season"].astype(str) + "01")

    # drop the Season column
    df.drop(columns=["Season"], inplace=True)

    # save to csv
    df.to_csv("data/processed/data_spaces.csv", index=False)


def process_permits_and_spaces():
    """
    Produces a combined file with permits and spaces data.
    """

    # process permits
    process_permits()

    # process spaces
    process_spaces()

    # Now we will merge the two datasets

    # load permits data from processed CSV
    df_permits = pd.read_csv("data/processed/data_permits.csv")

    # load spaces data
    df_spaces = pd.read_csv("data/processed/data_spaces.csv")

    # rename MC in spaces data to Oblast
    df_spaces.rename(columns={"MC": "Oblast"}, inplace=True)

    # merge permits and spaces data
    df = pd.merge(df_permits, df_spaces, on=["date", "Oblast"], how="outer")

    # drop rows where Oblast contains a dot
    df = df[~df["Oblast"].str.contains(r"\.")]

    # Rename Oblast to mestska_cast and XSUM to POP_CELKEM
    df.rename(columns={"Oblast": "mestska_cast", "XSUM": "POP_CELKEM"}, inplace=True)

    # drop column parent district
    df.drop(columns=["parent district"], inplace=True)

    # save to csv
    df.to_csv("data/processed/data_permits_and_spaces.csv", index=False)

    logging.info('Data saved to "data/processed/data_permits_and_spaces.csv"')


def process_permits_from_houses():
    # will create a "permits per zone" file

    # load the data

    # read each JSON file in data/downloaded/houses and run it through the process_json_data function
    # append the results to a list
    data_dir = "data/downloaded/houses"
    files = os.listdir(data_dir)
    permits_all_df = pd.DataFrame()

    for counter, file in enumerate(files, start=1):
        file_path = os.path.join(data_dir, file)

        logging.debug(f"Processing {file_path}")

        try:
            with open(file_path, encoding="utf-8") as f:
                data = json.load(f)
        except json.decoder.JSONDecodeError:
            logging.debug(f"Error while loading {file} (potentially missing data)")
            continue

        permits = process_json_data(data)
        permits_df = pd.DataFrame(permits)
        permits_df["date"] = utils.get_date_from_filename(file)
        permits_all_df = pd.concat([permits_all_df, permits_df], ignore_index=True)

        logging.info(f"Processed {counter} out of {len(files)} files")

    # load the mapping of houses to zones
    try:
        houses_to_zones_df = pd.read_csv("data/houses_useky_mapping.csv")
    except Exception:  # todo: better exception handling
        logging.info("Run `process.py domy_na_useky` first.")
        sys.exit(1)

    # rename CODE to house_code
    permits_all_df.rename(columns={"CODE": "house_code"}, inplace=True)

    # join the two dataframes on 'code'
    permits_all_df["house_code"] = permits_all_df["house_code"].astype("int64")
    permits_all_df = permits_all_df.merge(houses_to_zones_df, on="house_code")

    # group by date and usek_code and sum the values
    permits_all_df = permits_all_df.groupby(["date", "usek_code"]).sum().reset_index()

    # rename XSUM to POP_CELKEM
    permits_all_df.rename(columns={"XSUM": "POP_CELKEM"}, inplace=True)

    # drop the house_code column
    permits_all_df.drop(columns=["house_code"], inplace=True)

    # rename usek_code to kod_useku
    permits_all_df.rename(columns={"usek_code": "kod_useku"}, inplace=True)

    # rename
    permits_all_df.rename(
        columns={
            "XSUM": "pop_celkem",
            "R": "pop_rezidentska",
            "V": "pop_vlastnicka",
            "A": "pop_abonentska",
            "P": "pop_prenosna",
            "C": "pop_carsharing",
            "E": "pop_ekologicka",
            "O": "pop_ostatni",
            "S": "pop_socialni",
        },
        inplace=True,
    )

    # save to csv
    permits_all_df.to_csv("data/processed/data_permits_by_zone.csv", index=False)

    logging.info('Data saved to "data/processed/data_permits_by_zone.csv"')


if __name__ == "__main__":
    process_function = parse_arguments()
    process_function()
