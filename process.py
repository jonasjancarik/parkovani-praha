import os
import pandas as pd
import json
import sys
from src import process

# read TYPE_OF_DATA from command line
# if no argument is provided, default to ALL
if len(sys.argv) > 1:
    valid_args = [
        "PARKING",
        "PERMITS",
        "SPACES",
        "PERMITS_SPACES",
        "ALL",
        "USEKY_NA_ZSJ",
        "DOMY_NA_USEKY",
    ]

    # allow displaying help
    if sys.argv[1].lower() == "-h" or sys.argv[1].lower() == "--help":
        print(
            f"Usage: python process.py [TYPE_OF_DATA]\n\nTYPE_OF_DATA can be one of the following:\n{', '.join(valid_args)} (default)"
        )
        sys.exit(0)
    TYPE_OF_DATA = sys.argv[1].upper()
    if TYPE_OF_DATA not in valid_args:
        print(
            "Invalid argument. TYPE_OF_DATA can be one of the following:\nPARKING\nPERMITS\nSPACES\nPERMITS_SPACES\nALL (default)"
        )
        sys.exit(0)

    # if there is a second argument, it should be either zsj (default) or useky
    AREA_TYPE = "zsj"
    if len(sys.argv) > 2:
        if sys.argv[2].lower() == "useky":
            AREA_TYPE = "useky"
        elif sys.argv[2].lower() == "zsj":
            AREA_TYPE = "zsj"
        else:
            print(
                "Invalid argument. The second argument should be either zsj (default) or useky"
            )
else:
    TYPE_OF_DATA = "ALL"


def load_files_to_df(category):
    """
    Load TSV files from the specified category directory and merge them into a single dataframe.

    Parameters:
    category (str): The category directory name.

    Returns:
    pandas.DataFrame: The merged dataframe containing data from all files.
    """

    # get list of files in data/downloaded/permits

    try:
        files = os.listdir(f"data/downloaded/{category}")
    except FileNotFoundError:
        print(f"No files found in data/downloaded/{category}")
        sys.exit(0)

    # merge all files into one dataframe
    df = pd.DataFrame()
    for file in files:
        file_path = os.path.join(f"data/downloaded/{category}", file)
        df2 = pd.read_csv(file_path, encoding="utf-8", delimiter="\t")
        df = pd.concat([df, df2], ignore_index=True)

    return df


def proces_parked_cars():
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
            # else:
            #     if AREA_TYPE == "zsj":
            #         print(f"Skipping area {area['NAZ_ZSJ']} due to missing data")
            #     if AREA_TYPE == "useky":
            #         print(f"Skipping area {area['CODE']} due to missing data")
        print(f"{len(areas)}/{len(data['features'])}")
        if len(areas) == 0:
            print("no areas in the file had data")
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

    def enrich_dataframe(df, file):
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
        if AREA_TYPE == "zsj":
            df["mestska_cast"] = file.split("-")[0].replace("P0", "P")
        if AREA_TYPE == "useky":
            # split CODE column by - and take the first part
            df["mestska_cast"] = df["CODE"].str.split("-").str[0].replace("P0", "P")
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

        if AREA_TYPE == "zsj":
            new_column_names["KOD_ZSJ"] = "kod_zsj"
            new_column_names["NAZ_ZSJ"] = "nazev_zsj"
            new_column_names["Obyv_total"] = "obyvatel"
        elif AREA_TYPE == "useky":
            new_column_names["CODE"] = "kod_useku"
            new_column_names["CATEGORY"] = "kategorie"
        else:
            raise ValueError("Invalid data type (should be either zsj or useky)")

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
        if AREA_TYPE == "zsj":
            percent_columns = list(
                set(df.columns)
                - set(
                    [
                        "filename",
                        "mestska_cast",
                        "část dne",
                        "kod_zsj",
                        "nazev_zsj",
                        "parkovacich_mist_celkem",
                        "parkovacich_mist_v_zps",
                        "obyvatel",
                        "date",
                        "frekvence",
                    ]
                )
            )

        elif AREA_TYPE == "useky":
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
                    ]
                )
            )

        else:
            raise ValueError("Invalid data type (should be either zsj or useky)")

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

        df["mestska_cast"] = add_leading_zero_to_district(df, "mestska_cast")

        if AREA_TYPE == "useky":
            # let's join this with data from useky_zsj_mapping to get the ZSJ code and name
            df_useky_zsj_mapping = pd.read_csv("data/useky_zsj_mapping.csv")

            # rename column
            df_useky_zsj_mapping = df_useky_zsj_mapping.rename(
                columns={"code": "kod_useku"}
            )

            # join df with df_useky_zsj_mapping on kod_useku
            df = pd.merge(df, df_useky_zsj_mapping, on="kod_useku", how="left")

            # drop columns we don't need - mestska_cast_y
            df = df.drop(columns=["mestska_cast_y"])

        return df

    def save_to_csv(df, processed_dir):
        if not os.path.exists(processed_dir):
            os.makedirs(processed_dir)
        output_file = os.path.join(processed_dir, f"data_{AREA_TYPE}.csv")
        df.to_csv(output_file, index=False, encoding="utf-8")
        print("Data saved to:", output_file)

    # main part of the function

    # Directory paths
    data_dir = "data/downloaded/parked_cars"
    processed_dir = "data/processed"
    # create processed directory if it does not exist
    if not os.path.exists(processed_dir):
        os.makedirs(processed_dir)

    # File filtering and processing

    # Check area type
    # - if zsj (default), we will use files ending with J.json
    # - if useky, we will use files ending with A.json (data for sections - úseky)

    if AREA_TYPE == "zsj":
        files = [file for file in os.listdir(data_dir) if file.endswith("J.json")]
    elif AREA_TYPE == "useky":
        files = [file for file in os.listdir(data_dir) if file.endswith("A.json")]
    else:
        raise ValueError("Invalid area type (should be either zsj or useky)")
    df = pd.DataFrame()

    for counter, file in enumerate(files[:30], start=1):
        file_path = os.path.join(data_dir, file)
        print(file_path)
        try:
            with open(file_path, encoding="utf-8") as f:
                data = json.load(f)
        except json.decoder.JSONDecodeError:
            print(f"Error while reading {file} (potentially missing data)")
            continue

        areas = process_json_data(data)
        df2 = pd.DataFrame(areas)
        # check if dataframe isn't empty
        if not df2.empty:
            df2 = enrich_dataframe(df2, file)
        else:
            print(f"Skipping file {file} due to missing data")
            continue
        df = pd.concat([df, df2], ignore_index=True)
        print(f"Processed {counter} out of {len(files)} files")

    df = rename_and_calculate_columns(df)
    save_to_csv(df, processed_dir)


def add_leading_zero_to_district(df, column):
    """
    Add leading zeros to district numbers.

    This function takes a DataFrame `df` and a column name `column` as input.
    It adds leading zeros to the district numbers in the specified column, ensuring that the format is consistent.
    For example, if a district number is 'P1', it will be converted to 'P01'.
    However, if a district number is already in the format 'P05', it will not be modified.

    Parameters:
    - df (pandas.DataFrame): The DataFrame containing the data.
    - column (str): The name of the column containing the district numbers.

    Returns:
    - pandas.Series: A Series with the modified district numbers.
    """

    return df[column].str.replace(r"(?<=\bP)\d(?!\d)", "0\\g<0>", regex=True)


def process_permits():
    print("Processing parking permit data...")

    df = load_files_to_df("permits")

    # now drop rows where Oblast == 'Enum'
    df = df[df["Oblast"] != "Enum"]

    df["Oblast"] = add_leading_zero_to_district(df, "Oblast")

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

    print('Data saved to "data/processed/data_parking_permits.csv"')


def process_spaces():
    print("Processing parking spaces data...")

    df = load_files_to_df("spaces")

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

    print('Data saved to "data/processed/data_parking_permits_and_spaces.csv"')


if TYPE_OF_DATA == "PARKING":
    proces_parked_cars()
elif TYPE_OF_DATA == "PERMITS":
    process_permits()
elif TYPE_OF_DATA == "SPACES":
    process_spaces()
elif TYPE_OF_DATA == "PERMITS_SPACES":
    process_permits_and_spaces()
elif TYPE_OF_DATA == "ALL":
    proces_parked_cars()
    process_permits()
    process_spaces()
elif TYPE_OF_DATA == "USEKY_NA_ZSJ":
    process.map_useky_to_zsj()
elif TYPE_OF_DATA == "DOMY_NA_USEKY":
    process.map_houses_to_useky()
