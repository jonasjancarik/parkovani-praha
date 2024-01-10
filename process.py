import os
import pandas as pd
import json
import sys


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
            if AREA_TYPE == "zsj":
                print(f"Skipping area {area['NAZ_ZSJ']} due to missing data")
            if AREA_TYPE == "useky":
                print(f"Skipping area {area['CODE']} due to missing data")
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

    return df


def save_to_csv(df, processed_dir):
    if not os.path.exists(processed_dir):
        os.makedirs(processed_dir)
    output_file = os.path.join(processed_dir, f"data_{AREA_TYPE}.csv")
    df.to_csv(output_file, index=False, encoding="utf-8")
    print("Data saved to:", output_file)


# Main Script

# Directory paths
data_dir = "data/downloaded"
processed_dir = "data/processed"
# create processed directory if it does not exist
if not os.path.exists(processed_dir):
    os.makedirs(processed_dir)

# check CLI argument - the first argument should be either zsj (default) or useky
# if zsj (default), we will use files ending with J.json
# if useky, we will use files ending with A.json (data for sections - úseky)
AREA_TYPE = "zsj"
if len(sys.argv) > 1:
    if sys.argv[1] == "useky":
        AREA_TYPE = "useky"
    else:
        print(
            "Invalid argument. The first argument should be either zsj (default) or useky"
        )

# File filtering and processing
if AREA_TYPE == "zsj":
    files = [file for file in os.listdir(data_dir) if file.endswith("J.json")]
else:
    files = [file for file in os.listdir(data_dir) if file.endswith("A.json")]
df = pd.DataFrame()

for counter, file in enumerate(files, start=1):
    file_path = os.path.join(data_dir, file)
    try:
        with open(file_path, encoding="utf-8") as f:
            data = json.load(f)
    except json.decoder.JSONDecodeError:
        print(f"Error while reading {file} (potentially missing data)")
        continue

    areas = process_json_data(data)
    df2 = pd.DataFrame(areas)
    df2 = enrich_dataframe(df2, file)
    df = pd.concat([df, df2], ignore_index=True)
    print(f"Processed {counter} out of {len(files)} files")

df = rename_and_calculate_columns(df)
save_to_csv(df, processed_dir)
