import os
import pandas as pd
import json


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
            print(f"Skipping area {area['NAZ_ZSJ']} due to missing data")
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
    df["mestska_cast"] = file.split("-")[0]
    df["filename"] = file
    return df


def rename_and_calculate_columns(df):
    new_column_names = {
        "KOD_ZSJ": "kod_zsj",
        "NAZ_ZSJ": "nazev_zsj",
        "Obyv_total": "obyvatel",
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
                "kod_zsj",
                "nazev_zsj",
                "parkovacich_mist_celkem",
                "parkovacich_mist_v_zps",
            ]
        )
    )

    df[percent_columns] = df[percent_columns] / 100

    return df


def save_to_csv(df, processed_dir):
    if not os.path.exists(processed_dir):
        os.makedirs(processed_dir)
    output_file = os.path.join(processed_dir, "data.csv")
    df.to_csv(output_file, index=False, encoding="utf-8")
    print("Data saved to:", output_file)


# Main Script

# Directory paths
data_dir = "data/downloaded"
processed_dir = "data/processed"

# File filtering and processing
files = [
    file for file in os.listdir(data_dir) if file.endswith(".json") and "A" not in file
]  # todo: make work with A files (data for sections - úseky)
df = pd.DataFrame()

for file in files:
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

df = rename_and_calculate_columns(df)
save_to_csv(df, processed_dir)
