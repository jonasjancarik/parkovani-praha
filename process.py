import os
import pandas as pa
import json

# load list of files from data/downloaded
files = os.listdir("data/downloaded")

# remove files ending with A.json
files = [
    file for file in files if not file.endswith("A.json")
]  # todo: make work with section data (úseky)

# create empty dataframe
df = pa.DataFrame()

# iterate over files
for file in files:
    # read json file
    with open(f"data/downloaded/{file}", encoding="utf-8") as f:
        try:
            data = json.load(f)
        except json.decoder.JSONDecodeError:
            print(f"Error while reading {file} (potentially missing data)")
            continue

    areas = []  # area = ZSJ = základní sídelní jednotka

    for feature in data["features"]:
        area = {}
        for key, value in feature["properties"].items():
            area[key] = value

        # remove GraphData, GraphLegend, GraphData2, GraphLegend2 - these will be transforemed to separate fields
        for key in ["GraphData", "GraphLegend", "GraphData2", "GraphLegend2"]:
            area.pop(key)

        # remove unnecessary fields - fill, stroke, stroke-width, opacity, r
        for key in ["fill", "stroke", "stroke-width", "opacity", "r"]:
            area.pop(key)

        # parse GraphData and GraphLegend - these are lists of values
        for key in ["GraphData", "GraphLegend", "GraphData2", "GraphLegend2"]:
            feature["properties"][key] = (
                feature["properties"][key].replace("[", "").replace("]", "").split(",")
            )

        nan_detected = False

        for label_suffix in ["", "2"]:
            for i, datapoint in enumerate(
                feature["properties"][f"GraphData{label_suffix}"]
            ):
                try:
                    area[feature["properties"][f"GraphLegend{label_suffix}"][i]] = int(
                        datapoint
                    )
                except ValueError:
                    if datapoint == "NaN":
                        # NaN means no data - likely due to parking area not in operation. We will skip this area.
                        nan_detected = True

        if not nan_detected:
            areas.append(area)

    # create dataframe from list of areas
    df2 = pa.DataFrame(areas)
    # add column to distinguish day or night - day has "D_" in filename, night has "N_"
    if "D_" in file:
        df2["část dne"] = "den"
    elif "N_" in file:
        df2["část dne"] = "noc"
    elif "P_" in file:
        df2["část dne"] = "Po-Pá (MPD)"
    elif "S_" in file:
        df2["část dne"] = "So-Ne (MPD)"
    elif "W_" in file:
        df2["část dne"] = "Po-Pá"
    elif "X_" in file:
        df2["část dne"] = "So-Ne"
    else:
        raise ValueError("File name does not contain a known time period")

    # add column with district
    df2["mestska_cast"] = file.split("-")[0]  # split by "-" and take first part

    # add column with filename
    df2["filename"] = file

    # add to main dataframe
    df = pa.concat([df, df2], ignore_index=True)

# rename columns
df = df.rename(
    columns={
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
)

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

# create data/processed if it does not exist
if not os.path.exists("data/processed"):
    os.makedirs("data/processed")

# save to csv
print("Saving to data/processed/data.csv...")
df.to_csv("data/processed/data.csv", index=False, encoding="utf-8")

print("Done.")
