import os
from dataclasses import asdict
from pathlib import Path

import pandas as pd
import json
import sys
from src import mapping, utils
from src.parking_anomalies import (
    AnomalyDetectionConfig,
    RAW_COMPOSITION_COLUMNS,
    RAW_OCCUPANCY_COLUMNS,
    build_anomaly_audit,
    detect_monthly_anomaly_candidates,
    normalize_parking_observations,
    recompute_derived_parking_measures,
    summarize_value_changes,
    validate_processed_parking_data,
)
from src.parking_cleanup import apply_verified_capacity_regime_cleanup
import logging
from dotenv import load_dotenv
import zipfile
from tqdm import tqdm

# Load environment variables
load_dotenv()

# Set up basic configuration for logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),  # default to INFO
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def parse_arguments():
    valid_args = {
        "PARKING": lambda: process_parked_cars(apply_anomaly_detection=True),
        "PARKING_NO_ANOMALY": lambda: process_parked_cars(
            apply_anomaly_detection=False,
            output_file="data/processed/data_parking_no_anomaly.csv",
            anomaly_report_file=(
                "data/processed/data_parking_no_anomaly_report.json"
            ),
            write_zip=False,
        ),
        "PERMITS": process_permits,
        "SPACES": process_spaces,
        "PERMITS_SPACES": process_permits_and_spaces,
        "PERMITS_ZONES": process_permits_from_buildings,
        "ALL": lambda: [
            process_parked_cars(apply_anomaly_detection=True),
            process_permits(),
            process_spaces(),
            process_permits_and_spaces(),
            process_permits_from_buildings(),
        ],
        "ALL_NO_ANOMALY": lambda: [  # Allows skipping anomaly detection for the parking part when running ALL
            process_parked_cars(
                apply_anomaly_detection=False,
                output_file="data/processed/data_parking_no_anomaly.csv",
                anomaly_report_file=(
                    "data/processed/data_parking_no_anomaly_report.json"
                ),
                write_zip=False,
            ),
            process_permits(),
            process_spaces(),
            process_permits_and_spaces(),
            process_permits_from_buildings(),
        ],
        "USEKY_NA_ZSJ": mapping.map_zones_to_areas,
        "DOMY_NA_USEKY": mapping.map_buildings_to_zones,
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


def process_parked_cars(
    apply_anomaly_detection: bool = True,
    *,
    output_file: str | Path | None = None,
    anomaly_report_file: str | Path | None = None,
    write_zip: bool = True,
):
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

        logging.info("Renaming columns...")
        df.rename(columns=new_column_names, inplace=True)

        logging.info("Processing percentage columns...")
        # divide percentage values by 100
        percent_columns = list(
            set(df.columns)
            - set(
                [
                    "filename",
                    "mestska_cast",
                    "cast_dne",
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

        # process numbers for obsazenost and respektovanost, which are also percentages (but we'll keep them as percentages)
        df["obsazenost"] = df["obsazenost"] / 100
        df["respektovanost"] = df["respektovanost"] / 100

        logging.info("Calculating absolute values...")
        # calculate absolute values for percentage columns by multiplying by parkovacich_mist_v_zps (number or unreserved spaces in the zone) and obsazenost (occupancy)
        df[absolute_columns] = (
            df[percent_columns_with_affix]
            .multiply(
                df["parkovacich_mist_v_zps"],
                axis="index",
            )
            .multiply(df["obsazenost"], axis="index")
        )

        logging.info("Cleaning up temporary columns...")
        # now drop all the _pct columns as we won't need them anymore
        df.drop(columns=percent_columns_with_affix, inplace=True)

        logging.info("Recomputing derived parking measures...")
        df = recompute_derived_parking_measures(df)

        # add leading zero to district
        df["mestska_cast"] = utils.add_leading_zero_to_district(df, "mestska_cast")

        logging.info("Reordering columns...")
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
        resolved_output = Path(output_file or Path(processed_dir) / "data_parking.csv")
        resolved_output.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(resolved_output, index=False, encoding="utf-8")
        logging.info(f"Data saved to: {resolved_output}")

        # compress the file with zipfile
        if not write_zip:
            return
        try:
            logging.info("Compressing data...")
            with zipfile.ZipFile(
                str(resolved_output) + ".zip", "w", zipfile.ZIP_DEFLATED
            ) as zipf:
                zipf.write(resolved_output, resolved_output.name)
            logging.info(f"Data compressed to: {resolved_output}.zip")
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
    zones_to_areas_df["CODE"] = zones_to_areas_df["CODE"].str.upper()

    # Processing

    # Read all JSON files at once into a list of dictionaries
    data_list = []
    for file in tqdm(files, desc="Loading files"):
        file_path = os.path.join(data_dir, file)
        try:
            with open(file_path, encoding="utf-8") as f:
                data = json.load(f)
                # Add file name to the data for later reference
                processed_data = process_json_data(data)
                if processed_data:  # if not empty
                    for item in processed_data:
                        item["filename"] = file
                    data_list.extend(processed_data)
        except json.decoder.JSONDecodeError:
            logging.debug(f"Error while loading {file} (potentially missing data)")
            continue

    # Create DataFrame once from all data
    parked_cars_all_df = pd.DataFrame(data_list)

    # Enrich all data at once
    if not parked_cars_all_df.empty:
        logging.info("Enriching data...")
        # Add time period for all rows at once
        time_period = {
            "D_": "den",
            "N_": "noc",
            "P_": "Po-Pá (MPD)",
            "S_": "So-Ne (MPD)",
            "W_": "Po-Pá",
            "X_": "So-Ne",
        }

        logging.info("Adding time period...")
        parked_cars_all_df["cast_dne"] = parked_cars_all_df["filename"].map(
            lambda x: next(
                (time_period[key] for key in time_period if key in x), "Unknown"
            )
        )

        # Add dates all at once
        logging.info("Adding dates...")
        # Extract YYYYMM using string operations
        year_month = parked_cars_all_df["filename"].str.split("_").str[1].str[:6]
        # Convert to datetime and get end of month
        parked_cars_all_df["date"] = pd.to_datetime(
            year_month + "01", format="%Y%m%d"
        ) + pd.offsets.MonthEnd(0)

        logging.info("Normalizing source observations...")
        parked_cars_all_df, normalization_report = normalize_parking_observations(
            parked_cars_all_df
        )

        logging.info("Merging with zones_to_areas_df...")
        zones_to_areas_df = zones_to_areas_df.drop_duplicates()
        if zones_to_areas_df.duplicated("CODE").any():
            duplicate_codes = zones_to_areas_df.loc[
                zones_to_areas_df.duplicated("CODE", keep=False), "CODE"
            ].unique()
            raise ValueError(
                "Zone-to-area mapping is not unique for: "
                + ", ".join(sorted(duplicate_codes[:20]))
            )
        parked_cars_all_df = parked_cars_all_df.merge(
            zones_to_areas_df,
            on="CODE",
            how="left",
            validate="many_to_one",
            indicator="_zone_area_merge",
        )
        missing_zone_area = parked_cars_all_df["_zone_area_merge"].eq("left_only")
        missing_zone_area_codes = sorted(
            parked_cars_all_df.loc[missing_zone_area, "CODE"].unique().tolist()
        )
        inferred_district = (
            parked_cars_all_df["CODE"]
            .str.extract(r"^P0*(\d+)", expand=False)
            .astype("Int64")
            .map(lambda value: f"P{int(value):02d}" if pd.notna(value) else None)
        )
        parked_cars_all_df["mestska_cast"] = parked_cars_all_df[
            "mestska_cast"
        ].fillna(inferred_district)
        parked_cars_all_df["kod_zsj"] = parked_cars_all_df["kod_zsj"].fillna(
            "UNMAPPED-" + parked_cars_all_df["CODE"]
        )
        parked_cars_all_df["naz_zsj"] = parked_cars_all_df["naz_zsj"].fillna(
            "Nezařazený úsek " + parked_cars_all_df["CODE"]
        )
        parked_cars_all_df.drop(columns="_zone_area_merge", inplace=True)

        raw_parking_observations = parked_cars_all_df.copy()
        occupancy_detection_config = AnomalyDetectionConfig(
            minimum_absolute_deviation=25.0
        )
        composition_detection_config = AnomalyDetectionConfig(
            minimum_absolute_deviation=20.0
        )
        if apply_anomaly_detection:
            logging.info("Auditing raw monthly observations...")
            observation_candidates = detect_monthly_anomaly_candidates(
                raw_parking_observations,
                value_columns=[*RAW_OCCUPANCY_COLUMNS, *RAW_COMPOSITION_COLUMNS],
                config=composition_detection_config,
            )
            occupancy_candidate = observation_candidates["column"].isin(
                RAW_OCCUPANCY_COLUMNS
            )
            observation_candidates.loc[occupancy_candidate, "threshold"] = (
                observation_candidates.loc[occupancy_candidate, "threshold"].clip(
                    lower=occupancy_detection_config.minimum_absolute_deviation
                )
            )
            observation_candidates = observation_candidates.loc[
                observation_candidates["absolute_deviation"].gt(
                    observation_candidates["threshold"]
                )
            ].reset_index(drop=True)

            logging.info("Correcting verified temporary capacity regimes...")
            parked_cars_all_df, verified_regimes = (
                apply_verified_capacity_regime_cleanup(
                    parked_cars_all_df,
                    code_col="CODE",
                    date_col="date",
                    zps_capacity_col="PS_ZPS",
                    total_capacity_col="CELKEM_PS",
                    occupancy_col="Obs",
                    occupancy_scale=100.0,
                    category_col="CATEGORY",
                )
            )
            hierarchy_issues = (
                parked_cars_all_df["PS_ZPS"].notna()
                & parked_cars_all_df["CELKEM_PS"].notna()
                & parked_cars_all_df["PS_ZPS"].gt(
                    parked_cars_all_df["CELKEM_PS"]
                )
            )
            regime_records = []
            for regime in verified_regimes:
                record = asdict(regime)
                record["start_date"] = regime.start_date.date().isoformat()
                record["end_date"] = regime.end_date.date().isoformat()
                record["confirmed_at"] = regime.confirmed_at.date().isoformat()
                regime_records.append(record)
            capacity_corrections = {
                "mode": "verified paired regimes only",
                "verified_regimes": regime_records,
                "hierarchy_issues_reported": int(hierarchy_issues.sum()),
                "changes": summarize_value_changes(
                    raw_parking_observations,
                    parked_cars_all_df,
                    value_columns=["PS_ZPS", "CELKEM_PS", "Obs"],
                    key_columns=["CODE", "date", "cast_dne"],
                ),
            }
        else:
            logging.info("Skipping anomaly audit and capacity corrections.")
            observation_candidates = pd.DataFrame()
            capacity_corrections = {
                "mode": "disabled",
                "verified_regimes": [],
                "changes": {},
            }

        anomaly_audit = build_anomaly_audit(
            raw_parking_observations,
            observation_candidates,
            configs={
                "occupancy_and_respect": occupancy_detection_config,
                "composition_shares": composition_detection_config,
            },
            capacity_corrections=capacity_corrections,
            group_columns=("CODE", "cast_dne"),
        )
        anomaly_audit["source_normalization"] = asdict(normalization_report)
        anomaly_audit["zone_area_mapping"] = {
            "unmapped_rows_retained": int(missing_zone_area.sum()),
            "unmapped_zone_count": len(missing_zone_area_codes),
            "unmapped_zone_examples": missing_zone_area_codes[:100],
        }
        # rename columns to more readable names
        logging.info("Renaming columns...")
        parked_cars_all_df = rename_and_calculate_columns(parked_cars_all_df)

    # sort by date and oblast
    parked_cars_all_df.sort_values(["kod_useku", "date", "cast_dne"], inplace=True)

    if apply_anomaly_detection:
        logging.info("Validating processed parking data...")
        anomaly_audit["quality_gates"] = validate_processed_parking_data(
            parked_cars_all_df
        )
    else:
        anomaly_audit["quality_gates"] = {"status": "not enforced"}
    anomaly_audit["quality_policy"] = {
        "fatal": [
            "duplicate_logical_rows",
            "capacity_conflict_groups",
            "visitor_identity_mismatches",
            "total_identity_mismatches",
        ],
        "reported_only": ["zps_above_total_rows"],
    }
    resolved_report = Path(
        anomaly_report_file
        or Path(processed_dir) / "data_parking_anomaly_report.json"
    )
    resolved_report.parent.mkdir(parents=True, exist_ok=True)
    resolved_report.write_text(
        json.dumps(anomaly_audit, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logging.info("Anomaly audit saved to: %s", resolved_report)

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


def process_permits_from_buildings():
    # will create a "permits per zone" file

    # load the data

    # read each JSON file in data/downloaded/buildings and run it through the process_json_data function
    # append the results to a list
    data_dir = "data/downloaded/buildings"
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

    # load the mapping of buildings to zones
    try:
        buildings_to_zones_df = pd.read_csv("data/buildings_useky_mapping.csv")
    except Exception:  # todo: better exception handling
        logging.info("Run `process.py domy_na_useky` first.")
        sys.exit(1)

    # rename CODE to building_code
    permits_all_df.rename(columns={"CODE": "building_code"}, inplace=True)

    # join the two dataframes on 'code'
    permits_all_df["building_code"] = permits_all_df["building_code"].astype("int64")
    permits_all_df = permits_all_df.merge(buildings_to_zones_df, on="building_code")

    # group by date and usek_code and sum the values
    permits_all_df = permits_all_df.groupby(["date", "usek_code"]).sum().reset_index()

    # rename XSUM to POP_CELKEM
    permits_all_df.rename(columns={"XSUM": "POP_CELKEM"}, inplace=True)

    # drop the building_code column
    permits_all_df.drop(columns=["building_code"], inplace=True)

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
