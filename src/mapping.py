import json
import os
import pandas as pd
from shapely.geometry import Polygon, Point
import geopandas as gpd
from .utils import session_setup
import requests
import shutil
import time


def download_area_data():
    # list files in data/downloaded/parked_cars
    files = os.listdir("data/downloaded/parked_cars")

    # each file is prefixed with a district code, e.g. P01-OB_201809D_NJ.json
    # we need to extract the district codes and then for each district get the latest two files ending with _NA.json and _NJ.json
    # NA = smaller areas (úseky), NJ = larger areas (zsj)

    # extract district codes
    districts = list(set([file.split("-")[0] for file in files]))

    # prepare a http session
    s = session_setup(requests.Session())

    # for each district, get the latest file _NJ.json file (area-level data, which is not used for other analyses - we need it to get the area shapes)
    # todo: simplify, this is adapted from older code that worked only with the lastest _NA.json files

    for district in districts:
        files_district = [
            file
            for file in files
            if file.startswith(district) and (file.endswith("D_NA.json"))
        ]
        zone_data_file = sorted(files_district, reverse=True)[0]

        # download the NJ file
        nj_file_name = zone_data_file.replace("D_NA", "D_NJ")
        url = f"https://zps.tsk-praha.cz/puzzle/genmaps/{district}/{nj_file_name.split("-")[1]}"

        r = s.get(url)

        if r.status_code != 200:
            raise Exception(f"Error while downloading {url}")

        os.makedirs("data/downloaded/temp-area-data", exist_ok=True)

        with open(
            f"data/downloaded/temp-area-data/{nj_file_name}",
            "w",
            encoding="utf-8",
        ) as f:
            f.write(r.text)


def get_zones_polygons(district=None):
    zone_files = os.listdir("data/downloaded/parked_cars")
    if district:
        zone_files = [file for file in zone_files if file.startswith(district)]

    zone_polygons = []

    for i, file in enumerate(zone_files, start=1):
        print(f"Processing {i}/{len(zone_files)} ({file})", end="\r")
        try:
            zone_polygons_this_district = json_to_polygons(  # todo: this could definitely be more efficient, we don't have to convert to polys zones that we already have
                json.load(
                    open(
                        f"data/downloaded/parked_cars/{file}",
                        "r",
                        encoding="utf-8",
                    )
                )
            )
        except json.decoder.JSONDecodeError:  # sometimes the json is malformed
            continue

        # extract existing zone codes into a set
        existing_zone_codes = set([poly["CODE"] for poly in zone_polygons])

        for poly in zone_polygons_this_district:
            if poly["CODE"] not in existing_zone_codes:
                zone_polygons.append(poly)

    return zone_polygons


def json_to_polygons(json_data):
    """
    Convert TSK JSON data to Shapely polygons
    """

    def parse_coordinates(feature, house=False):
        if house and feature.get("geometry"):
            try:
                return Point(feature["geometry"]["coordinates"])
            except TypeError:
                return None

        try:
            return Polygon(feature["geometry"]["coordinates"][0])
        except TypeError:  # sometimes the coordinates are nested one level deeper
            try:
                return Polygon(feature["geometry"]["coordinates"][0][0])
            except TypeError:
                # sometimes the coordinates are empty (None)
                return None

    polygons = []
    for feature in json_data["features"]:
        if "KOD_ZSJ" in feature["properties"]:
            polygon_data = {
                "KOD_ZSJ": feature["properties"]["KOD_ZSJ"],
                "NAZ_ZSJ": feature["properties"]["NAZ_ZSJ"],
                "coordinates": parse_coordinates(feature),
            }
            if polygon_data["coordinates"]:
                polygons.append(polygon_data)
        elif "CODE" in feature["properties"]:
            if "-" in str(feature["properties"]["CODE"]):  # zone codes have a dash
                polygon_data = {
                    "CODE": feature["properties"]["CODE"],
                    "coordinates": parse_coordinates(feature),
                }
                if polygon_data["coordinates"]:
                    polygons.append(polygon_data)
            else:
                polygon_data = {
                    "CODE": feature["properties"]["CODE"],
                    "coordinates": parse_coordinates(feature, house=True),
                }
                if polygon_data["coordinates"]:
                    polygons.append(polygon_data)

    return polygons


def map_zones_to_areas():
    # start timer
    start_time = time.time()

    files_zone = os.listdir("data/downloaded/parked_cars")
    districts = list(set([file.split("-")[0] for file in files_zone]))

    download_area_data()

    overlaps_all = []
    unmatched_all = []

    for i, district in enumerate(districts, start=1):
        print(f"\nProcessing district {i}/{len(districts)} ({district})")

        # Load JSON data into larger_areas_json (areas) and smaller_areas_json (zones)

        # For smaller areas, we will cycle through all the districts' files in data/downloaded/parked_cars to get all the zones, even if they are not currently in use

        # find file in data/downloaded/temp-area-data that matches the district code
        area_data_file = list(
            filter(
                lambda x: x.startswith(district),
                os.listdir("data/downloaded/temp-area-data"),
            )
        )[0]

        larger_areas_polygons = json_to_polygons(
            json.load(
                open(
                    f"data/downloaded/temp-area-data/{area_data_file}",
                    "r",
                    encoding="utf-8",
                )
            )
        )

        smaller_areas_polygons = get_zones_polygons(district)

        # Define overlap threshold
        overlap_threshold = 0.50

        unmatched_polygons = []

        # Check for sufficient overlap
        for ii, small_poly in enumerate(smaller_areas_polygons, start=1):
            print(f"Processing polygon {ii}/{len(smaller_areas_polygons)}", end="\r")

            if small_poly["CODE"].split("-")[0] != district.replace("P0", "P"):
                continue  # skip if the district code doesn't match

            # hardcoded cases for smaller areas that are not matched for some reason

            if small_poly["CODE"] == "P4-1428":
                if small_poly["CODE"].split("-")[0] != "P4":
                    continue  # this P4 area appears in P1 data for some reason
                overlaps_all.append(
                    {
                        "kod_zsj": "127621",
                        "naz_zsj": "Na Zelené lišce",
                        "code": small_poly["CODE"],
                        "overlap": 1,
                        "mestska_cast": small_poly["CODE"].split("-")[0],
                    }
                )
                continue
            elif small_poly["CODE"] == "P4-1774":
                overlaps_all.append(
                    {
                        "kod_zsj": "127906",
                        "naz_zsj": "Děkanka",
                        "code": small_poly["CODE"],
                        "overlap": 1,
                        "mestska_cast": small_poly["CODE"].split("-")[0],
                    }
                )
                continue
            elif small_poly["CODE"] == "P9-9018":
                overlaps_all.append(
                    {
                        "kod_zsj": "131369",
                        "naz_zsj": "Na Vyhlídce",
                        "code": small_poly["CODE"],
                        "overlap": 1,
                        "mestska_cast": small_poly["CODE"].split("-")[0],
                    }
                )
                continue
            elif small_poly["CODE"] == "P7-0176":
                overlaps_all.append(
                    {
                        "kod_zsj": "305961",
                        "naz_zsj": "Letná",
                        "code": small_poly["CODE"],
                        "overlap": 1,
                        "mestska_cast": small_poly["CODE"].split("-")[0],
                    }
                )
                continue

            matched = False
            overlaps = []
            for large_poly in larger_areas_polygons:
                intersection = small_poly["coordinates"].intersection(
                    large_poly["coordinates"]
                )
                overlap_percentage = intersection.area / small_poly["coordinates"].area
                if overlap_percentage >= overlap_threshold:
                    # print(
                    #     f"Smaller area {small_poly['CODE']} is {overlap_percentage:.2%} inside larger area {large_poly['NAZ_ZSJ']}"
                    # )
                    overlaps_all.append(
                        {
                            "kod_zsj": large_poly["KOD_ZSJ"],
                            "naz_zsj": large_poly["NAZ_ZSJ"],
                            "code": small_poly["CODE"],
                            "overlap": overlap_percentage,
                            "mestska_cast": small_poly["CODE"].split("-")[0],
                        }
                    )
                    matched = True
                    break
                elif overlap_percentage > 0:
                    overlaps.append(
                        {
                            "overlap": overlap_percentage,
                            "large": large_poly,
                            "small": small_poly,
                        }
                    )
            if not matched:
                if not overlaps:
                    unmatched_polygons.append(small_poly)
                else:
                    # sort overlaps by overlap percentage
                    overlaps = sorted(
                        overlaps, key=lambda k: k["overlap"], reverse=True
                    )
                    # print(
                    #     f"--> Smaller area {small_poly['CODE']} is {overlaps[0]['overlap']:.2%} inside larger area {overlaps[0]['large']}"
                    # )
                    overlaps_all.append(
                        {
                            "kod_zsj": overlaps[0]["large"]["KOD_ZSJ"],
                            "naz_zsj": overlaps[0]["large"]["NAZ_ZSJ"],
                            "code": small_poly["CODE"],
                            "overlap": overlaps[0]["overlap"],
                            "mestska_cast": small_poly["CODE"].split("-")[0],
                        }
                    )

        unmatched_all += unmatched_polygons

    # convert overlaps_all to dataframe
    overlaps_all_df = pd.DataFrame(overlaps_all)

    # replace P with P0 in the mestka_cast column
    overlaps_all_df["mestska_cast"] = overlaps_all_df["mestska_cast"].str.replace(
        r"P(\d\b)", r"P0\g<1>", regex=True
    )

    # sort by code
    overlaps_all_df = overlaps_all_df.sort_values(by="code")

    # save overlaps_all to csv
    overlaps_all_df.to_csv("data/useky_zsj_mapping.csv", index=False)

    if unmatched_all:
        print(f"{len(unmatched_all)} smaller areas unmatched")
        print("Unmatched areas:")
        for poly in unmatched_all:
            print(poly["CODE"])

    # remove temporary files
    shutil.rmtree("data/downloaded/temp-area-data")

    # end tiemer
    print(f"Done in {time.time() - start_time:.2f} seconds.")


def map_houses_to_zones():
    # start timer
    start_time = time.time()

    useky_polygons = get_zones_polygons()

    print(f"Loaded {len(useky_polygons)} zones           ")

    # get houses coordinates from the latest file
    print("Loading houses...")

    houses_points = json_to_polygons(
        json.load(
            open(
                f"data/downloaded/houses/{sorted(os.listdir("data/downloaded/houses"), reverse=True)[0]}",
                "r",
                encoding="utf-8",
            )
        )
    )

    print("Mapping houses to zones...")

    # Convert data into GeoDataFrames
    houses_gdf = gpd.GeoDataFrame(houses_points, geometry="coordinates")
    useky_gdf = gpd.GeoDataFrame(useky_polygons, geometry="coordinates")

    # Spatial join on the nearest úsek
    joined_gdf = gpd.sjoin_nearest(
        houses_gdf, useky_gdf, how="left", distance_col="distance"
    )

    # Extract the columns we need
    final_gdf = joined_gdf[["CODE_left", "CODE_right"]]

    # Rename the columns
    final_gdf = final_gdf.rename(
        columns={"CODE_left": "house_code", "CODE_right": "usek_code"}
    )

    # sort by house_code
    final_gdf = final_gdf.sort_values(by="house_code")

    # Save the result to a CSV file
    final_gdf.to_csv("data/houses_useky_mapping.csv", index=False)

    # end timer
    print(f"Done in {time.time() - start_time:.2f} seconds.")
