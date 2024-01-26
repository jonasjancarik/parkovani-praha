import json
import os
import pandas as pd
from shapely.geometry import Polygon


def map_useky_to_zsj():
    # Function to convert JSON data to Shapely polygons
    def json_to_polygons(json_data):
        def parse_coordinates(feature):
            try:
                return Polygon(feature["geometry"]["coordinates"][0])
            except TypeError:  # sometimes the coordinates are nested one level deeper
                return Polygon(feature["geometry"]["coordinates"][0][0])

        polygons = []
        for feature in json_data["features"]:
            if "KOD_ZSJ" in feature["properties"]:
                polygons.append(
                    {
                        "KOD_ZSJ": feature["properties"]["KOD_ZSJ"],
                        "NAZ_ZSJ": feature["properties"]["NAZ_ZSJ"],
                        "coordinates": parse_coordinates(feature),
                    }
                )
            elif "CODE" in feature["properties"]:
                polygons.append(
                    {
                        "CODE": feature["properties"]["CODE"],
                        "coordinates": parse_coordinates(feature),
                    }
                )
        return polygons

    # list files in data/downloaded/parked_cars
    files = os.listdir("data/downloaded/parked_cars")

    # each file is prefixed with a district code, e.g. P01-OB_201809D_NJ.json
    # we need to extract the district codes and then for each district get the latest two files ending with _NA.json and _NJ.json
    # NA = smaller areas (úseky), NJ = larger areas (zsj)

    # extract district codes
    districts = list(set([file.split("-")[0] for file in files]))

    # extract latest two files ending with D_NA.json and D_NJ.json
    pairs_to_process = []
    for district in districts:
        files_district = [
            file
            for file in files
            if file.startswith(district)
            and (file.endswith("D_NA.json") or file.endswith("D_NJ.json"))
        ]
        files_district = sorted(files_district, reverse=True)
        pairs_to_process.append((files_district[0], files_district[1]))

    overlaps_all = []
    unmatched_all = []

    for i, pair_to_process in enumerate(pairs_to_process, start=1):
        print(f"\nProcessing pair {i}/{len(pairs_to_process)}")
        # Load your JSON data into larger_areas_json and smaller_areas_json
        smaller_areas_polygons = json_to_polygons(
            json.load(
                open(
                    f"data/downloaded/parked_cars/{pair_to_process[1]}",
                    "r",
                    encoding="utf-8",
                )
            )
        )
        larger_areas_polygons = json_to_polygons(
            json.load(
                open(
                    f"data/downloaded/parked_cars/{pair_to_process[0]}",
                    "r",
                    encoding="utf-8",
                )
            )
        )

        # Define overlap threshold
        overlap_threshold = 0.50

        unmatched_polygons = []

        # Check for sufficient overlap
        for ii, small_poly in enumerate(smaller_areas_polygons, start=1):
            print(f"Processing polygon {ii}/{len(smaller_areas_polygons)}", end="\r")

            if small_poly["CODE"].split("-")[0] != pair_to_process[1].split("-")[
                0
            ].replace("P0", "P"):
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
        r"P(\d\b)", "P0\g<1>", regex=True
    )

    # save overlaps_all to csv
    overlaps_all_df.to_csv("data/useky_zsj_mapping.csv", index=False)

    print(f"{len(unmatched_all)} smaller areas unmatched")

    print("Done.")
