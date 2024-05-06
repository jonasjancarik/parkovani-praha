import pandas as pd
# import numpy as np


def load_data(parking_path, permits_path):
    data_parking = pd.read_csv(parking_path)
    data_permits = pd.read_csv(permits_path)
    return data_parking, data_permits


def preprocess_data(data_parking, data_permits):
    data_merged = pd.merge(
        data_parking, data_permits, on=["kod_useku", "date"], how="left"
    )
    data_merged["date"] = pd.to_datetime(data_merged["date"])
    data_merged["year"] = data_merged["date"].dt.year
    return data_merged


# def calculate_changes(data_merged):
#     # Select only numeric columns for averaging; exclude identifier columns like 'kod_zsj' and 'year'
#     numeric_columns = data_merged.select_dtypes(include=[np.number]).columns.drop(
#         ["kod_zsj", "year"]
#     )

#     earliest_years = data_merged.groupby("naz_zsj")["year"].min()
#     latest_years = data_merged.groupby("naz_zsj")["year"].max()

#     results = []

#     for zsj, earliest_year in earliest_years.items():
#         latest_year = latest_years[zsj]
#         if latest_year == 2024:
#             latest_year -= 1

#         # Filter data for the earliest and latest year for this ZSJ
#         data_earliest = data_merged[
#             (data_merged["naz_zsj"] == zsj) & (data_merged["year"] == earliest_year)
#         ]
#         data_latest = data_merged[
#             (data_merged["naz_zsj"] == zsj) & (data_merged["year"] == latest_year)
#         ]

#         # Calculate the mean for numeric columns
#         mean_earliest = data_earliest[numeric_columns].mean()
#         mean_latest = data_latest[numeric_columns].mean()

#         # Calculate the percentage change and prepare the result row
#         change = ((mean_latest - mean_earliest) / mean_earliest) * 100
#         change["naz_zsj"] = zsj  # Add the human-readable ZSJ name for reference

#         # Also include the 'kod_zsj' and both years for reference
#         change["kod_zsj"] = data_earliest["kod_zsj"].iloc[0]
#         change["earliest_year"] = earliest_year
#         change["latest_year"] = latest_year

#         results.append(change)

#     # Combine all results into a single DataFrame
#     percentage_change = pd.DataFrame(results)
#     return percentage_change


def main():
    parking_path = "data/processed/data_parking.csv"
    permits_path = "data/processed/data_permits_by_zone.csv"

    data_parking, data_permits = load_data(parking_path, permits_path)
    data_merged = preprocess_data(data_parking, data_permits)

    data_merged.to_csv("data/processed/data_parking_and_permits.csv", index=False)

    # percentage_change = calculate_changes(data_merged)
    # print(percentage_change)


if __name__ == "__main__":
    main()
