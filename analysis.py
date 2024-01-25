import pandas as pd

# Read the data

permits_df = pd.read_csv("data/processed/data_parking_permits_and_spaces.csv")

# drop rows where Oblast contains a dot
permits_df = permits_df[~permits_df["Oblast"].str.contains("\.")]

# drop rows where XSUM is empty
permits_df = permits_df[permits_df["XSUM"].notnull()]

# drop rows where Oblast isn't between P01 and P10
permits_df = permits_df[
    permits_df["Oblast"].str.contains("P0[1-3]")
    # | permits_df["Oblast"].str.contains("P10")
]

# find the oldest and most recent date common to all oblasts
# so for each Oblast, find the oldest and most recent date, then find the newest of the oldest dates and the oldest of the newest dates

oblast_dates = {}

for oblast in permits_df["Oblast"].unique():
    oblast_df = permits_df[permits_df["Oblast"] == oblast]
    oblast_dates[oblast] = (oblast_df["date"].min(), oblast_df["date"].max())

# find the newest of the oldest dates and the oldest of the newest dates
oldest_date = max([x[0] for x in oblast_dates.values()])
newest_date = min([x[1] for x in oblast_dates.values()])

print(f"Oldest date: {oldest_date}")
print(f"Newest date: {newest_date}")

# drop rows where date is outside of the range
permits_df = permits_df[
    (permits_df["date"] >= oldest_date) & (permits_df["date"] <= newest_date)
]

# get a list of all columns starting with POP_
# pop_columns = [x for x in permits_df.columns if x.startswith("POP_")]

pop_columns = ["POP_R", "POP_A", "POP_P"]

# now for each Oblast, compare the number of POP_R in the oldest and newest row (date) and print the difference in percentage

for oblast in permits_df["Oblast"].unique():
    print(f"Oblast: {oblast}")
    oblast_df = permits_df[permits_df["Oblast"] == oblast]

    for pop_column in pop_columns:
        oldest_row = oblast_df[oblast_df["date"] == oldest_date]
        newest_row = oblast_df[oblast_df["date"] == newest_date]
        oldest_pop = oldest_row[pop_column].values[0]
        oldest_xsum = oldest_row["XSUM"].values[0]
        newest_pop = newest_row[pop_column].values[0]
        newest_xsum = newest_row["XSUM"].values[0]
        diff = (newest_pop / newest_xsum) - (oldest_pop / oldest_xsum)
        print(
            f"{pop_column}: {diff:.2%} ({oldest_pop / oldest_xsum:.2%} -> {newest_pop / newest_xsum:.2%}) ({oldest_pop} -> {newest_pop})"
        )

        # forecast the number of permits for 3 years from now

    # now for XSUM as a ratio of RES_PS + MIX_PS
    ratio_newest = newest_row["XSUM"].values[0] / (
        newest_row["RES_PS"].values[0] + newest_row["MIX_PS"].values[0]
    )
    ratio_oldest = oldest_row["XSUM"].values[0] / (
        oldest_row["RES_PS"].values[0] + oldest_row["MIX_PS"].values[0]
    )
    diff = ratio_newest - ratio_oldest

    print(
        f"POP / PS (RES+MIX): {diff:.2%} ({ratio_oldest:.2%} -> {ratio_newest:.2%}) ({oldest_row['XSUM'].values[0]} -> {newest_row['XSUM'].values[0]})"
    )
