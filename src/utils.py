import os
import pandas as pd
import sys


def load_files_to_df(category):
    files_path = f"data/downloaded/{category}"
    try:
        files = [os.path.join(files_path, f) for f in os.listdir(files_path)]
    except FileNotFoundError:
        print(f"No files found in {files_path}")
        sys.exit(1)

    df = pd.concat(
        [pd.read_csv(file, encoding="utf-8", delimiter="\t") for file in files],
        ignore_index=True,
    )
    return df


def get_date_from_filename(filename):
    # filenames look like P01-OB_202311D_NA.json, where 202311 is the year and month
    # - extract the year and month from the filename and use the last day of the month
    year_and_month = filename.split("_")[1][:6]
    year = year_and_month[:4]
    month = year_and_month[4:]
    date = pd.to_datetime(f"{year}-{month}-01") + pd.offsets.MonthEnd(0)
    return date


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
