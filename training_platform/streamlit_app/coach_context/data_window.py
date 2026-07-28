from datetime import timedelta

import pandas as pd


# ----------------------------------------------------------
# Date Helpers
# ----------------------------------------------------------

# Adds a Date column from Year, Month, and Day columns when possible.
# Existing invalid or missing date parts become empty dates and are filtered out later.
# Returns a dataframe copy with a Date column.

def add_date_column(dataframe: pd.DataFrame) -> pd.DataFrame:
    dated_frame = dataframe.copy()

    if {"Year", "Month", "Day"}.issubset(dated_frame.columns):
        dated_frame["Date"] = pd.to_datetime(dated_frame[["Year", "Month", "Day"]], errors="coerce")
        return dated_frame

    if "Date" in dated_frame.columns:
        dated_frame["Date"] = pd.to_datetime(dated_frame["Date"], errors="coerce")
        return dated_frame

    dated_frame["Date"] = pd.NaT
    return dated_frame


# Finds the newest valid date across all loaded context dataframes.
# Context windows use this as the anchor because source data can lag behind today's date.
# Returns a pandas Timestamp or None.

def find_latest_context_date(dataframes: list[pd.DataFrame]) -> pd.Timestamp | None:
    latest_dates = []

    for dataframe in dataframes:
        if dataframe.empty:
            continue

        dated_frame = add_date_column(dataframe)
        latest_date = dated_frame["Date"].max()

        if pd.notna(latest_date):
            latest_dates.append(latest_date)

    if not latest_dates:
        return None

    return max(latest_dates)


# Calculates the inclusive start and end dates for a selected data-context window.
# All available has no bounds, while no data is handled before filtering.
# Returns a tuple of optional start/end timestamps.

def get_window_bounds(window: str, anchor_date: pd.Timestamp | None) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    if window == "All available (expensive)" or anchor_date is None:
        return None, None

    normalized_anchor = anchor_date.normalize()

    if window == "Yesterday":
        target_date = normalized_anchor - timedelta(days=1)
        return target_date, target_date

    if window == "Last 7 days":
        return normalized_anchor - timedelta(days=6), normalized_anchor

    if window == "Last 14 days":
        return normalized_anchor - timedelta(days=13), normalized_anchor

    if window == "Last month":
        return normalized_anchor - timedelta(days=30), normalized_anchor

    if window == "Last 3 months":
        return normalized_anchor - timedelta(days=90), normalized_anchor

    if window == "Last year":
        return normalized_anchor - timedelta(days=365), normalized_anchor

    return None, None


# Filters one dataframe to the selected context window.
# Filtering keeps all original columns and only uses the temporary Date column internally.
# Returns the filtered dataframe.

def filter_dataframe_by_window(dataframe: pd.DataFrame, window: str, anchor_date: pd.Timestamp | None) -> pd.DataFrame:
    if dataframe.empty or window == "All available (expensive)":
        return dataframe

    dated_frame = add_date_column(dataframe)
    start_date, end_date = get_window_bounds(window, anchor_date)

    if start_date is None or end_date is None:
        return dataframe

    filtered_frame = dated_frame[(dated_frame["Date"] >= start_date) & (dated_frame["Date"] <= end_date)]

    return filtered_frame.drop(columns=["Date"], errors="ignore")
