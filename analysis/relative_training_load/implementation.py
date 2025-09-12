# ----------------------------------------------------- 
# Libraries
# -----------------------------------------------------

import os
import sys
repo_root = os.path.abspath(os.getcwd())  
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

import pandas as pd
import numpy as np
import gspread

from src import config
from src import help_functions as hf
from analysis.relative_training_load import help_functions as rtl_hf

# "config"
BASELINE_WINDOW = 90
RECENT_WINDOW = 21
LAMBDA_BASE = 0.978
hasr_tl_WEIGHT_B1 = 0.15
hasr_tl_WEIGHT_B2 = 0.45
hasr_tl_WEIGHT_B3 = 0.4
QUANTILE_LOW = 0.60,
QUANTILE_HIGH = 0.85,
AGG_VARIABLE = "Training load"
AGG_VARIABLE_NAME_DICT = {
    "Training load": "TL"
    }

# Column names mapping
column_names_excel_internal_mapping = {
    f"Aggregate variable": "agg_var",
    f"Baseline B1 {AGG_VARIABLE_NAME_DICT[AGG_VARIABLE]}": f"baseline_b1_{AGG_VARIABLE_NAME_DICT[AGG_VARIABLE]}_weighted_mean",
    f"Baseline B2 {AGG_VARIABLE_NAME_DICT[AGG_VARIABLE]}": f"baseline_b2_{AGG_VARIABLE_NAME_DICT[AGG_VARIABLE]}_weighted_mean",
    f"Baseline B3 {AGG_VARIABLE_NAME_DICT[AGG_VARIABLE]}": f"baseline_b3_{AGG_VARIABLE_NAME_DICT[AGG_VARIABLE]}_weighted_mean",
    f"Recent B1 {AGG_VARIABLE_NAME_DICT[AGG_VARIABLE]}": f"recent_b1_{AGG_VARIABLE_NAME_DICT[AGG_VARIABLE]}_weighted_mean",
    f"Recent B2 {AGG_VARIABLE_NAME_DICT[AGG_VARIABLE]}": f"recent_b2_{AGG_VARIABLE_NAME_DICT[AGG_VARIABLE]}_weighted_mean",
    f"Recent B3 {AGG_VARIABLE_NAME_DICT[AGG_VARIABLE]}": f"recent_b3_{AGG_VARIABLE_NAME_DICT[AGG_VARIABLE]}_weighted_mean"
}
column_names_internal_excel_mapping = {value: key for key, value in column_names_excel_internal_mapping.items()}

# Get data
googleDrive_client = gspread.authorize(config.DRIVE_CREDENTIALS)
training_data, _ = hf.import_google_sheet(googleDrive_client=googleDrive_client, filename=config.DRIVE_TP_LOG_FILENAMES[0], sheet_name="Raw Training Data")
hasr_tl_data, _ = hf.import_google_sheet(googleDrive_client=googleDrive_client, filename=config.DRIVE_TP_LOG_FILENAMES[0], sheet_name=f"HASR-{AGG_VARIABLE_NAME_DICT[AGG_VARIABLE]}")

# "Clean" data
training_data = hf.data_safe_convert_to_numeric(training_data)
hasr_tl_data = hf.data_safe_convert_to_numeric(hasr_tl_data)

# Fill HASR-TL data Y-M-D values based on Training data [other columns are nan, what we actually want]
last_date_training_data = pd.to_datetime(training_data[["Year", "Month", "Day"]]).max()
last_date_hasr_tl_data = pd.to_datetime(hasr_tl_data[["Year", "Month", "Day"]]).max()
if last_date_hasr_tl_data < last_date_training_data:

    new_hasr_tl_data_dates = pd.date_range(
        start = last_date_hasr_tl_data + pd.Timedelta(days=1), 
        end = last_date_training_data, 
        freq = "D"
        )
    
    new_hasr_tl_data_rows = pd.DataFrame({
        "Year": new_hasr_tl_data_dates.year, 
        "Month": new_hasr_tl_data_dates.month, 
        "Day": new_hasr_tl_data_dates.day,
        "Aggregate variable": AGG_VARIABLE
        })
    
    hasr_tl_data = pd.concat([hasr_tl_data, new_hasr_tl_data_rows], ignore_index = True)
    
# Prepare data for calculations
training_data["Datetime"] = pd.to_datetime(training_data[["Year", "Month", "Day"]])
training_data = training_data.sort_values(by="Datetime").reset_index(drop=True)
tl_data = (
    training_data
    .groupby("Datetime")
    .agg({AGG_VARIABLE: "sum"})
    .reset_index()
)

hasr_tl_data["Datetime"] = pd.to_datetime(hasr_tl_data[["Year", "Month", "Day"]])

# Sort by datetime, just to be really sure
tl_data = tl_data.sort_values("Datetime", ascending=True).reset_index(drop=True)
hasr_tl_data = hasr_tl_data.sort_values("Datetime", ascending=True).reset_index(drop=True)


# Baseline and Recent window weights
baseline_window_days = range(1, BASELINE_WINDOW+1)
baseline_window_weights = np.array([LAMBDA_BASE ** (j-1) for j in baseline_window_days])
baseline_window_normalized_weights = baseline_window_weights / sum(baseline_window_weights)

recent_window_days = range(1, RECENT_WINDOW+1)
recent_window_weights = np.array([LAMBDA_BASE ** (j-1) for j in recent_window_days])
recent_window_normalized_weights = recent_window_weights / sum(recent_window_weights)

# -------------------------------
# GOGO!
# -------------------------------

# Define parameters
base_tl_data = tl_data
hasr_tl_data = hasr_tl_data
baseline_window = BASELINE_WINDOW
baseline_weights = baseline_window_normalized_weights
recent_window = RECENT_WINDOW
recent_weights = recent_window_normalized_weights
quantile_low = QUANTILE_LOW
quantile_high = QUANTILE_HIGH
agg_variable = AGG_VARIABLE

# Go, calculate


# Calculate missing Baseline SLA 
missing_baseline_mask = hasr_tl_data[[
    f"Baseline B1 {AGG_VARIABLE_NAME_DICT[AGG_VARIABLE]}", 
    f"Baseline B2 {AGG_VARIABLE_NAME_DICT[AGG_VARIABLE]}", 
    f"Baseline B3 {AGG_VARIABLE_NAME_DICT[AGG_VARIABLE]}"]
    ].isna().any(axis=1)

# Missing -> Fill row by row
for idx in hasr_tl_data.index[missing_baseline_mask]:

        first_baseline_row = idx - baseline_window + 1
        last_baseline_row = idx

        # Do we have enough history to calculate baseline buckets values?
        if (first_baseline_row >= 0):

            # Baseline set
            baseline_set = (
                base_tl_data
                .loc[first_baseline_row : last_baseline_row, ["Datetime", agg_variable]]
                .sort_values("Datetime", ascending=False)
                [agg_variable]
                .reset_index(drop=True)
                )
            
            # Aggregate variable values & Weights for each bucket
            quantile_low_baseline = rtl_hf.weighted_quantile(values=baseline_set, weights=baseline_weights, quantile=quantile_low)
            quantile_high_baseline = rtl_hf.weighted_quantile(values=baseline_set, weights=baseline_weights, quantile=quantile_high)

            b1_baseline_set = baseline_set[baseline_set <= quantile_low_baseline]
            b1_baseline_weights = baseline_weights[baseline_set <= quantile_low_baseline]

            b2_baseline_set = baseline_set[(baseline_set > quantile_low_baseline) & (baseline_set <= quantile_high_baseline)]
            b2_baseline_weights = baseline_weights[(baseline_set > quantile_low_baseline) & (baseline_set <= quantile_high_baseline)]

            b3_baseline_set = baseline_set[baseline_set > quantile_high_baseline]
            b3_baseline_weights = baseline_weights[baseline_set > quantile_high_baseline]

            # Fill selected row baseline buckets values
            hasr_tl_data.at[idx, f"Baseline B1 {AGG_VARIABLE_NAME_DICT[AGG_VARIABLE]}"] = rtl_hf.weighted_mean(b1_baseline_set, b1_baseline_weights)
            hasr_tl_data.at[idx, f"Baseline B2 {AGG_VARIABLE_NAME_DICT[AGG_VARIABLE]}"] = rtl_hf.weighted_mean(b2_baseline_set, b2_baseline_weights)
            hasr_tl_data.at[idx, f"Baseline B3 {AGG_VARIABLE_NAME_DICT[AGG_VARIABLE]}"] = rtl_hf.weighted_mean(b3_baseline_set, b3_baseline_weights)

            # Proportions in each bucket
            # hasr_tl_data.at[idx, f"baseline_b1_{variable_name}_proportion"] = len(b1_baseline_set)/len(baseline_set)
            # hasr_tl_data.at[idx, f"baseline_b2_{variable_name}_proportion"] = len(b2_baseline_set)/len(baseline_set)
            # hasr_tl_data.at[idx, f"baseline_b3_{variable_name}_proportion"] = len(b3_baseline_set)/len(baseline_set)


print("AA")
