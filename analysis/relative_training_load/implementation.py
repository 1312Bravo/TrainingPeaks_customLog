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

# Excel column names
BASELINE_SLA_COLUMN_NAMES = [
     f"Baseline B1 {AGG_VARIABLE_NAME_DICT[AGG_VARIABLE]}",
     f"Baseline B2 {AGG_VARIABLE_NAME_DICT[AGG_VARIABLE]}",
     f"Baseline B3 {AGG_VARIABLE_NAME_DICT[AGG_VARIABLE]}"
     ]

BASELINE_SLA_PROPORTION_COLUMN_NAMES = [
     f"Baseline B1 prop. [%]",
     f"Baseline B2 prop. [%]",
     f"Baseline B3 prop. [%]"
     ]

RECENT_SLA_COLUMN_NAMES = [
     f"Recent B1 {AGG_VARIABLE_NAME_DICT[AGG_VARIABLE]}",
     f"Recent B2 {AGG_VARIABLE_NAME_DICT[AGG_VARIABLE]}",
     f"Recent B3 {AGG_VARIABLE_NAME_DICT[AGG_VARIABLE]}"
     ]

RECENT_SLA_PROPORTION_COLUMN_NAMES = [
     f"Recent B1 prop. [%]",
     f"Recent B2 prop. [%]",
     f"Recent B3 prop. [%]"
     ]


REQUIRED_COLUMNS_ORDER = []
for i in [0,1,2]:
     REQUIRED_COLUMNS_ORDER += [BASELINE_SLA_COLUMN_NAMES[i]]
     REQUIRED_COLUMNS_ORDER += [BASELINE_SLA_PROPORTION_COLUMN_NAMES[i]]
     REQUIRED_COLUMNS_ORDER += [RECENT_SLA_COLUMN_NAMES[i]]
     REQUIRED_COLUMNS_ORDER += [RECENT_SLA_PROPORTION_COLUMN_NAMES[i]]

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

# -------------------------------
# Check if all columns are present, if not, fill them with NaN 
# -------------------------------

for col in REQUIRED_COLUMNS_ORDER:
    if col not in hasr_tl_data.columns:
         hasr_tl_data[col] = np.nan

hasr_tl_data = hasr_tl_data[
     ["Datetime", "Year", "Month", "Day"] +
     ["Aggregate variable"] +
     REQUIRED_COLUMNS_ORDER
     ]

# -------------------------------
# Calculate missing Baseline SLA 
# -------------------------------

missing_baseline_mask = (
     hasr_tl_data
     [BASELINE_SLA_COLUMN_NAMES]
     .isna()
     .any(axis=1)
     )

# Fill row by row
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
            hasr_tl_data.at[idx, BASELINE_SLA_COLUMN_NAMES[0]] = rtl_hf.weighted_mean(b1_baseline_set, b1_baseline_weights)
            hasr_tl_data.at[idx, BASELINE_SLA_COLUMN_NAMES[1]] = rtl_hf.weighted_mean(b2_baseline_set, b2_baseline_weights)
            hasr_tl_data.at[idx, BASELINE_SLA_COLUMN_NAMES[2]] = rtl_hf.weighted_mean(b3_baseline_set, b3_baseline_weights)

            # Proportions in each bucket
            hasr_tl_data.at[idx, BASELINE_SLA_PROPORTION_COLUMN_NAMES[0]] = len(b1_baseline_set)/len(baseline_set)
            hasr_tl_data.at[idx, BASELINE_SLA_PROPORTION_COLUMN_NAMES[1]] = len(b2_baseline_set)/len(baseline_set)
            hasr_tl_data.at[idx, BASELINE_SLA_PROPORTION_COLUMN_NAMES[2]] = len(b3_baseline_set)/len(baseline_set)

# -------------------------------
# Calculate missing Recent SLA 
# -------------------------------

missing_recent_mask = (
     hasr_tl_data
     [RECENT_SLA_COLUMN_NAMES]
     .isna()
     .any(axis=1)
     )

# Fill row by row
for idx in hasr_tl_data.index[missing_recent_mask]:

        first_recent_row = idx - recent_window + 1
        last_recent_row = idx

        first_baseline_row_idx = idx - recent_window - baseline_window + 1
        last_baseline_row_idx = idx - recent_window

        # Do we have enough history to calculate recent buckets values?
        if (first_baseline_row_idx >= 0):

            # Baseline set and quantiles
            baseline_set_idx = (
                base_tl_data
                .loc[first_baseline_row_idx : last_baseline_row_idx, ["Datetime", agg_variable]]
                .sort_values("Datetime", ascending=False)
                [agg_variable]
                .reset_index(drop=True)
                )
            
            qunatile_low_baseline_idx = rtl_hf.weighted_quantile(values=baseline_set_idx, weights=baseline_weights, quantile=quantile_low)
            quantile_high_baseline_idx = rtl_hf.weighted_quantile(values=baseline_set_idx, weights=baseline_weights, quantile=quantile_high)

            # Aggregate variable values & Weights for each bucket
            recent_set = (
                base_tl_data
                .loc[first_recent_row : last_recent_row, ["Datetime", agg_variable]]
                .sort_values("Datetime", ascending=False)
                [agg_variable]
                .reset_index(drop=True)
                )

            # Values for each bucket
            b1_recent_set = recent_set[recent_set <= qunatile_low_baseline_idx]
            b1_recent_weights = recent_weights[recent_set <= qunatile_low_baseline_idx]

            b2_recent_set = recent_set[(recent_set > qunatile_low_baseline_idx) & (recent_set <= quantile_high_baseline_idx)]
            b2_recent_weights = recent_weights[(recent_set > qunatile_low_baseline_idx) & (recent_set <= quantile_high_baseline_idx)]

            b3_recent_set = recent_set[recent_set > quantile_high_baseline_idx]
            b3_recent_weights = recent_weights[recent_set > quantile_high_baseline_idx]

            # Weighted means for each bucket
            hasr_tl_data.at[idx, RECENT_SLA_COLUMN_NAMES[0]] = rtl_hf.weighted_mean(b1_recent_set, b1_recent_weights)
            hasr_tl_data.at[idx, RECENT_SLA_COLUMN_NAMES[1]] = rtl_hf.weighted_mean(b2_recent_set, b2_recent_weights)
            hasr_tl_data.at[idx, RECENT_SLA_COLUMN_NAMES[2]] = rtl_hf.weighted_mean(b3_recent_set, b3_recent_weights)

            # Proportions in each bucket
            hasr_tl_data.at[idx, RECENT_SLA_PROPORTION_COLUMN_NAMES[0]] = len(b1_recent_set)/len(recent_set)
            hasr_tl_data.at[idx, RECENT_SLA_PROPORTION_COLUMN_NAMES[1]] = len(b2_recent_set)/len(recent_set)
            hasr_tl_data.at[idx, RECENT_SLA_PROPORTION_COLUMN_NAMES[2]] = len(b3_recent_set)/len(recent_set)

print("AA")
