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
import contextlib
from io import StringIO

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
BASELINE_SLA_VALUE_COLUMN_NAMES = [
     f"Baseline B1 {AGG_VARIABLE_NAME_DICT[AGG_VARIABLE]}",
     f"Baseline B2 {AGG_VARIABLE_NAME_DICT[AGG_VARIABLE]}",
     f"Baseline B3 {AGG_VARIABLE_NAME_DICT[AGG_VARIABLE]}"
     ]

BASELINE_SLA_PROPORTION_COLUMN_NAMES = [
     f"Baseline B1 prop. [%]",
     f"Baseline B2 prop. [%]",
     f"Baseline B3 prop. [%]"
     ]

RECENT_SLA_VALUE_COLUMN_NAMES = [
     f"Recent B1 {AGG_VARIABLE_NAME_DICT[AGG_VARIABLE]}",
     f"Recent B2 {AGG_VARIABLE_NAME_DICT[AGG_VARIABLE]}",
     f"Recent B3 {AGG_VARIABLE_NAME_DICT[AGG_VARIABLE]}"
     ]

RECENT_SLA_PROPORTION_COLUMN_NAMES = [
     f"Recent B1 prop. [%]",
     f"Recent B2 prop. [%]",
     f"Recent B3 prop. [%]"
     ]

REQUIRED_COLUMNS_ORDER = ["Year", "Month", "Day", "Aggregate variable"]
for i in [0,1,2]:
     REQUIRED_COLUMNS_ORDER += [BASELINE_SLA_VALUE_COLUMN_NAMES[i]]
     REQUIRED_COLUMNS_ORDER += [BASELINE_SLA_PROPORTION_COLUMN_NAMES[i]]
     REQUIRED_COLUMNS_ORDER += [RECENT_SLA_VALUE_COLUMN_NAMES[i]]
     REQUIRED_COLUMNS_ORDER += [RECENT_SLA_PROPORTION_COLUMN_NAMES[i]]

# Get data
googleDrive_client = gspread.authorize(config.DRIVE_CREDENTIALS)
training_data_raw, training_data_sheet = hf.import_google_sheet(googleDrive_client=googleDrive_client, filename=config.DRIVE_TP_LOG_FILENAMES[0], sheet_name="Raw Training Data")
hasr_tl_data_raw, hasr_tl_data_sheet = hf.import_google_sheet(googleDrive_client=googleDrive_client, filename=config.DRIVE_TP_LOG_FILENAMES[0], sheet_name=f"HASR-{AGG_VARIABLE_NAME_DICT[AGG_VARIABLE]}")

# "Clean" data
training_data = hf.data_safe_convert_to_numeric(training_data_raw.copy(deep=True))
hasr_tl_data = hf.data_safe_convert_to_numeric(hasr_tl_data_raw.copy(deep=True))
    
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
# We work based on Datetime
# -------------------------------

base_tl_data = base_tl_data.set_index("Datetime").sort_index()
hasr_tl_data = hasr_tl_data.set_index("Datetime").sort_index()

base_tl_data_datetimes = base_tl_data.index
last_hasr_tl_data_datetime = hasr_tl_data.index.max()
missing_hasr_tl_data_datetimes = base_tl_data.loc[base_tl_data.index > last_hasr_tl_data_datetime].index

# -------------------------------
# Calculate missing HASR-TL values
# -------------------------------

# Fill row by row
for date in missing_hasr_tl_data_datetimes:

        # Baseline window ~> Baseline window immediately after the recent window
        first_baseline_date = date - pd.Timedelta(days=recent_window + baseline_window - 1)
        last_baseline_date = date - pd.Timedelta(days=recent_window)

        # Recent window ~> Recent window including "date"
        first_recent_date = date - pd.Timedelta(days=recent_window-1)
        last_recent_date = date

        # -------------------------------
        # BASELINE VALUES
        # -------------------------------

        # We have enough history to calculate baseline buckets values
        if first_baseline_date in base_tl_data_datetimes:

            # Baseline set
            baseline_set = (
                base_tl_data
                .loc[(base_tl_data.index >= first_baseline_date) & (base_tl_data.index <= last_baseline_date), agg_variable]
                .sort_index(ascending=False)
                .reset_index(drop=True)
                )
            
            # Buckets sets & Weights
            quantile_low_baseline = rtl_hf.weighted_quantile(values=baseline_set, weights=baseline_weights, quantile=quantile_low)
            quantile_high_baseline = rtl_hf.weighted_quantile(values=baseline_set, weights=baseline_weights, quantile=quantile_high)

            b1_baseline_set = baseline_set[baseline_set <= quantile_low_baseline]
            b1_baseline_weights = baseline_weights[baseline_set <= quantile_low_baseline]

            b2_baseline_set = baseline_set[(baseline_set > quantile_low_baseline) & (baseline_set <= quantile_high_baseline)]
            b2_baseline_weights = baseline_weights[(baseline_set > quantile_low_baseline) & (baseline_set <= quantile_high_baseline)]

            b3_baseline_set = baseline_set[baseline_set > quantile_high_baseline]
            b3_baseline_weights = baseline_weights[baseline_set > quantile_high_baseline]

            # Aggregate buckets values
            baseline_b1_value = rtl_hf.weighted_mean(b1_baseline_set, b1_baseline_weights)
            baseline_b2_value = rtl_hf.weighted_mean(b2_baseline_set, b2_baseline_weights)
            baseline_b3_value = rtl_hf.weighted_mean(b3_baseline_set, b3_baseline_weights)

            # Proportions in each bucket
            baseline_b1_proportion = len(b1_baseline_set)/len(baseline_set) * 100
            baseline_b2_proportion = len(b2_baseline_set)/len(baseline_set) * 100
            baseline_b3_proportion = len(b3_baseline_set)/len(baseline_set) * 100
        
        # We do not have enough history to calculate baseline buckets values
        else:
            baseline_b1_value = np.nan
            baseline_b2_value = np.nan
            baseline_b3_value = np.nan

            baseline_b1_proportion = np.nan
            baseline_b2_proportion = np.nan 
            baseline_b3_proportion = np.nan
             

        # -------------------------------
        # RECENT VALUES
        # -------------------------------

        # We have enough history to calculate recent buckets values
        if first_recent_date in base_tl_data_datetimes:

            # Recent set
            recent_set = (
                base_tl_data
                .loc[(base_tl_data.index >= first_recent_date) & (base_tl_data.index <= last_recent_date), agg_variable]
                .sort_index(ascending=False)
                .reset_index(drop=True)
            )

            # Aggregate variable values & Weights for each bucket
            b1_recent_set = recent_set[recent_set <= quantile_low_baseline]
            b1_recent_weights = recent_weights[recent_set <= quantile_low_baseline]

            b2_recent_set = recent_set[(recent_set > quantile_low_baseline) & (recent_set <= quantile_high_baseline)]
            b2_recent_weights = recent_weights[(recent_set > quantile_low_baseline) & (recent_set <= quantile_high_baseline)]

            b3_recent_set = recent_set[recent_set > quantile_high_baseline]
            b3_recent_weights = recent_weights[recent_set > quantile_high_baseline]

            # Aggregate buckets values
            recent_b1_value = rtl_hf.weighted_mean(b1_recent_set, b1_recent_weights)
            recent_b2_value = rtl_hf.weighted_mean(b2_recent_set, b2_recent_weights)
            recent_b3_value = rtl_hf.weighted_mean(b3_recent_set, b3_recent_weights)

            # Proportions in each bucket
            recent_b1_proportion = len(b1_recent_set)/len(recent_set) * 100
            recent_b2_proportion = len(b2_recent_set)/len(recent_set) * 100
            recent_b3_proportion = len(b3_recent_set)/len(recent_set) * 100
        
        # We do not have enough history to calculate recent buckets values
        else:
            recent_b1_value = np.nan
            recent_b2_value = np.nan
            recent_b3_value = np.nan

            recent_b1_proportion = np.nan
            recent_b2_proportion = np.nan
            recent_b3_proportion = np.nan

        # -------------------------------
        # ADD NEW ROW
        # -------------------------------

        # Fill selected row baseline buckets values
        new_date_hasr_tl_data_row_dict = {
                "Year": date.year,
                "Month": date.month,
                "Day": date.day,
                "Aggregate variable": agg_variable,

                BASELINE_SLA_VALUE_COLUMN_NAMES[0]: round(baseline_b1_value, 2),
                BASELINE_SLA_VALUE_COLUMN_NAMES[1]: round(baseline_b2_value, 2),
                BASELINE_SLA_VALUE_COLUMN_NAMES[2]: round(baseline_b3_value, 2),

                BASELINE_SLA_PROPORTION_COLUMN_NAMES[0]: round(baseline_b1_proportion, 2),
                BASELINE_SLA_PROPORTION_COLUMN_NAMES[1]: round(baseline_b2_proportion, 2),
                BASELINE_SLA_PROPORTION_COLUMN_NAMES[2]: round(baseline_b3_proportion, 2),

                RECENT_SLA_VALUE_COLUMN_NAMES[0]: round(recent_b1_value, 2),
                RECENT_SLA_VALUE_COLUMN_NAMES[1]: round(recent_b2_value, 2),
                RECENT_SLA_VALUE_COLUMN_NAMES[2]: round(recent_b3_value, 2),

                RECENT_SLA_PROPORTION_COLUMN_NAMES[0]: round(recent_b1_proportion, 2),
                RECENT_SLA_PROPORTION_COLUMN_NAMES[1]: round(recent_b2_proportion, 2),
                RECENT_SLA_PROPORTION_COLUMN_NAMES[2]: round(recent_b3_proportion, 2),

        }

        for col in REQUIRED_COLUMNS_ORDER:
            if col not in new_date_hasr_tl_data_row_dict:
                new_date_hasr_tl_data_row_dict[col] = np.nan

        # Write to sheet
        with contextlib.redirect_stdout(StringIO()):
            new_date_hasr_tl_data_row_dict = hf.clean_data(new_date_hasr_tl_data_row_dict)
            new_date_hasr_tl_data_row = pd.DataFrame([new_date_hasr_tl_data_row_dict], columns=REQUIRED_COLUMNS_ORDER)
            new_date_hasr_tl_data_row_sheet_format = new_date_hasr_tl_data_row.values.tolist()
            hasr_tl_data_sheet.append_rows(new_date_hasr_tl_data_row_sheet_format)  


