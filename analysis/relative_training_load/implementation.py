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
HASTRL_WEIGHT_B1 = 0.15
HASTRL_WEIGHT_B2 = 0.45
HASTRL_WEIGHT_B3 = 0.4
QUANTILE_LOW = 0.60,
QUANTILE_HIGH = 0.85,
AGG_METRIC = "Training load"

# Column names mapping
column_names_excel_internal_mapping = {
    "Baseline B1 TL": "baseline_b1_TrainingLoad_weighted_mean",
    "Baseline B2 TL": "baseline_b2_TrainingLoad_weighted_mean",
    "Baseline B3 TL": "baseline_b3_TrainingLoad_weighted_mean",
    "Recent B1 TL": "recent_b1_TrainingLoad_weighted_mean",
    "Recent B2 TL": "recent_b2_TrainingLoad_weighted_mean",
    "Recent B3 TL": "recent_b3_TrainingLoad_weighted_mean"
}
column_names_internal_excel_mapping = {value: key for key, value in column_names_excel_internal_mapping.items()}

# Get data
googleDrive_client = gspread.authorize(config.DRIVE_CREDENTIALS)
training_data, _ = hf.import_google_sheet(googleDrive_client=googleDrive_client, filename=config.DRIVE_TP_LOG_FILENAMES[0], sheet_index=0)
hastrl_data, _ = hf.import_google_sheet(googleDrive_client=googleDrive_client, filename=config.DRIVE_TP_LOG_FILENAMES[0], sheet_index=1)

# "Clean" data
training_data = hf.data_safe_convert_to_numeric(training_data)
hastrl_data = hf.data_safe_convert_to_numeric(hastrl_data)

# Fill HASTRL data Y-M-D values based on Training data [other columns are nan, what we actually want]
last_date_training_data = pd.to_datetime(training_data[["Year", "Month", "Day"]]).max()
last_date_hastrl_data = pd.to_datetime(hastrl_data[["Year", "Month", "Day"]]).max()
if last_date_hastrl_data < last_date_training_data:

    new_hastrl_data_dates = pd.date_range(
        start = last_date_hastrl_data + pd.Timedelta(days=1), 
        end = last_date_training_data, 
        freq = "D"
        )
    
    new_hastrl_data_rows = pd.DataFrame({
        "Year": new_hastrl_data_dates.year, 
        "Month": new_hastrl_data_dates.month, 
        "Day": new_hastrl_data_dates.day
        })
    
    hastrl_data = pd.concat([hastrl_data, new_hastrl_data_rows], ignore_index = True)
    
# Prepare data for calculations
training_data["Datetime"] = pd.to_datetime(training_data[["Year", "Month", "Day"]])
training_data = training_data.sort_values(by="Date").reset_index(drop=True)
tl_data = (
    training_data
    .groupby("Datetime")
    .agg({AGG_METRIC: "sum"})
    .reset_index()
)

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
df = tl_data
baseline_window = BASELINE_WINDOW
baseline_weights = baseline_window_normalized_weights
recent_window = RECENT_WINDOW
recent_weights = recent_window_normalized_weights
quantile_low = QUANTILE_LOW
quantile_high = QUANTILE_HIGH
agg_metric = AGG_METRIC

print("AA")