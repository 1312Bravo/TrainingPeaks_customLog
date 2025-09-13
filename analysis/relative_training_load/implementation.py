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
HASR_TL_WEIGHTS = [0.15, 0.45, 0.4]
QUANTILE_LOW = 0.60
QUANTILE_HIGH = 0.85
AGG_VARIABLE = "Training load"
AGG_VARIABLE_NAME_DICT = {
    "Training load": "TL"
    }

# Excel column names
BASELINE_SLA_VALUE_COLUMN_NAMES = [
     "Baseline B1",
     "Baseline B2",
     "Baseline B3"
     ]

BASELINE_SLA_PROPORTION_COLUMN_NAMES = [
     "Baseline B1 prop. [%]",
     "Baseline B2 prop. [%]",
     "Baseline B3 prop. [%]"
     ]

RECENT_SLA_VALUE_COLUMN_NAMES = [
     "Recent B1",
     "Recent B2",
     "Recent B3"
     ]

RECENT_SLA_PROPORTION_COLUMN_NAMES = [
     "Recent B1 prop. [%]",
     "Recent B2 prop. [%]",
     "Recent B3 prop. [%]"
     ]

# Not in use
BASELINE_WITHIN_WINDOW_SLA_COMPARISON_COLUMN_NAMES = [
    "Baseline B2/B1",
    "Baseline B3/B2",
    "Baseline B3/B1",
]

# Not in use
RECENT_WITHIN_WINDOW_SLA_COMPARISON_COLUMN_NAMES = [
    "Recent B2/B1",
    "Recent B3/B2",
    "Recent B3/B1",
]

# Not in use
RECENT_BASELINE_BUCKET_SLA_COMPARISON_COLUMN_NAMES = [
     "Recent/Baseline B1",
     "Recent/Baseline B2",
     "Recent/Baseline B3",
]

HASR_TL_COLUMN_NAMES = [
     f"HASR-{AGG_VARIABLE_NAME_DICT[AGG_VARIABLE]}",
     f"HASR-{AGG_VARIABLE_NAME_DICT[AGG_VARIABLE]} Baseline",
     f"HASR-{AGG_VARIABLE_NAME_DICT[AGG_VARIABLE]} Recent",
]

REQUIRED_COLUMNS_ORDER = ["Year", "Month", "Day", "Weekday", "Aggregate variable"]
REQUIRED_COLUMNS_ORDER += HASR_TL_COLUMN_NAMES
for i in [0,1,2]:
     REQUIRED_COLUMNS_ORDER += [RECENT_SLA_VALUE_COLUMN_NAMES[i]]
     REQUIRED_COLUMNS_ORDER += [BASELINE_SLA_VALUE_COLUMN_NAMES[i]]
     REQUIRED_COLUMNS_ORDER += [RECENT_SLA_PROPORTION_COLUMN_NAMES[i]]
     REQUIRED_COLUMNS_ORDER += [BASELINE_SLA_PROPORTION_COLUMN_NAMES[i]]

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
hasrl_tl_weights = HASR_TL_WEIGHTS
print(f"Baseline window = {baseline_window} \
      Recent window = {recent_window} \
      Quantile low = {quantile_low} \
      Quantile high = {quantile_high} \
      HASR-TL Weights = {hasrl_tl_weights} \
")

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
        print("\nDate = {}".format(date.date()))

        # Baseline window ~> Baseline window immediately after the recent window
        first_baseline_date = date - pd.Timedelta(days=recent_window + baseline_window - 1)
        last_baseline_date = date - pd.Timedelta(days=recent_window)
        print("~> Baseline dates: {} - {}".format(first_baseline_date.date(), last_baseline_date.date()))

        # Recent window ~> Recent window including "date"
        first_recent_date = date - pd.Timedelta(days=recent_window-1)
        last_recent_date = date
        print("~> Recent dates: {} - {}".format(first_recent_date.date(), last_recent_date.date()))

        # -------------------------------
        # BASELINE VALUES
        # -------------------------------

        # We have enough history to calculate baseline buckets values
        if first_baseline_date in base_tl_data_datetimes:
            print("~> Baseline SLA: Calculating SLA and Proportions ...")

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
            print("~> Baseline SLA: Not enough data avaliable")

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
        if (first_recent_date in base_tl_data_datetimes) and (first_baseline_date in base_tl_data_datetimes):
            print("~> Recent SLA: Calculating SLA and Proportions ...")

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
            print("~> Recent SLA: Not enough baseline data to calculate quantiles")

            recent_b1_value = np.nan
            recent_b2_value = np.nan
            recent_b3_value = np.nan

            recent_b1_proportion = np.nan
            recent_b2_proportion = np.nan
            recent_b3_proportion = np.nan

        # -------------------------------
        # BUCKET LEVEL DIAGNOSTICS 
        # -------------------------------

        print("~> Within Baseline SLA comparison ...")
        baseline_b2_b1 = baseline_b2_value / baseline_b1_value
        baseline_b3_b2 = baseline_b3_value / baseline_b2_value
        baseline_b3_b1 = baseline_b3_value / baseline_b1_value

        print("~> Within Recent SLA comparison ...")
        recent_b2_b1 = recent_b2_value / recent_b1_value
        recent_b3_b2 = recent_b3_value / recent_b2_value
        recent_b3_b1 = recent_b3_value / recent_b1_value
        
        #print("~> Recent vs. Baseline Bucket SLA comparison ...")
        recent_baseline_b1 = recent_b1_value / baseline_b1_value
        recent_baseline_b2 = recent_b2_value / baseline_b2_value
        recent_baseline_b3 = recent_b3_value / baseline_b3_value

        # -------------------------------
        # FINAL HASR-TL 
        # -------------------------------

        print("~> HASR-TL calculation ...")
        hasr_tl_baseline = (
            hasrl_tl_weights[0] * baseline_b1_value + 
            hasrl_tl_weights[1] * baseline_b2_value +
            hasrl_tl_weights[2] * baseline_b3_value
            )
        
        hasr_tl_recent = (
            hasrl_tl_weights[0] * recent_b1_value + 
            hasrl_tl_weights[1] * recent_b2_value +
            hasrl_tl_weights[2] * recent_b3_value
            )
        
        hasr_tl = hasr_tl_recent / hasr_tl_baseline

        # -------------------------------
        # ADD NEW ROW
        # -------------------------------

        # Fill selected row baseline buckets values
        new_date_hasr_tl_data_row_dict = {
                "Year": date.year,
                "Month": date.month,
                "Day": date.day,
                "Weekday": date.strftime("%A"),
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

                # Not in use
                # BASELINE_WITHIN_WINDOW_SLA_COMPARISON_COLUMN_NAMES[0]: round(baseline_b2_b1, 2) if not np.isnan(baseline_b2_b1) else np.nan,
                # BASELINE_WITHIN_WINDOW_SLA_COMPARISON_COLUMN_NAMES[1]: round(baseline_b3_b2, 2) if not np.isnan(baseline_b3_b2) else np.nan,
                # BASELINE_WITHIN_WINDOW_SLA_COMPARISON_COLUMN_NAMES[2]: round(baseline_b3_b1, 2) if not np.isnan(baseline_b3_b1) else np.nan,

                # RECENT_WITHIN_WINDOW_SLA_COMPARISON_COLUMN_NAMES[0]: round(recent_b2_b1, 2) if not np.isnan(recent_b2_b1) else np.nan,
                # RECENT_WITHIN_WINDOW_SLA_COMPARISON_COLUMN_NAMES[1]: round(recent_b3_b2, 2) if not np.isnan(recent_b3_b2) else np.nan,
                # RECENT_WITHIN_WINDOW_SLA_COMPARISON_COLUMN_NAMES[2]: round(recent_b3_b1, 2) if not np.isnan(recent_b3_b1) else np.nan,

                # RECENT_BASELINE_BUCKET_SLA_COMPARISON_COLUMN_NAMES[0]: round(recent_baseline_b1, 2) if not np.isnan(recent_baseline_b1) else np.nan,
                # RECENT_BASELINE_BUCKET_SLA_COMPARISON_COLUMN_NAMES[1]: round(recent_baseline_b2, 2) if not np.isnan(recent_baseline_b2) else np.nan,
                # RECENT_BASELINE_BUCKET_SLA_COMPARISON_COLUMN_NAMES[2]: round(recent_baseline_b3, 2) if not np.isnan(recent_baseline_b3) else np.nan,

                HASR_TL_COLUMN_NAMES[0]: round(hasr_tl, 2) if not np.isnan(hasr_tl) else np.nan,
                HASR_TL_COLUMN_NAMES[1]: round(hasr_tl_baseline, 2) if not np.isnan(hasr_tl_baseline) else np.nan,
                HASR_TL_COLUMN_NAMES[2]: round(hasr_tl_recent, 2) if not np.isnan(hasr_tl_recent) else np.nan,

        }

        for col in REQUIRED_COLUMNS_ORDER:
            if col not in new_date_hasr_tl_data_row_dict:
                new_date_hasr_tl_data_row_dict[col] = np.nan

        # Write to sheet
        print("~> Writing to HASR-TL data to sheet ...")
        with contextlib.redirect_stdout(StringIO()):
            new_date_hasr_tl_data_row_dict = hf.clean_data(new_date_hasr_tl_data_row_dict)
            new_date_hasr_tl_data_row = pd.DataFrame([new_date_hasr_tl_data_row_dict], columns=REQUIRED_COLUMNS_ORDER)
            new_date_hasr_tl_data_row_sheet_format = new_date_hasr_tl_data_row.values.tolist()
            hasr_tl_data_sheet.append_rows(new_date_hasr_tl_data_row_sheet_format)  


