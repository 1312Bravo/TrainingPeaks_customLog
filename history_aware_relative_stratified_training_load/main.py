# ----------------------------------------------------- 
# Libraries
# -----------------------------------------------------

# Librarires
import pandas as pd
import numpy as np
import gspread
import contextlib
from io import StringIO

# Set up repo root path
import os
import sys
repo_root = os.path.abspath(os.getcwd())  
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

# Help functions & "Main" functions
from src import config
from src import help_functions as hf
from history_aware_relative_stratified_training_load import config as sub_config
from history_aware_relative_stratified_training_load import help_functions as rtl_hf

# Logging
from src.log_config import setup_logger
logger = setup_logger(name=__name__)

# Silence third-party debug logs
import logging
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)

# -------------------------------
# Main: Prepare data, Calculate HASR-TL values and write to sheet
# -------------------------------
def prepare_calculate_write_hasr_tl(garmin_email, activity_log_file_name):
    logger.info("Running: Main ~ Analysis - History Aware Relative Stratified - Training Load")

    # Define "input parameters"
    agg_variable = sub_config.AGG_VARIABLE
    baseline_window = sub_config.BASELINE_WINDOW
    recent_window = sub_config.RECENT_WINDOW
    baseline_weights = sub_config.BASELINE_WINDOW_NORMALIZED_WEIGHTS
    recent_weights = sub_config.RECENT_WINDOW_NORMALIZED_WEIGHTS
    quantile_tl_minute_hard = sub_config.QUANTILE_TL_MINUTE_HARD
    quantile_duration_long = sub_config.QUANTILE_DURATION_LONG
    hasrl_tl_weights = sub_config.HASR_TL_WEIGHTS
    
    # About
    logger.info("About user ~> Garmin email: {} ~> activity file name: {}".format(
        garmin_email, 
        activity_log_file_name, 
        ))
    
    logger.info(
        f"Baseline window = {baseline_window}, "
        f"Recent window = {recent_window}, "
        f"Quantile TL per minute ~ Hard = {quantile_tl_minute_hard}, "
        f"Quantile duration ~ Long = {quantile_duration_long}, "
        f"HASR-TL Weights = {hasrl_tl_weights}"
    )

    # -------------------------------
    # Get and prepare data
    # -------------------------------

    # Get data
    googleDrive_client = gspread.authorize(config.DRIVE_CREDENTIALS)

    logger.info("Opening and preparing Activity Log file")
    try:
        activity_data_raw, _ = hf.import_google_sheet(
            googleDrive_client = googleDrive_client, 
            filename = activity_log_file_name, 
            sheet_name = config.BASIC_ACTIVITY_STATISTICS_SHEET_NAME
            )
    except Exception as e:
        logger.error(f"Error opening Activity Log file: {e}")
        raise
    
    logger.info("Opening and preparing HASR-TL Log file")
    try:
        hasr_tl_data_raw, hasr_tl_data_sheet = hf.import_google_sheet(
            googleDrive_client = googleDrive_client, 
            filename = activity_log_file_name, 
            sheet_name = sub_config.HASR_TL_SHEET_NAME
            )
    except Exception as e:
        logger.error(f"Error opening HASR-TL Log file: {e}")
        raise

    # "Clean" data
    activity_data = hf.data_safe_convert_to_numeric(activity_data_raw.copy(deep=True))
    hasr_tl_data = hf.data_safe_convert_to_numeric(hasr_tl_data_raw.copy(deep=True))

    # Prepare data for calculations
    activity_data["Datetime"] = pd.to_datetime(
        activity_data["Year"].astype(str) + "-" +
        activity_data["Month"].astype(str) + "-" +
        activity_data["Day"].astype(str) + " " +
        activity_data["Start time"].fillna("00:00").astype(str),
        format="%Y-%m-%d %H:%M",
        errors="coerce"
        )
    
    activity_data = activity_data.sort_values(by="Datetime").reset_index(drop=True)
    base_tl_data = activity_data[["Datetime", "Duration [h]", sub_config.AGG_VARIABLE]].copy()
    base_tl_data[sub_config.AGG_VARIABLE+" minute"] = base_tl_data[sub_config.AGG_VARIABLE] / (base_tl_data["Duration [h]"] * 60)

    base_tl_data[sub_config.AGG_VARIABLE] = base_tl_data[sub_config.AGG_VARIABLE].fillna(0)
    base_tl_data[sub_config.AGG_VARIABLE+" minute"] = base_tl_data[sub_config.AGG_VARIABLE+" minute"].fillna(0)
    base_tl_data["Duration [h]"] = base_tl_data["Duration [h]"].fillna(0)

    hasr_tl_data["Datetime"] = pd.to_datetime(
        hasr_tl_data["Year"].astype(str) + "-" +
        hasr_tl_data["Month"].astype(str) + "-" +
        hasr_tl_data["Day"].astype(str) + " " +
        hasr_tl_data["Start time"].astype(str),
        format="%Y-%m-%d %H:%M",
        errors="coerce"
        )
        
    # -------------------------------
    # We work based on Datetime
    # -------------------------------

    base_tl_data = base_tl_data.set_index("Datetime").sort_index()
    hasr_tl_data = hasr_tl_data.set_index("Datetime").sort_index()

    base_tl_data_datetimes = base_tl_data.index.date
    last_hasr_tl_data_datetime = hasr_tl_data.index.max().normalize()
    missing_hasr_tl_data_datetimes = base_tl_data.loc[base_tl_data.index.normalize() > last_hasr_tl_data_datetime].index

    # -------------------------------
    # Calculate missing HASR-TL values
    # -------------------------------
    logger.info("Calculate and write missing HASR-TL values")

    # Fill row by row
    for date_full in missing_hasr_tl_data_datetimes:
            date = date_full.date()
            logger.info("Date = {}".format(date))

            # Baseline window ~> Baseline window immediately after the recent window
            first_baseline_date = date - pd.Timedelta(days=recent_window + baseline_window - 1)
            last_baseline_date = date - pd.Timedelta(days=recent_window)
            logger.debug("Baseline dates: {} - {}".format(first_baseline_date, last_baseline_date))

            # Recent window ~> Recent window including "date"
            first_recent_date = date - pd.Timedelta(days=recent_window-1)
            last_recent_date = date
            logger.debug("Recent dates: {} - {}".format(first_recent_date, last_recent_date))

            # -------------------------------
            # BASELINE VALUES
            # -------------------------------

            # We have enough history to calculate baseline buckets values
            if first_baseline_date in base_tl_data_datetimes:
                logger.debug("Baseline SLA: Calculating SLA and Proportions")

                # Baseline set
                baseline_set = (
                    base_tl_data
                    .loc[
                        (base_tl_data.index.normalize() >= pd.Timestamp(first_baseline_date)) & (base_tl_data.index.normalize() <= pd.Timestamp(last_baseline_date)), 
                        [sub_config.AGG_VARIABLE, sub_config.AGG_VARIABLE+" minute", "Duration [h]"]]
                    .sort_index(ascending=False)
                    .assign(date = lambda x: x.index.normalize())
                    .reset_index(drop=True)
                    )
                
                # Match weights to baseline set
                baseline_set_weights = (
                    pd.DataFrame({
                        "date": pd.date_range(start=first_baseline_date, end=last_baseline_date, freq="D"),
                        "weight": baseline_weights
                    })
                    .reset_index(drop=True)
                    .merge(baseline_set[["date"]], on="date", how="right")
                    .sort_values("date", ascending=True)
                    .reset_index(drop=True)
                    ["weight"]
                    )
                
                # Buckets sets & Weights
                quantile_hard = rtl_hf.get_weighted_quantile_value(
                    quantile=quantile_tl_minute_hard, 
                    values=baseline_set[sub_config.AGG_VARIABLE+" minute"].to_numpy(), 
                    weights=baseline_set_weights.to_numpy()
                    )
                hard_baseline_mask = baseline_set[sub_config.AGG_VARIABLE+" minute"] > quantile_hard
                hard_baseline_set = baseline_set.loc[hard_baseline_mask, sub_config.AGG_VARIABLE]
                hard_baseline_set_weights = baseline_set_weights[hard_baseline_mask]
                
                easy_long_baseline_set = baseline_set[~hard_baseline_mask]
                easy_long_baseline_set_weights = baseline_set_weights[~hard_baseline_mask]
                quantile_long = rtl_hf.get_weighted_quantile_value(
                    quantile=quantile_duration_long, 
                    values=easy_long_baseline_set["Duration [h]"].to_numpy(), 
                    weights=easy_long_baseline_set_weights.to_numpy()
                    )

                long_baseline_mask = easy_long_baseline_set["Duration [h]"] > quantile_long
                long_baseline_set = easy_long_baseline_set.loc[long_baseline_mask, sub_config.AGG_VARIABLE]
                long_baseline_set_weights = easy_long_baseline_set_weights[long_baseline_mask]

                easy_baseline_mask = easy_long_baseline_set["Duration [h]"] <= quantile_long
                easy_baseline_set = easy_long_baseline_set.loc[easy_baseline_mask, sub_config.AGG_VARIABLE]
                easy_baseline_set_weights = easy_long_baseline_set_weights[easy_baseline_mask]

                # Aggregate buckets values
                baseline_easy_value = rtl_hf.get_weighted_mean(easy_baseline_set, easy_baseline_set_weights)
                baseline_hard_value = rtl_hf.get_weighted_mean(hard_baseline_set, hard_baseline_set_weights)
                baseline_long_value = rtl_hf.get_weighted_mean(long_baseline_set, long_baseline_set_weights)

                # Proportions in each bucket
                baseline_easy_proportion = len(easy_baseline_set)/len(baseline_set) * 100
                baseline_hard_proportion = len(hard_baseline_set)/len(baseline_set) * 100
                baseline_long_proportion = len(long_baseline_set)/len(baseline_set) * 100
            
            # We do not have enough history to calculate baseline buckets values
            else:
                logger.debug("Baseline SLA: Not enough data avaliable")

                baseline_easy_value = np.nan
                baseline_hard_value = np.nan
                baseline_long_value = np.nan

                baseline_easy_proportion = np.nan
                baseline_hard_proportion = np.nan 
                baseline_long_proportion = np.nan
                
            # -------------------------------
            # RECENT VALUES
            # -------------------------------

            # We have enough history to calculate recent buckets values
            if (first_recent_date in base_tl_data_datetimes) and (first_baseline_date in base_tl_data_datetimes):
                logger.debug("Recent SLA: Calculating SLA and Proportions")

                # Recent set
                recent_set = (
                    base_tl_data
                    .loc[
                        (base_tl_data.index.normalize() >= pd.Timestamp(first_recent_date)) & (base_tl_data.index.normalize() <= pd.Timestamp(last_recent_date)),  
                        [sub_config.AGG_VARIABLE, sub_config.AGG_VARIABLE+" minute", "Duration [h]"]]
                    .sort_index(ascending=False)
                    .assign(date = lambda x: x.index.normalize())
                    .reset_index(drop=True)
                )

                # Match weights to recemt set
                recent_set_weights = (
                    pd.DataFrame({
                        "date": pd.date_range(start=first_recent_date, end=last_recent_date, freq="D"),
                        "weight": recent_weights
                    })
                    .reset_index(drop=True)
                    .merge(recent_set[["date"]], on="date", how="right")
                    .sort_values("date", ascending=True)
                    .reset_index(drop=True)
                    ["weight"]
                    )

                # Recent session classification 
                recent_session = recent_set.iloc[0]
                recent_session_overall_percentile_rank = rtl_hf.get_weighted_percentile_rank(
                    value=recent_session[sub_config.AGG_VARIABLE], 
                    values=baseline_set[sub_config.AGG_VARIABLE].to_numpy(), 
                    weights=baseline_set_weights.to_numpy()
                    )
                
                if recent_session[sub_config.AGG_VARIABLE+" minute"] > quantile_hard:
                    recent_session_class = "Hard"
                    recent_session_class_percentile_rank = rtl_hf.get_weighted_percentile_rank(
                        value=recent_session[sub_config.AGG_VARIABLE], 
                        values=hard_baseline_set.to_numpy(), 
                        weights=hard_baseline_set_weights.to_numpy()
                    )
                elif recent_session["Duration [h]"] > quantile_long:
                    recent_session_class = "Long"
                    recent_session_class_percentile_rank = rtl_hf.get_weighted_percentile_rank(
                        value=recent_session[sub_config.AGG_VARIABLE], 
                        values=long_baseline_set.to_numpy(), 
                        weights=long_baseline_set_weights.to_numpy()
                    )
                else:
                    recent_session_class = "Easy" 
                    recent_session_class_percentile_rank = rtl_hf.get_weighted_percentile_rank(
                        value=recent_session[sub_config.AGG_VARIABLE], 
                        values=easy_baseline_set.to_numpy(), 
                        weights=easy_baseline_set_weights.to_numpy()
                    )

                # Aggregate variable values & Weights for each bucket
                hard_recent_mask = recent_set[sub_config.AGG_VARIABLE+" minute"] > quantile_hard
                hard_recent_set = recent_set.loc[hard_recent_mask, sub_config.AGG_VARIABLE]
                hard_recent_set_weights = recent_set_weights[hard_recent_mask]

                easy_long_recent_set = recent_set[~hard_recent_mask]
                easy_long_recent_set_weights = recent_set_weights[~hard_recent_mask]

                long_recent_mask = easy_long_recent_set["Duration [h]"] > quantile_long
                long_recent_set = easy_long_recent_set.loc[long_recent_mask, sub_config.AGG_VARIABLE]
                long_recent_set_weights = easy_long_recent_set_weights[long_recent_mask]

                easy_recent_mask = easy_long_recent_set["Duration [h]"] <= quantile_long
                easy_recent_set = easy_long_recent_set.loc[easy_recent_mask, sub_config.AGG_VARIABLE]
                easy_recent_set_weights = easy_long_recent_set_weights[easy_recent_mask]

                # Aggregate buckets values
                recent_easy_value = rtl_hf.get_weighted_mean(easy_recent_set, easy_recent_set_weights)
                recent_hard_value = rtl_hf.get_weighted_mean(hard_recent_set, hard_recent_set_weights)
                recent_long_value = rtl_hf.get_weighted_mean(long_recent_set, long_recent_set_weights)

                # Proportions in each bucket
                recent_easy_proportion = len(easy_recent_set)/len(recent_set) * 100
                recent_hard_proportion = len(hard_recent_set)/len(recent_set) * 100
                recent_long_proportion = len(long_recent_set)/len(recent_set) * 100
            
            # We do not have enough history to calculate recent buckets values
            else:
                logger.debug("Recent SLA: Not enough baseline data to calculate quantiles")

                recent_session_overall_percentile_rank = np.nan
                recent_session_class = np.nan
                recent_session_class_percentile_rank = np.nan

                recent_easy_value = np.nan
                recent_hard_value = np.nan
                recent_long_value = np.nan

                recent_easy_proportion = np.nan
                recent_hard_proportion = np.nan
                recent_long_proportion = np.nan

            # -------------------------------
            # BUCKET LEVEL DIAGNOSTICS 
            # -------------------------------

            logger.debug("Within Baseline SLA comparison")
            baseline_hard_easy = baseline_hard_value / baseline_easy_value
            baseline_long_hard = baseline_long_value / baseline_hard_value
            baseline_long_easy = baseline_long_value / baseline_easy_value

            logger.debug("Within Recent SLA comparison")
            recent_hard_easy = recent_hard_value / recent_easy_value
            recent_long_hard = recent_long_value / recent_hard_value
            recent_long_easy = recent_long_value / recent_easy_value
            
            logger.debug("Recent vs. Baseline Bucket SLA comparison")
            recent_baseline_b1 = recent_easy_value / baseline_easy_value
            recent_baseline_b2 = recent_hard_value / baseline_hard_value
            recent_baseline_b3 = recent_long_value / baseline_long_value

            # -------------------------------
            # FINAL HASR-TL 
            # -------------------------------

            logger.debug("HASR-TL calculation")
            hasr_tl_baseline = (
                hasrl_tl_weights[0] * baseline_easy_value + 
                hasrl_tl_weights[1] * baseline_hard_value +
                hasrl_tl_weights[2] * baseline_long_value
                )
            
            hasr_tl_recent = (
                hasrl_tl_weights[0] * recent_easy_value + 
                hasrl_tl_weights[1] * recent_hard_value +
                hasrl_tl_weights[2] * recent_long_value
                )
            
            hasr_tl = hasr_tl_recent / hasr_tl_baseline

            # -------------------------------
            # ADD NEW ROW
            # -------------------------------

            # Fill selected row baseline buckets values
            logger.debug("Preparing row to add")
            activity_help = activity_data.query("Datetime == @date_full").iloc[0]
            new_date_hasr_tl_data_row_dict = {
                    "Year": date_full.year,
                    "Month": date_full.month,
                    "Day": date_full.day,
                    "Start time": date_full.strftime("%H:%M"),
                    "Weekday": date.strftime("%A"),
                    "Description": activity_help["Description"],
                    "Activity type": activity_help["Activity type"],
                    "Aggregate variable": agg_variable,

                    sub_config.BASELINE_SLA_VALUE_COLUMN_NAMES[0]: round(baseline_easy_value, 2),
                    sub_config.BASELINE_SLA_VALUE_COLUMN_NAMES[1]: round(baseline_hard_value, 2),
                    sub_config.BASELINE_SLA_VALUE_COLUMN_NAMES[2]: round(baseline_long_value, 2),

                    sub_config.BASELINE_SLA_PROPORTION_COLUMN_NAMES[0]: round(baseline_easy_proportion, 2),
                    sub_config.BASELINE_SLA_PROPORTION_COLUMN_NAMES[1]: round(baseline_hard_proportion, 2),
                    sub_config.BASELINE_SLA_PROPORTION_COLUMN_NAMES[2]: round(baseline_long_proportion, 2),
                    
                    sub_config.RECENT_SESSION_CLASS_COLUMN_NAMES[0]: round(recent_session_overall_percentile_rank, 2) if not np.isnan(recent_session_overall_percentile_rank) else np.nan,
                    sub_config.RECENT_SESSION_CLASS_COLUMN_NAMES[1]: recent_session_class,
                    sub_config.RECENT_SESSION_CLASS_COLUMN_NAMES[2]: round(recent_session_class_percentile_rank, 2) if not np.isnan(recent_session_class_percentile_rank) else np.nan,

                    sub_config.RECENT_SLA_VALUE_COLUMN_NAMES[0]: round(recent_easy_value, 2),
                    sub_config.RECENT_SLA_VALUE_COLUMN_NAMES[1]: round(recent_hard_value, 2),
                    sub_config.RECENT_SLA_VALUE_COLUMN_NAMES[2]: round(recent_long_value, 2),

                    sub_config.RECENT_SLA_PROPORTION_COLUMN_NAMES[0]: round(recent_easy_proportion, 2),
                    sub_config.RECENT_SLA_PROPORTION_COLUMN_NAMES[1]: round(recent_hard_proportion, 2),
                    sub_config.RECENT_SLA_PROPORTION_COLUMN_NAMES[2]: round(recent_long_proportion, 2),

                    # Not in use
                    # sub_config.BASELINE_WITHIN_WINDOW_SLA_COMPARISON_COLUMN_NAMES[0]: round(baseline_hard_easy, 2) if not np.isnan(baseline_hard_easy) else np.nan,
                    # sub_config.BASELINE_WITHIN_WINDOW_SLA_COMPARISON_COLUMN_NAMES[1]: round(baseline_long_hard, 2) if not np.isnan(baseline_long_hard) else np.nan,
                    # sub_config.BASELINE_WITHIN_WINDOW_SLA_COMPARISON_COLUMN_NAMES[2]: round(baseline_long_easy, 2) if not np.isnan(baseline_long_easy) else np.nan,

                    # sub_config.RECENT_WITHIN_WINDOW_SLA_COMPARISON_COLUMN_NAMES[0]: round(recent_hard_easy, 2) if not np.isnan(recent_hard_easy) else np.nan,
                    # sub_config.RECENT_WITHIN_WINDOW_SLA_COMPARISON_COLUMN_NAMES[1]: round(recent_long_hard, 2) if not np.isnan(recent_long_hard) else np.nan,
                    # sub_config.RECENT_WITHIN_WINDOW_SLA_COMPARISON_COLUMN_NAMES[2]: round(recent_long_easy, 2) if not np.isnan(recent_long_easy) else np.nan,

                    # sub_config.RECENT_BASELINE_BUCKET_SLA_COMPARISON_COLUMN_NAMES[0]: round(recent_baseline_easy, 2) if not np.isnan(recent_baseline_easy) else np.nan,
                    # sub_config.RECENT_BASELINE_BUCKET_SLA_COMPARISON_COLUMN_NAMES[1]: round(recent_baseline_hard, 2) if not np.isnan(recent_baseline_hard) else np.nan,
                    # sub_config.RECENT_BASELINE_BUCKET_SLA_COMPARISON_COLUMN_NAMES[2]: round(recent_baseline_long, 2) if not np.isnan(recent_baseline_long) else np.nan,

                    sub_config.HASR_TL_COLUMN_NAMES[0]: round(hasr_tl, 2) if not np.isnan(hasr_tl) else np.nan,
                    sub_config.HASR_TL_COLUMN_NAMES[1]: round(hasr_tl_recent, 2) if not np.isnan(hasr_tl_recent) else np.nan,
                    sub_config.HASR_TL_COLUMN_NAMES[2]: round(hasr_tl_baseline, 2) if not np.isnan(hasr_tl_baseline) else np.nan,

            }

            for col in sub_config.REQUIRED_COLUMNS_ORDER:
                if col not in new_date_hasr_tl_data_row_dict:
                    new_date_hasr_tl_data_row_dict[col] = np.nan

            # Write to sheet
            logger.debug("Writing to HASR-TL data to sheet")
            with contextlib.redirect_stdout(StringIO()):
                new_date_hasr_tl_data_row_dict = hf.clean_data(new_date_hasr_tl_data_row_dict)
                new_date_hasr_tl_data_row = pd.DataFrame([new_date_hasr_tl_data_row_dict], columns=sub_config.REQUIRED_COLUMNS_ORDER)
                new_date_hasr_tl_data_row_sheet_format = new_date_hasr_tl_data_row.values.tolist()
                hasr_tl_data_sheet.append_rows(new_date_hasr_tl_data_row_sheet_format)  
    
    logger.info("Done: Main ~ Analysis - History Aware Relative Stratified - Training Load")



