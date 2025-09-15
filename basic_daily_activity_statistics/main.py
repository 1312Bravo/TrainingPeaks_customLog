# ----------------------------------------------------- 
# Libraries
# -----------------------------------------------------

import pandas as pd
import numpy as np
import datetime
from garminconnect import Garmin
import gspread
import contextlib
from io import StringIO

# Set up repo root path
import os
import sys
repo_root = os.path.abspath(os.getcwd())  
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

# Import help functions
from src import config
from src import help_functions as hf
from basic_daily_activity_statistics import config as sub_config
from basic_daily_activity_statistics.daily_statistics import get_prepare_single_day_daily_statistics
from basic_daily_activity_statistics.activity_statistics import get_prepare_single_day_activity_statistics

# Logging
from src.log_config import setup_logger
logger = setup_logger(name=__name__)

# -----------------------------------------------------
# Main: Get and write basic Daily & Activity statistics for single user
# -----------------------------------------------------
def get_write_basic_daily_activity_statistics(garmin_email, garmin_password, training_log_file_name, daily_log_file_name):
    logger.info("Running: Main ~ Basic Daily & Activity Statistics")

    # About
    logger.info("About user ~> email: {} ~> activity file name: {} &  daily file name: {}".format(
        garmin_email, 
        training_log_file_name, 
        daily_log_file_name, 
        ))

    # ----------------------------------------------------- 
    # Authenticate
    # -----------------------------------------------------

    # Garmin API
    logger.info("Authenticating Garmin Connect API")
    try:
        garminClient = Garmin(garmin_email, garmin_password)
        garminClient.login()
    except Exception as e:
        logger.error(f"Error Authenticating Garmin Connect API: {e}")
        raise

    # Google drive API
    logger.info("Authenticating Google Drive API")
    try:
        googleDrive_client = gspread.authorize(config.DRIVE_CREDENTIALS)
    except Exception as e:
        logger.error(f"Error Authenticating Google Drive API: {e}")
        raise


    # ----------------------------------------------------- 
    # Prepare drive file to write into
    # -----------------------------------------------------

    # Daily Logs
    logger.info("Opening and preparing Daily Log file")
    try:
        daily_log_df, daily_log_sheet = hf.import_google_sheet(
            googleDrive_client = googleDrive_client, 
            filename = daily_log_file_name, 
            sheet_name = config.BASIC_DAILY_STATISTICS_SHEET_NAME
            )
    except Exception as e:
        logger.error(f"Error opening Daily Log file: {e}")
        raise

    # Training Logs
    logger.info("Opening and preparing Training Log file")
    try:
        training_log_df, training_log_sheet = hf.import_google_sheet(
            googleDrive_client = googleDrive_client, 
            filename = training_log_file_name, 
            sheet_name = config.BASIC_ACTIVITY_STATISTICS_SHEET_NAME)
    except Exception as e:
        logger.error(f"Error opening Training Log file: {e}")
        raise

    # ----------------------------------------------------- 
    # Calculate and write daily statistics to Drive sheet
    # -----------------------------------------------------
    logger.info("Prepare and write daily statistics")

    # Dates ~ From last date on sheet (+1) to yesterday (today + 1)
    dailyStats_lastDate = datetime.datetime(int(daily_log_df.iloc[-1]["Year"]), int(daily_log_df.iloc[-1]["Month"]), int(daily_log_df.iloc[-1]["Day"])).date() + datetime.timedelta(days=1)
    dailyStats_startDate = np.min([dailyStats_lastDate, datetime.date.today() - datetime.timedelta(days=1)])
    dailyStats_endDate = datetime.date.today() - datetime.timedelta(days=1)
    if dailyStats_startDate <= dailyStats_endDate:
        dailyStats_dateList = [(dailyStats_startDate + datetime.timedelta(days=i)) for i in range((dailyStats_endDate - dailyStats_startDate).days+1)]
    else: 
        dailyStats_dateList = []

    if dailyStats_dateList:
        for singleDate in dailyStats_dateList:
            logger.debug("Single day = {}".format(singleDate))

            # Calculate
            singleDay_dailyStats_dict = get_prepare_single_day_daily_statistics(garminClient, singleDate)

            # Write
            if not daily_log_sheet.row_values(1):
                daily_log_sheet.insert_row(sub_config.DAILY_LOG_EXPECTED_HEADERS, index=1)
            with contextlib.redirect_stdout(StringIO()):
                daily_log_raw = hf.clean_data(singleDay_dailyStats_dict)
                daily_log_df = pd.DataFrame([daily_log_raw])
                daily_log_sheetFormat = daily_log_df.values.tolist()
                daily_log_sheet.append_rows(daily_log_sheetFormat)  

    else:
        logger.debug("All daily statistics to {} (yesterday) already entered".format(dailyStats_endDate))

    # ----------------------------------------------------- 
    # Calculate and write activity statistics to Drive sheet
    # -----------------------------------------------------
    logger.info("Prepare and write activity statistics")

    # Dates ~ From last date on sheet (+1) to yesterday (today + 1)
    activityStats_lastDate = datetime.datetime(int(training_log_df.iloc[-1]["Year"]), int(training_log_df.iloc[-1]["Month"]), int(training_log_df.iloc[-1]["Day"])).date() + datetime.timedelta(days=1)
    activityStats_startDate = np.min([activityStats_lastDate, datetime.date.today() - datetime.timedelta(days=1)])
    activityStats_endDate = datetime.date.today() - datetime.timedelta(days=1)
    if activityStats_startDate <= activityStats_endDate:
        activityStats_dateList = [(activityStats_startDate + datetime.timedelta(days=i)) for i in range((activityStats_endDate - activityStats_startDate).days+1)]
    else: 
        activityStats_dateList = []

    if activityStats_dateList:
        for singleDate in activityStats_dateList:
            logger.debug("Single day = {}".format(singleDate))

            # Calculate
            singleDay_activityStats_dict = get_prepare_single_day_activity_statistics(garminClient, singleDate)

            # Write
            if not training_log_sheet.row_values(1):
                training_log_sheet.insert_row(sub_config.TRAINING_LOG_EXPECTED_HEADERS, index=1)
            for i in reversed(range(len(singleDay_activityStats_dict))):
                with contextlib.redirect_stdout(StringIO()):
                    training_log_raw = hf.clean_data(singleDay_activityStats_dict["activity_{}".format(i)])
                    training_log_df = pd.DataFrame([training_log_raw])
                    training_log_sheetFormat = training_log_df.values.tolist()
                    training_log_sheet.append_rows(training_log_sheetFormat)
    
    else:
        logger.debug("All activity statistics to {} (yesterday) already entered".format(activityStats_endDate))

    logger.info("Done: Main ~ Basic Daily & Activity Statistics")