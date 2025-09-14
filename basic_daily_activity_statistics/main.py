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

# -----------------------------------------------------
# Main: Get and write basic Daily & Activity statistics for single user
# -----------------------------------------------------
def get_write_basic_daily_activity_statistics(user_email, user_password, user_tpLogFilename, user_dailyLogFilename):
    print("\nRunning: Main ~ Basic Daily & Activity Statistics ... {}".format(datetime.datetime.now()))

    # About
    print("About user ~> email: {} ~> activity file name: {} &  daily file name: {}".format(
        user_email, 
        user_tpLogFilename, 
        user_dailyLogFilename, 
        ))

    # ----------------------------------------------------- 
    # Authenticate
    # -----------------------------------------------------

    # Garmin API
    print("Authenticating Garmin Connect API ...")
    try:
        garminClient = Garmin(user_email, user_password)
        garminClient.login()
    except Exception as e:
        print("~> Error: {}".format(e))

    # Google drive API
    print("Authenticating Google Drive API ...")
    try:
        googleDrive_client = gspread.authorize(config.DRIVE_CREDENTIALS)
    except Exception as e:
        print("~> Error: {}".format(e))


    # ----------------------------------------------------- 
    # Prepare drive file to write into
    # -----------------------------------------------------

    # Daily Logs
    print("Opening and preparing Daily Log file ...")
    try:
        daily_log_df, daily_log_sheet = hf.import_google_sheet(
            googleDrive_client = googleDrive_client, 
            filename = user_dailyLogFilename, 
            sheet_name = config.BASIC_DAILY_STATISTICS_SHEET_NAME
            )
    except Exception as e:
        print("Error: {}".format(e))

    # TP Logs
    print("Opening and preparing TP Log file ...")
    try:
        tp_log_df, tp_log_sheet = hf.import_google_sheet(
            googleDrive_client = googleDrive_client, 
            filename = user_tpLogFilename, 
            sheet_name = config.BASIC_ACTIVITY_STATISTICS_SHEET_NAME)
    except Exception as e:
        print("Error {}".format(e))

    # ----------------------------------------------------- 
    # Calculate and write daily statistics to Drive sheet
    # -----------------------------------------------------
    print("Prepare and write daily statistics ...")

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
            print("~> Single day = {} ...".format(singleDate))

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
        print("~> All daily statistics to {} (yesterday) already entered".format(dailyStats_endDate))

    # ----------------------------------------------------- 
    # Calculate and write activity statistics to Drive sheet
    # -----------------------------------------------------
    print("Prepare and write activity statistics ...")

    # Dates ~ From last date on sheet (+1) to yesterday (today + 1)
    activityStats_lastDate = datetime.datetime(int(tp_log_df.iloc[-1]["Year"]), int(tp_log_df.iloc[-1]["Month"]), int(tp_log_df.iloc[-1]["Day"])).date() + datetime.timedelta(days=1)
    activityStats_startDate = np.min([activityStats_lastDate, datetime.date.today() - datetime.timedelta(days=1)])
    activityStats_endDate = datetime.date.today() - datetime.timedelta(days=1)
    if activityStats_startDate <= activityStats_endDate:
        activityStats_dateList = [(activityStats_startDate + datetime.timedelta(days=i)) for i in range((activityStats_endDate - activityStats_startDate).days+1)]
    else: 
        activityStats_dateList = []

    if activityStats_dateList:
        for singleDate in activityStats_dateList:
            print("~> Single day = {} ...".format(singleDate))

            # Calculate
            singleDay_activityStats_dict = get_prepare_single_day_activity_statistics(garminClient, singleDate)

            # Write
            if not tp_log_sheet.row_values(1):
                tp_log_sheet.insert_row(sub_config.TP_LOG_EXPECTED_HEADERS, index=1)
            for i in reversed(range(len(singleDay_activityStats_dict))):
                with contextlib.redirect_stdout(StringIO()):
                    tp_log_raw = hf.clean_data(singleDay_activityStats_dict["activity_{}".format(i)])
                    tp_log_df = pd.DataFrame([tp_log_raw])
                    tp_log_sheetFormat = tp_log_df.values.tolist()
                    tp_log_sheet.append_rows(tp_log_sheetFormat)
    
    else:
        print("~> All activity statistics to {} (yesterday) already entered".format(activityStats_endDate))

    print("Done! ... {}".format(datetime.datetime.now()))