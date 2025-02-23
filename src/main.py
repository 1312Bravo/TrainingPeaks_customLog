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

import config
from helpFunctions import clean_data 
from singleDay_dailyStats import singleDay_dailyStats
from singleDay_activityStats import singleDay_activityStats

def driveReport_singleUser(user_email, user_password, user_tpLogFilename, user_dailyLogFilename):

    # ----------------------------------------------------- 
    # Authenticate
    # -----------------------------------------------------

    print("1. Authentication ...")

    # Garmin API
    print("Authenticating Garmin Connect API ...")
    try:
        garminClient = Garmin(user_email, user_password)
        garminClient.login()
        print("~> Login to Garmin Connect API successful! :)")
    except Exception as e:
        print("~> Error logging to Garmin Connect API: {} :(".format(e))

    # Google drive API
    print("Authenticating Google Drive API ...")
    try:
        googleDrive_client = gspread.authorize(config.DRIVE_CREDENTIALS)
        print("~> Authentication to Google Drive API successful! :)")
    except Exception as e:
        print("~> Error authenticating to Google Drive API: {} :(".format(e))


    # ----------------------------------------------------- 
    # Prepare drive file to write into
    # -----------------------------------------------------

    # Daily Logs
    print("\n2. Opening and preparing Daily Log file ...")
    try:
        daily_log_file = googleDrive_client.open(user_dailyLogFilename)
        daily_log_sheet = daily_log_file.get_worksheet(0)
        daily_log_data = daily_log_sheet.get_all_values()
        daily_log_df = pd.DataFrame(daily_log_data[1:], columns=daily_log_data[0]) 
        print("~> Daily Log file succesfully imported and available for formating! :)")
    except Exception as e:
        print("Error opening Daily Log: {}".format(e))

    # TP Logs
    print("\n3. Opening and preparing TP Log file ...")
    try:
        tp_log_file = googleDrive_client.open(user_tpLogFilename)
        tp_log_sheet = tp_log_file.get_worksheet(0)
        tp_log_data = tp_log_sheet.get_all_values()
        tp_log_df = pd.DataFrame(tp_log_data[1:], columns=tp_log_data[0]) 
        print("~> TP Log file succesfully imported and available for formating! :)")
    except Exception as e:
        print("Error opening TP Log: {}".format(e))

    # ----------------------------------------------------- 
    # Calculate and write daily statistics to Drive sheet
    # -----------------------------------------------------
    print("\n4.1 Prepare and write daily statistics ...")

    # Dates ~ From last date on sheet (+1) to yesterday (today + 1)
    dailyStats_lastDate = datetime.datetime(int(daily_log_df.iloc[-1]["Year"]), int(daily_log_df.iloc[-1]["Month"]), int(daily_log_df.iloc[-1]["Day"])).date() + datetime.timedelta(days=1)
    dailyStats_startDate = np.min([dailyStats_lastDate, datetime.date.today() - datetime.timedelta(days=1)])
    dailyStats_endDate = datetime.date.today() - datetime.timedelta(days=1)
    if dailyStats_startDate < dailyStats_endDate:
        dailyStats_dateList = [(dailyStats_startDate + datetime.timedelta(days=i)) for i in range((dailyStats_endDate - dailyStats_startDate).days+1)]
    else: 
        dailyStats_dateList = []

    if dailyStats_dateList:
        for singleDate in dailyStats_dateList:
            print("~> Single day = {} ...".format(singleDate))

            # Calculate
            singleDay_dailyStats_dict = singleDay_dailyStats(garminClient, singleDate)

            # Write
            if not daily_log_sheet.row_values(1):
                daily_log_sheet.insert_row(config.DAILY_LOG_EXPECTED_HEADERS, index=1)
            with contextlib.redirect_stdout(StringIO()):
                daily_log_raw = clean_data(singleDay_dailyStats_dict)
                daily_log_df = pd.DataFrame([daily_log_raw])
                daily_log_sheetFormat = daily_log_df.values.tolist()
                daily_log_sheet.append_rows(daily_log_sheetFormat)  

    else:
        print("~> All daily statistics to {} (yesterday) already entered".format(dailyStats_endDate))

    # ----------------------------------------------------- 
    # Calculate and write activity statistics to Drive sheet
    # -----------------------------------------------------
    print("\n4.2 Prepare and write activity statistics ...")

    # Dates ~ From last date on sheet (+1) to yesterday (today + 1)
    activityStats_lastDate = datetime.datetime(int(tp_log_df.iloc[-1]["Year"]), int(tp_log_df.iloc[-1]["Month"]), int(tp_log_df.iloc[-1]["Day"])).date() + datetime.timedelta(days=1)
    activityStats_startDate = np.min([activityStats_lastDate, datetime.date.today() - datetime.timedelta(days=1)])
    activityStats_endDate = datetime.date.today() - datetime.timedelta(days=1)
    if activityStats_startDate < activityStats_endDate:
        activityStats_dateList = [(activityStats_startDate + datetime.timedelta(days=i)) for i in range((activityStats_endDate - activityStats_startDate).days+1)]
    else: 
        activityStats_dateList = []

    if activityStats_dateList:
        for singleDate in activityStats_dateList:
            print("~> Single day = {} ...".format(singleDate))

            # Calculate
            singleDay_activityStats_dict = singleDay_activityStats(garminClient, singleDate)

            # Write
            if not tp_log_sheet.row_values(1):
                tp_log_sheet.insert_row(config.TP_LOG_EXPECTED_HEADERS, index=1)
            for i in reversed(range(len(singleDay_activityStats_dict))):
                with contextlib.redirect_stdout(StringIO()):
                    tp_log_raw = clean_data(singleDay_activityStats_dict["activity_{}".format(i)])
                    tp_log_df = pd.DataFrame([tp_log_raw])
                    tp_log_sheetFormat = tp_log_df.values.tolist()
                    tp_log_sheet.append_rows(tp_log_sheetFormat)
    
    else:
        print("~> All activity statistics to {} (yesterday) already entered".format(activityStats_endDate))

# -----------------------------------------------------
# -----------------------------------------------------

def driveReport_allUsers():

     for email, password, tp_log, daily_log in zip(
        config.GARMIN_EMAILS, config.GARMIN_PASSWORDS, 
        config.DRIVE_TP_LOG_FILENAMES, config.DRIVE_DAILY_LOG_FILENAMES
        ):

        print("\n-----------------------------------------------------")
        print("Working on user {} [{}...] ... {}".format(email, password[:4], datetime.datetime.now()))
        print("-----------------------------------------------------\n")
        driveReport_singleUser(email, password, tp_log, daily_log)
        print("\nDone for user {}! ... {}\n".format(email, datetime.datetime.now()))

# -----------------------------------------------------
# -----------------------------------------------------

if __name__ == "__main__":
    print("\nGoGo! ~> {}".format(datetime.datetime.now()))
    driveReport_allUsers()
    print("Done! ~> {}\n".format(datetime.datetime.now()))

