# ----------------------------------------------------- 
# Libraries
# -----------------------------------------------------
from google.oauth2.service_account import Credentials

from dotenv import load_dotenv
import os

# ----------------------------------------------------- 
# Environmental variables
# -----------------------------------------------------

load_dotenv()
USER_CONFIGURATIONS = {

    "urh": {
        "garmin_email": os.getenv("GARMIN_EMAIL_URH"),
        "garmin_password": os.getenv("GARMIN_PASSWORD_URH"),
        "gdrive_activity_log_filename": os.getenv("ACTIVITY_LOG_URH"),
        "gdrive_daily_log_filename": os.getenv("DAILY_LOG_URH"),
    },

    "maja": {
        "garmin_email": os.getenv("GARMIN_EMAIL_MAJA"),
        "garmin_password": os.getenv("GARMIN_PASSWORD_MAJA"),
        "gdrive_activity_log_filename": os.getenv("ACTIVITY_LOG_MAJA"),
        "gdrive_daily_log_filename": os.getenv("DAILY_LOG_MAJA"),
    },

}

DRIVE_CREDENTIALS = Credentials.from_service_account_file("googleDrive_secrets.json", scopes= ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
BASIC_DAILY_STATISTICS_SHEET_NAME = "Raw Daily Data"
BASIC_ACTIVITY_STATISTICS_SHEET_NAME = "Raw Activity Data"

BASIC_DAILY_ACTIVITY_STATISTICS_USERS = ["urh"]
HISTORY_AWARE_RELATIVE_STRATIFIED_ACTIVITY_LOG_USERS = ["urh"]