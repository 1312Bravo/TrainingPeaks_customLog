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

GARMIN_EMAILS = os.getenv("GARMIN_EMAILS").split(",")
GARMIN_PASSWORDS = os.getenv("GARMIN_PASSWORDS").split(",")
DRIVE_TP_LOG_FILENAMES =  os.getenv("GOOGLEDRIVE_TP_LOG_FILENAMES").split(",")
DRIVE_DAILY_LOG_FILENAMES =  os.getenv("GOOGLEDRIVE_DAILY_LOG_FILENAMES").split(",")
DRIVE_CREDENTIALS = Credentials.from_service_account_file("googleDrive_secrets.json", scopes= ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])

# ----------------------------------------------------- 
# OTHER
# -----------------------------------------------------

TP_LOG_EXPECTED_HEADERS = [
    "Year", "Month", "Day", "Weekday", "Description", "Activity type", "Start time", "Location", "Distance [km]", "Duration [h]", 
    "Elevation gain [m]", "Average pace [min/km] or speed [km/h]", "Gradient adjusted pace [min/km]",
    "Average heart rate", "Maximum heart rate", "Normalized power [w]", "Calories [kcal]",
    "Aerobic training effect", "Aerobic training effect message", "Anaerobic training effect",
    "Anaerobic training effect message", "Training effect label", "Training load", "Vo2Max value",
    "Time in Z1 [h]", "Time in Z2 [h]", "Time in Z3 [h]", "Time in Z4 [h]", "Time in Z5 [h]",
    "10% heart rate [1]", "10% heart rate [2]", "10% heart rate [3]", "10% heart rate [4]", "10% heart rate [5]", "10% heart rate [6]", "10% heart rate [7]", "10% heart rate [8]", "10% heart rate [9]", "10% heart rate [10]",
]

DAILY_LOG_EXPECTED_HEADERS = [
    "Year", "Month", "Day", "Weekday",
    "Resting HR", "Sleep score", "Sleep time [h]", "HRV", "HRV baseline lower", "HRV baseline upper",
    "Meters ascended [m]", "Highly active time [h]", "Active time [h]", "Sedentary time [h]",
    "vo2Max", "Hill score", "Endurance score",
    "Low aerobic load", "High aerobic load", "Anaerobic load"
    ]