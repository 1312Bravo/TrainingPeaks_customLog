# ----------------------------------------------------- 
# Libraries
# -----------------------------------------------------

import gspread

import os
os.chdir("..")  
print(f"Current wd = {os.getcwd()}")
from src import config
from src import help_functions as hf

# googleDrive_client = gspread.authorize(config.DRIVE_CREDENTIALS)
# training_data, _ = hf.import_google_sheet(googleDrive_client=googleDrive_client, filename=config.DRIVE_TP_LOG_FILENAMES[0], sheet_index=1)

print("AA")