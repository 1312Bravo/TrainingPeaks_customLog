# ----------------------------------------------------- 
# Libraries
# -----------------------------------------------------

import datetime

# Set up repo root path
import os
import sys
repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

# Help functions & "Main" functions
from daily_jobs import config
from daily_statistics_job.main import get_write_basic_daily_activity_statistics
from hasr_tl_job.main import prepare_calculate_write_hasr_tl

# Logging
from daily_jobs.log_config import setup_logger
logger = setup_logger(name=__name__)

# GOGO!
if __name__ == "__main__":
    logger.info("Go Main!")
    
    # Get and write basic Daily & Activity statistics for all selected users
    for user in config.BASIC_DAILY_ACTIVITY_STATISTICS_USERS:
        user_config = config.USER_CONFIGURATIONS[user]
        get_write_basic_daily_activity_statistics(
            garmin_email = user_config["garmin_email"], 
            garmin_password = user_config["garmin_password"] , 
            activity_log_file_name = user_config["gdrive_activity_log_filename"], 
            daily_log_file_name = user_config["gdrive_daily_log_filename"]
        )

    # Get and write History Aware Relative Stratified Training Load for all selected users
    for user in config.HISTORY_AWARE_RELATIVE_STRATIFIED_ACTIVITY_LOG_USERS:
        user_config = config.USER_CONFIGURATIONS[user]
        prepare_calculate_write_hasr_tl(
            garmin_email = user_config["garmin_email"], 
            activity_log_file_name = user_config["gdrive_activity_log_filename"]
        )

    logger.info("Done: Main")
