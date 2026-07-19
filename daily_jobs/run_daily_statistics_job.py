# -----------------------------------------------------
# Libraries
# -----------------------------------------------------

import os
import sys

repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from daily_jobs import config
from daily_jobs.log_config import setup_logger
from daily_statistics_job.main import get_write_basic_daily_activity_statistics

logger = setup_logger(name=__name__)


# -----------------------------------------------------
# Main: Run basic daily and activity statistics job
# -----------------------------------------------------

if __name__ == "__main__":
    logger.info("Go Daily Statistics Job")

    for user in config.BASIC_DAILY_ACTIVITY_STATISTICS_USERS:
        user_config = config.USER_CONFIGURATIONS[user]
        get_write_basic_daily_activity_statistics(
            garmin_email = user_config["garmin_email"],
            garmin_password = user_config["garmin_password"],
            activity_log_file_name = user_config["gdrive_activity_log_filename"],
            daily_log_file_name = user_config["gdrive_daily_log_filename"]
        )

    logger.info("Done: Daily Statistics Job")
