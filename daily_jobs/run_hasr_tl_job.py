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
from hasr_tl_job.main import prepare_calculate_write_hasr_tl

logger = setup_logger(name=__name__)


# -----------------------------------------------------
# Main: Run HASR-TL job
# -----------------------------------------------------

if __name__ == "__main__":
    logger.info("Go HASR-TL Job")

    for user in config.HISTORY_AWARE_RELATIVE_STRATIFIED_ACTIVITY_LOG_USERS:
        user_config = config.USER_CONFIGURATIONS[user]
        prepare_calculate_write_hasr_tl(
            garmin_email = user_config["garmin_email"],
            activity_log_file_name = user_config["gdrive_activity_log_filename"]
        )

    logger.info("Done: HASR-TL Job")
