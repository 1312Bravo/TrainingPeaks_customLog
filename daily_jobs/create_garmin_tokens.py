# -----------------------------------------------------
# Libraries
# -----------------------------------------------------

from pathlib import Path

# Set up repo root path
import os
import sys
repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from daily_jobs import config
from daily_jobs import help_functions as hf
from daily_jobs.log_config import setup_logger

logger = setup_logger(name=__name__)


# -----------------------------------------------------
# Main: Create Garmin token store locally
# -----------------------------------------------------

if __name__ == "__main__":
    logger.info("Create Garmin tokens")

    tokenstore = repo_root / ".garminconnect"
    os.environ["GARMINTOKENS"] = str(tokenstore)

    user_config = config.USER_CONFIGURATIONS["urh"]

    hf.authenticate_garmin_connect_api(
        garmin_email=user_config["garmin_email"],
        garmin_password=user_config["garmin_password"]
    )

    logger.info("Garmin tokens created in: {}".format(tokenstore))
    logger.info("Do not commit this folder. It is ignored by .gitignore.")
