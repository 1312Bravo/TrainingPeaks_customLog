# -----------------------------------------------------
# Libraries
# -----------------------------------------------------

import argparse
import getpass
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from garminconnect import Garmin


# -----------------------------------------------------
# Helpers
# -----------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TOKENSTORE = REPO_ROOT / ".garminconnect"


def _get_env_name(user, field):
    return "{}_{}".format(field, user.upper())


def _get_credentials(user):
    load_dotenv(REPO_ROOT / ".env")

    email = os.getenv(_get_env_name(user, "GARMIN_EMAIL"))
    password = os.getenv(_get_env_name(user, "GARMIN_PASSWORD"))

    if not email:
        email = input("Garmin email: ").strip()

    if not password:
        password = getpass.getpass("Garmin password: ")

    return email, password


def _prompt_mfa():
    return input("Garmin MFA code: ").strip()


# -----------------------------------------------------
# Main
# -----------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Create Garmin token store after completing MFA locally."
    )
    parser.add_argument(
        "--user",
        default="urh",
        help="User key used in .env variable names, for example GARMIN_EMAIL_URH.",
    )
    parser.add_argument(
        "--tokenstore",
        default=str(DEFAULT_TOKENSTORE),
        help="Directory or json file where Garmin tokens should be saved.",
    )
    args = parser.parse_args()

    email, password = _get_credentials(args.user)
    tokenstore = Path(args.tokenstore).expanduser().resolve()

    garmin_client = Garmin(
        email,
        password,
        prompt_mfa = _prompt_mfa
    )
    garmin_client.login(tokenstore = str(tokenstore))

    token_file = tokenstore
    if token_file.is_dir() or token_file.suffix.lower() != ".json":
        token_file = token_file / "garmin_tokens.json"

    print("Garmin tokens saved to: {}".format(token_file))
    print("Add the file content as GitHub secret GARMIN_TOKENS_JSON.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
