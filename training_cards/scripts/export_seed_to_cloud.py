from training_cards.cloud_store import export_and_upload_seed_library
from training_cards.google_drive_client import GoogleDriveClient


def main() -> None:
    export_and_upload_seed_library(GoogleDriveClient())
    print("Exported seed cards and uploaded training-card library to Google Drive.")


if __name__ == "__main__":
    main()
