from training_cards.cloud_store import upload_cached_library
from training_cards.google_drive_client import GoogleDriveClient


def main() -> None:
    upload_cached_library(GoogleDriveClient())
    print("Uploaded cached training-card library to Google Drive.")


if __name__ == "__main__":
    main()
