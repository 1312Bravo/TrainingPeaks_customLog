from training_cards.cloud_store import download_cloud_library
from training_cards.google_drive_client import GoogleDriveClient


def main() -> None:
    cache_dir = download_cloud_library(GoogleDriveClient())
    print(f"Downloaded and validated cloud library into: {cache_dir}")


if __name__ == "__main__":
    main()
