from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path

from training_cards.cloud_config import CARD_TYPE_FOLDER_IDS, GOOGLE_DRIVE_LIBRARY, GoogleDriveLibraryConfig
from training_cards.json_store import (
    CARDS_ROOT,
    MANIFEST_FILE_NAME,
    export_card_library_to_json,
    load_card_library_from_json,
)
from training_cards.schemas import BaseTrainingCard
from training_cards.seed_registry import ALL_SEED_CARDS


@dataclass(frozen=True, slots=True)
class DriveItem:
    id: str
    title: str
    file_or_folder: str


# Return the canonical Google Drive folder URL for the card library.
def get_cloud_library_url(config: GoogleDriveLibraryConfig = GOOGLE_DRIVE_LIBRARY) -> str:
    return config.root_folder_url


# Export the current Python seed cards into the ignored local cloud cache.
def export_seed_library_to_cache(config: GoogleDriveLibraryConfig = GOOGLE_DRIVE_LIBRARY) -> Path:
    export_card_library_to_json(ALL_SEED_CARDS, config.local_cache_dir)
    return config.local_cache_dir


# Load and validate the current local cloud-library cache.
def load_cached_cloud_library(config: GoogleDriveLibraryConfig = GOOGLE_DRIVE_LIBRARY) -> list[BaseTrainingCard]:
    return load_card_library_from_json(config.local_cache_dir)


# Download the Drive library into local cache through a concrete Drive client.
def download_cloud_library(client, config: GoogleDriveLibraryConfig = GOOGLE_DRIVE_LIBRARY) -> Path:
    config.local_cache_dir.mkdir(parents = True, exist_ok = True)
    _clear_cached_json_files(config.local_cache_dir)

    cards_dir = config.local_cache_dir / CARDS_ROOT
    cards_dir.mkdir(parents = True, exist_ok = True)

    manifest = _find_drive_item(client.list_folder(config.root_folder_id), MANIFEST_FILE_NAME)
    client.download_file(manifest.id, config.local_cache_dir / MANIFEST_FILE_NAME)

    for card_type, folder_id in CARD_TYPE_FOLDER_IDS.items():
        type_dir = cards_dir / card_type
        type_dir.mkdir(parents = True, exist_ok = True)

        for item in client.list_folder(folder_id):
            if item.file_or_folder == "file" and item.title.endswith(".json"):
                client.download_file(item.id, type_dir / item.title)

    load_card_library_from_json(config.local_cache_dir)

    return config.local_cache_dir


# Upload local cache files to Drive, updating existing files and creating missing ones.
def upload_cached_library(client, config: GoogleDriveLibraryConfig = GOOGLE_DRIVE_LIBRARY) -> None:
    load_card_library_from_json(config.local_cache_dir)
    root_items = client.list_folder(config.root_folder_id)

    _upsert_file(client, config.local_cache_dir / MANIFEST_FILE_NAME, config.root_folder_id, MANIFEST_FILE_NAME, root_items)

    cards_dir = config.local_cache_dir / CARDS_ROOT

    for card_type, folder_id in CARD_TYPE_FOLDER_IDS.items():
        folder_items = client.list_folder(folder_id)

        for path in sorted((cards_dir / card_type).glob("*.json")):
            _upsert_file(client, path, folder_id, path.name, folder_items)


# Export seed cards into local cache, validate them, then upload to Drive.
def export_and_upload_seed_library(client, config: GoogleDriveLibraryConfig = GOOGLE_DRIVE_LIBRARY) -> None:
    export_seed_library_to_cache(config)
    upload_cached_library(client, config)


def _find_drive_item(items: list[DriveItem], title: str) -> DriveItem:
    for item in items:
        if item.title == title:
            return item

    raise FileNotFoundError(f"Google Drive item not found: {title}")


# Remove old local JSON before downloading a fresh cloud copy.
def _clear_cached_json_files(cache_dir: Path) -> None:
    manifest_path = cache_dir / MANIFEST_FILE_NAME

    if manifest_path.exists():
        manifest_path.unlink()

    for path in (cache_dir / CARDS_ROOT).glob("*/*.json"):
        path.unlink()


def _upsert_file(client, local_path: Path, folder_id: str, file_name: str, existing_items: list[DriveItem]) -> None:
    existing_file = next(
        (item for item in existing_items if item.file_or_folder == "file" and item.title == file_name),
        None,
    )

    if existing_file is None:
        client.upload_file(local_path, folder_id, file_name, "application/json")
        return

    client.update_file(existing_file.id, local_path, "application/json")
