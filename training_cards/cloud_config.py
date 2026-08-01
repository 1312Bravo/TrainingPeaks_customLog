from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path

# ----------------------------------------------------------
# Cloud Library Location
# ----------------------------------------------------------
# These IDs point to the first Google Drive JSON library upload.

PACKAGE_ROOT = Path(__file__).resolve().parent
DEFAULT_LOCAL_CACHE_DIR = PACKAGE_ROOT / "local_cache" / "cloud_library"


@dataclass(frozen=True, slots=True)
class GoogleDriveLibraryConfig:
    library_name: str
    root_folder_id: str
    root_folder_url: str
    cards_folder_id: str
    macro_folder_id: str
    mezzo_folder_id: str
    micro_folder_id: str
    session_folder_id: str
    local_cache_dir: Path = DEFAULT_LOCAL_CACHE_DIR


GOOGLE_DRIVE_LIBRARY = GoogleDriveLibraryConfig(
    library_name = "training_cards_library",
    root_folder_id = "1Y7lXD-wr3kQH9QVbsi_nrkK9ihDrKKPV",
    root_folder_url = "https://drive.google.com/drive/folders/1Y7lXD-wr3kQH9QVbsi_nrkK9ihDrKKPV",
    cards_folder_id = "16f6LwjbETatKN4pK42jvkrOovvhZsNWy",
    macro_folder_id = "17gVX2WAvKE5PcgkD9ln8q6kLi-3RTRad",
    mezzo_folder_id = "1MiZJr7_FSOQB-_Ssf_gzDmaFoEsUD9DP",
    micro_folder_id = "18adUB4YRfQ9WJo_9aoBp9EdBkmiCgsWG",
    session_folder_id = "1daSKUCE1odlMtEubTt8xHtY5DU7Hqy8a",
)

CARD_TYPE_FOLDER_IDS = {
    "macro": GOOGLE_DRIVE_LIBRARY.macro_folder_id,
    "mezzo": GOOGLE_DRIVE_LIBRARY.mezzo_folder_id,
    "micro": GOOGLE_DRIVE_LIBRARY.micro_folder_id,
    "session": GOOGLE_DRIVE_LIBRARY.session_folder_id,
}
