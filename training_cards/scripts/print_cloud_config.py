from training_cards.cloud_config import GOOGLE_DRIVE_LIBRARY


def main() -> None:
    print("Training cards cloud library")
    print(f"Name: {GOOGLE_DRIVE_LIBRARY.library_name}")
    print(f"URL: {GOOGLE_DRIVE_LIBRARY.root_folder_url}")
    print(f"Local cache: {GOOGLE_DRIVE_LIBRARY.local_cache_dir}")
    print(f"Root folder ID: {GOOGLE_DRIVE_LIBRARY.root_folder_id}")
    print(f"Cards folder ID: {GOOGLE_DRIVE_LIBRARY.cards_folder_id}")
    print(f"Macro folder ID: {GOOGLE_DRIVE_LIBRARY.macro_folder_id}")
    print(f"Mezzo folder ID: {GOOGLE_DRIVE_LIBRARY.mezzo_folder_id}")
    print(f"Micro folder ID: {GOOGLE_DRIVE_LIBRARY.micro_folder_id}")
    print(f"Session folder ID: {GOOGLE_DRIVE_LIBRARY.session_folder_id}")


if __name__ == "__main__":
    main()
