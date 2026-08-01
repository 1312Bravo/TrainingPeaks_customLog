# Training Cards

This folder is the isolated workspace for designing reusable training cards before they are connected to the Training Platform app.

## Working Order

1. Define the coach prompt used to create and review cards.
2. Define or revise the card classes and validation rules.
3. Decide the card folder structure and naming conventions.
4. Build seed macro, mezzo, micro, and session cards.
5. Review the cards for coaching usefulness and consistency.
6. Plug the finished card system into `training_platform/` later.

## Current Direction

- Keep this folder independent from the Streamlit app until the training logic is ready.
- Store card schemas separately from card content.
- Use cloud JSON as the source of truth.
- Use Python schemas and helpers to validate, create, edit, export, and later upload cards.

## Current Library Draft

The current draft contains 38 cards:

- 6 macro cards
- 9 mezzo cards
- 9 micro cards
- 14 session cards

Use `training_cards/registry.py` as the central import point for the active library. It loads the downloaded JSON cache from Google Drive.

Tags are currently free-text strings. Keep them short and consistent; do not convert them into enums until real app filtering shows that stricter control is needed.

## JSON Library Shape

The planned cloud format is a JSON library with a `manifest.json` at the root and one JSON file per card under `cards/`.

The manifest answers the basic library questions:

- What library is this?
- Which schema version does it use?
- How many cards should exist?
- When was it last updated?
- Where are cards stored?
- Which card IDs and slugs exist?

Cards now have both an `id` and a `slug`.

- `id` is the stable technical reference used by card relationships and app logic.
- `slug` is the readable file/url name used for JSON filenames and cloud paths.

Card IDs now use stable numbered identifiers such as `macro_001`, while slugs keep the readable names such as `base-development`.

The active loading flow is:

1. Google Drive JSON is the source of truth.
2. `training_cards/local_cache/cloud_library/` is the downloaded working copy.
3. `training_cards/registry.py` loads and validates that local JSON cache.

Python seed cards still exist as backup/export material, but they are not the active registry source.

See `training_cards/notes/cloud_storage_notes.md` for the full storage workflow.

Cloud configuration and workflow helpers:

- `training_cards/cloud_config.py`: Google Drive folder IDs, root URL, and local cache path.
- `training_cards/cloud_store.py`: cache export/load helpers and Drive sync workflow functions.
- `training_cards/google_drive_client.py`: service-account Google Drive API client.
- `training_cards/seed_registry.py`: backup Python seed card list used only for seed export.

Useful local commands:

```powershell
py -m training_cards.scripts.print_cloud_config
py -m training_cards.scripts.export_cache
py -m training_cards.scripts.validate_cache
py -m training_cards.scripts.download_cloud_library
py -m training_cards.scripts.upload_cache
py -m training_cards.scripts.export_seed_to_cloud
```

## Code And Content

Schema, registry, serialization, and JSON storage files include comments because they explain how the library works.

Individual card files are intentionally written like structured data. Keep comments in card files rare; the card fields themselves should carry the coaching content.

