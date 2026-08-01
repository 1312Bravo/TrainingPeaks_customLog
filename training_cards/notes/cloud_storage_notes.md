# Training Card Cloud Storage Notes

These notes describe the intended storage model for the training-card library.

## Direction

The source of truth is the cloud JSON card library.

Python should be the tooling layer around that library:

- Download the cloud library into a local cache.
- Validate the downloaded JSON against the Python schemas.
- Create or edit cards locally through Python helpers.
- Export the edited cards back to JSON.
- Upload the updated JSON library back to cloud storage.

The Training Platform app should eventually load cards from cloud JSON, validate them with the Python schemas, and then use the validated card objects in the app.

## Recommended Cloud Location

Use Google Drive first.

Why Google Drive fits this phase:

- It is simple file storage, which matches one JSON file per card.
- It is easy to inspect and replace files manually when needed.
- It can hold a normal folder structure with `manifest.json` and `cards/`.
- It fits a private personal library better than a public static file setup.
- Python can later download and upload the files through the Google Drive connector/API.

GitHub is also a good option if we want version history and review for every card change. It is less convenient as a private editable card library for the app unless we intentionally treat card changes like code changes.

A database such as Supabase can wait until the app needs in-app editing, multi-user access, permissions, or search/filtering that becomes awkward with plain files.

## Current Google Drive Library

Created on 2026-08-01.

```text
training_cards_library
https://drive.google.com/drive/folders/1Y7lXD-wr3kQH9QVbsi_nrkK9ihDrKKPV
```

Folder IDs:

- Root library folder: `1Y7lXD-wr3kQH9QVbsi_nrkK9ihDrKKPV`
- `cards`: `16f6LwjbETatKN4pK42jvkrOovvhZsNWy`
- `cards/macro`: `17gVX2WAvKE5PcgkD9ln8q6kLi-3RTRad`
- `cards/mezzo`: `1MiZJr7_FSOQB-_Ssf_gzDmaFoEsUD9DP`
- `cards/micro`: `18adUB4YRfQ9WJo_9aoBp9EdBkmiCgsWG`
- `cards/session`: `1daSKUCE1odlMtEubTt8xHtY5DU7Hqy8a`

Initial upload:

- `manifest.json`: 1 file
- `cards/macro`: 6 files
- `cards/mezzo`: 9 files
- `cards/micro`: 9 files
- `cards/session`: 14 files

## Python Cloud Reference

The Google Drive library location is stored in:

```text
training_cards/cloud_config.py
```

Use this helper to get the canonical folder URL:

```python
from training_cards.cloud_store import get_cloud_library_url

print(get_cloud_library_url())
```

Local cache/export/load helpers live in:

```text
training_cards/cloud_store.py
```

Core helper functions:

- `export_seed_library_to_cache()`
- `load_cached_cloud_library()`
- `download_cloud_library(client)`
- `upload_cached_library(client)`
- `export_and_upload_seed_library(client)`

## Drive Client Boundary

Normal project Python uses a service account, similar to the existing Google Drive and Google Sheets jobs in this repository.

The concrete client lives in:

```text
training_cards/google_drive_client.py
```

By default it reads:

```text
googleDrive_secrets.json
```

You can override that with:

```text
TRAINING_CARDS_GOOGLE_SERVICE_ACCOUNT_FILE
```

or:

```text
GOOGLE_APPLICATION_CREDENTIALS
```

The Google Drive folder must be shared with the service-account email before local Python can download or upload files.

`training_cards/cloud_store.py` expects a Drive client object with these methods:

- `list_folder(folder_id)`
- `download_file(file_id, output_path)`
- `upload_file(local_path, folder_id, file_name, mime_type)`
- `update_file(file_id, local_path, mime_type)`

The current implementation is `GoogleDriveClient`. This loose shape still lets us later connect another implementation if needed:

- Google Drive API with the current service account.
- Google Drive API with OAuth credentials later, if needed.
- A Codex-only adapter when working inside Codex tools.
- A test/mock client for validation.

The important project logic already exists separately from the authentication choice.

## Local Script Commands

Run these from the repository root.

```powershell
py -m training_cards.scripts.print_cloud_config
py -m training_cards.scripts.export_cache
py -m training_cards.scripts.validate_cache
py -m training_cards.scripts.download_cloud_library
py -m training_cards.scripts.upload_cache
py -m training_cards.scripts.export_seed_to_cloud
```

Command meanings:

- `print_cloud_config`: show the configured Drive folder IDs, URL, and local cache path.
- `export_cache`: export Python seed cards into the ignored local cache.
- `validate_cache`: validate the local cache against `manifest.json`.
- `download_cloud_library`: download Drive JSON into local cache and validate it.
- `upload_cache`: upload local cache files to Drive, updating existing files by name and creating missing files.
- `export_seed_to_cloud`: export Python seed cards to cache, validate, and upload to Drive.

## Cloud Folder Shape

The cloud folder should mirror the local cache shape:

```text
training_cards_library/
  manifest.json
  cards/
    macro/
      return-to-consistency.json
      base-development.json
    mezzo/
    micro/
    session/
```

Card file names use `slug`.

Card relationships use `id`.

## Manifest

`manifest.json` is the library table of contents.

It answers:

- What library is this?
- Which schema version does it use?
- Which library version is this?
- How many cards should exist?
- When was it last updated?
- Where are card files stored?
- Which card IDs and slugs exist?

Example:

```json
{
  "library_id": "running_training_cards",
  "schema_version": "1.0.0",
  "library_version": "0.1.0",
  "updated_at": "2026-08-01",
  "cards_root": "cards",
  "card_count": 38,
  "cards": [
    {
      "id": "macro_001",
      "slug": "return-to-consistency",
      "card_type": "macro",
      "title": "Return To Consistency"
    }
  ]
}
```

## Local Cache

The local cache is a temporary working copy of the cloud library. It is also what `training_cards/registry.py` loads.

Current local cache path:

```text
training_cards/local_cache/cloud_library/
```

The local cache is ignored by git. It should be safe to delete and recreate from cloud.

When `download_cloud_library` runs, old cached JSON files are removed before the fresh cloud copy is downloaded. This keeps deleted cloud cards from lingering locally.

The local cache should not silently sync both ways. Use explicit steps:

1. Download cloud to local cache.
2. Validate local cache.
3. Edit or create cards locally.
4. Validate again.
5. Upload local cache to cloud.

This avoids accidental overwrites when both cloud and local files changed.

Run download, validation, and upload sequentially. Do not run download and upload at the same time, because upload validates the local cache while download writes files.

## App Loading

Later, the Training Platform app should load from cloud JSON.

Recommended app behavior:

- Download/read `manifest.json`.
- Check `schema_version`.
- Read the card files listed in the manifest.
- Validate each card with the Python schemas.
- Check `card_count`, duplicate IDs, duplicate slugs, missing files, and broken references.
- Use validated cards inside the app.

The active `training_cards/registry.py` no longer depends on Python seed card files. It loads the local JSON cache, which should be refreshed from Google Drive.

## Manifest Strictness

Manifest strictness means how strongly the app trusts and checks `manifest.json`.

Recommended default:

- Strict for schema compatibility, duplicate IDs, missing files, and broken references.
- Warning-only for `library_version` changes.
- Strict for `card_count` mismatch once cloud becomes source of truth.

In practice:

- If `schema_version` is unsupported, stop loading.
- If a referenced card ID does not exist, stop loading.
- If two cards share the same ID, stop loading.
- If two cards share the same slug, stop loading.
- If `card_count` says 38 but only 37 cards load, stop loading.

This prevents the app from quietly showing an incomplete or broken card library.

## Versioning

`schema_version` describes the shape of the JSON fields.

Bump `schema_version` when the schema changes in a way that affects JSON compatibility, such as:

- Adding a required field.
- Removing a field.
- Renaming a field.
- Changing the meaning or type of a field.

`library_version` describes the card content.

Bump `library_version` when the cards change but the schema stays compatible, such as:

- Adding a card.
- Editing card text.
- Updating references.
- Changing tags.

Current expectation:

- Schema starts at `1.0.0`.
- The schema can stay stable unless we redefine fields.
- Library starts at `0.1.0` while content is still draft.

## Current Transition

Python seed cards still exist and can export the JSON library, but they are now backup/export material rather than the active registry source.

Current state:

1. Google Drive JSON is the source of truth.
2. Local cache is downloaded from Google Drive.
3. `training_cards/registry.py` loads the local JSON cache.
4. Python seed cards remain available through `training_cards/seed_registry.py`.
5. Later, the Training Platform app can import from `training_cards/registry.py` without caring whether cards started as JSON or Python.
