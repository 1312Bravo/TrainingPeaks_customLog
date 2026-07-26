import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

from google.oauth2 import service_account
from googleapiclient.discovery import build

from config import get_chat_archive_sheet_url, get_memory_environment, get_structured_notes_doc_url


# ----------------------------------------------------------
# Google Memory Configuration
# ----------------------------------------------------------

CHAT_ARCHIVE_TAB_NAME = "Chat Archive"
CHAT_ARCHIVE_HEADERS = [
    "Timestamp UTC",
    "Memory Environment",
    "Mode",
    "User Email",
    "Memory Action",
    "Structured Topic",
    "Question",
    "Coach Answer",
    "Model",
    "Input Tokens",
    "Output Tokens",
    "Total Tokens",
    "Estimated Cost USD",
]
GOOGLE_MEMORY_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/documents",
]


# ----------------------------------------------------------
# Result Helpers
# ----------------------------------------------------------

# Builds a small result object that the UI can display without knowing Google API details.
# This keeps success, skipped, and failed memory writes shaped the same way.
# Returns a dictionary with ok/status/message fields.

def build_memory_result(ok: bool, status: str, message: str) -> dict:
    return {"ok": ok, "status": status, "message": message}


# ----------------------------------------------------------
# Credential Helpers
# ----------------------------------------------------------

# Reads the service account credentials from either a JSON string or a local JSON file path.
# Local development should usually use TRAINING_PLATFORM_GOOGLE_SERVICE_ACCOUNT_FILE.
# Returns Google credentials ready for Sheets and Docs API calls.

def get_google_credentials():
    credentials_json = os.getenv("TRAINING_PLATFORM_GOOGLE_SERVICE_ACCOUNT_JSON")
    credentials_file = os.getenv("TRAINING_PLATFORM_GOOGLE_SERVICE_ACCOUNT_FILE") or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    if credentials_json:
        credentials_info = json.loads(credentials_json)
        return service_account.Credentials.from_service_account_info(credentials_info, scopes=GOOGLE_MEMORY_SCOPES)

    if credentials_file:
        credentials_path = Path(credentials_file).expanduser()
        return service_account.Credentials.from_service_account_file(credentials_path, scopes=GOOGLE_MEMORY_SCOPES)

    raise RuntimeError("Google service account credentials are not configured.")


# Creates a Google API service client for one Workspace API.
# The credentials helper keeps secret loading in one place.
# Returns a googleapiclient service object.

def build_google_service(api_name: str, api_version: str):
    return build(api_name, api_version, credentials=get_google_credentials(), cache_discovery=False)


# ----------------------------------------------------------
# Google File Helpers
# ----------------------------------------------------------

# Extracts the file ID from a Google Docs or Sheets URL.
# This lets config keep normal browser links while API calls use raw IDs.
# Returns the file ID string.

def extract_google_file_id(google_url: str) -> str:
    match = re.search(r"/d/([^/]+)", google_url)

    if not match:
        raise ValueError("Could not read a Google file ID from the configured URL.")

    return match.group(1)


# ----------------------------------------------------------
# Chat Archive Writes
# ----------------------------------------------------------

# Checks the first row of the active archive tab and writes headers when the sheet is empty.
# This keeps the Google Sheet understandable when the first real coach message is saved.
# Returns nothing; it updates the sheet only when needed.

def ensure_chat_archive_headers(sheets_service, spreadsheet_id: str) -> None:
    header_response = sheets_service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=f"'{CHAT_ARCHIVE_TAB_NAME}'!A1:M1").execute()
    current_headers = header_response.get("values", [[]])[0]

    if current_headers[: len(CHAT_ARCHIVE_HEADERS)] == CHAT_ARCHIVE_HEADERS:
        return

    sheets_service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"'{CHAT_ARCHIVE_TAB_NAME}'!A1:M1",
        valueInputOption="USER_ENTERED",
        body={"values": [CHAT_ARCHIVE_HEADERS]},
    ).execute()


# Builds the row saved for one coach exchange.
# One row is intentionally raw history; readable notes are handled separately in Google Docs.
# Returns a list of cell values for the Chat Archive sheet.

def build_chat_archive_row(question: str, answer: str, owner_mode: bool, user_email: str | None, memory_action: str, structured_topic: str | None, usage: dict | None) -> list:
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    mode = "full" if owner_mode else "demo"
    usage = usage or {}

    return [
        now,
        get_memory_environment(),
        mode,
        user_email or "",
        memory_action,
        structured_topic or "",
        question,
        answer,
        usage.get("model", ""),
        usage.get("input_tokens", ""),
        usage.get("output_tokens", ""),
        usage.get("total_tokens", ""),
        usage.get("estimated_cost_usd", ""),
    ]


# Appends one coach exchange to the active Chat Archive Google Sheet.
# This is the first real memory write; Structured Notes can build on the same credentials later.
# Returns a small status dictionary for the chat UI.

def append_chat_archive(question: str, answer: str, owner_mode: bool, user_email: str | None, memory_action: str, structured_topic: str | None, usage: dict | None) -> dict:
    if not owner_mode:
        return build_memory_result(False, "skipped", "Memory was not saved because this chat is not in full owner mode.")

    archive_sheet_url = get_chat_archive_sheet_url()
    if not archive_sheet_url:
        return build_memory_result(False, "skipped", "Chat archive sheet is not configured.")

    try:
        spreadsheet_id = extract_google_file_id(archive_sheet_url)
        sheets_service = build_google_service("sheets", "v4")
        row = build_chat_archive_row(question, answer, owner_mode, user_email, memory_action, structured_topic, usage)
        ensure_chat_archive_headers(sheets_service, spreadsheet_id)
        sheets_service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"'{CHAT_ARCHIVE_TAB_NAME}'!A:M",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": [row]},
        ).execute()
    except Exception as error:
        return build_memory_result(False, "failed", f"Chat archive write failed: {error}")

    return build_memory_result(True, "saved", "Saved to Chat Archive.")


# ----------------------------------------------------------
# Structured Notes Placeholder
# ----------------------------------------------------------

# Checks whether the structured notes doc is configured.
# The first version only links the doc; topic-aware editing comes after archive writing is proven.
# Returns a small status dictionary for the chat UI.

def check_structured_notes_target() -> dict:
    if get_structured_notes_doc_url():
        return build_memory_result(False, "planned", "Structured Notes doc is configured, but automatic note updates are not wired yet.")

    return build_memory_result(False, "skipped", "Structured Notes doc is not configured yet.")
