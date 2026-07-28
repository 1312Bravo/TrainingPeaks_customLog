import pandas as pd

from coach_context.context_options import DataContextSettings
from coach_context.data_window import find_latest_context_date
from coach_context.source_contexts import build_activity_statistics_context, build_daily_data_context, build_hasr_tl_context, load_context_sheet_tab
from coach_context.structured_notes_context import build_structured_notes_context
from data_sources import get_visible_sheets


# ----------------------------------------------------------
# Context Metadata
# ----------------------------------------------------------

# Loads the row-based dataframes that can provide a date anchor for the selected sources.
# Structured Notes are not included because they are topic text, not dated rows yet.
# Returns a list of dataframes.

def load_anchor_dataframes(visible_sheets: list[dict], sources: list[str]) -> list:
    dataframes = []

    if "Daily data" in sources:
        dataframes.append(load_context_sheet_tab(visible_sheets, "Daily", "Raw Daily Data"))

    if "Activity statistics" in sources:
        dataframes.append(load_context_sheet_tab(visible_sheets, "Activity", "Raw Activity Data"))

    if "HASR-TL" in sources:
        dataframes.append(load_context_sheet_tab(visible_sheets, "Activity", "HASR-TL"))

    return dataframes


# Creates a short UI label for the selected context.
# This is shown under the coach answer so the user knows what data was sent.
# Returns a readable label.

def describe_data_context(settings: DataContextSettings) -> str:
    if settings.window == "No data" or not settings.sources:
        return "No data"

    return f"{settings.window}, {', '.join(settings.sources)}"


# Chooses the date anchor used for context windows.
# Full mode uses today's calendar date, while demo mode uses the newest sample date.
# Returns a pandas Timestamp or None.

def choose_context_anchor_date(owner_mode: bool, dataframes: list) -> pd.Timestamp | None:
    if owner_mode:
        return pd.Timestamp.today().normalize()

    return find_latest_context_date(dataframes)


# ----------------------------------------------------------
# Context Builder
# ----------------------------------------------------------

# Builds the complete data-context text block for one coach question.
# It reads selected sources, filters row-based sources by window, and preserves all columns.
# Returns prompt-ready text or an empty string when no data should be sent.

def build_data_context(owner_mode: bool, settings: DataContextSettings) -> str:
    if settings.window == "No data" or not settings.sources:
        return ""

    visible_sheets = get_visible_sheets(owner_mode)
    anchor_dataframes = load_anchor_dataframes(visible_sheets, settings.sources)
    latest_data_date = find_latest_context_date(anchor_dataframes)
    anchor_date = choose_context_anchor_date(owner_mode, anchor_dataframes)
    sections = []

    if "Daily data" in settings.sources:
        sections.append(build_daily_data_context(visible_sheets, settings.window, anchor_date))

    if "Activity statistics" in settings.sources:
        sections.append(build_activity_statistics_context(visible_sheets, settings.window, anchor_date))

    if "HASR-TL" in settings.sources:
        sections.append(build_hasr_tl_context(visible_sheets, settings.window, anchor_date))

    if owner_mode and "Structured Notes" in settings.sources:
        sections.append(build_structured_notes_context())

    anchor_text = anchor_date.date().isoformat() if anchor_date is not None else "unknown"
    latest_data_text = latest_data_date.date().isoformat() if latest_data_date is not None else "unknown"

    return "\n\n".join(
        [
            "# Data Context",
            f"Context window: {settings.window}",
            f"Selected sources: {', '.join(settings.sources)}",
            f"Window anchor date: {anchor_text}",
            f"Latest available row date: {latest_data_text}",
            "Use this data as the selected context for the user's question. If the data does not contain the answer, say that clearly.",
            *sections,
        ]
    )
