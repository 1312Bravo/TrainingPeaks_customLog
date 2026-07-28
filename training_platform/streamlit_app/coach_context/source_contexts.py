from io import StringIO

import pandas as pd

from coach_context.data_window import filter_dataframe_by_window
from data_sources import find_sheet_by_title_part, get_sheet_tabs, load_sheet_tab_data


# ----------------------------------------------------------
# Text Formatting
# ----------------------------------------------------------

# Converts a dataframe into compact CSV text for the coach.
# CSV preserves all columns without needing extra table dependencies.
# Returns a string that can be inserted into the model context.

def dataframe_to_context_csv(dataframe: pd.DataFrame) -> str:
    if dataframe.empty:
        return "No rows available for this selection."

    output_buffer = StringIO()
    dataframe.to_csv(output_buffer, index=False, lineterminator="\n")

    return output_buffer.getvalue().strip()


# Builds one named context section from a dataframe.
# The section keeps source metadata visible so the coach knows what it is reading.
# Returns a Markdown-style text block.

def build_dataframe_context_section(section_title: str, dataframe: pd.DataFrame, window: str, anchor_date: pd.Timestamp | None) -> str:
    filtered_frame = filter_dataframe_by_window(dataframe, window, anchor_date)

    return f"## {section_title}\nRows included: {len(filtered_frame)}\n\n```csv\n{dataframe_to_context_csv(filtered_frame)}\n```"


# ----------------------------------------------------------
# Sheet Source Builders
# ----------------------------------------------------------

# Loads one visible sheet tab by matching sheet title and tab title.
# It uses the same metadata as the dashboard, but loads all rows for the coach.
# Returns a dataframe.

def load_context_sheet_tab(visible_sheets: list[dict], sheet_title_part: str, tab_title: str) -> pd.DataFrame:
    sheet = find_sheet_by_title_part(visible_sheets, sheet_title_part)

    if not sheet:
        return pd.DataFrame()

    for sheet_tab in get_sheet_tabs(sheet):
        if sheet_tab["title"] == tab_title:
            return load_sheet_tab_data(sheet, sheet_tab)

    return pd.DataFrame()


# Builds context from the Raw Daily Data tab.
# All columns are preserved; only rows are filtered by the selected window.
# Returns a text block for the coach prompt.

def build_daily_data_context(visible_sheets: list[dict], window: str, anchor_date: pd.Timestamp | None) -> str:
    dataframe = load_context_sheet_tab(visible_sheets, "Daily", "Raw Daily Data")

    return build_dataframe_context_section("Daily Data", dataframe, window, anchor_date)


# Builds context from the Raw Activity Data tab.
# All columns are preserved; only rows are filtered by the selected window.
# Returns a text block for the coach prompt.

def build_activity_statistics_context(visible_sheets: list[dict], window: str, anchor_date: pd.Timestamp | None) -> str:
    dataframe = load_context_sheet_tab(visible_sheets, "Activity", "Raw Activity Data")

    return build_dataframe_context_section("Activity Statistics", dataframe, window, anchor_date)


# Builds context from the HASR-TL tab.
# All columns are preserved; only rows are filtered by the selected window.
# Returns a text block for the coach prompt.

def build_hasr_tl_context(visible_sheets: list[dict], window: str, anchor_date: pd.Timestamp | None) -> str:
    dataframe = load_context_sheet_tab(visible_sheets, "Activity", "HASR-TL")

    return build_dataframe_context_section("HASR-TL", dataframe, window, anchor_date)
