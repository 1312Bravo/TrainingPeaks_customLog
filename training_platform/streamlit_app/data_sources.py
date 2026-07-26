import pandas as pd
import streamlit as st


# ----------------------------------------------------------
# Google Sheet Column Definitions
# ----------------------------------------------------------

ACTIVITY_PREVIEW_COLUMNS = [
    "Year",
    "Month",
    "Day",
    "Weekday",
    "Description",
    "Activity type",
    "Start time",
    "Location",
    "Distance [km]",
    "Duration [h]",
    "Elevation gain [m]",
    "Average pace [min/km] or speed [km/h]",
    "Gradient adjusted pace [min/km]",
    "Average heart rate",
    "Maximum heart rate",
    "Normalized power [w]",
    "Calories [kcal]",
    "Aerobic training effect",
    "Aerobic training effect message",
    "Anaerobic training effect",
    "Anaerobic training effect message",
    "Training effect label",
    "Training load",
    "Vo2Max value",
    "Time in Z1 [h]",
    "Time in Z2 [h]",
    "Time in Z3 [h]",
    "Time in Z4 [h]",
    "Time in Z5 [h]",
    "10% heart rate [1]",
    "10% heart rate [2]",
    "10% heart rate [3]",
    "10% heart rate [4]",
    "10% heart rate [5]",
    "10% heart rate [6]",
    "10% heart rate [7]",
    "10% heart rate [8]",
    "10% heart rate [9]",
    "10% heart rate [10]",
]

DAILY_PREVIEW_COLUMNS = [
    "Year",
    "Month",
    "Day",
    "Weekday",
    "Resting HR",
    "Sleep score",
    "Sleep time [h]",
    "HRV",
    "HRV baseline lower",
    "HRV baseline upper",
    "Meters ascended [m]",
    "Highly active time [h]",
    "Active time [h]",
    "Sedentary time [h]",
    "vo2Max",
    "Hill score",
    "Endurance score",
    "Low aerobic load",
    "High aerobic load",
    "Anaerobic load",
]


# ----------------------------------------------------------
# Google Sheet Source Definitions
# ----------------------------------------------------------

PRIVATE_GOOGLE_SHEETS = [
    {
        "title": "Activity Statistics Urh",
        "description": "Activity-level training data with raw activity records and HASR-TL metrics.",
        "url": "https://docs.google.com/spreadsheets/d/11oJAhAphaHDnGHdknI8TrZjeIy7Aq2DqJ8lInu_XL_M/edit?gid=412740509#gid=412740509",
        "csv_url": "https://docs.google.com/spreadsheets/d/11oJAhAphaHDnGHdknI8TrZjeIy7Aq2DqJ8lInu_XL_M/export?format=csv&gid=412740509",
        "tabs": ["Raw Activity Data", "HASR-TL"],
        "sheet_tabs": [
            {"title": "Raw Activity Data", "csv_url": "https://docs.google.com/spreadsheets/d/11oJAhAphaHDnGHdknI8TrZjeIy7Aq2DqJ8lInu_XL_M/export?format=csv&gid=412740509", "columns": ACTIVITY_PREVIEW_COLUMNS, "table_rows": "default"},
            {"title": "HASR-TL", "csv_url": "https://docs.google.com/spreadsheets/d/11oJAhAphaHDnGHdknI8TrZjeIy7Aq2DqJ8lInu_XL_M/gviz/tq?tqx=out:csv&sheet=HASR-TL", "columns": [], "table_rows": []},
        ],
        "preview": "Recent rows include activity descriptions such as Ajdovscina Trail Running and Ajdovscina - Run Workout.",
        "columns": ACTIVITY_PREVIEW_COLUMNS,
        "table_rows": [
            ["2026", "7", "24", "Friday", "Ajdovscina Trail Running", "Trail Running", "16:25", "Ajdovscina", "15.1", "1.59", "622", "6.36", "5.46", "141", "158"],
            ["2026", "7", "23", "Thursday", "Ajdovscina - Run Workout", "Trail Running", "15:44", "Ajdovscina", "13.1", "1.16", "304", "5.32", "5.11", "150", "182"],
            ["2026", "7", "22", "Wednesday", "Ajdovscina Trail Running", "Trail Running", "17:47", "Ajdovscina", "10.9", "1.39", "727", "7.70", "5.76", "141", "156"],
            ["2026", "7", "21", "Tuesday", "Kranj - Run Workout", "Running", "06:20", "Kranj", "14.9", "1.16", "32", "4.68", "4.67", "147", "179"],
            ["2026", "7", "20", "Monday", "Rest", "", "", "", "", "", "", "", "", "", ""],
            ["2026", "7", "19", "Sunday", "Nova Gorica Trail Running", "Trail Running", "11:00", "Nova Gorica", "21.7", "2.88", "1173", "7.97", "6.33", "146", "166"],
            ["2026", "7", "18", "Saturday", "Ajdovscina Trail Running", "Trail Running", "18:08", "Ajdovscina", "13.3", "1.7", "902", "7.80", "5.84", "144", "166"],
            ["2026", "7", "17", "Friday", "Ajdovscina Trail Running", "Trail Running", "18:08", "Ajdovscina", "8.2", "1", "455", "7.35", "5.82", "142", "158"],
            ["2026", "7", "16", "Thursday", "Kranj - Run Workout", "Running", "18:40", "Kranj", "15.2", "1.16", "29", "4.61", "4.61", "156", "184"],
            ["2026", "7", "15", "Wednesday", "Rest", "", "", "", "", "", "", "", "", "", ""],
            ["2026", "7", "14", "Tuesday", "Ajdovscina Trail Running", "Trail Running", "17:43", "Ajdovscina", "13.9", "1.66", "776", "7.18", "5.61", "142", "159"],
        ],
    },
    {
        "title": "Daily Statistics Urh",
        "description": "Daily wellness and summary metrics, including fields such as resting HR.",
        "url": "https://docs.google.com/spreadsheets/d/12Nd6dxw6wux5hFYeiei8l0OXqXG4aBpASzTdXb3rpdw/edit?gid=0#gid=0",
        "csv_url": "https://docs.google.com/spreadsheets/d/12Nd6dxw6wux5hFYeiei8l0OXqXG4aBpASzTdXb3rpdw/export?format=csv&gid=0",
        "tabs": ["Raw Daily Data"],
        "sheet_tabs": [
            {"title": "Raw Daily Data", "csv_url": "https://docs.google.com/spreadsheets/d/12Nd6dxw6wux5hFYeiei8l0OXqXG4aBpASzTdXb3rpdw/export?format=csv&gid=0", "columns": DAILY_PREVIEW_COLUMNS, "table_rows": "default"},
        ],
        "preview": "Recent rows include daily metrics for dates such as 2026-07-24, 2026-07-23, and 2026-07-22.",
        "columns": DAILY_PREVIEW_COLUMNS,
        "table_rows": [
            ["2026", "7", "24", "Friday", "48", "81", "6.71", "65", "60", "76", "644", "1.64", "0.55", "10.64", "60"],
            ["2026", "7", "23", "Thursday", "46", "77", "6.18", "66", "60", "76", "324", "1.28", "0.39", "12.45", "60.2"],
            ["2026", "7", "22", "Wednesday", "49", "75", "5.51", "54", "60", "76", "757", "1.4", "0.86", "12.11", "60.2"],
            ["2026", "7", "21", "Tuesday", "47", "91", "7.12", "67", "60", "76", "41", "1.37", "0.57", "14.95", "60.3"],
            ["2026", "7", "20", "Monday", "48", "83", "5.69", "65", "60", "76", "23", "0.14", "0.42", "17.75", "59.8"],
            ["2026", "7", "19", "Sunday", "48", "94", "9.04", "64", "60", "75", "1190", "2.67", "1.05", "11.24", "59.8"],
            ["2026", "7", "18", "Saturday", "47", "95", "9.32", "67", "60", "74", "899", "1.9", "1.23", "11.55", "59.9"],
            ["2026", "7", "17", "Friday", "50", "76", "6.06", "57", "59", "74", "487", "1.08", "0.93", "15.93", "59.9"],
            ["2026", "7", "16", "Thursday", "46", "86", "7.67", "71", "59", "74", "52", "1.34", "0.57", "14.41", "59.9"],
            ["2026", "7", "15", "Wednesday", "46", "78", "6.4", "77", "59", "75", "19", "0.17", "0.41", "17.02", "59.6"],
            ["2026", "7", "14", "Tuesday", "47", "83", "7.61", "75", "59", "75", "813", "1.69", "0.96", "13.74", "59.6"],
        ],
    },
]

DEMO_GOOGLE_SHEETS = [
    {
        "title": "Activity Statistics Sample",
        "description": "Sample activity-level training data with raw activity records and HASR-TL metrics.",
        "url": "https://docs.google.com/spreadsheets/d/1o5Y9_AM_8baj5DnB2AE9nYXU_ZSPsyFW6VjRbljSWIw/edit?gid=412740509#gid=412740509",
        "csv_url": "https://docs.google.com/spreadsheets/d/1o5Y9_AM_8baj5DnB2AE9nYXU_ZSPsyFW6VjRbljSWIw/export?format=csv&gid=412740509",
        "tabs": ["Raw Activity Data", "HASR-TL"],
        "sheet_tabs": [
            {"title": "Raw Activity Data", "csv_url": "https://docs.google.com/spreadsheets/d/1o5Y9_AM_8baj5DnB2AE9nYXU_ZSPsyFW6VjRbljSWIw/export?format=csv&gid=412740509", "columns": ACTIVITY_PREVIEW_COLUMNS, "table_rows": "default"},
            {"title": "HASR-TL", "csv_url": "https://docs.google.com/spreadsheets/d/1o5Y9_AM_8baj5DnB2AE9nYXU_ZSPsyFW6VjRbljSWIw/gviz/tq?tqx=out:csv&sheet=HASR-TL", "columns": [], "table_rows": []},
        ],
        "preview": "Recent sample rows include activities such as Ajdovscina Running, Ajdovscina Trail Running, and Rest.",
        "columns": ACTIVITY_PREVIEW_COLUMNS,
        "table_rows": [
            ["2025", "4", "10", "Thursday", "Ajdovscina Running", "Running", "17:00", "Ajdovscina", "12.1", "1.43", "264", "7.14", "6.78", "122", "136"],
            ["2025", "4", "9", "Wednesday", "Ajdovscina Trail Running", "Trail Running", "17:07", "Ajdovscina", "7.7", "1.62", "662", "13.29", "8.71", "101", "143"],
            ["2025", "4", "8", "Tuesday", "Ajdovscina - LT + 1x20' SSR", "Trail Running", "17:51", "Ajdovscina", "13.9", "1.44", "647", "6.26", "5.25", "147", "174"],
            ["2025", "4", "7", "Monday", "Rest", "", "", "", "", "", "", "", "", "", ""],
            ["2025", "4", "6", "Sunday", "Slope Workout", "Indoor Biking", "19:16", "Unknown", "30.2", "1.5", "875", "20.13", "", "127", "138"],
            ["2025", "4", "5", "Saturday", "Ajdovscina Trail Running", "Trail Running", "10:22", "Ajdovscina", "13.7", "2.11", "1232", "9.27", "5.99", "149", "169"],
            ["2025", "4", "4", "Friday", "Ajdovscina Road Biking", "Road Biking", "17:12", "Ajdovscina", "54", "1.91", "688", "", "", "135", "165"],
            ["2025", "4", "3", "Thursday", "Ajdovscina - DT 2x15' 10-15% naklon + hiter s", "Trail Running", "13:10", "Ajdovscina", "14.4", "1.6", "842", "6.64", "5.14", "160", "180"],
            ["2025", "4", "2", "Wednesday", "Ajdovscina Running", "Running", "18:18", "Ajdovscina", "12.4", "0.97", "126", "4.70", "4.62", "150", "180"],
            ["2025", "4", "1", "Tuesday", "Ajdovscina Trail Running", "Trail Running", "17:42", "Ajdovscina", "11.5", "1.34", "704", "6.98", "5.58", "142", "164"],
            ["2025", "3", "31", "Monday", "Rest", "", "", "", "", "", "", "", "", "", ""],
        ],
    },
    {
        "title": "Daily Statistics Sample",
        "description": "Sample daily wellness and summary metrics, including fields such as resting HR.",
        "url": "https://docs.google.com/spreadsheets/d/1k-JIYE5z9a-2IqcslkliL_Y6CdvcNeZ7NoVe8wDbo8I/edit?gid=0#gid=0",
        "csv_url": "https://docs.google.com/spreadsheets/d/1k-JIYE5z9a-2IqcslkliL_Y6CdvcNeZ7NoVe8wDbo8I/export?format=csv&gid=0",
        "tabs": ["Raw Daily Data"],
        "sheet_tabs": [
            {"title": "Raw Daily Data", "csv_url": "https://docs.google.com/spreadsheets/d/1k-JIYE5z9a-2IqcslkliL_Y6CdvcNeZ7NoVe8wDbo8I/export?format=csv&gid=0", "columns": DAILY_PREVIEW_COLUMNS, "table_rows": "default"},
        ],
        "preview": "Recent sample rows include daily metrics for dates such as 2025-04-10, 2025-04-09, and 2025-04-08.",
        "columns": DAILY_PREVIEW_COLUMNS,
        "table_rows": [
            ["2025", "4", "10", "Thursday", "48", "93", "6.8", "61", "52", "66", "278", "1.59", "0.3", "12.27", "60.5"],
            ["2025", "4", "9", "Wednesday", "49", "84", "6.69", "59", "52", "66", "674", "0.99", "1.07", "10.65", "60.5"],
            ["2025", "4", "8", "Tuesday", "48", "90", "6", "67", "51", "65", "675", "1.81", "0.21", "11.56", "60.5"],
            ["2025", "4", "7", "Monday", "47", "84", "6.34", "74", "51", "65", "44", "0.08", "0.97", "14.26", "60.6"],
            ["2025", "4", "6", "Sunday", "46", "86", "9.11", "72", "51", "64", "33", "0.34", "0.55", "14.01", "60.6"],
            ["2025", "4", "5", "Saturday", "50", "85", "7.95", "63", "51", "64", "1455", "1.84", "2.07", "5.96", "60.6"],
            ["2025", "4", "4", "Friday", "49", "77", "5.38", "62", "51", "64", "26", "0.31", "0.54", "13.15", "60.7"],
            ["2025", "4", "3", "Thursday", "50", "85", "6.42", "61", "51", "64", "870", "1.79", "0.86", "9.09", "60.7"],
            ["2025", "4", "2", "Wednesday", "51", "87", "6.51", "63", "51", "64", "471", "1.51", "0.87", "10.49", "60.7"],
            ["2025", "4", "1", "Tuesday", "48", "81", "6.2", "81", "51", "64", "0", "0.02", "0", "0.16", "60.7"],
            ["2025", "3", "31", "Monday", "52", "84", "5.43", "70", "51", "64", "16", "0.14", "0.85", "14.28", "60.6"],
        ],
    },
]


# ----------------------------------------------------------
# Sheet Loading Helpers
# ----------------------------------------------------------

# Loads preview rows from one specific sheet tab CSV export when possible.
# Falls back to static rows for raw tabs if the sheet is not reachable from the local app.
# Returns a small dataframe suitable for st.dataframe.

@st.cache_data(ttl=300)
def build_preview_table(sheet: dict, sheet_tab: dict) -> pd.DataFrame:
    try:
        return pd.read_csv(sheet_tab["csv_url"], nrows=50).fillna("")
    except Exception:
        columns = sheet["columns"] if sheet_tab.get("table_rows") == "default" else sheet_tab.get("columns", [])
        table_rows = sheet["table_rows"] if sheet_tab.get("table_rows") == "default" else sheet_tab.get("table_rows", [])

        if not columns:
            return pd.DataFrame()

        # Pad shorter fallback rows so all known sheet columns are displayed.
        padded_rows = []
        for row in table_rows:
            padded_rows.append(row + [""] * (len(columns) - len(row)))

        return pd.DataFrame(padded_rows, columns=columns)


# Loads all rows from one specific sheet tab CSV export when possible.
# The coach context uses this fuller load instead of the 50-row dashboard preview.
# Returns a dataframe with empty values normalized for text export.

@st.cache_data(ttl=300)
def load_sheet_tab_data(sheet: dict, sheet_tab: dict) -> pd.DataFrame:
    try:
        return pd.read_csv(sheet_tab["csv_url"]).fillna("")
    except Exception:
        columns = sheet["columns"] if sheet_tab.get("table_rows") == "default" else sheet_tab.get("columns", [])
        table_rows = sheet["table_rows"] if sheet_tab.get("table_rows") == "default" else sheet_tab.get("table_rows", [])

        if not columns:
            return pd.DataFrame()

        # Keep the fallback shape aligned with the live CSV columns used by the dashboard.
        padded_rows = []
        for row in table_rows:
            padded_rows.append(row + [""] * (len(columns) - len(row)))

        return pd.DataFrame(padded_rows, columns=columns)


# Returns the configured tabs for one source sheet.
# Older sheet metadata can still fall back to a single tab using the sheet-level CSV URL.
# Returns a list of tab dictionaries.

def get_sheet_tabs(sheet: dict) -> list[dict]:
    if "sheet_tabs" in sheet:
        return sheet["sheet_tabs"]

    return [{"title": sheet["tabs"][0], "csv_url": sheet["csv_url"], "columns": sheet["columns"], "table_rows": "default"}]


# Chooses the sheet set for the current access mode.
# Full mode uses private owner sheets, while demo mode uses sample sheets.
# Returns the list of visible sheet metadata dictionaries.

def get_visible_sheets(owner_mode: bool) -> list[dict]:
    return PRIVATE_GOOGLE_SHEETS if owner_mode else DEMO_GOOGLE_SHEETS


# Finds one visible sheet by title text.
# This lets the page layout place Daily and Activity panels intentionally.
# Returns the matching sheet dictionary or None.

def find_sheet_by_title_part(sheets: list[dict], title_part: str) -> dict | None:
    for sheet in sheets:
        if title_part.lower() in sheet["title"].lower():
            return sheet

    return None
