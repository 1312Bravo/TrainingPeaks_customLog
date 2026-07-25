import os
from pathlib import Path

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from openai import OpenAI


# ----------------------------------------------------------
# App Configuration
# ----------------------------------------------------------

# This is the owner account for the future full/private version of the app.
# Until Google login is configured, this check falls back to demo mode.
OWNER_EMAIL = "pecek.urh@gmail.com"
APP_ROOT = Path(__file__).resolve().parents[1]
PROMPTS_ROOT = APP_ROOT / "prompts"
DEFAULT_OPENAI_MODEL = "gpt-5-mini"
MODEL_PRICES_PER_1M_TOKENS = {
    "gpt-5-mini": {
        "input": 0.25,
        "output": 2.00,
    }
}

# Load local secrets from training_platform/.env before reading environment variables.
# The .env file is ignored by git and should never be committed.
load_dotenv(APP_ROOT / ".env")

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

PRIVATE_GOOGLE_SHEETS = [
    {
        "title": "Activity Statistics Urh",
        "description": "Activity-level training data with raw activity records and HASR-TL metrics.",
        "url": "https://docs.google.com/spreadsheets/d/11oJAhAphaHDnGHdknI8TrZjeIy7Aq2DqJ8lInu_XL_M/edit?gid=412740509#gid=412740509",
        "csv_url": "https://docs.google.com/spreadsheets/d/11oJAhAphaHDnGHdknI8TrZjeIy7Aq2DqJ8lInu_XL_M/export?format=csv&gid=412740509",
        "tabs": ["Raw Activity Data", "HASR-TL"],
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


# Page settings are kept near the top because Streamlit expects them before normal page content is rendered.
st.set_page_config(
    page_title="Training Platform",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ----------------------------------------------------------
# Access Mode Helpers
# ----------------------------------------------------------

# Checks whether Streamlit auth has been configured in secrets.
# When it is not configured, the app should still run in public demo mode.
# Returns True when an [auth] section exists.

def is_auth_configured() -> bool:
    try:
        return "auth" in st.secrets
    except Exception:
        return False


# Checks whether the current browser session is logged in through Streamlit auth.
# Streamlit only exposes is_logged_in after auth is configured.
# Returns True only for authenticated sessions.

def is_logged_in() -> bool:
    return bool(getattr(st.user, "is_logged_in", False))


# Checks whether Streamlit auth identifies the owner account.
# For now, local development has no login configured, so this safely returns demo mode.
# Returns True only when the logged-in email matches OWNER_EMAIL.

def is_owner() -> bool:
    # Guard against local/no-auth Streamlit runs and non-owner accounts.
    if not is_logged_in():
        return False

    return st.user.get("email") == OWNER_EMAIL


# Renders login/logout controls without blocking demo mode.
# Public users can keep using demo mode, while the owner can log in for full mode.
# Returns nothing; it writes auth controls to the sidebar.

def render_auth_controls() -> None:
    with st.sidebar:
        st.subheader("Access")

        if not is_auth_configured():
            st.info("Google login is not configured yet. Demo mode is active.")
            return

        if is_logged_in():
            st.write(st.user.get("email", "Logged in"))
            st.button("Log out", on_click=st.logout)
        else:
            st.caption("Log in with Google to unlock full mode if this is the owner account.")
            st.button("Log in with Google", on_click=st.login)


# ----------------------------------------------------------
# Agent Chat
# ----------------------------------------------------------

# Reads a prompt Markdown file from the prompts folder.
# Missing prompt files return an empty string so the app can still load.
# Returns the prompt text as a string.

def read_prompt_file(file_name: str) -> str:
    prompt_path = PROMPTS_ROOT / file_name

    # Keep prompt loading forgiving while the prompt files are still being edited.
    if not prompt_path.exists():
        return ""

    return prompt_path.read_text(encoding="utf-8").strip()


# Builds the coach instructions from editable prompt files.
# It adds a temporary privacy boundary until training context is deliberately connected.
# Returns the full instruction text sent to the model.

def build_coach_instructions(owner_mode: bool) -> str:
    prompt_parts = [
        read_prompt_file("coach_system.md"),
        read_prompt_file("coach_response_style.md"),
        read_prompt_file("safety_boundaries.md"),
    ]

    # This keeps the coach honest until we wire authenticated Google Sheet context.
    context_boundary = "You do not currently have live access to the user's private training data inside this chat. Do not claim that you analyzed private sheets unless explicit context is provided."
    mode_boundary = "The app is in full owner mode." if owner_mode else "The app is in demo mode. Keep answers generic and do not reference private user data."

    return "\n\n".join([part for part in prompt_parts + [context_boundary, mode_boundary] if part])


# Reads the OpenAI API key from Streamlit secrets or environment variables.
# This keeps secrets outside source code.
# Returns the API key string or None.

def get_openai_api_key() -> str | None:
    try:
        return st.secrets.get("OPENAI_API_KEY") or os.getenv("OPENAI_API_KEY")
    except Exception:
        return os.getenv("OPENAI_API_KEY")


# Safely reads a token count from the OpenAI usage object.
# The SDK can expose usage as attributes, while some tests or future code may use dictionaries.
# Returns an integer token count.

def get_usage_value(usage: object, key: str) -> int:
    if not usage:
        return 0

    if isinstance(usage, dict):
        return int(usage.get(key) or 0)

    return int(getattr(usage, key, 0) or 0)


# Estimates the API cost for one model response from token usage.
# Prices are kept in one local table so we can update them when the chosen model changes.
# Returns a small dictionary used by the chat UI.

def estimate_response_cost(model: str, usage: object) -> dict:
    input_tokens = get_usage_value(usage, "input_tokens")
    output_tokens = get_usage_value(usage, "output_tokens")
    total_tokens = get_usage_value(usage, "total_tokens") or input_tokens + output_tokens
    prices = MODEL_PRICES_PER_1M_TOKENS.get(model)

    if not prices:
        return {"model": model, "input_tokens": input_tokens, "output_tokens": output_tokens, "total_tokens": total_tokens, "estimated_cost_usd": None}

    input_cost = input_tokens / 1_000_000 * prices["input"]
    output_cost = output_tokens / 1_000_000 * prices["output"]

    return {"model": model, "input_tokens": input_tokens, "output_tokens": output_tokens, "total_tokens": total_tokens, "estimated_cost_usd": input_cost + output_cost}


# Formats token and cost metadata for display under an assistant message.
# Very tiny costs are still shown with enough decimals to be useful during testing.
# Returns a short caption string or None.

def format_cost_caption(usage_summary: dict | None) -> str | None:
    if not usage_summary:
        return None

    cost = usage_summary.get("estimated_cost_usd")
    token_text = f"{usage_summary['input_tokens']} input tokens, {usage_summary['output_tokens']} output tokens"

    if cost is None:
        return f"Usage estimate: {token_text}. Cost not configured for `{usage_summary['model']}`."

    return f"Estimated API cost: ${cost:.6f} ({token_text}, model `{usage_summary['model']}`)."


# Calls the OpenAI API with the current chat history.
# The model receives coach prompts plus the recent user/assistant messages.
# Returns the assistant response text and usage metadata for cost display.

def get_agent_response(owner_mode: bool) -> dict:
    api_key = get_openai_api_key()

    if not api_key:
        return {"content": "The chat UI is ready, but `OPENAI_API_KEY` is not configured yet. Add the key locally and restart the app to enable real coach replies.", "usage": None}

    client = OpenAI(api_key=api_key)
    model = os.getenv("OPENAI_MODEL") or DEFAULT_OPENAI_MODEL
    chat_messages = [{"role": message["role"], "content": message["content"]} for message in st.session_state.agent_messages]

    try:
        response = client.responses.create(
            model=model,
            instructions=build_coach_instructions(owner_mode),
            input=chat_messages,
        )
    except Exception as error:
        return {"content": f"The coach API call failed: {error}", "usage": None}

    return {"content": response.output_text, "usage": estimate_response_cost(model, response.usage)}


# Renders one chat message with optional API usage/cost metadata.
# User messages do not have cost metadata; assistant API replies may have it.
# Returns nothing; it writes one message bubble to the page.

def render_chat_message(message: dict) -> None:
    with st.chat_message(message["role"]):
        st.write(message["content"])

        cost_caption = format_cost_caption(message.get("usage"))
        if cost_caption:
            st.caption(cost_caption)


# Creates the in-browser chat history for the current Streamlit session.
# Later we can decide what should be saved permanently.
# Returns nothing; it updates Streamlit session state.

def initialize_chat() -> None:
    # Add the first assistant message only once per session.
    if "agent_messages" not in st.session_state:
        st.session_state.agent_messages = [
            {
                "role": "assistant",
                "content": "Hi. I am your trail running coach. Ask me about training ideas, recovery, planning, or how to think about the data we are wiring into this app.",
            }
        ]


# Renders the chat area that will later connect to the real AI agent.
# It shows different captions for full mode and demo mode.
# Returns nothing; it writes UI elements to the Streamlit page.

def render_agent_chat(owner_mode: bool) -> None:
    st.subheader("Agent chat")

    # Explain whether this chat can use private context.
    if owner_mode:
        st.caption("Full mode chat. Later this can use your private training context.")
    else:
        st.caption("Demo mode chat. Private training context is not available.")

    initialize_chat()

    for message in st.session_state.agent_messages:
        render_chat_message(message)

    prompt = st.chat_input("Ask the agent...")
    if prompt:
        # Save and display the user's message.
        st.session_state.agent_messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.write(prompt)

        # Ask the real coach when the API key is configured; otherwise show setup guidance.
        with st.chat_message("assistant"):
            with st.spinner("Coach is thinking..."):
                response = get_agent_response(owner_mode)
                st.write(response["content"])

                cost_caption = format_cost_caption(response["usage"])
                if cost_caption:
                    st.caption(cost_caption)

        st.session_state.agent_messages.append({"role": "assistant", "content": response["content"], "usage": response["usage"]})


# ----------------------------------------------------------
# Google Sheet Data Sources
# ----------------------------------------------------------

# Loads preview rows from the sheet CSV export when possible.
# Falls back to static rows if the sheet is not reachable from the local app.
# Returns a small dataframe suitable for st.dataframe.

@st.cache_data(ttl=300)
def build_preview_table(sheet: dict) -> pd.DataFrame:
    try:
        return pd.read_csv(sheet["csv_url"], nrows=50).fillna("")
    except Exception:
        columns = sheet["columns"]

        # Pad shorter fallback rows so all known sheet columns are displayed.
        padded_rows = []
        for row in sheet["table_rows"]:
            padded_rows.append(row + [""] * (len(columns) - len(row)))

        return pd.DataFrame(padded_rows, columns=columns)


# Renders known Google Sheet files that the app and agent should eventually use.
# For now this is a visible source registry with links and metadata.
# Returns nothing; it writes source cards to the Streamlit page.

def render_google_sheet_sources(owner_mode: bool) -> None:
    st.subheader("Google Sheet data sources")

    if owner_mode:
        st.caption("Full mode can later connect these sheets as private training context for the agent.")
        visible_sheets = PRIVATE_GOOGLE_SHEETS
    else:
        st.caption("Demo mode uses sample sheets. Private owner sheets are hidden.")
        visible_sheets = DEMO_GOOGLE_SHEETS

    for sheet in visible_sheets:
        with st.container(border=True):
            st.markdown(f"**{sheet['title']}**")
            st.write(sheet["description"])
            st.caption(f"Tabs: {', '.join(sheet['tabs'])}")
            st.caption(sheet["preview"])

            # Show a compact preview so the user sees the data shape without opening Google Sheets.
            st.dataframe(build_preview_table(sheet), width="stretch", height=260, hide_index=True)

            st.link_button("Open Google Sheet", sheet["url"])


# ----------------------------------------------------------
# Page Layout
# ----------------------------------------------------------

# Top-level page shell. The two modes should share the same app structure later, with full mode unlocking private data and owner-only actions.
st.caption("✉ pecek.urh@gmail.com")
st.title("Training Platform")

render_auth_controls()
owner_mode = is_owner()

if owner_mode:
    st.success("Full app mode")
    st.caption("Private training data, notes, and AI coach features will live here.")
else:
    st.info("Demo mode")
    st.caption("Public/demo experience. Private training data is hidden.")

st.divider()
render_google_sheet_sources(owner_mode)

st.divider()
render_agent_chat(owner_mode)
