import streamlit as st

from auth import is_owner, render_auth_controls
from styles import apply_custom_styles
from views.coach_chat import render_agent_chat
from views.data_dashboard import render_data_dashboard
from views.header import render_page_header, render_top_reference


# ----------------------------------------------------------
# Page Setup
# ----------------------------------------------------------

# Hide Streamlit's local developer toolbar controls so our own top utility row has room.
# This mirrors `.streamlit/config.toml`, but also works when the app is launched from another folder.
st.set_option("client.toolbarMode", "minimal")

# Page settings are kept near the top because Streamlit expects them before normal page content is rendered.
st.set_page_config(
    page_title="Training Platform",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ----------------------------------------------------------
# Page Layout
# ----------------------------------------------------------

# Top-level page shell. The two modes should share the same app structure later, with full mode unlocking private data and owner-only actions.
apply_custom_styles()

owner_mode = is_owner()
reference_column, access_column = st.columns([1.35, 1], gap="large")

with reference_column:
    render_top_reference()

with access_column:
    render_auth_controls()

render_page_header(owner_mode)

st.divider()
render_data_dashboard(owner_mode)

st.divider()
render_agent_chat(owner_mode)
