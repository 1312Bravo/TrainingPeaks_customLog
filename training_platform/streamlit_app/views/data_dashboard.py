import streamlit as st

from config import DATA_TABLE_HEIGHT
from data_sources import build_preview_table, find_sheet_by_title_part, get_sheet_tabs, get_visible_sheets


# ----------------------------------------------------------
# Data Dashboard View
# ----------------------------------------------------------

# Renders one Google Sheet panel with metadata, a scrollable table, and source link.
# The table uses live CSV exports when reachable and falls back to bundled sample rows.
# Returns nothing; it writes one panel to the page.

def render_sheet_panel(sheet: dict | None) -> None:
    if not sheet:
        st.warning("Sheet configuration is missing.")
        return

    with st.container(border=True):
        st.markdown(f"**{sheet['title']}**")
        sheet_tabs = get_sheet_tabs(sheet)

        for streamlit_tab, sheet_tab in zip(st.tabs([sheet_tab["title"] for sheet_tab in sheet_tabs]), sheet_tabs):
            with streamlit_tab:
                preview_table = build_preview_table(sheet, sheet_tab)

                if preview_table.empty:
                    st.info("This sheet tab is configured, but no preview rows are available yet.")
                else:
                    st.dataframe(preview_table, width="stretch", height=DATA_TABLE_HEIGHT, hide_index=True)

        st.link_button("Open Google Sheet", sheet["url"])


# Renders the top data dashboard area.
# Daily statistics are placed on the left and activity statistics on the right.
# Returns nothing; it writes the two data panels to the page.

def render_data_dashboard(owner_mode: bool) -> None:
    st.subheader("Training data")

    if owner_mode:
        st.caption("Full mode shows private owner sheets.")
    else:
        st.caption("Demo mode shows sample sheets. Private owner sheets are hidden.")

    visible_sheets = get_visible_sheets(owner_mode)
    daily_sheet = find_sheet_by_title_part(visible_sheets, "Daily")
    activity_sheet = find_sheet_by_title_part(visible_sheets, "Activity")
    daily_column, activity_column = st.columns(2, gap="large")

    with daily_column:
        render_sheet_panel(daily_sheet)

    with activity_column:
        render_sheet_panel(activity_sheet)
