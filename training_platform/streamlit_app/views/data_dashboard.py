import streamlit as st

from config import DATA_TABLE_HEIGHT
from data_sources import build_preview_table, find_sheet_by_title_part, get_sheet_tabs, get_visible_sheets, load_sheet_tab_data


# ----------------------------------------------------------
# Data Dashboard View
# ----------------------------------------------------------

# Renders one dataframe with the app's standard table sizing.
# Sheet values are already loaded as display text, so this keeps rendering simple and predictable.
# Returns nothing; it writes the dataframe to the page.

def render_data_table(table, height: int) -> None:
    st.dataframe(table, width="stretch", height=height, hide_index=True)


# Builds a stable Streamlit key for repeated table buttons.
# It keeps buttons unique across sheet panels and sheet tabs.
# Returns a readable widget key.

def build_table_button_key(sheet: dict, sheet_tab: dict) -> str:
    key_parts = [sheet["title"], sheet_tab["title"]]
    cleaned_parts = ["_".join(part.lower().split()) for part in key_parts]
    return f"open_full_table_{'_'.join(cleaned_parts)}"


# Shows one expanded in-app table dialog.
# Google Sheets remain backend sources, while users inspect the table inside the app.
# Returns nothing; it writes a modal table view.

@st.dialog("Full table", width="large")
def render_full_table_dialog(sheet_title: str, sheet_tab_title: str, table) -> None:
    st.markdown(f"**{sheet_title} / {sheet_tab_title}**")

    if table.empty:
        st.info("This sheet tab is configured, but no table rows are available yet.")
        return

    st.caption(f"{len(table)} rows x {len(table.columns)} columns")
    render_data_table(table, height=720)


# Renders one Google Sheet-backed panel with tabs, a preview table, and an expanded table action.
# The preview uses live CSV exports when reachable and falls back to bundled sample rows.
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
                    render_data_table(preview_table, height=DATA_TABLE_HEIGHT)

                if st.button("Open full table", key=build_table_button_key(sheet, sheet_tab), icon=":material/open_in_full:", width="stretch"):
                    full_table = load_sheet_tab_data(sheet, sheet_tab)
                    render_full_table_dialog(sheet["title"], sheet_tab["title"], full_table)


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
