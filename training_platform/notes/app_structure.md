# App Structure

## 2026-07-26

- `streamlit_app/app.py` should stay as the Streamlit entry point and high-level page shell.
- `streamlit_app/config.py` should hold shared constants, paths, environment loading, model defaults, and pricing tables.
- `streamlit_app/auth.py` should hold login, owner-mode, demo-mode, and sidebar access controls.
- `streamlit_app/ai_coach.py` should hold prompt loading, OpenAI calls, token usage, and cost-estimate helpers.
- `streamlit_app/data_sources.py` should hold Google Sheet metadata, source-tab configuration, and table-loading helpers.
- `streamlit_app/styles.py` should hold small app-level visual styling.
- `streamlit_app/views/` should hold Streamlit page sections, such as the data dashboard and coach chat.
- Keep `app.py` small over time; when a feature grows, move its logic into a focused module instead of adding another large block to the entry file.
- Prefer normal Streamlit rerun/refresh-friendly changes during UI iteration; app config changes, module renames, and toolbar/client settings may require a full Streamlit restart.
