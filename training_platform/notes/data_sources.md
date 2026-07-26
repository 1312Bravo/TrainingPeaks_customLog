# Data Sources

## 2026-07-25

- The app should show Google Sheet data sources created by this project.
- `Activity Statistics Urh` should be included as an app data source.
- `Daily Statistics Urh` should be included as an app data source.
- Demo mode should show sample Google Sheets instead of the owner's private Google Sheets.
- Full mode should show the owner's private Google Sheets only when the logged-in Google account is `pecek.urh@gmail.com`.
- `Activity Statistics Sample` should be used as the demo activity-level data source.
- `Daily Statistics Sample` should be used as the demo daily-statistics data source.
- The app should show nice table previews for Google Sheet data sources, not only links to the source sheets.
- Demo mode table previews should use sample-sheet data.
- Full mode table previews should use private-sheet data only when owner access is confirmed.
- Table previews should load from the actual sheet source when possible, so newly added columns do not appear empty because of stale hardcoded preview rows.
- Statistics panels should support multiple source tabs per Google Sheet, so activity data can show both raw activity rows and HASR-TL, and daily data can grow with additional tabs later.
- Each app sheet tab should have its own CSV/export source so the preview can load the correct Google Sheet tab instead of only the first sheet.
- The agent should eventually be able to use these Google Sheets as context.
- Agent access to sheet contents should be implemented only through secure/authenticated access and should respect demo/full mode privacy.
