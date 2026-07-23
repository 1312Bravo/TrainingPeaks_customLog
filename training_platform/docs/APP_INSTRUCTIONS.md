# Training Platform Instructions

This file explains how to run and use the training platform app. Later, it can
also describe how the app works.

## Current App

- The app is currently built with Streamlit.
- The app entry point is `streamlit_app/app.py`.
- The app runs locally by default.
- The app is intended to have a full owner mode and a public/demo mode.

## Open The App

From PowerShell:

```powershell
Set-Location -LiteralPath "C:\Users\Urh\Desktop\Urh\Github Repositories\Training Peaks Custom Log\TrainingPeaks_customLog\training_platform"
.\start_app.cmd
```

From Command Prompt:

```cmd
cd /d "C:\Users\Urh\Desktop\Urh\Github Repositories\Training Peaks Custom Log\TrainingPeaks_customLog\training_platform"
start_app.cmd
```

You can also double-click `start_app.cmd` in the `training_platform` folder.

Then open:

```text
http://127.0.0.1:8501
```

## Notes

- Keep this file focused on practical app instructions and behavior.
- Use `notes/` for durable product decisions, requirements, and architecture
  notes.

## Access Modes

- Full mode is intended for `pecek.urh@gmail.com`.
- Demo mode is intended for everyone else.
- Google login is not configured yet. When it is added, the app should use the
  logged-in email to decide which mode to show.
