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
- Google login uses Streamlit OIDC auth.
- The app should use the logged-in email to decide which mode to show.

## Google Login Setup

Create a real local secrets file from the example:

```text
training_platform/.streamlit/secrets.toml
```

Use this shape:

```toml
[auth]
redirect_uri = "http://localhost:8501/oauth2callback"
cookie_secret = "replace_with_a_strong_random_secret"
client_id = "replace_with_google_client_id"
client_secret = "replace_with_google_client_secret"
server_metadata_url = "https://accounts.google.com/.well-known/openid-configuration"
```

In Google Cloud / Google Auth Platform, configure the OAuth client redirect URI:

```text
http://localhost:8501/oauth2callback
```

Do not commit `.streamlit/secrets.toml`.

## AI Chat Setup

The chat uses the OpenAI API when `OPENAI_API_KEY` is available.

For local setup, create `training_platform/.env` from `.env.example`:

```text
OPENAI_API_KEY=your_api_key_here
```

Then start the app normally:

```powershell
.\start_app.cmd
```

Do not commit API keys into the repository. For deployment later, put the key into the deployment provider's secrets, such as GitHub Secrets or Streamlit secrets.
