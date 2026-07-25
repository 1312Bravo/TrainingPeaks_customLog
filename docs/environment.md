# Environment Files

This project currently uses two local `.env` files.

## Root `.env`

Path:

```text
.env
```

Use this for existing repository jobs, such as Garmin, Google Drive, daily jobs, and statistics pipelines.

## Training Platform `.env`

Path:

```text
training_platform/.env
```

Use this for the Streamlit app, including:

```text
OPENAI_API_KEY=...
OPENAI_MODEL=...
```

## Why Keep Them Separate

The root jobs and the Streamlit app are related, but they have different runtime needs.

Keeping separate `.env` files avoids mixing app secrets with job secrets and makes deployment easier later.

## Git Safety

Both `.env` files are ignored by git:

- root `.gitignore` ignores `.env`
- `training_platform/.gitignore` ignores `.env`

Do not commit real API keys or secrets.
