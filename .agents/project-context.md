# Project Context

Last reviewed: 2026-07-19

## Scope

This repository maintains Python automations for custom TrainingPeaks-style logs.
The current working focus is the daily statistics job in `daily_statistics_job`.

The repo has three main code areas:

- `daily_jobs`: shared configuration, Google Sheets helpers, logging, and runner scripts for the daily job family.
- `daily_statistics_job`: daily Garmin summary rows and per-activity rows.
- `hasr_tl_job`: downstream HASR-TL calculation based on the activity log.

## Runtime And Secrets

Required Python dependencies are listed in `requirements.txt`:

- `numpy`
- `pandas`
- `google-auth`
- `python-dotenv`
- `garminconnect`
- `gspread`

Local development should use the Conda environment named `Sandbox`:

- Python: `C:\Users\Urh\anaconda3\envs\Sandbox\python.exe`
- Version at review time: Python `3.13.1`
- This matches the GitHub Actions Python version and has the pinned project dependencies installed.

VS Code workspace settings pin `python.defaultInterpreterPath` to the `Sandbox` interpreter.
If imports fail with `ModuleNotFoundError`, first confirm VS Code is using `Sandbox`, not Conda `base` or the Windows `py -3` Python 3.11 launcher.
VS Code also sets `python.terminal.executeInFileDir` to `false` so the Run button keeps the workspace root as the working directory.
The Code Runner extension is configured separately with `code-runner.executorMap.python` because Code Runner otherwise runs `python -u ...`, which can hit the broken Windows Store Python alias and fail with exit code `9009`.
Code Runner should run from `$workspaceRoot`.

Path handling convention:

- Do not use `os.getcwd()` to infer the repository root.
- Resolve paths relative to `__file__`, for example `Path(__file__).resolve().parents[1]` in `daily_jobs/config.py`.
- This is needed because VS Code may run a file from its containing folder, especially when clicking the Run icon on `daily_statistics_job/main.py`.
- `.env` and `googleDrive_secrets.json` are loaded from `daily_jobs.config.REPO_ROOT`, not the current working directory.

Secrets are intentionally local and ignored by git:

- `.env`
- `googleDrive_secrets.json`

Do not print or commit secret values.

`daily_jobs/config.py` loads `.env`, builds `USER_CONFIGURATIONS`, creates Google Drive credentials from `googleDrive_secrets.json`, and defines sheet/tab names.
At review time, both configured jobs run for user `urh`.

## GitHub Actions

Workflow:

- `.github/workflows/main.yml`

Behavior:

- Runs on GitHub Actions `ubuntu-latest`.
- Uses Python `3.13.1`.
- Installs `requirements.txt`.
- Creates `.env` from GitHub secret `ENV_VARS`.
- Appends `ENV=prod` to `.env`.
- Creates `googleDrive_secrets.json` from GitHub secret `GOOGLE_DRIVE_SECRETS`.
- Runs `python daily_jobs/run_all.py`.

Schedule:

- Cron is `0 1 * * *`, which is 01:00 UTC daily.
- This is 02:00 in Slovenia during CET and 03:00 during CEST.
- The workflow can also be started manually with `workflow_dispatch`.

## Main Entry Point

Run path:

1. `daily_jobs/run_all.py`
2. For each user in `config.BASIC_DAILY_ACTIVITY_STATISTICS_USERS`, call `get_write_basic_daily_activity_statistics(...)`.
3. For each user in `config.HISTORY_AWARE_RELATIVE_STRATIFIED_ACTIVITY_LOG_USERS`, call `prepare_calculate_write_hasr_tl(...)`.

This means the basic daily/activity job feeds the HASR-TL job through the Google Sheet activity log.

Single-job runners:

- `daily_jobs/run_daily_statistics_job.py`
- `daily_jobs/run_hasr_tl_job.py`

## Basic Daily Activity Job

Main function:

- `daily_statistics_job/main.py::get_write_basic_daily_activity_statistics`

Inputs:

- Garmin email and password from `.env`.
- Google Sheet filenames from `.env`.
- Sheet names from `daily_jobs/config.py`:
  - `Raw Daily Data`
  - `Raw Activity Data`

Flow:

1. Authenticate Garmin Connect.
2. Authenticate Google Drive through `gspread.authorize(config.DRIVE_CREDENTIALS)`.
3. Load existing daily and activity sheets with `daily_jobs.help_functions.import_google_sheet`.
4. Determine missing daily dates from last sheet date + 1 through yesterday.
5. For each missing daily date, call `get_prepare_single_day_daily_statistics` and append one row.
6. Determine missing activity dates from last sheet date + 1 through yesterday.
7. For each missing activity date, call `get_prepare_single_day_activity_statistics` and append one row per activity, or one "Rest" row when no activities exist.

The job assumes the Google Sheets already have at least one data row because it uses `daily_log_df.iloc[-1]` and `activity_log_df.iloc[-1]` before checking headers.

## Daily Statistics Module

File:

- `daily_statistics_job/daily_statistics.py`

Garmin endpoints used:

- `get_stats`
- `get_training_readiness`
- `get_training_status`
- `get_hrv_data`
- `get_hill_score`
- `get_endurance_score`

Output columns are defined in `daily_statistics_job/config.py` as `DAILY_LOG_EXPECTED_HEADERS`.

Important behavior:

- Converts seconds to hours for sleep and active/sedentary time.
- Pulls HRV summary and baseline bounds.
- Pulls VO2 max, hill score, endurance score, and monthly low/high/anaerobic loads.
- Replaces `np.nan` with empty strings before writing.

Risks to remember:

- `get_hill_score` and `get_endurance_score` are called twice each in short-circuit expressions.
- Some nested Garmin structures are assumed present when parent objects exist.
- `round(np.nan)` raises in some contexts, so missing numeric handling should be tightened before broadening use.

## Activity Statistics Module

File:

- `daily_statistics_job/activity_statistics.py`

Garmin endpoints used:

- `get_activities_by_date`
- `get_activity_hr_in_timezones`
- `get_activity_splits`
- `get_activity_details`

Output columns are defined in `daily_statistics_job/config.py` as `ACTIVITY_LOG_EXPECTED_HEADERS`.

Activity behavior:

- No activity produces a single "Rest" row.
- Running activity pace is stored as min/km.
- Cycling speed is stored as km/h.
- Other activity types get mostly empty derived speed/power metrics.
- HR zone durations are written as hours for zones 1-5.
- Activity detail metrics are expanded into one-second rows, then grouped by elapsed minute, then summarized by 10 quantiles of heart rate.

Risks to remember:

- External Garmin responses are trusted heavily; missing keys or unexpected empty structures can break the job.
- `np.isnan(...)` is used on values that may sometimes be `None` or non-float.
- Activity detail metrics assume `activityDetailMetrics` and `metricDescriptors` exist.
- Repeating rows by elapsed seconds can become memory-heavy for long activities.
- The fallback HR zones dataframe has a typo column `lowBoundary:`; current output only uses `zone` and `duration`, but this is still a smell.
- The 10th HR quantile guard checks row 9 existence but tests `iloc[8]` for NaN.
- There is a header typo: `Anerobic training effect`; changing it would affect existing sheets and downstream consumers.

## Shared Helpers

File:

- `daily_jobs/help_functions.py`

Functions:

- `import_google_sheet`: opens a Google Sheet worksheet and returns a dataframe plus worksheet object.
- `safe_convert_to_numeric`: converts empty strings to `np.nan`, numeric-looking values to numbers, and leaves other values as strings.
- `data_safe_convert_to_numeric`: applies safe conversion column by column.
- `replace_nan_with_empty_string`: recursively replaces float NaN with `""`.
- `clean_data`: recursively cleans values before Google Sheets writes.

Risks to remember:

- `import_google_sheet` assumes the sheet has a header row.
- `replace_nan_with_empty_string` and `clean_data` only handle float NaN directly, not all pandas nullable types.

## HASR-TL Downstream Job

Files:

- `hasr_tl_job/main.py`
- `hasr_tl_job/config.py`
- `hasr_tl_job/help_functions.py`

Purpose:

- Reads `Raw Activity Data`.
- Uses `Training load` and `Duration [h]`.
- Writes a sheet named `HASR-TL`.
- Uses a 90-day baseline window and 21-day recent window.
- Classifies recent sessions as Easy, Hard, or Long using weighted quantiles.

The saved image `analysissaved_filed.png` is a plot of baseline and recent load weighting.
The notebook `hasr_tl_job/analysis.ipynb` explores weighting, training load distribution, HASR-TL, bucket diagnostics, and recent training classification.

## Git And Generated Files

Generated/cache files are present:

- `__pycache__` folders
- `.pyc` files

New instruction assets copied into this repo:

- `AGENTS.md`
- `SKILL.md`
- `codex-instructions-notes.md`
- `.agents/project-context.md`

## Instruction Sync

Source instruction repo:

- `C:\Users\Urh\Desktop\Urh\Github Repositories\Codex-Instructions`

Preferred update path:

1. Check `Codex-Instructions/Sync-CodexInstructions.py`.
2. Use it to sync `AGENTS.md` and `SKILL.md` when Python is available.
3. If Python is not available on PATH, copy the same two files directly from the source repo.
4. Refresh `codex-instructions-notes.md` from `Codex-Instructions/notes.md` when those explanatory notes should travel with this project.
5. Keep `.agents/project-context.md` local to this project unless explicitly asked to replace it.

## Good Future Improvements

High-value daily job improvements:

1. Add safer empty-sheet/header bootstrapping before using `iloc[-1]`.
2. Add small helper functions for Garmin nested access and numeric rounding.
3. Cache repeated Garmin calls for hill score, endurance score, activity details, HR zones, and splits.
4. Add schema/order validation before appending rows to Google Sheets.
5. Add unit tests around daily/activity preparation with fake Garmin clients.
6. Consider replacing one-second row expansion with weighted aggregation to avoid memory pressure.
7. Decide whether to preserve or migrate the existing `Anerobic training effect` column typo.

## Working Style

Follow repository-level `AGENTS.md` and `SKILL.md`.
For analysis/code changes, prefer clear, explicit data preparation and sanity checks.
Keep source data and user outputs intact unless the user explicitly asks to change them.
