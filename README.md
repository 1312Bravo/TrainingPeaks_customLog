# Custom daily statistics and overall activity numbers

Codex setup notes are in `docs/codex_setup.md`.
Environment-file notes are in `docs/environment.md`.

Run all daily jobs:

```powershell
python daily_jobs/run_all.py
```

Run only the raw daily/activity statistics job:

```powershell
python daily_jobs/run_daily_statistics_job.py
```

Two files missing on git because of secrets and passwords:
- .env
- googleDrive_secrets.json

They are done in Secrets :)
