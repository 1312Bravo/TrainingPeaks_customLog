# AGENTS.md

## Purpose
This repository stores custom TrainingPeaks/Garmin training jobs and the new `training_platform` app.

Use this file for project-specific guidance only. Reusable workflows should live as installed Codex skills, not as copied root-level `SKILL.md` files.

## Working Principles
- Inspect the existing files before editing.
- Keep changes small, readable, and reversible.
- Preserve user work and validated outputs.
- Avoid renames, moves, or deletes unless they are clearly needed.
- Keep the repository organized around existing jobs and the `training_platform` app.

## Project Areas
- `daily_jobs/` runs local job wrappers.
- `daily_statistics_job/` contains daily/activity statistics logic.
- `hasr_tl_job/` contains HASR/TL job logic.
- `training_platform/` contains the Streamlit app and its own scoped instructions.

## Reusable Skills
- Use `data-science-project-workflow` for notebooks, dataframes, modeling, plotting, and analysis style.
- Use `vibecode-app-builder` for collaborative app-building work in `training_platform/`.
- Use `utmb-research` only for UTMB-specific projects and data.
- These reusable skills are installed globally under `C:\Users\Urh\.codex\skills\`.
- Their source copies live in `C:\Users\Urh\Desktop\Urh\Github Repositories\Codex-Instructions\skills\`.
- Do not recreate large copied `SKILL.md` files in this project; update the source skill in `Codex-Instructions` and reinstall it when reusable guidance changes.

## Verification
- Check changed files after edits.
- Run focused commands/tests when available and relevant.
- Summarize what changed and note any follow-up.
