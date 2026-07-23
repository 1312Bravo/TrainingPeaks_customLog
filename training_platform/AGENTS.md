# AGENTS.md

## Scope
These instructions apply to work inside `training_platform/`.

## Product Notes
- When the user gives app functionality, product behavior, data model, workflow, AI-coach behavior, storage, privacy, or portability instructions, record the durable requirement in `notes/`.
- Do not record purely visual styling requests in product notes, such as rounding, spacing, colors, or small layout polish, unless the visual detail changes product behavior or expresses a durable design principle.
- Keep notes implementation-neutral enough that the app could later be rebuilt in another stack, such as Streamlit, without losing the intended behavior.
- Prefer short, dated entries with the source idea and the practical implication.
- If a note belongs to an existing note file, update that file instead of creating a new one.

## App Direction
- The current app direction is Streamlit.
- Keep the platform connected to the existing training data/jobs in the parent repository while keeping app-specific code under `training_platform/`.
- Keep practical app usage instructions in `docs/APP_INSTRUCTIONS.md`.
