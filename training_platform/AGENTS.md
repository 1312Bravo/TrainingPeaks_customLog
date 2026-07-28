# AGENTS.md

## Scope
These instructions apply to work inside `training_platform/`.

## Reusable Skill
- Follow the reusable `vibecode-app-builder` skill for collaborative app-building workflow.

## Product Notes
- When the user gives app functionality, product behavior, data model, workflow, AI-coach behavior, storage, privacy, or portability instructions, record the durable requirement in `notes/`.
- Do not record purely visual styling requests in product notes, such as rounding, spacing, colors, or small layout polish, unless the visual detail changes product behavior or expresses a durable design principle.
- Keep notes implementation-neutral enough that the app could later be rebuilt in another stack, such as Streamlit, without losing the intended behavior.
- Prefer short, dated entries with the source idea and the practical implication.
- If a note belongs to an existing note file, update that file instead of creating a new one.

## Collaboration Rules
- If it is unclear whether a feature belongs in demo mode, full mode, or both, ask the user before implementing that access decision.
- Ask the user before making bigger product, architecture, data-storage, authentication, deployment, or AI-behavior decisions.
- Small implementation details may be chosen conservatively without asking when they follow existing project direction.

## Code Maintenance
- Keep code simple and readable because the user may want to edit it directly later.
- Prefer clear names, straightforward functions, and small files over clever abstractions.
- Prefer single-line text, strings, comments, and list items when they fit comfortably and express one idea.
- Use multiline formatting when a line is genuinely too long, the structure is clearer when separated, separate lines express separate meanings, or the syntax naturally benefits from multiple lines.
- For Python code, organize larger files with clear section dividers:

```python
# ----------------------------------------------------------
# Section Title / Short Summary
# ----------------------------------------------------------
```

- For Python functions, prefer short comments above the function explaining what it does, the main steps, and what it returns.
- Keep one blank line between a Python function description comment block and the `def` line.
- Do not use docstring blocks for these casual function explanations unless a real public API docstring is useful.
- Inside Python functions, add short comments for important logic blocks, such as `# Calculate aggregates across years`.
- From time to time, review the app code and clean up small messes before they accumulate.
- Avoid leaving temporary prototypes, unused code, or duplicated logic in place once the direction is clear.
- Add helpful comments that explain what important parts do and why they exist,
  especially around app structure, access control, data loading, storage, and
  AI behavior.
- Keep comments balanced: enough for the user to understand and edit the code,
  but not so many that obvious lines are explained one by one.

## App Direction
- The current app direction is Streamlit.
- Keep the platform connected to the existing training data/jobs in the parent repository while keeping app-specific code under `training_platform/`.
- Keep practical app usage instructions in `docs/APP_INSTRUCTIONS.md`.
- Keep AI coach prompts in `prompts/` as editable Markdown files, separate from app code.
