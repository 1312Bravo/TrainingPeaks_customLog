# Research Notebook Style Prompt

## Purpose

Use this file as the working style guide for the Bayesian performance research notebook.

The notebook is exploratory. It may contain templates, ideas, rough analyses, visual checks, and model experiments. It does not need to be a strict production pipeline.

The main notebook is `research/research.ipynb`. Exploratory helpers may be placed as Python scripts in the same `research/` directory. Do not create a helper script only to hide a tiny one-off calculation, but do not hesitate to create one when a function becomes substantial, reusable, easier to test separately, or clearer outside the notebook.

The notebook will be written collaboratively. Only add substantial ideas, decisions, analyses, or code patterns when the user asks for them or we have agreed on them. Do not silently turn exploratory suggestions into project decisions.

Do not update `PLAN.md` automatically while working in the notebook. Update the plan only when the user explicitly asks and the content has been agreed.

## Project Style

Match the existing style of notebooks and scripts in this repository, especially `hasr_tl_job/analysis.ipynb`:

- Use clear section comments such as `# Libraries`, `# Get Data`, `# Prepare data`, `# Research`, `# Analysis`, `# Plots`, `# Fit`, `# Predict`, and `# Model diagnostic`.
- Use short markdown headings such as `### Libraries & Data`, `### HASR-TL`, `### Bucket level diagnostics`, and `### Model diagnostic`.
- Keep the notebook readable as a sequence of small, direct cells.
- Prefer explicit code over dense one-liners or premature abstraction.
- Use plain, direct printouts for summaries and takeaways.
- Keep exploratory code understandable enough to rerun and modify.
- Preserve the existing repository vocabulary and naming where it is still appropriate.

## Notebook Workflow

Organize the notebook as a research story rather than a production pipeline:

1. Libraries and helpers.
2. Get data.
3. About the data.
4. Prepare data.
5. Explore and visualize.
6. Try a simple baseline.
7. Explore a Bayesian formulation.
8. Inspect uncertainty and diagnostics.
9. Record observations, questions, and possible next steps.

The notebook does not need to complete every phase in every session. Add a short markdown note when a section is intentionally incomplete or only a placeholder.

Keep cells ordered so the notebook can usually run from top to bottom. Avoid hidden state, unexplained variables created many cells earlier, stale outputs, and duplicated transformations.

Exploratory scratch work is allowed, but label it clearly and keep it separate from the main analysis path when it becomes confusing.

## Imports and Setup

Use the existing project pattern for imports and setup:

```python
# Libraries and helpers

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import datetime
import matplotlib.dates as mdates

# Helpers
from daily_jobs import config
from daily_jobs import help_functions as hf

# Configs
pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.max_colwidth", None)
```

Add imports only when they are used. Keep imports grouped and easy to scan. Use the repository's configured environment and existing helpers where possible.

Use the existing Google Sheets authentication and import helpers rather than creating a second data-access pattern. Never print or expose credentials.

## Data Handling

- Treat raw Google Sheets as read-only.
- Use `hf.import_google_sheet(...)` and existing conversion helpers when they fit the analysis.
- Preserve raw dataframes and create explicit working copies.
- Use names such as `training_data`, `daily_data`, `hasr_tl_data`, `data_model`, `plot_data`, and `plot_data_recent` when they describe the role clearly.
- Use `.copy()` deliberately when creating a working dataframe.
- Use `.reset_index(drop=True)` when normalizing indexes after filtering or slicing.
- Use `.query(...)` when it makes a filter clearer.
- Use `.assign(...)` for readable derived-column steps.
- Use `.merge(...)` with explicit `on`, `how`, and `validate` arguments when appropriate.
- Check dates, duplicate keys, data types, missing values, and suspicious zeros before modeling.
- Document filtering, joins, dropped rows, missing-value handling, and derived features in nearby markdown or comments.
- Do not silently replace missing values with zero.
- Keep activity-level and daily-level data conceptually separate until an intentional aggregation step is defined.

## Naming and Dataframe Style

- Use descriptive names that show the role of an object.
- Use `df_...` for analysis tables when that improves clarity.
- Use `_df` for tables intended for inspection or plotting.
- Use `_agg` for grouped or aggregated results.
- Use names such as `data_model`, `features`, `target`, `fit_model`, `predictions`, `residuals`, and `posterior_summary` when those roles apply.
- Keep feature selection explicit instead of hiding it inside a long expression.
- Group related columns together and add a short comment above each group.
- Use intermediate objects when they make the analysis easier to read.
- Do not create helper functions just to shorten a small exploratory calculation.
- When reusable logic becomes substantial, move it into a project module only after we agree that it belongs there.

## Comments and Markdown

- Put a short comment directly above a meaningful code block.
- Use comments to explain intent, assumptions, or important logic, not obvious syntax.
- For functions, place a short helper comment above the function explaining what it does, the main steps, and what it returns.
- Avoid large docstring blocks for casual research helpers unless a real public API docstring is useful.
- Use markdown to explain the research question before the code that investigates it.
- Separate hypotheses, observations, interpretations, and decisions.
- Clearly label ideas as exploratory when they are not yet agreed.
- Keep conclusions proportional to the evidence.
- Explicitly distinguish association from causation.

Useful research markdown patterns include:

```markdown
### Question

Does recent training load appear to be associated with the next few days of running performance?

### Working hypothesis

Higher recent load may be associated with higher short-term fatigue, but the relationship is expected to be noisy.

### Observation

...

### Interpretation

...
```

## Plotting Style

Follow the existing plotting style:

- Prefer `fig, ax = plt.subplots(...)`.
- Use simple layouts such as `1,2`, `2,2`, or `3,2`.
- Use compact subplot indexing such as `ax[0,0]`.
- Set titles, x-axis labels, y-axis labels, units, and legends explicitly.
- Use `sns.scatterplot`, `sns.lineplot`, `sns.histplot`, `sns.kdeplot`, and `sns.regplot` where appropriate.
- Use simple, consistent colors.
- Use light grids such as `ax.grid(alpha=0.5)` when they improve readability.
- Use reference lines for baselines, thresholds, and neutral comparisons.
- Use date locators and formatters for time-series plots.
- Keep annotations limited to information that supports the interpretation.
- Finish plot blocks with `plt.tight_layout()` followed by `plt.show()`.

Example style:

```python
fig, ax = plt.subplots(1, 2, figsize=(25, 4))

ax[0].plot(plot_data["Datetime"], plot_data["metric"], color="blue", label="Metric")
ax[0].set_title("Metric over time")
ax[0].set_xlabel("Date")
ax[0].set_ylabel("Metric value")
ax[0].legend()
ax[0].grid(alpha=0.5)

plt.tight_layout()
plt.show()
```

## Bayesian Research Style

Keep the Bayesian learning path understandable:

- Start with a simple baseline before introducing a more complex Bayesian model.
- Explain the prior, likelihood, and posterior in plain language before fitting a model.
- State what quantity is uncertain and what the data are expected to tell us about it.
- Keep priors visible and explain why they are reasonable.
- Use posterior summaries, credible intervals, posterior predictive checks, and simple diagnostic plots.
- Prefer a small interpretable model over a large model with unclear meaning.
- Use deterministic random seeds when sampling or simulating.
- Record model assumptions and limitations near the model code.
- Treat convergence and diagnostics as part of the analysis, not as optional decoration.
- Do not interpret a posterior association as a causal training effect.

The initial project should focus on understandable Bayesian updating, regression, and eventually a simple dynamic/state-space formulation. Do not introduce Gaussian processes, particle filters, or causal models unless we deliberately start a separate advanced investigation.

## Research Notebook Boundaries

- This notebook is for learning and exploration, not production automation.
- Do not change the existing daily jobs or HASR-TL job from exploratory notebook work.
- Do not write back to the raw Google Sheets unless explicitly requested.
- Do not hide unstable experiments inside reusable project code.
- Do not present a rough exploratory metric as the final fitness metric without discussion and validation.
- Do not add a new model, feature, or interpretation to the project plan without explicit agreement.
- Keep outputs reviewable: show shapes, schemas, samples, summary statistics, and relevant plots.

## Collaboration Rule

The user will decide what becomes part of the research direction. Codex should help write the notebook, explain the statistical ideas, point out risks, and suggest next steps, but should not silently expand the scope or convert suggestions into agreed decisions.
