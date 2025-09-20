# Libraries
import numpy as np

# Basic values
BASELINE_WINDOW = 90
RECENT_WINDOW = 21
LAMBDA_BASE = 0.978
HASR_TL_WEIGHTS = [0.15, 0.45, 0.4]
QUANTILE_TL_MINUTE_HARD = 0.70
QUANTILE_DURATION_LONG = 0.80
AGG_VARIABLE = "Training load"
AGG_VARIABLE_NAME_DICT = {
    "Training load": "TL"
    }

baseline_window_days = range(1, BASELINE_WINDOW+1)
baseline_window_weights = np.array([LAMBDA_BASE ** (j-1) for j in baseline_window_days])
BASELINE_WINDOW_NORMALIZED_WEIGHTS = baseline_window_weights / sum(baseline_window_weights)

recent_window_days = range(1, RECENT_WINDOW+1)
recent_window_weights = np.array([LAMBDA_BASE ** (j-1) for j in recent_window_days])
RECENT_WINDOW_NORMALIZED_WEIGHTS = recent_window_weights / sum(recent_window_weights)

# Google sheets
HASR_TL_SHEET_NAME = f"HASR-{AGG_VARIABLE_NAME_DICT[AGG_VARIABLE]}"

# Excel column names
BASELINE_SLA_VALUE_COLUMN_NAMES = [
     "Baseline B1",
     "Baseline B2",
     "Baseline B3"
     ]

BASELINE_SLA_PROPORTION_COLUMN_NAMES = [
     "Baseline B1 prop. [%]",
     "Baseline B2 prop. [%]",
     "Baseline B3 prop. [%]"
     ]

RECENT_SLA_VALUE_COLUMN_NAMES = [
     "Recent B1",
     "Recent B2",
     "Recent B3"
     ]

RECENT_SLA_PROPORTION_COLUMN_NAMES = [
     "Recent B1 prop. [%]",
     "Recent B2 prop. [%]",
     "Recent B3 prop. [%]"
     ]

# Not in use
BASELINE_WITHIN_WINDOW_SLA_COMPARISON_COLUMN_NAMES = [
    "Baseline B2/B1",
    "Baseline B3/B2",
    "Baseline B3/B1",
]

# Not in use
RECENT_WITHIN_WINDOW_SLA_COMPARISON_COLUMN_NAMES = [
    "Recent B2/B1",
    "Recent B3/B2",
    "Recent B3/B1",
]

# Not in use
RECENT_BASELINE_BUCKET_SLA_COMPARISON_COLUMN_NAMES = [
     "Recent/Baseline B1",
     "Recent/Baseline B2",
     "Recent/Baseline B3",
]

HASR_TL_COLUMN_NAMES = [
     f"HASR-{AGG_VARIABLE_NAME_DICT[AGG_VARIABLE]}",
     f"HASR-{AGG_VARIABLE_NAME_DICT[AGG_VARIABLE]} Recent",
     f"HASR-{AGG_VARIABLE_NAME_DICT[AGG_VARIABLE]} Baseline",
]

RECENT_SESSION_CLASS_COLUMN_NAMES = [
     "Session Baseline Rank",
     "Session Baseline Class"
]

REQUIRED_COLUMNS_ORDER = ["Year", "Month", "Day", "Weekday", "Description", "Activity type", "Start time", "Aggregate variable"]
REQUIRED_COLUMNS_ORDER += RECENT_SESSION_CLASS_COLUMN_NAMES
REQUIRED_COLUMNS_ORDER += HASR_TL_COLUMN_NAMES
for i in [0,1,2]:
     REQUIRED_COLUMNS_ORDER += [RECENT_SLA_VALUE_COLUMN_NAMES[i]]
     REQUIRED_COLUMNS_ORDER += [BASELINE_SLA_VALUE_COLUMN_NAMES[i]]
     REQUIRED_COLUMNS_ORDER += [RECENT_SLA_PROPORTION_COLUMN_NAMES[i]]
     REQUIRED_COLUMNS_ORDER += [BASELINE_SLA_PROPORTION_COLUMN_NAMES[i]]

