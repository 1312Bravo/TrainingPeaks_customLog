from dataclasses import dataclass


# ----------------------------------------------------------
# Data Context Options
# ----------------------------------------------------------

DATA_CONTEXT_WINDOWS = [
    "No data",
    "Yesterday",
    "Last 7 days",
    "Last 14 days",
    "Last month",
    "Last 3 months",
    "Last year",
    "All available (expensive)",
]

DATA_CONTEXT_SOURCES = [
    "Daily data",
    "Activity statistics",
    "HASR-TL",
    "Structured Notes",
]

DEFAULT_DATA_CONTEXT_WINDOW = "No data"
DEFAULT_DATA_CONTEXT_SOURCES = [
    "Daily data",
    "Activity statistics",
    "HASR-TL",
]


# ----------------------------------------------------------
# Data Context Settings
# ----------------------------------------------------------

# Stores the data-context choices for one coach question.
# The chat view creates this object, and the context builder turns it into prompt text.
# Returns a simple typed object that stays easy to pass through the app.

@dataclass
class DataContextSettings:
    window: str
    sources: list[str]
