import os
from pathlib import Path

from dotenv import load_dotenv


# ----------------------------------------------------------
# App Configuration
# ----------------------------------------------------------

# This is the owner account for the full/private version of the app.
# Demo mode remains the safe default when auth is missing or another account is logged in.
OWNER_EMAIL = "pecek.urh@gmail.com"
APP_ROOT = Path(__file__).resolve().parents[1]
PROMPTS_ROOT = APP_ROOT / "prompts"
DEFAULT_OPENAI_MODEL = "gpt-5-mini"
DATA_TABLE_HEIGHT = 320
DEFAULT_MEMORY_ENV = "dev"
MEMORY_ENV_LABELS = {
    "dev": "Dev",
    "prod": "Prod",
}
MEMORY_ACTIONS = [
    "No memory",
    "Chat Archive",
    "Chat Archive & Create Notes",
]
STRUCTURED_NOTE_TOPIC_OPTIONS = [
    "Coach suggests topic",
    "Recovery",
    "Heart rate",
    "Training load",
    "Workout planning",
    "Trail running skills",
    "Race preparation",
    "Nutrition",
    "Gear",
    "New topic",
]

# Prices are per one million tokens and are used only for in-app estimates.
# The OpenAI billing dashboard remains the source of truth for actual charges.
MODEL_PRICES_PER_1M_TOKENS = {
    "gpt-5-mini": {
        "input": 0.25,
        "output": 2.00,
    }
}

# Load local secrets from training_platform/.env before reading environment variables.
# The .env file is ignored by git and should never be committed.
load_dotenv(APP_ROOT / ".env")


# ----------------------------------------------------------
# Runtime Helpers
# ----------------------------------------------------------

# Reads the configured OpenAI model and falls back when the environment value is blank.
# This prevents an empty OPENAI_MODEL value from causing a model-not-found API error.
# Returns the model name used for coach calls.

def get_openai_model() -> str:
    return os.getenv("OPENAI_MODEL") or DEFAULT_OPENAI_MODEL


# Reads which coach memory target should be used.
# Local work should normally use dev; deployment can switch this to prod.
# Returns a safe known value even when the environment variable is missing or mistyped.

def get_memory_environment() -> str:
    memory_env = (os.getenv("TRAINING_PLATFORM_MEMORY_ENV") or DEFAULT_MEMORY_ENV).strip().lower()

    if memory_env in MEMORY_ENV_LABELS:
        return memory_env

    return DEFAULT_MEMORY_ENV


# Converts the active memory environment into a short UI label.
# This helps us see whether the app is writing toward dev or prod before real writes are enabled.
# Returns a display label for the active target.

def get_memory_environment_label() -> str:
    return MEMORY_ENV_LABELS[get_memory_environment()]


# Reads the Google Sheet URL used for the raw coach chat archive.
# This stays in local/deployment secrets instead of source code because it can point to private memory.
# Returns the configured URL or None.

def get_chat_archive_sheet_url() -> str | None:
    memory_env = get_memory_environment().upper()
    env_specific_url = os.getenv(f"TRAINING_PLATFORM_CHAT_ARCHIVE_SHEET_URL_{memory_env}")

    return env_specific_url or os.getenv(f"TRAINING_PLATFORM_MEMORY_SHEET_URL_{memory_env}") or os.getenv("TRAINING_PLATFORM_MEMORY_SHEET_URL") or None


# Reads the Google Docs URL used for readable structured coach notes.
# Structured notes belong in Docs because they should feel like topic-based notes, not rows.
# Returns the configured URL or None.

def get_structured_notes_doc_url() -> str | None:
    memory_env = get_memory_environment().upper()

    return os.getenv(f"TRAINING_PLATFORM_STRUCTURED_NOTES_DOC_URL_{memory_env}") or None
