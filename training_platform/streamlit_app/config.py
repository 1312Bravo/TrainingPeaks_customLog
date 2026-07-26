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
