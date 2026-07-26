import os

import streamlit as st
from openai import OpenAI

from config import MODEL_PRICES_PER_1M_TOKENS, PROMPTS_ROOT, get_openai_model


# ----------------------------------------------------------
# Prompt Loading
# ----------------------------------------------------------

# Reads a prompt Markdown file from the prompts folder.
# Missing prompt files return an empty string so the app can still load.
# Returns the prompt text as a string.

def read_prompt_file(file_name: str) -> str:
    prompt_path = PROMPTS_ROOT / file_name

    # Keep prompt loading forgiving while the prompt files are still being edited.
    if not prompt_path.exists():
        return ""

    return prompt_path.read_text(encoding="utf-8").strip()


# Builds the coach instructions from editable prompt files.
# It adds a temporary privacy boundary until training context is deliberately connected.
# Returns the full instruction text sent to the model.

def build_coach_instructions(owner_mode: bool) -> str:
    prompt_parts = [
        read_prompt_file("coach_system.md"),
        read_prompt_file("coach_response_style.md"),
        read_prompt_file("safety_boundaries.md"),
    ]

    # This keeps the coach honest until we wire authenticated Google Sheet context.
    context_boundary = "You do not currently have live access to the user's private training data inside this chat. Do not claim that you analyzed private sheets unless explicit context is provided."
    mode_boundary = "The app is in full owner mode." if owner_mode else "The app is in demo mode. Keep answers generic and do not reference private user data."

    return "\n\n".join([part for part in prompt_parts + [context_boundary, mode_boundary] if part])


# ----------------------------------------------------------
# API Keys And Usage
# ----------------------------------------------------------

# Reads the OpenAI API key from Streamlit secrets or environment variables.
# This keeps secrets outside source code.
# Returns the API key string or None.

def get_openai_api_key() -> str | None:
    try:
        return st.secrets.get("OPENAI_API_KEY") or os.getenv("OPENAI_API_KEY")
    except Exception:
        return os.getenv("OPENAI_API_KEY")


# Safely reads a token count from the OpenAI usage object.
# The SDK can expose usage as attributes, while some tests or future code may use dictionaries.
# Returns an integer token count.

def get_usage_value(usage: object, key: str) -> int:
    if not usage:
        return 0

    if isinstance(usage, dict):
        return int(usage.get(key) or 0)

    return int(getattr(usage, key, 0) or 0)


# Estimates the API cost for one model response from token usage.
# Prices are kept in one local table so we can update them when the chosen model changes.
# Returns a small dictionary used by the chat UI.

def estimate_response_cost(model: str, usage: object) -> dict:
    input_tokens = get_usage_value(usage, "input_tokens")
    output_tokens = get_usage_value(usage, "output_tokens")
    total_tokens = get_usage_value(usage, "total_tokens") or input_tokens + output_tokens
    prices = MODEL_PRICES_PER_1M_TOKENS.get(model)

    if not prices:
        return {"model": model, "input_tokens": input_tokens, "output_tokens": output_tokens, "total_tokens": total_tokens, "estimated_cost_usd": None}

    input_cost = input_tokens / 1_000_000 * prices["input"]
    output_cost = output_tokens / 1_000_000 * prices["output"]

    return {"model": model, "input_tokens": input_tokens, "output_tokens": output_tokens, "total_tokens": total_tokens, "estimated_cost_usd": input_cost + output_cost}


# Formats token and cost metadata for display under an assistant message.
# Very tiny costs are still shown with enough decimals to be useful during testing.
# Returns a short caption string or None.

def format_cost_caption(usage_summary: dict | None) -> str | None:
    if not usage_summary:
        return None

    cost = usage_summary.get("estimated_cost_usd")
    token_text = f"{usage_summary['input_tokens']} input tokens, {usage_summary['output_tokens']} output tokens"

    if cost is None:
        return f"Usage estimate: {token_text}. Cost not configured for `{usage_summary['model']}`."

    return f"Estimated API cost: ${cost:.6f} ({token_text}, model `{usage_summary['model']}`)."


# ----------------------------------------------------------
# Coach Response
# ----------------------------------------------------------

# Calls the OpenAI API with the current chat history.
# The model receives coach prompts plus the recent user/assistant messages.
# Returns the assistant response text and usage metadata for cost display.

def get_agent_response(owner_mode: bool) -> dict:
    api_key = get_openai_api_key()

    if not api_key:
        return {"content": "The chat UI is ready, but `OPENAI_API_KEY` is not configured yet. Add the key locally and restart the app to enable real coach replies.", "usage": None}

    client = OpenAI(api_key=api_key)
    model = get_openai_model()
    chat_messages = [{"role": message["role"], "content": message["content"]} for message in st.session_state.agent_messages]

    try:
        response = client.responses.create(
            model=model,
            instructions=build_coach_instructions(owner_mode),
            input=chat_messages,
        )
    except Exception as error:
        return {"content": f"The coach API call failed: {error}", "usage": None}

    return {"content": response.output_text, "usage": estimate_response_cost(model, response.usage)}
