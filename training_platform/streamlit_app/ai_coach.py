import json
import os
import re

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
# It adds selected data context only when the app explicitly provides it.
# Returns the full instruction text sent to the model.

def build_coach_instructions(owner_mode: bool, data_context: str = "") -> str:
    prompt_parts = [
        read_prompt_file("coach_system.md"),
        read_prompt_file("coach_response_style.md"),
        read_prompt_file("safety_boundaries.md"),
    ]

    # This keeps the coach honest: it can use only the context text the app sends.
    context_boundary = "You do not have direct live access to Google Sheets or Google Docs. Use only the selected Data Context included in this prompt, and say clearly when the provided context is insufficient."
    mode_boundary = "The app is in full owner mode." if owner_mode else "The app is in demo mode. Keep answers generic and do not reference private user data."
    language_boundary = "Answer in the same language as the user's latest message unless the user explicitly asks for another language."
    data_context_block = data_context.strip()

    return "\n\n".join([part for part in prompt_parts + [context_boundary, mode_boundary, language_boundary, data_context_block] if part])


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


# Estimates token count from text length when provider token usage is not available yet.
# This is intentionally approximate and used only for pre-call context visibility.
# Returns an integer token estimate.

def estimate_text_tokens(text: str) -> int:
    if not text:
        return 0

    return max(1, round(len(text) / 4))


# Estimates input-only cost for a text block using the configured model price table.
# Actual response cost is still shown from the OpenAI API usage after the call.
# Returns a small dictionary for UI captions.

def estimate_input_text_cost(model: str, text: str) -> dict:
    estimated_tokens = estimate_text_tokens(text)
    prices = MODEL_PRICES_PER_1M_TOKENS.get(model)
    estimated_cost = None

    if prices:
        estimated_cost = estimated_tokens / 1_000_000 * prices["input"]

    return {"model": model, "estimated_tokens": estimated_tokens, "estimated_input_cost_usd": estimated_cost}


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


# Formats the approximate data-context size and input-cost estimate.
# This covers only the selected context text, not prompts, chat history, or output tokens.
# Returns a short caption string or None.

def format_context_estimate_caption(context_estimate: dict | None) -> str | None:
    if not context_estimate or not context_estimate.get("estimated_tokens"):
        return None

    cost = context_estimate.get("estimated_input_cost_usd")
    token_text = f"~{context_estimate['estimated_tokens']} data-context input tokens"

    if cost is None:
        return f"Data context estimate: {token_text}. Cost not configured for `{context_estimate['model']}`."

    return f"Selected data context only: {token_text}, about ${cost:.6f} input cost before prompts/history/output."


# ----------------------------------------------------------
# Coach Response
# ----------------------------------------------------------

# Calls the OpenAI API with the current chat history and selected data context.
# The model receives coach prompts, optional data context, and recent user/assistant messages.
# Returns the assistant response text and usage metadata for cost display.

def get_agent_response(owner_mode: bool, data_context: str = "") -> dict:
    api_key = get_openai_api_key()

    if not api_key:
        return {"content": "The chat UI is ready, but `OPENAI_API_KEY` is not configured yet. Add the key locally and restart the app to enable real coach replies.", "usage": None}

    client = OpenAI(api_key=api_key)
    model = get_openai_model()
    chat_messages = [{"role": message["role"], "content": message["content"]} for message in st.session_state.agent_messages]

    try:
        response = client.responses.create(
            model=model,
            instructions=build_coach_instructions(owner_mode, data_context),
            input=chat_messages,
        )
    except Exception as error:
        return {"content": f"The coach API call failed: {error}", "usage": None}

    return {"content": response.output_text, "usage": estimate_response_cost(model, response.usage)}


# ----------------------------------------------------------
# English Memory Normalization
# ----------------------------------------------------------

# Parses a memory-normalization response into English question and answer fields.
# The prompt asks for JSON, but this stays safe if parsing fails.
# Returns a dictionary with English text and optional error.

def parse_memory_normalization_response(response_text: str, fallback_question: str, fallback_answer: str) -> dict:
    clean_response_text = response_text.strip()
    json_match = re.search(r"\{.*\}", clean_response_text, flags=re.DOTALL)

    if json_match:
        clean_response_text = json_match.group(0)

    try:
        parsed_response = json.loads(clean_response_text)
    except Exception:
        return {"question_en": fallback_question, "answer_en": fallback_answer, "error": "Memory normalization response was not valid JSON."}

    return {
        "question_en": parsed_response.get("question_en") or fallback_question,
        "answer_en": parsed_response.get("answer_en") or fallback_answer,
        "error": None,
    }


# Translates one coach exchange into English for saved memory only.
# The user-facing answer remains in the user's language; Sheets and Docs stay English.
# Returns English question/answer text and usage metadata.

def normalize_exchange_for_memory(question: str, answer: str) -> dict:
    api_key = get_openai_api_key()

    if not api_key:
        return {"ok": False, "question_en": question, "answer_en": answer, "usage": None, "error": "`OPENAI_API_KEY` is not configured."}

    client = OpenAI(api_key=api_key)
    model = get_openai_model()
    input_text = f"Question:\n{question}\n\nAnswer:\n{answer}"

    try:
        response = client.responses.create(
            model=model,
            instructions='Translate this coach exchange to English for storage. Preserve meaning, training terminology, numbers, units, and nuance. Return only valid JSON with keys "question_en" and "answer_en".',
            input=input_text,
        )
    except Exception as error:
        return {"ok": False, "question_en": question, "answer_en": answer, "usage": None, "error": str(error)}

    parsed_response = parse_memory_normalization_response(response.output_text, question, answer)

    return {
        "ok": parsed_response["error"] is None,
        "question_en": parsed_response["question_en"],
        "answer_en": parsed_response["answer_en"],
        "usage": estimate_response_cost(model, response.usage),
        "error": parsed_response["error"],
    }


# ----------------------------------------------------------
# Structured Note Generation
# ----------------------------------------------------------

# Parses the note-generation response into topic and note text.
# The prompt asks for TOPIC and NOTE labels, but this stays forgiving if the model drifts.
# Returns a dictionary with topic and note.

def parse_structured_note_response(response_text: str, fallback_topic: str | None) -> dict:
    topic = fallback_topic if fallback_topic and fallback_topic not in ["Coach suggests topic", "New topic"] else "General Coaching"
    note = response_text.strip()

    if response_text.startswith("TOPIC:") and "\n\nNOTE:" in response_text:
        topic_part, note_part = response_text.split("\n\nNOTE:", 1)
        topic = topic_part.replace("TOPIC:", "", 1).strip() or topic
        note = note_part.strip()

    return {"topic": topic, "note": note}


# Creates a general, reusable structured note from one coach Q&A exchange.
# This uses no selected training-data context by default so notes stay broadly useful.
# Returns topic, note text, and usage metadata.

def generate_structured_note(question: str, answer: str, selected_topic: str | None) -> dict:
    api_key = get_openai_api_key()

    if not api_key:
        return {"ok": False, "topic": selected_topic, "note": "", "usage": None, "error": "`OPENAI_API_KEY` is not configured."}

    client = OpenAI(api_key=api_key)
    model = get_openai_model()
    topic_instruction = "Choose the best topic yourself." if selected_topic in ["Coach suggests topic", "New topic"] else f"Use this topic unless it is clearly wrong: {selected_topic}."
    input_text = f"{topic_instruction}\n\nUser question:\n{question}\n\nCoach answer:\n{answer}"

    try:
        response = client.responses.create(
            model=model,
            instructions=read_prompt_file("structured_note_generation.md"),
            input=input_text,
        )
    except Exception as error:
        return {"ok": False, "topic": selected_topic, "note": "", "usage": None, "error": str(error)}

    parsed_note = parse_structured_note_response(response.output_text, selected_topic)

    return {"ok": True, "topic": parsed_note["topic"], "note": parsed_note["note"], "usage": estimate_response_cost(model, response.usage), "error": None}
