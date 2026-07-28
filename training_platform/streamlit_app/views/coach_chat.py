import threading
import uuid

import streamlit as st

from ai_coach import estimate_input_text_cost, format_context_estimate_caption, format_cost_caption, generate_structured_note, get_agent_response, normalize_exchange_for_memory
from coach_context.context_builder import build_data_context, describe_data_context
from coach_context.context_options import DATA_CONTEXT_SOURCES, DATA_CONTEXT_WINDOWS, DEFAULT_DATA_CONTEXT_SOURCES, DEFAULT_DATA_CONTEXT_WINDOW, DataContextSettings
from config import MEMORY_ACTIONS, STRUCTURED_NOTE_TOPIC_OPTIONS, get_memory_environment_label, get_openai_model
from memory.coach_google_memory import append_chat_archive, append_structured_note


# ----------------------------------------------------------
# Coach Chat View
# ----------------------------------------------------------

MEMORY_JOBS = {}
MEMORY_JOBS_LOCK = threading.Lock()


# ----------------------------------------------------------
# Background Memory Jobs
# ----------------------------------------------------------

# Stores the latest status for one background memory job.
# The chat renderer reads this registry on later Streamlit reruns.
# Returns nothing; it updates the in-memory registry.

def set_memory_job_status(job_id: str, status: str, results: list[dict] | None = None, structured_note_usage: dict | None = None, memory_language_usage: dict | None = None) -> None:
    with MEMORY_JOBS_LOCK:
        MEMORY_JOBS[job_id] = {
            "status": status,
            "results": results or [],
            "structured_note_usage": structured_note_usage,
            "memory_language_usage": memory_language_usage,
        }


# Reads the current status for one background memory job.
# Missing jobs can happen after a server reload, so the UI handles that gently.
# Returns a status dictionary or None.

def get_memory_job_status(job_id: str | None) -> dict | None:
    if not job_id:
        return None

    with MEMORY_JOBS_LOCK:
        return MEMORY_JOBS.get(job_id)


# Runs English normalization, Chat Archive writing, and optional Structured Notes writing after the coach answer is visible.
# This function must not call Streamlit UI APIs because it runs in a background thread.
# Returns nothing; it writes status into MEMORY_JOBS.

def run_memory_job(job_id: str, question: str, answer: str, owner_mode: bool, user_email: str | None, memory_action: str, structured_topic: str | None, answer_usage: dict | None) -> None:
    results = []
    structured_note_usage = None
    memory_language_usage = None
    memory_question = question
    memory_answer = answer

    try:
        english_memory = normalize_exchange_for_memory(question, answer)
        memory_question = english_memory["question_en"]
        memory_answer = english_memory["answer_en"]
        memory_language_usage = english_memory.get("usage")

        if not english_memory["ok"]:
            results.append({"ok": False, "status": "warning", "message": f"English memory normalization had a problem, so the original text was used: {english_memory['error']}"})

        results.append(append_chat_archive(memory_question, memory_answer, owner_mode, user_email, memory_action, structured_topic, answer_usage))

        if memory_action == "Chat Archive & Create Notes":
            structured_note = generate_structured_note(memory_question, memory_answer, structured_topic)
            structured_note_usage = structured_note.get("usage")

            if structured_note["ok"]:
                results.append(append_structured_note(structured_note["topic"], structured_note["note"], owner_mode))
            else:
                results.append({"ok": False, "status": "failed", "message": f"Structured note generation failed: {structured_note['error']}"})

        set_memory_job_status(job_id, "done", results, structured_note_usage, memory_language_usage)
    except Exception as error:
        results.append({"ok": False, "status": "failed", "message": f"Background memory save failed: {error}"})
        set_memory_job_status(job_id, "failed", results, structured_note_usage, memory_language_usage)


# Starts a background memory job for one answered coach message.
# The app can show the answer immediately while memory work continues.
# Returns the job id used by the renderer.

def start_memory_job(question: str, answer: str, owner_mode: bool, user_email: str | None, memory_action: str, structured_topic: str | None, answer_usage: dict | None) -> str:
    job_id = str(uuid.uuid4())
    set_memory_job_status(job_id, "running")
    worker = threading.Thread(target=run_memory_job, args=(job_id, question, answer, owner_mode, user_email, memory_action, structured_topic, answer_usage), daemon=True)
    worker.start()

    return job_id


# Renders one chat message with optional API usage/cost metadata.
# User messages do not have cost metadata; assistant API replies may have it.
# Returns nothing; it writes one message bubble to the page.

def render_chat_message(message: dict) -> None:
    with st.chat_message(message["role"]):
        st.write(message["content"])

        cost_caption = format_cost_caption(message.get("usage"))
        if cost_caption:
            st.caption(cost_caption)

        for memory_result in message.get("memory_results", []):
            st.caption(memory_result["message"])

        memory_job = get_memory_job_status(message.get("memory_job_id"))
        if memory_job:
            if memory_job["status"] == "running":
                st.caption("Memory save is running in the background.")
            else:
                for memory_result in memory_job["results"]:
                    st.caption(memory_result["message"])

        data_context_label = message.get("data_context_label")
        if data_context_label:
            st.caption(f"Data context used: {data_context_label}")

        context_estimate_caption = format_context_estimate_caption(message.get("data_context_estimate"))
        if context_estimate_caption:
            st.caption(context_estimate_caption)

        structured_note_usage = message.get("structured_note_usage") or (memory_job or {}).get("structured_note_usage")
        note_usage_caption = format_cost_caption(structured_note_usage)
        if note_usage_caption:
            st.caption(f"Structured note generation: {note_usage_caption}")

        memory_language_usage = message.get("memory_language_usage") or (memory_job or {}).get("memory_language_usage")
        memory_language_caption = format_cost_caption(memory_language_usage)
        if memory_language_caption:
            st.caption(f"English memory normalization: {memory_language_caption}")


# Reads simple warnings from the generated data context.
# This keeps source-load problems visible in the chat without sending raw tracebacks.
# Returns a list of warning strings.

def get_data_context_warnings(data_context: str) -> list[str]:
    warnings = []

    if "could not be read" in data_context:
        warnings.append("Some selected data context could not be read. Check Google credentials and sharing.")

    if "No rows available for this selection." in data_context:
        warnings.append("Some selected data sources had no rows for this window.")

    return warnings


# Checks whether the coach response is a real answer or an app/API failure message.
# Structured Notes should not be generated from error text.
# Returns True when the response looks usable.

def is_successful_coach_response(response: dict) -> bool:
    content = response.get("content", "")

    if content.startswith("The coach API call failed:"):
        return False

    if "`OPENAI_API_KEY` is not configured" in content:
        return False

    return True


# Reads the current Streamlit user email when auth is available.
# Missing auth or demo sessions return None.
# Returns an email string or None.

def get_current_user_email() -> str | None:
    try:
        return st.user.get("email")
    except Exception:
        return None


# Sends one user prompt to the coach and stores the assistant response.
# This keeps the input form and chat history rendering separate.
# Returns nothing; it updates Streamlit session state.

def send_agent_message(prompt: str, owner_mode: bool, memory_action: str, structured_topic: str | None, data_context_settings: DataContextSettings) -> None:
    clean_prompt = prompt.strip()

    if not clean_prompt:
        return

    data_context_label = describe_data_context(data_context_settings)
    st.session_state.agent_messages.append({"role": "user", "content": clean_prompt, "memory_action": memory_action, "structured_topic": structured_topic, "data_context_label": data_context_label})

    with st.spinner("Coach is reading context and thinking..."):
        data_context = build_data_context(owner_mode, data_context_settings)
        data_context_estimate = estimate_input_text_cost(get_openai_model(), data_context)
        response = get_agent_response(owner_mode, data_context)

    memory_results = []
    memory_job_id = None

    for warning in get_data_context_warnings(data_context):
        memory_results.append({"ok": False, "status": "warning", "message": warning})

    if memory_action in ["Chat Archive", "Chat Archive & Create Notes"]:
        if is_successful_coach_response(response):
            memory_job_id = start_memory_job(clean_prompt, response["content"], owner_mode, get_current_user_email(), memory_action, structured_topic, response["usage"])
        else:
            memory_results.append({"ok": False, "status": "skipped", "message": "Memory save was skipped because the coach answer failed."})

    if memory_action == "Chat Archive & Create Notes" and not is_successful_coach_response(response):
        memory_results.append({"ok": False, "status": "skipped", "message": "Structured note was skipped because the coach answer failed."})

    st.session_state.agent_messages.append(
        {
            "role": "assistant",
            "content": response["content"],
            "usage": response["usage"],
            "memory_results": memory_results,
            "memory_job_id": memory_job_id,
            "data_context_label": data_context_label,
            "data_context_estimate": data_context_estimate,
        }
    )


# Renders the data-context controls for the current coach question.
# These controls decide what source data is sent to the coach before it answers.
# Returns the selected data context settings.

def render_data_context_controls(owner_mode: bool) -> DataContextSettings:
    st.markdown("**Before answer**")
    st.caption("Choose what data the coach can read for this question.")
    window = st.selectbox("Data window", DATA_CONTEXT_WINDOWS, index=DATA_CONTEXT_WINDOWS.index(DEFAULT_DATA_CONTEXT_WINDOW))

    if window == "No data":
        st.caption("No sheet or notes data will be sent to the coach.")
        return DataContextSettings(window=window, sources=[])

    source_options = DATA_CONTEXT_SOURCES if owner_mode else [source for source in DATA_CONTEXT_SOURCES if source != "Structured Notes"]
    default_sources = [source for source in DEFAULT_DATA_CONTEXT_SOURCES if source in source_options]
    sources = st.multiselect("Data sources", source_options, default=default_sources)

    if window == "All available (expensive)":
        st.caption("This can send a lot of rows and may cost more.")

    data_context_settings = DataContextSettings(window=window, sources=sources)
    data_context = build_data_context(owner_mode, data_context_settings)
    context_estimate = estimate_input_text_cost(get_openai_model(), data_context)
    context_estimate_caption = format_context_estimate_caption(context_estimate)

    if context_estimate_caption:
        st.caption(context_estimate_caption)

    return data_context_settings


# Renders the memory/action controls for the current coach question.
# These controls decide what should happen with this specific question-answer pair later.
# Returns the selected memory action and structured-note destination.

def render_memory_controls(owner_mode: bool) -> tuple[str, str | None]:
    st.markdown("**After answer**")
    st.caption("Choose what should be saved after the coach replies.")

    if not owner_mode:
        st.caption("Memory writing is full-mode only. Demo chats are not saved.")
        return "No memory", None

    memory_label = get_memory_environment_label()
    st.caption(f"Memory target: {memory_label}")

    memory_action = st.selectbox("What should happen with this answer?", MEMORY_ACTIONS, index=0)

    if memory_action != "Chat Archive & Create Notes":
        return memory_action, None

    structured_topic = st.selectbox("Structured Notes destination", STRUCTURED_NOTE_TOPIC_OPTIONS, index=0)

    return memory_action, structured_topic


# Creates the in-browser chat history for the current Streamlit session.
# Later we can decide what should be saved permanently.
# Returns nothing; it updates Streamlit session state.

def initialize_chat() -> None:
    # Add the first assistant message only once per session.
    if "agent_messages" not in st.session_state:
        st.session_state.agent_messages = [
            {
                "role": "assistant",
                "content": "Hi. I am your trail running coach. Ask me about training ideas, recovery, planning, or how to think about the data we are wiring into this app.",
            }
        ]


# Renders the chat area that will later connect to the real AI agent.
# It shows different captions for full mode and demo mode.
# Returns nothing; it writes UI elements to the Streamlit page.

def render_agent_chat(owner_mode: bool) -> None:
    st.subheader("Coach chat")

    # Explain whether this chat can use private context.
    if owner_mode:
        st.caption("Full mode chat. Later this can use your private training context.")
    else:
        st.caption("Demo mode chat. Private training context is not available.")

    initialize_chat()
    answers_column, input_column = st.columns([3, 2], gap="large")

    with input_column:
        with st.container():
            st.markdown("**Ask the coach**")

            with st.container(border=True):
                data_context_settings = render_data_context_controls(owner_mode)

            with st.container(border=True):
                memory_action, structured_topic = render_memory_controls(owner_mode)

            with st.form("coach_message_form", clear_on_submit=True):
                prompt = st.text_area("Message", placeholder="Ask about training, recovery, planning, or your data...", height=170, label_visibility="collapsed")
                submitted = st.form_submit_button("Send", use_container_width=True)

            if submitted:
                send_agent_message(prompt, owner_mode, memory_action, structured_topic, data_context_settings)

    with answers_column:
        with st.container(border=True):
            for message in st.session_state.agent_messages:
                render_chat_message(message)
