import streamlit as st

from ai_coach import estimate_input_text_cost, format_context_estimate_caption, format_cost_caption, get_agent_response
from coach_context.context_builder import build_data_context, describe_data_context
from coach_context.context_options import DATA_CONTEXT_SOURCES, DATA_CONTEXT_WINDOWS, DEFAULT_DATA_CONTEXT_SOURCES, DEFAULT_DATA_CONTEXT_WINDOW, DataContextSettings
from config import COACH_REASONING_EFFORT_OPTIONS, COACH_SPEED_OPTIONS, DEFAULT_MEMORY_ACTION, MEMORY_ACTIONS, STRUCTURED_NOTE_TOPIC_OPTIONS, get_coach_model_options, get_memory_environment_label, get_openai_model
from memory.background_jobs import get_memory_job_status, start_memory_job


# ----------------------------------------------------------
# Coach Chat View
# ----------------------------------------------------------


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

def send_agent_message(prompt: str, owner_mode: bool, memory_action: str, structured_topic: str | None, data_context_settings: DataContextSettings, coach_settings: dict) -> None:
    clean_prompt = prompt.strip()

    if not clean_prompt:
        return

    data_context_label = describe_data_context(data_context_settings)
    st.session_state.agent_messages.append({"role": "user", "content": clean_prompt, "memory_action": memory_action, "structured_topic": structured_topic, "data_context_label": data_context_label})

    with st.spinner("Coach is reading context and thinking..."):
        data_context = build_data_context(owner_mode, data_context_settings)
        data_context_estimate = estimate_input_text_cost(coach_settings["model"], data_context)
        response = get_agent_response(owner_mode, data_context, coach_settings["model"], coach_settings["reasoning_effort"], coach_settings["speed"])

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

def render_data_context_controls(owner_mode: bool, model: str) -> DataContextSettings:
    st.markdown("**Data context**")
    window = st.selectbox("Window", DATA_CONTEXT_WINDOWS, index=DATA_CONTEXT_WINDOWS.index(DEFAULT_DATA_CONTEXT_WINDOW))

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
    context_estimate = estimate_input_text_cost(model, data_context)
    context_estimate_caption = format_context_estimate_caption(context_estimate)

    if context_estimate_caption:
        st.caption(context_estimate_caption)

    return data_context_settings


# Renders model and response behavior controls for the current coach question.
# Defaults preserve the original hardcoded app behavior unless the user changes them.
# Returns a small dictionary passed to the coach API call.

def render_coach_settings_controls() -> dict:
    model_options = get_coach_model_options()
    model_column, effort_column, speed_column = st.columns([1.35, 1, 1], gap="small")

    with model_column:
        model = st.selectbox("Model", model_options, index=model_options.index(get_openai_model()))

    with effort_column:
        reasoning_effort = st.selectbox("Effort", COACH_REASONING_EFFORT_OPTIONS, index=COACH_REASONING_EFFORT_OPTIONS.index("Default"))

    with speed_column:
        speed = st.selectbox("Speed", COACH_SPEED_OPTIONS, index=COACH_SPEED_OPTIONS.index("Default"))

    return {"model": model, "reasoning_effort": reasoning_effort, "speed": speed}


# Renders the memory/action controls for the current coach question.
# These controls decide what should happen with this specific question-answer pair later.
# Returns the selected memory action and structured-note destination.

def render_memory_controls(owner_mode: bool) -> tuple[str, str | None]:
    if not owner_mode:
        st.caption("Memory writing is full-mode only. Demo chats are not saved.")
        return "No memory", None

    memory_label = get_memory_environment_label()
    st.caption(f"Memory target: {memory_label}")

    memory_action = st.selectbox("Save mode", MEMORY_ACTIONS, index=MEMORY_ACTIONS.index(DEFAULT_MEMORY_ACTION))

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

            with st.expander("Coach settings", expanded=False):
                coach_settings = render_coach_settings_controls()

            with st.expander("Data context", expanded=False):
                data_context_settings = render_data_context_controls(owner_mode, coach_settings["model"])

            with st.expander("After answer", expanded=False):
                memory_action, structured_topic = render_memory_controls(owner_mode)

            with st.form("coach_message_form", clear_on_submit=True):
                prompt = st.text_area("Message", placeholder="Ask about training, recovery, planning, or your data...", height=170, label_visibility="collapsed")
                submitted = st.form_submit_button("Send", width="stretch")

            if submitted:
                send_agent_message(prompt, owner_mode, memory_action, structured_topic, data_context_settings, coach_settings)

    with answers_column:
        with st.container(border=True):
            for message in st.session_state.agent_messages:
                render_chat_message(message)
