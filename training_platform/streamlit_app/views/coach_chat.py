import streamlit as st

from ai_coach import format_cost_caption, get_agent_response
from coach_context.context_builder import build_data_context, describe_data_context
from coach_context.context_options import DATA_CONTEXT_SOURCES, DATA_CONTEXT_WINDOWS, DEFAULT_DATA_CONTEXT_SOURCES, DEFAULT_DATA_CONTEXT_WINDOW, DataContextSettings
from config import MEMORY_ACTIONS, STRUCTURED_NOTE_TOPIC_OPTIONS, get_chat_archive_sheet_url, get_memory_environment_label, get_structured_notes_doc_url
from memory.coach_google_memory import append_chat_archive, check_structured_notes_target


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

        data_context_label = message.get("data_context_label")
        if data_context_label:
            st.caption(f"Data context used: {data_context_label}")


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
        response = get_agent_response(owner_mode, data_context)

    memory_results = []

    if memory_action in ["Chat Archive", "Chat Archive & Create Notes"]:
        memory_results.append(append_chat_archive(clean_prompt, response["content"], owner_mode, get_current_user_email(), memory_action, structured_topic, response["usage"]))

    if memory_action == "Chat Archive & Create Notes":
        memory_results.append(check_structured_notes_target())

    st.session_state.agent_messages.append({"role": "assistant", "content": response["content"], "usage": response["usage"], "memory_results": memory_results, "data_context_label": data_context_label})


# Renders the data-context controls for the current coach question.
# These controls decide what source data is sent to the coach before it answers.
# Returns the selected data context settings.

def render_data_context_controls(owner_mode: bool) -> DataContextSettings:
    st.markdown("**Data context**")
    window = st.selectbox("Data window", DATA_CONTEXT_WINDOWS, index=DATA_CONTEXT_WINDOWS.index(DEFAULT_DATA_CONTEXT_WINDOW))

    if window == "No data":
        st.caption("No sheet or notes data will be sent to the coach.")
        return DataContextSettings(window=window, sources=[])

    source_options = DATA_CONTEXT_SOURCES if owner_mode else [source for source in DATA_CONTEXT_SOURCES if source != "Structured Notes"]
    default_sources = [source for source in DEFAULT_DATA_CONTEXT_SOURCES if source in source_options]
    sources = st.multiselect("Data sources", source_options, default=default_sources)

    if window == "All available (expensive)":
        st.caption("This can send a lot of rows and may cost more.")

    return DataContextSettings(window=window, sources=sources)


# Renders the memory/action controls for the current coach question.
# These controls decide what should happen with this specific question-answer pair later.
# Returns the selected memory action and structured-note destination.

def render_memory_controls(owner_mode: bool) -> tuple[str, str | None]:
    st.markdown("**Memory action**")

    if not owner_mode:
        st.caption("Memory writing is full-mode only. Demo chats are not saved.")
        return "No memory", None

    memory_label = get_memory_environment_label()
    archive_sheet_url = get_chat_archive_sheet_url()
    structured_notes_url = get_structured_notes_doc_url()
    st.caption(f"Memory target: {memory_label}")

    if archive_sheet_url:
        st.link_button("Open chat archive sheet", archive_sheet_url, use_container_width=True)
    else:
        st.caption("Chat archive sheet is not configured yet.")

    memory_action = st.selectbox("What should happen with this answer?", MEMORY_ACTIONS, index=0)

    if memory_action != "Chat Archive & Create Notes":
        return memory_action, None

    if structured_notes_url:
        st.link_button("Open structured notes doc", structured_notes_url, use_container_width=True)
    else:
        st.caption("Structured notes doc is not configured yet.")

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
        with st.container(border=True):
            st.markdown("**Ask the coach**")
            data_context_settings = render_data_context_controls(owner_mode)
            st.divider()
            memory_action, structured_topic = render_memory_controls(owner_mode)
            st.divider()

            with st.form("coach_message_form", clear_on_submit=True):
                prompt = st.text_area("Message", placeholder="Ask about training, recovery, planning, or your data...", height=170, label_visibility="collapsed")
                submitted = st.form_submit_button("Send", use_container_width=True)

            if submitted:
                send_agent_message(prompt, owner_mode, memory_action, structured_topic, data_context_settings)

    with answers_column:
        with st.container(border=True):
            for message in st.session_state.agent_messages:
                render_chat_message(message)
