import streamlit as st

from ai_coach import format_cost_caption, get_agent_response


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


# Sends one user prompt to the coach and stores the assistant response.
# This keeps the input form and chat history rendering separate.
# Returns nothing; it updates Streamlit session state.

def send_agent_message(prompt: str, owner_mode: bool) -> None:
    clean_prompt = prompt.strip()

    if not clean_prompt:
        return

    st.session_state.agent_messages.append({"role": "user", "content": clean_prompt})

    with st.spinner("Coach is thinking..."):
        response = get_agent_response(owner_mode)

    st.session_state.agent_messages.append({"role": "assistant", "content": response["content"], "usage": response["usage"]})


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

            with st.form("coach_message_form", clear_on_submit=True):
                prompt = st.text_area("Message", placeholder="Ask about training, recovery, planning, or your data...", height=170, label_visibility="collapsed")
                submitted = st.form_submit_button("Send", use_container_width=True)

            if submitted:
                send_agent_message(prompt, owner_mode)

    with answers_column:
        with st.container(border=True):
            for message in st.session_state.agent_messages:
                render_chat_message(message)
