import streamlit as st

# This is the owner account for the future full/private version of the app. Until Google login is configured, this check falls back to demo mode.
OWNER_EMAIL = "pecek.urh@gmail.com"


# Page settings are kept near the top because Streamlit expects them before normal page content is rendered.
st.set_page_config(
    page_title="Training Platform",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded",
)


def is_owner() -> bool:
    """Return True only when Streamlit auth identifies the configured owner."""

    # Local development currently has no login configured. In that case, `st.user` may exist but not contain an email, so demo mode is safest.
    if not hasattr(st, "user"):
        return False

    return st.user.get("email") == OWNER_EMAIL


def initialize_chat() -> None:
    """Create the in-browser chat history for the current Streamlit session."""

    # This is temporary session memory.
    # Later we can decide what should be saved permanently, and whether saved chats belong only to full mode.
    if "agent_messages" not in st.session_state:
        st.session_state.agent_messages = [
            {
                "role": "assistant",
                "content": "Hi. This is the future agent window. I am not connected to the real AI coach yet, but this is where we will talk.",
            }
        ]


def render_agent_chat(owner_mode: bool) -> None:
    """Render the chat area that will later connect to the real AI agent."""

    st.subheader("Agent chat")

    if owner_mode:
        st.caption("Full mode chat. Later this can use your private training context.")
    else:
        st.caption("Demo mode chat. Private training context is not available.")

    initialize_chat()

    for message in st.session_state.agent_messages:
        with st.chat_message(message["role"]):
            st.write(message["content"])

    prompt = st.chat_input("Ask the agent...")
    if prompt:
        st.session_state.agent_messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.write(prompt)

        response = "The chat window is working, but the real AI agent is not connected yet. Next we can decide how to connect it securely."
        st.session_state.agent_messages.append({"role": "assistant", "content": response})
        with st.chat_message("assistant"):
            st.write(response)


# Top-level page shell. The two modes should share the same app structure later, with full mode unlocking private data and owner-only actions.
st.caption("✉ pecek.urh@gmail.com")
st.title("Training Platform")

owner_mode = is_owner()

if owner_mode:
    st.success("Full app mode")
    st.caption("Private training data, notes, and AI coach features will live here.")
else:
    st.info("Demo mode")
    st.caption("Public/demo experience. Private training data is hidden.")

st.divider()
render_agent_chat(owner_mode)
