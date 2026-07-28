import streamlit as st

from coach_context.structured_notes_context import read_structured_notes_text


# ----------------------------------------------------------
# Structured Notes Parsing
# ----------------------------------------------------------

# Splits the Google Doc text into topic blocks using Markdown-style level-two headings.
# Notes written by the app use `## Topic`, so this keeps the app view aligned with storage.
# Returns a list of topic dictionaries.

def parse_note_topics(notes_text: str) -> list[dict]:
    topics = []
    current_topic = None
    current_lines = []

    for line in notes_text.splitlines():
        if line.startswith("## "):
            if current_topic:
                topics.append({"title": current_topic, "content": "\n".join(current_lines).strip()})

            current_topic = line.replace("## ", "", 1).strip()
            current_lines = []
        elif current_topic:
            current_lines.append(line)

    if current_topic:
        topics.append({"title": current_topic, "content": "\n".join(current_lines).strip()})

    return topics


# Splits one topic block into optional subtopic blocks using level-three headings.
# If no subtopics exist, the topic content is shown as a single block.
# Returns a list of subtopic dictionaries.

def parse_note_subtopics(topic_content: str) -> list[dict]:
    subtopics = []
    intro_lines = []
    current_subtopic = None
    current_lines = []

    for line in topic_content.splitlines():
        if line.startswith("### "):
            if current_subtopic:
                subtopics.append({"title": current_subtopic, "content": "\n".join(current_lines).strip()})
            elif intro_lines:
                subtopics.append({"title": "Overview", "content": "\n".join(intro_lines).strip()})

            current_subtopic = line.replace("### ", "", 1).strip()
            current_lines = []
        elif current_subtopic:
            current_lines.append(line)
        else:
            intro_lines.append(line)

    if current_subtopic:
        subtopics.append({"title": current_subtopic, "content": "\n".join(current_lines).strip()})
    elif intro_lines:
        subtopics.append({"title": "Overview", "content": "\n".join(intro_lines).strip()})

    return [subtopic for subtopic in subtopics if subtopic["content"] or subtopic["title"]]


# ----------------------------------------------------------
# Structured Notes Components
# ----------------------------------------------------------

# Renders one selected topic and optional selected subtopic.
# The left side acts as navigation, while the right side stays focused on one note.
# Returns nothing; it writes the topic browser.

def render_notes_browser(notes_text: str) -> None:
    topics = parse_note_topics(notes_text)

    if not topics:
        st.markdown(notes_text)
        return

    navigation_column, content_column = st.columns([1, 2.4], gap="large")

    with navigation_column:
        with st.container(border=True):
            st.markdown("**Topics**")
            topic_titles = ["No topic selected"] + [topic["title"] for topic in topics]
            selected_topic_title = st.radio("Topic", topic_titles, index=0, label_visibility="collapsed")

            if selected_topic_title == "No topic selected":
                selected_topic = None
                subtopics = []
                selected_subtopic = None
            else:
                selected_topic = next(topic for topic in topics if topic["title"] == selected_topic_title)
                subtopics = parse_note_subtopics(selected_topic["content"])

            if selected_topic and len(subtopics) > 1:
                st.markdown("**Subtopics**")
                subtopic_titles = [subtopic["title"] for subtopic in subtopics]
                selected_subtopic_title = st.radio("Subtopic", subtopic_titles, label_visibility="collapsed")
                selected_subtopic = next(subtopic for subtopic in subtopics if subtopic["title"] == selected_subtopic_title)
            elif selected_topic and subtopics:
                selected_subtopic = subtopics[0]
            elif selected_topic:
                selected_subtopic = {"title": "Overview", "content": selected_topic["content"]}

    with content_column:
        if selected_topic and selected_subtopic:
            with st.container(border=True):
                st.markdown(f"## {selected_topic['title']}")

                if selected_subtopic["title"] != "Overview":
                    st.markdown(f"### {selected_subtopic['title']}")

                st.markdown(selected_subtopic["content"])


# ----------------------------------------------------------
# Structured Notes View
# ----------------------------------------------------------

# Renders the coach's structured notes inside the app.
# Google Docs remains the storage layer, but this view makes the notes feel native to the app.
# Returns nothing; it writes the notes panel to the page.

def render_structured_notes(owner_mode: bool) -> None:
    st.subheader("Structured notes")

    if not owner_mode:
        st.caption("Structured Notes are available only in full owner mode.")
        st.info("Demo mode does not show private coach notes.")
        return

    notes_text = read_structured_notes_text()

    if notes_text in ["Structured Notes doc is empty.", "Structured Notes doc is not configured."]:
        st.info(notes_text)
        return

    if notes_text.startswith("Structured Notes could not be read."):
        st.warning(notes_text)
        return

    render_notes_browser(notes_text)
