from config import get_structured_notes_doc_url
from memory.coach_google_memory import build_google_service, extract_google_file_id


# ----------------------------------------------------------
# Google Docs Text Extraction
# ----------------------------------------------------------

# Reads text runs from a Google Docs structural element.
# Google Docs stores paragraphs as nested elements, so this helper flattens readable text.
# Returns extracted text.

def extract_text_from_element(element: dict) -> str:
    if "paragraph" not in element:
        return ""

    text_parts = []
    for paragraph_element in element["paragraph"].get("elements", []):
        text_run = paragraph_element.get("textRun")

        if text_run:
            text_parts.append(text_run.get("content", ""))

    return "".join(text_parts)


# Reads the configured Structured Notes Google Doc.
# This is not date-filtered yet because notes are topic-based rather than row-based.
# Returns the document text or a short unavailable message.

def read_structured_notes_text() -> str:
    structured_notes_url = get_structured_notes_doc_url()

    if not structured_notes_url:
        return "Structured Notes doc is not configured."

    try:
        document_id = extract_google_file_id(structured_notes_url)
        docs_service = build_google_service("docs", "v1")
        document = docs_service.documents().get(documentId=document_id).execute()
    except Exception as error:
        return f"Structured Notes could not be read: {error}"

    body_content = document.get("body", {}).get("content", [])
    text = "".join(extract_text_from_element(element) for element in body_content).strip()

    return text or "Structured Notes doc is empty."


# Builds the Structured Notes context section.
# The section is included only when the user selects this source.
# Returns a text block for the coach prompt.

def build_structured_notes_context() -> str:
    return f"## Structured Notes\n{read_structured_notes_text()}"
