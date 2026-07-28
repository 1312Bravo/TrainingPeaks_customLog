import threading
import uuid

from ai_coach import generate_structured_note, normalize_exchange_for_memory
from memory.coach_google_memory import append_chat_archive, append_structured_note


# ----------------------------------------------------------
# Background Memory Jobs
# ----------------------------------------------------------

MEMORY_JOBS = {}
MEMORY_JOBS_LOCK = threading.Lock()


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
