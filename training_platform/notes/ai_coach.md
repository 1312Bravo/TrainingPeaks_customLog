# AI Coach

## 2026-07-23

- The app should include an AI coach specialized in endurance and trail running.
- Coach prompts should live in `prompts/` as editable Markdown files, separate from app code.
- Initial prompt files are examples/placeholders and should be refined as the app develops.
- The coach should eventually use training context, but only after authentication and secure AI access are configured.
- The app chat should use the OpenAI API through a server-side/local environment variable or Streamlit secret named `OPENAI_API_KEY`.
- Local development should support reading `OPENAI_API_KEY` from `training_platform/.env`.
- The app should not store the OpenAI API key in source code.
- Deployment should use a real secrets store, such as GitHub Secrets, Streamlit secrets, or the hosting provider's environment variables.
- Until private sheet context is deliberately wired, the AI coach should answer as a trail-running coach without pretending it has analyzed the user's private training data.

## 2026-07-25

- For now, use OpenAI API billing for the in-app coach and monitor real usage before making a long-term decision.
- Keep API usage cost-controlled by sending compact training summaries by default, not full raw sheets on every message.
- Codex can also act as a parallel coach/build partner in this project chat while the in-app coach matures.
- OpenAI API billing was enabled for testing; watch actual usage and adjust budget/recharge settings if the app starts using more than expected.
- The app should show an estimated API cost under each real coach answer, based on returned token usage and the configured model price table.
- These per-answer costs are estimates for visibility; the OpenAI billing dashboard remains the source of truth for actual charges and account balance.

## 2026-07-26

- Coach memory should eventually be stored in Google account files, not local files, because the app may be used from multiple devices.
- Memory writes should be full-mode only unless explicitly changed later.
- The chat UI should include configurable handling for each question-answer pair. This is the main product concept: before or while asking, the user chooses what should happen with this specific coach exchange.
- Use a memory/action mode selector in the chat:
  - `No memory`: answer the question without saving anything.
  - `Chat Archive`: save the raw question and coach answer only.
  - `Chat Archive & Create Notes`: save the raw question and answer, then create or update a structured note in the chosen destination.
- The chat UI should let the user configure where the structured answer should go, such as an existing topic, a new topic, or another future note destination.
- Structured notes should be readable notes, not a database-like tag table.
- Use a Google Doc named `Training Platform Coach Structured Notes` for book-style structured notes.
- `Structured Notes` should be organized by topic, with new notes added under an existing topic when appropriate.
- Topic selection should be part of the chat flow. The user can choose from current topics, and if none fits, the coach can suggest example topics before a new topic is created.
- Google Sheets should store `Chat Archive` because it is row-based history.
- Use Google Sheets named `Training Platform Coach Chat Memory` for chat archive history.
- Google Docs should store `Structured Notes` because they should read like topic-based notes, not database rows.
- Archive sheet and structured notes doc URLs are stored in local/deployment configuration, not committed source code.
- First implementation should show chat memory controls before writing data, then add Google Sheet write behavior once the control flow feels right.
- Coach memory should have separate dev and prod Google files so local testing does not pollute the real deployed memory.
- Local development should default to the dev archive sheet and dev structured notes doc through `TRAINING_PLATFORM_MEMORY_ENV=dev`.
- Deployment should use the prod archive sheet and prod structured notes doc by setting `TRAINING_PLATFORM_MEMORY_ENV=prod` in deployment secrets.
- The first real connection should write `Chat Archive` rows with service-account credentials stored in ignored local secrets or deployment secrets.
- Local service-account credentials can use `TRAINING_PLATFORM_GOOGLE_SERVICE_ACCOUNT_FILE`; deployment should use `TRAINING_PLATFORM_GOOGLE_SERVICE_ACCOUNT_JSON` or an equivalent secret value.
- Automatic `Structured Notes` document updates should come after chat archive writing is proven, because topic-aware document editing is a more delicate behavior.
- Add `Data context` controls that are separate from `Memory action`.
- `Data context` controls what source data the coach receives before answering; `Memory action` controls what the app saves after answering.
- Data context windows should include `No data`, `Yesterday`, `Last 7 days`, `Last 14 days`, `Last month`, `Last 3 months`, `Last year`, and `All available (expensive)`.
- Data context sources should include `Daily data`, `Activity statistics`, `HASR-TL`, and `Structured Notes`.
- First implementation should send filtered raw rows with all columns for selected row-based sources, not summaries.
- Later we can add summary tables or column pruning if raw context becomes too expensive, slow, or noisy.
- `Structured Notes` should be readable as context, but it is topic-based and not date-filtered yet.
- Data context size/cost estimates should be shown as approximate data-context input estimates only.
- Actual API cost after the answer remains the better billing visibility because it includes prompts, chat history, output tokens, and provider token accounting.
- `Chat Archive & Create Notes` should first answer the user, then create a general reusable structured note from the Q&A, then append it to the Structured Notes Google Doc.
- Structured notes should avoid user-specific diary detail by default and preserve durable coaching guidance, decision rules, warning signs, and practical next actions.
- First Structured Notes writing can append topic-labeled blocks; smarter merging into existing topic sections can come later.
- The visible coach answer should use the same language as the user's question unless the user asks for another language.
- Saved Coach Chat Memory and Structured Notes should always be written in English.
- If the user asks in another language, translate the question and coach answer to English before saving to Coach Chat Memory or generating Structured Notes.
- Data-context cost estimates are only approximate selected-context input estimates; full API cost depends on prompts, chat history, model reasoning, output length, and provider token accounting.
- Coach answers should appear before slower memory work finishes.
- Chat Archive and Structured Notes writes can run as a background memory job after the visible answer is added to the chat.
- The app should have a native `Structured notes` tab that reads from the Structured Notes Google Doc, so notes feel like they live inside the app even though Google Docs remains the storage layer.
- Structured Notes should be browsable by topic, with focused selected-topic/subtopic display instead of one full-width document dump.
- Coach chat controls should keep the same options but group them by workflow: `Before answer` for Data context and `After answer` for Memory action.
- Coach chat should expose model, effort, and speed controls, but defaults should preserve the previous hardcoded behavior unless changed for one question.
- Model selection should default to the configured OpenAI model, currently `gpt-5-mini` unless overridden by local/deployment configuration.
- Effort should default to `Default`, which means the app does not send an explicit reasoning-effort setting.
- Speed should default to `Default`, which means the app uses the normal coach response style without an extra speed/detail instruction.
- Data context controls should be collapsed by default so the chat input stays compact.
- Full-mode `After answer` should default to `Chat Archive`, because this quietly builds useful memory without creating structured notes every time.
