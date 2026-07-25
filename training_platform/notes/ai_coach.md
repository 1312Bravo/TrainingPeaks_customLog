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
