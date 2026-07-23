# Security And Auth

## 2026-07-23

- The app is expected to include an AI chat area.
- If the app is made available to other people, it should identify the user before allowing private/personal AI features.
- The app should not assume that a public link means the current user is Urh.
- A future auth approach should support restricting personal training features to the intended user, for example by login email.
- OpenAI/Codex access should be handled through a secure server-side/API-key setup, not by exposing secrets in the browser or public app UI.
- The app should have two access modes:
  - Full app for the owner account, currently `pecek.urh@gmail.com`.
  - Demo/public version for users who are not logged in as the owner.
- The demo/public version should not expose private training data, private notes, saved AI answers, secrets, or owner-only coach context.
- Demo and full modes should mostly share the same app structure and screens.
- Full mode should unlock owner-only features and private data within the same general experience, rather than becoming a completely separate app.
- The app should include an agent/chat window.
- Until authentication and AI API access are configured, the chat window should not use private training context or pretend that responses are personalized.
