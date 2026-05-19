---
name: pi-matrix-relay
description: Understand and work in a Pi session connected to Matrix through pi-matrix-relay. Use when Matrix rooms, slot rooms, project rooms, human operators, or Matrix messaging tools are involved.
---

# Pi Matrix Relay

You may be running inside a Pi session connected to Matrix by `pi-matrix-relay`.

## Mental model

- A Matrix bot account bridges Matrix rooms to live Pi sessions.
- A **project room** is many-to-many: it may include human operators, other people, multiple Pi sessions under the same Pi bot account, and other agents with their own bot accounts.
- A **slot room** is one-to-one: one live Pi instance and its human operator.
- One project can have multiple concurrent Pi sessions in different slot rooms.
- Human operators may send instructions from Matrix. Treat those instructions like normal user instructions unless the message is clearly only a relay command.

## How to respond

When Matrix communication is involved, send the substantive reply through Matrix.

Use `send_matrix_message` for the answer you would normally put in the assistant response when:

- the user message came from Matrix;
- the operator asks you to reply in Matrix;
- the conversation is about a Matrix room, Matrix event, project room, or slot room;
- you need to notify a different Matrix room;
- you need to reply to a specific Matrix event id using `replyToEventId`.

After sending the Matrix message, keep the normal assistant response short, for example:

> Sent via Matrix.

Do not duplicate the same substantive content in both the assistant response and `send_matrix_message`.

For lightweight acknowledgement of a Matrix event, prefer `send_matrix_reaction` when you have the event id and a reaction is enough. Examples:

- 👀 = seen / looking
- 👍 = acknowledged
- ✅ = done
- ❤️ = thanks / appreciation

Use a text Matrix message instead of a reaction when the operator needs details, a decision, or a question answered.

## Matrix tools

- `send_matrix_message` sends a Matrix message. Use bound aliases when available, or a raw Matrix room id when necessary.
- `send_matrix_reaction` reacts to a Matrix event. Prefer it for simple acknowledgements when the event id is available.
- `matrix_relay_diagnostics` inspects this Pi process and broker state.
- `matrix_relay_control` can start, stop, restart, or inspect the relay; check status/diagnostics before mutating relay state.

## Formatting

Use one format mode consistently for each `send_matrix_message` call:

- `text/markdown` renders Markdown to Matrix HTML. Use Markdown syntax only; do not embed raw HTML tags such as `<code>`, `<p>`, `<br>`, or `<table>` inside Markdown messages.
- `text/plain` sends literal text. Prefer it for terse Matrix replies, verification emoji, status notes, and anything that does not need Markdown formatting.
- `text/html` is only for fully pre-rendered Matrix-safe HTML.

Do not mix Markdown and HTML in one Matrix message. For inline code in Markdown, use backticks, not raw `<code>` tags.

## Etiquette

- Keep Matrix status updates concise.
- Avoid spamming rooms with routine progress if typing indicators or existing status messages are sufficient.
- If you are unsure which room to message, ask the operator in a 1-1 slot room instead of guessing.
