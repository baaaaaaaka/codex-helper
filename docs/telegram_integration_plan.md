# Telegram Integration Plan

Status: research captured, no implementation yet.

## Goal

Add an explicit Telegram mode to `codex-helper` so an allowlisted Telegram user can interact with local Codex sessions, receive Codex replies, and eventually exchange files and images without changing the existing `codex-proxy` default behavior.

## Recommended Shape

- Add a `telegram` top-level CLI command, separate from the existing TUI, history, run, proxy, and upgrade commands.
- Default to Telegram Bot API long polling. Keep webhook support as an advanced later option.
- Treat Telegram as a remote-control surface and require explicit local pairing before any chat can drive local Codex.
- Use a non-TTY Codex runner for Telegram, initially based on `codex exec --json` and `codex exec resume --json`.
- Preserve the current proxy preference, Codex install, history, standard approval runtime, and effective-path behavior by reusing existing helpers instead of reimplementing them.

## Command Sketch

- `codex-proxy telegram run`: run the bot in the foreground.
- `codex-proxy telegram pair`: print a short-lived local pairing code for `/start <code>`.
- `codex-proxy telegram daemon`: internal long-lived process entrypoint.
- `codex-proxy telegram status`: show bot, pairing, queue, and Codex readiness without printing secrets.
- `codex-proxy telegram service install --user`: later phase for systemd user service, macOS LaunchAgent, or Windows service/task setup.

## Module Sketch

- `internal/cli/telegram.go`: Cobra command wiring.
- `internal/telegrambot`: Telegram Bot API client, update parser, polling loop, outbound sender.
- `internal/telegrambot/state`: durable offset, pending update queue, dedupe records, dead-letter records.
- `internal/telegrambot/authz`: allowlist, local pairing, command permissions, sensitive operation confirmation.
- `internal/telegrambot/media`: file download/upload, path isolation, size checks, MIME checks, outbox handling.
- `internal/service`: optional shared service-install helpers for Telegram and existing proxy daemons.

## Reliability Rules

- Poll with `offset = last_acked_update_id + 1`.
- Persist raw updates before dispatch.
- Advance the ack only after a command is processed and the result is persisted.
- Deduplicate by `update_id` so restarts are at-least-once but user-visible effects are idempotent.
- Separate receiver, dispatcher, and worker layers.
- Keep same-chat work serialized. Keep global Codex worker count low by default.
- Implement bounded queues, retry with exponential backoff and jitter, and dead-letter records for permanently failing updates.
- Rate-limit outbound Telegram messages per chat and globally. Respect Telegram 429 `retry_after`.

## Security Rules

- Default to private chats only.
- Bind both `from.id` and `chat.id`; do not trust username.
- Require local `telegram pair` for first authorization.
- Never log bot tokens, Telegram file URLs, full local paths, `auth.json`, or unredacted config.
- Store the bot token in an environment variable or a local 0600 secret file, not in git-tracked files.
- Disable destructive operations in the first implementation: arbitrary shell, self-update, `upgrade-codex`, proxy stop, unrestricted file sending, and full raw history export.
- Download Telegram files only into private cache/inbox directories by default.
- Send local files only from a controlled outbox or the selected project root after explicit confirmation.
- Treat Telegram account compromise as equivalent to remote access by the allowlisted user. Sensitive actions need local confirmation or a local one-time PIN.

## Phasing

1. Polling, pairing, allowlist, durable offset queue, `/status`, limited `/history list`, limited `/history show`, outbound rate limiter.
2. Text conversation through `codex exec --json` and `codex exec resume --json`; session mapping with `/new`, `/resume`, `/sessions`.
3. File and image input/output with cache/inbox/outbox boundaries and explicit export confirmation.
4. Background service install helpers, webhook mode, and possible Codex app-server or SDK based streaming.

## Compatibility Notes

- Do not change the default `codex-proxy` or `cxp` behavior.
- Do not start Telegram automatically from install scripts.
- Do not mix Telegram token data into existing proxy profiles.
- Reuse existing config store locking and atomic writes for non-secret state.
- Reuse existing effective path resolution so root, sudo, Windows, macOS, and service-account behavior stays compatible.
- Reuse existing history discovery and Codex install/launch helpers where possible.
