# Teams Integration Security Threat Model

Status: implementation guardrail for the delegated Microsoft Teams bridge.

## Scope

This document covers the Teams mode that uses Microsoft Graph delegated auth and
normal user chats. It does not cover Azure Bot app-only operation, Teams channel
send, Slack, other chat transports, or direct OpenAI API use. Any non-Teams
transport needs its own app/permission review and threat model before it can
reuse the durable bridge state.

## Assets

- Microsoft Graph access and refresh tokens.
- Teams control chat, session chats, messages, and referenced files.
- Codex prompts, Codex output, thread ids, turn ids, and local Codex state.
- Teams bridge state, outbox records, owner heartbeat, service unit, and
  temporary attachment files.
- The helper binary, managed Codex binary, proxy configuration, and service
  environment.

## Trust Boundaries

- Microsoft Graph and the tenant policy boundary.
- The logged-in Microsoft account. The bridge acts as this user and must not
  pretend to be an independent bot identity.
- Teams chats. The bridge must process only configured control/session chats and
  only messages authored by the configured Teams user.
- The local machine boundary, including WSL, Windows browser handoff, systemd
  user services, logs, filesystem permissions, and local malware risk.
- The Codex CLI boundary. Teams mode must use the same managed install, proxy,
  yolo, root/sudo, and `CODEX_HOME`/`CODEX_DIR` behavior as existing helper
  commands.

## Authentication And Tokens

- Use delegated auth only. Normal chat send/create/read is performed on behalf
  of the logged-in user.
- Default scopes are intentionally narrow. Broad or unexpected scopes fail
  closed unless an explicit unsafe-scope override is set.
- Token cache files must be written with private permissions and must not be
  copied into Teams state.
- Token cache parent directories must also be private, non-symlink directories.
  A `0600` token file inside a shared or symlinked directory is not sufficient.
- File upload uses a separate opt-in file-write token cache so the normal
  chat/control bridge can keep narrower delegated scopes.
- Logs, status output, doctor output, errors, and service unit generation must
  not print access tokens, refresh tokens, full Teams message bodies, Teams
  chat ids, message ids, drive item ids, Teams URLs, or full local
  prompt/attachment paths.
- `teams doctor` is local-only by default. `teams doctor --live` is the explicit
  operator action that calls Graph `/me` and optionally reads the configured
  control chat.
- `teams auth logout` is the supported local token removal path.

## Message Isolation

- The control chat is the dashboard. Each Codex conversation is represented by a
  dedicated Teams chat plus a durable local session record.
- Multi-machine deployments must use separate `machine_id + Teams account +
  helper profile` identities and separate control chats. A bridge instance must
  not process another identity's control chat unless the user explicitly
  rebinds that machine identity.
- Control/work chat title markers are presentation only. Emoji, text markers,
  sanitized cwd basenames, or user-renamed titles must not be used for routing,
  permissions, recovery, or dedupe.
- The bridge must ignore messages from other users even if they appear in a
  configured chat.
- Session helper commands are slash-only. Plain text such as `status` or
  `close` remains Codex input.
- Numbered dashboard selections are helper-owned commands and must not be sent
  to Codex for interpretation.
- Numbered dashboard selections are valid only in the control chat and only for
  the current dashboard view. Plain numbers in work chats remain Codex input.
- Routine dashboard output should use short labels and path fingerprints rather
  than full local paths. Exact paths belong behind an explicit details command
  or local CLI diagnostic.
- Historical Codex sessions must not be mirrored into Teams by default. Import
  to Teams requires an explicit user action and durable import checkpoints.
- Teams private chats reduce accidental exposure but do not defeat tenant admin,
  retention, eDiscovery, endpoint compromise, or a compromised logged-in
  account.

## Graph Requests

- Graph access stays behind a path allowlist. User-controlled text must not
  become an arbitrary Graph path or URL fetch.
- User-facing Graph errors must be redacted before display. Method/path strings
  that include chat ids, message ids, drive ids, `nextLink` values, or local
  file paths are diagnostics for private debug logs only after redaction.
- Retry behavior is bounded: refresh on 401, respect `Retry-After` for 429, and
  use bounded retries for transient 5xx responses.
- Safe `nextLink` handling must verify that paginated requests stay under the
  expected chat/message endpoint.

## Attachments

- Inline Teams hosted content is supported only through the Graph
  `/hostedContents/{id}/$value` endpoint extracted from the message HTML.
- Ordinary Teams file references are supported only when all of these are true:
  the attachment `contentType` is `reference`, `contentUrl` is HTTPS, the URL has
  no userinfo, the host is a SharePoint host, and the user has opted into
  `Files.Read` or `Files.ReadWrite`.
- The bridge must not directly download a user-supplied `contentUrl`. It converts
  the allowed reference URL to Graph `/shares/{shareId}/driveItem/content`.
- `http`, `file`, relative URLs, non-SharePoint hosts, userinfo URLs, forwarded
  messages, cards, and unknown attachment types are rejected with a user-facing
  unsupported-transfer message.
- Downloaded files are stored in private temp directories, bounded by count and
  byte limits, passed to the Codex prompt as local files, and cleaned up after
  the turn or retry attempt.
- Attachment handling must reject or explicitly report every unsupported or
  over-limit attachment. Truncating after a count limit without telling the user
  is not acceptable.
- Local temporary attachment paths should be exposed to Codex through stable,
  neutral aliases where possible. Full temp paths can leak usernames, message
  ids, or state layout and should not be used for ordinary no-attachment turns.
- Outbound file send is explicit only. `teams send-file` may upload a local path
  when the operator passes `--allow-local-path`; session `/send-file` accepts
  only relative paths under the Teams outbound root.
- Outbound uploads use a fixed default OneDrive folder, a size cap, a narrow
  extension allowlist, symlink rejection, private local staging, and Teams
  reference attachments. The bridge must not automatically upload arbitrary
  Codex output without an explicit command.
- The default remote folder should remain the Teams-compatible
  `Microsoft Teams Chat Files` folder unless the operator explicitly overrides
  it. Generated upload names must avoid collisions without exposing raw prompts,
  full paths, Teams ids, or other sensitive routing metadata.
- Automatic artifact handoff, when added, must consume only a helper-defined
  manifest, validate paths under the controlled outbound root, hash/stage names
  to avoid collisions, and preserve the model's visible text rather than
  relying on fragile output filtering.

## Durable State And Recovery

- Persist inbound events before running Codex.
- Persist turn state before starting Codex.
- Persist outbox records before sending Teams output.
- All bridge-originated Teams messages must go through durable outbox first,
  including control replies, ACKs, helper errors, history imports, final
  chunks, artifact links, and owner notifications. The allowed exceptions are
  local terminal output such as device-code auth prompts and local doctor
  output.
- Long Teams output must be fully rendered, split, validated, and persisted as
  all outbox parts before the first part is sent.
- Mark outbox records sent only after Graph send succeeds.
- Graph send is at-least-once. If Graph accepts a message and the helper crashes
  before the outbox item is marked sent, a duplicate Teams message is possible.
  Recovery must never rerun Codex to repair that state.
- Duplicate Teams message ids must not rerun Codex.
- Queued outbox messages may be resent after restart; ambiguous running turns
  must not be silently replayed.
- Derived caches are not durable truth. Dashboard projections, discovery scan
  caches, renderer previews, Graph diagnostic windows, and upload hash
  acceleration must be rebuildable from durable state, Codex history, or
  Teams/Graph state.
- Graph ingestion checkpoints, transcript import/catch-up checkpoints,
  per-chat sequence, and delivery retry/rate-limit state are durable state, not
  disposable cache.
- Bad derived cache must be ignored and rebuilt. It must not cause lost turns,
  duplicate Codex execution, duplicate Teams sends, or a skipped import
  checkpoint.
- Split long Teams output before sending. Live Graph chat sends in this tenant
  rejected HTML `body.content` at `102,290` bytes, so production output should
  be chunked well below that boundary and sent in durable outbox order.
- Per-chat rate limiting must preserve outbox order for chunks, imported
  history, artifacts, ACKs, and notifications while respecting Graph
  `Retry-After`.
- A `Retry-After` or poison message in one chat must not globally block other
  chats. Within a chat, later messages must not overtake earlier sequence
  numbers.
- Owner reminders may use a Teams self-mention, but mention targets must be
  constructed from the authenticated user record. Do not trust Codex output,
  user prompt text, or replayed history to define arbitrary mention ids or
  mention HTML.
- Helper-sent self-mention messages must keep the normal helper prefix and
  durable sent-id tracking so the bridge continues to ignore its own messages
  on the next poll.
- State schema migrations must be explicit and tested before enabling new
  product features. Notification intent, import origin, machine identity,
  outbox part metadata, and artifact metadata must be structured state, not
  parsed back from Teams HTML.
- A stale owner can be recovered only when owner metadata indicates it is gone or
  the operator explicitly uses force recovery.
- Automatic recovery may resume polling, rebuild derived indexes, flush pending
  outbox, reacquire owner heartbeat, and continue queued work that was durably
  accepted but not started. It must not rerun a Codex turn whose execution may
  have happened but whose final state cannot be proven.
- Local Codex discovery is eventually consistent. Missed filesystem events,
  polling delays, or stale dashboard caches may delay visibility, but must not
  advance import checkpoints, skip Teams inbound messages, duplicate Codex
  execution, or reorder sent transcript records.

## Background Service And Upgrade

- Foreground `teams run` is useful for testing, but it dies with the terminal or
  SSH session. Durable operation should use the explicit user-level service
  backend for the platform: Linux `systemd --user`, WSL per-user Windows
  Scheduled Task by default, macOS LaunchAgent, or Windows per-user Task
  Scheduler.
- `teams service install` writes the unit but does not enable or start it.
  `teams service enable`, `start`, `stop`, `restart`, and `disable` are explicit
  operator actions.
- On Linux systems where the user manager exits after logout, the operator may
  need OS-level linger configuration outside this tool before expecting service
  survival across full logout or reboot. Terminal close and ordinary SSH
  disconnect are only reliable while the user service manager remains alive.
- Service supervisors restart failed processes, but clean operator stops and
  upgrade drains are allowed to stay stopped. Bridge lease validation and owner
  heartbeat let restarted or standby processes resume from durable state without
  replaying completed work.
- Helper upgrade integration drains new work, waits for active work to settle,
  flushes outbox/state, stops the active service around binary replacement, and
  restarts the service after the upgrade when it was active before.
- Stable foreground exec turns cannot currently be interrupted mid-process by a
  Teams `/cancel`. Running-turn cancellation is explicitly reported as
  unsupported; queued turns can be canceled durably.

## Residual Risks

- A compromised Microsoft account or local machine can still expose messages,
  files, and tokens.
- Tenant policies, retention, eDiscovery, and Teams clients may expose messages
  outside the bridge's control.
- `Files.Read`/`Files.ReadWrite` expands what the delegated token can read or
  write; it should remain opt-in and is required only for ordinary Teams file
  references and explicit outbound file send.
- The experimental app-server runner remains optional until protocol
  compatibility and performance probes across Codex versions are complete.
- Streaming Teams output, automatic/rich outbound transfer UX, and true mid-turn
  interrupt for the stable exec runner are deferred features.
