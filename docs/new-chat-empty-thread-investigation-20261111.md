# New chat, installation aliases and copied-message receipts

Base: `aa03f9ec6ad90b5bfcbda0cc5c8f53472bad7e9b`, inspected 2026-11-11.
Worktree branch: `investigate/new-chat-empty-thread-20261111`.

## Repairs

- New chat: the helper leases a backend; the native TUI creates its thread.
  The helper subscribes before returning the endpoint and binds the native
  `thread/started` ID before reading subsequent input or approval events.
  No pre-created empty thread, resume handoff, hidden turn, retry delay or
  rollout-file modification is needed. Creation failure closes the unbound
  conversation; release remains idempotent. A legacy helper returning a
  pre-created ID is rejected and its lease released, preventing a different
  TUI thread from silently escaping helper ownership.
- Installation: a resolving symlink is healthy. Only `ELOOP` invokes the
  existing `.prev` repair. Dangling links and permission errors retain their
  meanings. General path-identity comparisons are unchanged.
- Receipts: a normal message link identifies the current chat/message.
  Copied receipt text and old markers cannot establish this identity.
  The receipt label links to its own message; its displayed text, separator,
  original HTML, code order and existing attachment handling are preserved.
  HTML tokenization recognizes normalized attributes without depending on
  comments, local attempt records or visible-text guesses.

The message-link format follows [Microsoft's documented chat message links](https://learn.microsoft.com/en-us/microsoftteams/platform/concepts/build-and-test/deep-link-teams).
The combined fake-Graph regression stores the applied receipt, loses its PATCH
response, strips comments, reserializes the stored HTML, recreates the adapter,
and retries. The external message remains unchanged and the receipt PATCH
count stays at one. This is not a live Teams-client roundtrip claim.

## Regression evidence

The original alias and quoted-receipt implementations fail the new regression
cases. The original New chat implementation also fails the real TUI acceptance
flow with `thread/resume ... no rollout found`. Tests against the fixed helper
reach native input, complete real native turns using an offline model fixture,
and restore the same persisted thread.

The native probe records the zero-input resume limitation separately from
product acceptance. Both Codex 0.166.0 and 0.167.1 can read metadata but reject
zero-input resume. Supporting that resume operation is not a CXP test assertion.

`TestRealCodexNewChatTUI` runs an isolated helper, the actual CXP selector's New
chat action and the native Codex TUI under a PTY. It checks empty disconnect,
two simultaneous windows and directories, first-turn native approval ownership,
first and second responses, persisted user turns, helper/thread identity and
resume. Every native app-server must belong to the helper. Only Microsoft Graph
and Responses are local fakes; no user input or history is pre-seeded.

Run native compatibility checks with:

```sh
bash scripts/ci/run_shared_codex_contracts.sh /absolute/path/to/native/codex
```

Compilation is inert on the host. All native processes run inside disposable,
network-disabled Docker containers with fresh state and dummy tokens. The CI
job pins both affected versions; the Linux release monitor checks new versions.
Missing native binary, required PTY, helper binary or acceptance-test name fails
that job. Failure logs include terminal output, helper diagnostics and model
request information. Ordinary CI needs no private Graph captures or credentials.

## Existing migration and unresolved clipboard behavior

The base already contains aa03f9ec's legacy token JSON migration repair. Keep
its tests for extra fields, exact token values, atomic rollback, read-only
operations and repeat opens without resurrecting archived credentials. No new
compatibility layer is introduced.

Private source/destination Graph captures and the first inbound snapshot place
the displaced code at the destination's end before this request's receipt edit.
The source message retains the original code position. The checked-in reduced
HTML fixture preserves that structure without private chat IDs or conversation
content. Renderer, readback and annotation tests preserve the supplied order.
They cannot establish whether the Teams clipboard/client moved the code. No
renderer modification or claim that clipboard movement is fixed is included.
The new receipt link still needs a live Teams save/readback check before this
candidate is promoted to a stable release.

## Cost and scope

The new binding writes one thread row once per new native thread. Streaming
notifications perform only the method check; no transcript scan or growing JSON
rewrite is added. Receipt retries tokenize one bounded message and perform no
PATCH after external identity is found. No table, polling loop, compatibility
state machine or second event relay is added. Golden HTML changes replace the
old receipt comment with the current message link; they do not accept changed
visible conversation text. The running host helper is not modified.
