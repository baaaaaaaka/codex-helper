# Teams Codex fork contract

This document freezes the safety contract for the native Codex fork and the
Teams history publication workflow. The fork remains no-ship until every
condition in the last section is satisfied.

## Native Codex protocol

The implementation uses the app-server `thread/fork` method. The request has
one and only one cutoff selector:

- `lastTurnId`: the last completed turn selected from the durable Teams store;
- `beforeTurnId`: reserved for callers that already have an authoritative
  boundary and must not be sent together with `lastTurnId`.

The request also sets `excludeTurns`, `deferGoalContinuation`, and
`ephemeral=false`. The child response is decoded from the native thread
envelope. `createdAt` and `updatedAt` are Unix-second timestamps; RFC3339 and
integer strings are accepted only as compatibility input for older fixtures.
The native thread relationship is `forkedFromId`; `forkedFromTurnId` is not a
protocol proof and is never required for recovery.

The app-server fork call is not assumed to be idempotent. If the request may
have reached Codex but the response is lost, the caller must not retry blindly.
Recovery may adopt a child only when all of the following hold:

1. the candidate is within the durable fork creation window;
2. the candidate points to the fenced parent thread;
3. `thread/read` proves the durable cutoff turn is the candidate's latest
   completed turn; and
4. exactly one candidate passes the proof and every potential candidate was
   readable.

Zero, multiple, or incompletely readable candidates remain
`blocked_ambiguous`.

## Durable phase contract

| Phase | Parent fence | Recovery meaning |
| --- | --- | --- |
| `requested` / `parent_fenced` | held | cutoff and manifest work may resume |
| `snapshot_materialized` | held | `NativeForkIntentAt` distinguishes before-request from unknown boundary |
| `codex_forked` / `child_chat_staged` / `history_publishing` | held | resume only the next durable external step |
| `history_verified` / `activated` | held | child is not exposed until the parent link is durably sent |
| `link_sent` | released | successful terminal state |
| `failed` | released only for pre-external, non-ambiguous failure | terminal failure |
| `blocked_ambiguous` | held | requires authoritative reconciliation or manual handling |
| `abandoned` | explicit policy decision | terminal cancellation with the recorded reason |

`activated` is deliberately not terminal. A process can restart after child
activation and before the parent link is sent; recovery must continue that
operation while the parent remains fenced.

Every fork mutation uses the current control-lease holder and generation as a
compare-and-swap owner. The owner is checked before and after external work.
An old owner cannot advance the operation after takeover.

## History and Graph contract

The history manifest is immutable and records the source transcript fingerprint,
cutoff record/line, byte offsets, prefix hash, ordinal, role, body hash, and
chunk metadata. The visible publication order is fixed:

```text
history chunks -> history-complete marker -> child activation -> parent link
```

Each chunk has a stable outbox ID, ordinal, part count, and rendered body hash.
The helper sends the parent progress message without a child URL. It releases
the URL only after the marker and every history item are durably proven sent.
While the parent is fenced, only read-only `status`, `stats`, `details`,
`help`, and `default` commands are accepted; all other helper mutations are
rejected or deferred.

For an uncertain Graph response, retry is allowed only after a provenance
marker identifies the same operation/outbox item. That result is recorded as
`duplicate-settled` with the original Graph message ID. A Graph message that
is merely similar, or is not yet visible through the read path, is not proof.

## Crash boundaries

| Boundary | Required recovery |
| --- | --- |
| before `NativeForkIntentAt` | return to `parent_fenced`; native fork was not allowed to start |
| after intent, before child record | block ambiguous; reconcile native child by the proof above |
| after child/chat creation, before local checkpoint | keep the parent fenced and reconcile by operation ID/external ID |
| during history outbox send | recover the existing outbox row; do not create a second visible item without provenance proof |
| after `history_verified` or `activated` | resume marker/link delivery; never expose the URL early |
| after lease takeover | stale owner writes and external continuations must fail owner validation |

## No-ship conditions

Do not ship this feature if any of these are true:

- a real numeric native Thread response cannot be decoded;
- a lost native response can silently bind the wrong child;
- stale lease owners can mutate the operation or perform external work;
- `activated` recovery can release the parent fence before the link proof;
- Graph response loss can create duplicate visible history without provenance;
- the Docker smoke, race tests, `go vet`, or the complete Go test suite fails;
- performance review finds a new unbounded transcript read or full-state rewrite;
- credentialed live Codex/Graph behavior has been assumed rather than verified.

The Docker harness intentionally uses deterministic fakes and no credentials.
The single-container smoke is complemented by
`scripts/ci/teams_codex_fork_compose_smoke.sh`, which runs the durable state
and ordering fault matrix across JSON and SQLite stores. Each case uses
`sut-a`, `sut-b`, a fake Codex service, a fake Graph service, a controller, and
an observer on an internal Compose network. The matrix covers the happy path,
lost native response, lost Graph response, restart after activation, and
owner takeover. It is a release safety gate for deterministic state and
ordering, not a substitute for a separately authorized live integration test
against credentialed Codex and Graph services.
