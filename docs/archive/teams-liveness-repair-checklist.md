# Teams service liveness repair — Phase 1 checklist

This is the executable checklist for the focused liveness repair in this
worktree. The acceptance rule is that every production change below has a
regression test and that the regression remains green in the full package,
full repository, and race runs.

This phase targets the failures observed in the recent Teams service reports:
heartbeat starvation or `SQLITE_BUSY_SNAPSHOT`, malformed unrelated runtime
metadata blocking ownership, a replacement child being restarted before its
first owner heartbeat, a claimed lease leaking when listener initialization
fails, and oversized Graph error bodies hiding the real HTTP status.

## Scope and invariants

- [x] Preserve the intentional TUI-vs-Teams active-writer behavior: a Teams
      request rejected because TUI owns the Codex thread is not automatically
      retried.
- [x] Do not change transcript/history repair, voice features, or global Graph
      attachment limits in this phase.
- [x] Keep liveness fail-closed for unknown ownership, invalid required state,
      and non-retryable storage errors.
- [x] Keep unrelated chats and the service running when one optional runtime
      row or one Graph response is faulty.
- [x] Do not advance durable state before the corresponding transaction or
      delivery operation commits.

## Workspace and source inventory

- [x] Work in the isolated worktree
      `/home/baka/.local/state/codex-helper-workspaces/teams-liveness-repair-20260827`.
- [x] Base the branch on the current `origin/main` at
      `5ef095d596d59e491f03c98c8229776d0a11f561`.
- [x] Leave the original user worktree and its unrelated changes untouched.
- [x] Trace the affected ownership, SQLite runtime, bridge lifecycle, local
      supervisor/watchdog, and Graph response paths before editing.
- [x] Keep the existing broad state, history, outbox, and migration APIs as the
      compatibility surface; narrow only the liveness operations needed here.

## SQLite liveness path

- [x] Add a cached runtime access path so owner heartbeat and lease validation
      do not wait behind `Store.mu`, the state-file flock, or a long full-state
      callback.
- [x] Share the single physical SQLite handle between runtime and normal store
      work, while serializing handle close/rebind/migration with the runtime
      mutex; this avoids cross-handle WAL snapshot conflicts.
- [x] Open a runtime-first handle without setup PRAGMAs or permission changes;
      retain schema validation and a bounded busy timeout.
- [x] Make heartbeat, owner read, lease validation, claim, release, and clear
      paths decode only the required typed runtime rows.
- [x] Fall back to `state_json` only when the required projection is
      incomplete, and overlay only ownership/lease fields so corrupt optional
      service-control, upgrade, or auto-update rows do not block liveness.
- [x] Preserve a newer owner heartbeat or lease in a concurrent full-state
      save; treat the pair of explicit null owner rows written by current
      clear paths as a durable clear, while retaining rc16/older one-copy
      compatibility.
- [x] Make `ClearOwner` use an atomic targeted owner-row clear on SQLite, so a
      stale full-state write cannot resurrect the cleared owner.
- [x] Recognize SQLite busy extended errors, including `SQLITE_BUSY_SNAPSHOT`,
      and retry the complete short heartbeat operation from a fresh snapshot.
- [x] Quiesce and close the shared SQLite handle before SQLite migration can
      replace the database inode or pointer.

## Bridge and ownership lifecycle

- [x] Arm exact machine/generation cleanup immediately after a control lease is
      claimed, including the initialization-failure path.
- [x] Clear the matching owner and release only the captured lease tuple during
      listener shutdown; do not release a later generation.
- [x] Retry only transient SQLite busy heartbeat failures; surface other
      storage/ownership failures to the listener and treat context cancellation
      as normal shutdown.
- [x] Avoid rewriting the lease on every frequent heartbeat while it has ample
      remaining lifetime; validate it read-only and refresh before midpoint.
- [x] Keep owner and lease checks fail-closed when the lease is missing,
      expired, or owned by another machine/generation.

## Supervisor and watchdog lifecycle

- [x] Identify the actual managed child, including the single-store path, and
      carry its PID/start time and supervisor ownership into the snapshot.
- [x] Apply startup grace only to the verified managed child while it is within
      the bounded owner-heartbeat initialization window.
- [x] Suppress only stale-evidence restart decisions during that grace window;
      preserve concrete administrative, test, and identity-mismatch restarts.
- [x] Keep the child health checker active during startup rather than skipping
      it; startup grace converts only the known previous-generation stale
      reasons to a no-op.
- [x] Preserve existing external/injected-child and mismatched-child behavior.

## Graph response handling

- [x] Use one bounded-plus-one JSON response reader and return a typed
      `GraphResponseTooLargeError`.
- [x] Close response bodies on all paths and avoid decoding partial JSON.
- [x] For non-2xx responses, preserve `GraphStatusError`, HTTP status, and
      `Retry-After` even when the diagnostic body is oversized or malformed.
- [x] Leave binary/attachment limits unchanged.

## Regression tests and CI

- [x] Cover heartbeat progress behind a blocked full-state callback:
      `TestRecordOwnerHeartbeatDoesNotWaitBehindFullStateUpdate`.
- [x] Cover corrupt optional runtime rows and restart/reopen behavior:
      `TestRecordOwnerHeartbeatIsolatedFromUnrelatedCorruptRuntimeRow`,
      `TestLivenessFallbackIgnoresCorruptOptionalRuntimeRow`, and
      `TestControlLeaseClaimIgnoresUnrelatedCorruptRuntimeRow`.
- [x] Cover stale full-state owner resurrection and rc16 compatibility:
      `TestSQLiteFullStateUpdateDoesNotResurrectClearedOwner` plus the existing
      rc16 fixture tests.
- [x] Cover initial owner-heartbeat failure cleanup:
      `TestBridgeListenDoesNotLeakControlLeaseWhenInitialOwnerHeartbeatFails`.
- [x] Cover oversized successful/error Graph responses and status precedence:
      `TestGraphOversizedJSONResponseIsNotMisclassifiedAsCorruptJSON` and
      `TestGraphOversizedErrorPreservesHTTPStatusAndRetryAfter`.
- [x] Cover saturated SQLite/Graph worker pressure without losing heartbeat:
      `TestTeamsOwnershipStressSQLiteHeartbeatSurvivesSaturatedGraphWorkersCI`.
- [x] Cover watchdog and local-supervisor startup grace:
      `TestTeamsRuntimeSafetyWatchdogDoesNotRestartLiveManagedChildBeforeOwnerRegistration`
      and `TestTeamsServiceLocalSupervisorDoesNotRestartChildBeforeOwnerRegistration`.
- [x] Add exact non-zero CI selectors for the liveness tests; run the
      supervisor-specific test on Linux and keep the ownership stress job on
      Linux.
- [x] Validate the modified workflow with actionlint.

## Verification record

- [x] `gofmt` and `git diff --check` pass.
- [x] Focused liveness selectors pass in `internal/cli`,
      `internal/teams/store`, and `internal/teams`.
- [x] Full affected-package test run passes:
      `go test ./internal/teams/store ./internal/teams ./internal/cli -count=1`.
- [x] Full repository test run passes: `go test ./... -count=1`.
- [x] Serial race run passes:
      `go test -race -p 1 ./internal/teams/store ./internal/teams ./internal/cli -count=1`.
- [x] The liveness benchmark runs without goroutine/OpenDB-reference growth;
      the watchdog benchmark reports bounded allocations for the existing
      snapshot path. No new full-state decode was added to the heartbeat hot
      path.
- [x] Modified workflow lint passes:
      `go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.11 .github/workflows/ci.yml`.
- [x] No Teams helper/service was restarted, replaced, or deployed from this
      worktree.

## Explicitly deferred follow-up work

These are deliberately not claimed as completed by Phase 1 and remain separate
work items because they require a larger migration or platform-specific test
contract:

- [ ] A separate per-service control database with exact-token owner/lease CAS
      and one atomic owner-plus-lease transaction.
- [ ] Full cross-process/native-manager containment, pidfd/Job Object/launchd
      contracts, crash-unknown reconciliation, and breaker redesign.
- [ ] Real disk-full, `IOERR`, fsync/rename crash-boundary, commit-unknown,
      Docker multi-process, and no-message-live-Graph tests.
- [ ] History cursor/page/record oversized quarantine, stale/cyclic Graph
      continuation recovery, and transcript source-proof changes.
- [x] Code-phase checklist completed without a service restart; commit,
      prerelease, and `main` integration are handled by the separate release
      workflow requested after implementation.
