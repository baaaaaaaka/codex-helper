# Teams history recovery and live-turn isolation checklist

This checklist is the implementation contract for the linked Codex transcript
and Teams turn failures seen after the rc8 source-proof changes.

The target behavior is:

> A history stream may pause at a narrow, explainable recovery boundary, but it
> must not make the live Teams conversation unusable. Proven history resumes
> automatically; unproven history is quarantined or requires an explicit
> recovery choice. The service never silently skips or blindly re-executes work.

## Worktree execution record

- [x] Created branch `fix/teams-history-recovery-20260821` in a separate
      worktree at `/home/baka/.local/state/codex-helper-workspaces/teams-history-recovery-20260821`.
- [x] Fetched upstream and verified both `HEAD` and `origin/main` are
      `a7bf658f1dde265399b70322e587a2933979b23d`.
- [x] Left the original `/home/baka/project/codex-helper` worktree unchanged;
      its pre-existing dirty files were not copied into this worktree.
- [x] Implemented the focused repair: history is no longer a live-turn gate,
      unresolved legacy execution gets a durable isolated live branch, normal
      large tails advance in bounded resumable prefixes, and pathological
      records are silently quarantined without repeated scans or notices.
- [x] Added JSON/SQLite parity tests, restart/durable-binding coverage, mock
      Teams-sink history tests, oversized-record parser tests, race coverage,
      and benchmark smoke coverage.
- [x] No Teams service restart, source reload, real Teams send, release, or
      prerelease was performed from this worktree.
- [ ] Full Graph uncertain-send reconciliation and a versioned legacy-data
      migration tool remain a separate follow-up; this patch preserves the
      existing outbox/ledger behavior and does not claim to solve remote
      accepted-but-unconfirmed delivery globally.

Verification recorded for this worktree:

- `go test ./internal/teams/store ./internal/teams ./internal/codexrunner ./internal/cli -count=1` — passed.
- Targeted `go test -race ./internal/teams/store ./internal/teams ...` — passed.
- `go test ./... -count=1` — passed.
- `go vet ./internal/teams/...` — passed.
- Selected linked-history/SQLite benchmarks with `-benchtime=1x` — passed.
- The broader queued-turn benchmark profile still reports a temporary-directory
  cleanup error (`directory not empty`) after the benchmark body; it is tracked
  as a fixture cleanup issue, not a functional test failure.

Docker verification recorded for the persistent debug container
`cxp-teams-source-proof-debug-rc17`:

- [x] Confirmed the container remains `network=none`, has no Teams/Codex auth,
      and is not running a Teams helper; its only long-lived process is the
      idle debug shell.
- [x] Built the new worktree binary and static `internal/teams` and
      `internal/teams/store` test binaries, placing them under an isolated
      debug-binaries directory without replacing the rc17 binary.
- [x] New binary `teams status`, `teams doctor`, and `teams service doctor`
      completed successfully; the expected no-auth/no-service diagnostics were
      reported and no message-send path was available.
- [x] Targeted live-branch, durable-binding, bounded-tail, history-watch, and
      oversized-record tests passed inside the container.
- [x] Full `internal/teams/store` and `internal/teams` test binaries passed
      inside the container after copying their repository fixture files.
- [ ] A real Graph poll/send cannot be tested in this container by design;
      end-to-end delivery remains covered by mock sinks and the host test suite.

## Scope and invariants

- [x] Work only from a fresh branch based on the latest `origin/main`.
- [x] Preserve the pre-existing dirty worktree and do not mix its changes into
      this repair.
- [x] Treat live turn admission, local history ingest, and Teams history
      backfill as separate lanes.
- [x] A history delivery problem must not be a global or conversation-wide
      turn-admission gate.
- [x] A true execution-ownership ambiguity may fence only the affected
      execution/branch; it must not cause an unrelated history backlog to queue
      every new user message.
- [x] Never advance a cursor past data that is not durably ledgered or
      explicitly represented as a quarantined gap.
- [ ] Never re-run a model turn or side-effecting tool because message delivery
      was uncertain. Existing outbox/terminal fencing is retained, but the
      complete remote-delivery audit is outside this focused patch.
- [ ] Do not claim Graph/Teams exactly-once unless the remote API provides an
      idempotency or reconciliation guarantee. The focused patch does not add
      that guarantee.
- [x] Keep source proof and execution ownership as separate checks.
- [x] Keep the normal live path bounded and free of full-transcript scans.

## Phase 0: baseline and evidence

- [x] Confirm the original worktree has unrelated uncommitted changes.
- [x] Fetch `origin/main` and record the starting commit in the worktree.
- [x] Audit the current history reader, checkpoint, outbox, ownership-anchor,
      queue, and user-notice implementations.
- [ ] Run the focused Teams history/queue tests before changing code and record
      the baseline result.
- [x] Add regression fixtures for the observed unresolved-anchor, paused-history,
      and oversized-tail shapes using a mock Teams sink; no test sends a real
      Teams message. The fixtures test the failure classes rather than relying
      on production chat IDs such as s508/s510/s511/s512.
- [x] Document which behavior already exists on `origin/main` and which gap is
      still responsible for an unresolved session remaining unusable.

## Phase 1: state model and user-visible contract

- [ ] Define distinct internal states for `healthy`, `waiting_partial_tail`,
      `deferred_budget`, `oversized_record`, `malformed_record`,
      `source_changed`, `ownership_uncertain`, `delivery_uncertain`,
      `quarantined`, `manual_recovery_required`, and `complete`.
- [x] Make `has_more`/budget exhaustion a scan result, not a checkpoint state
      that implicitly authorizes skipping.
- [x] Define the three gates explicitly:
      - history delivery gate: never blocks a new live turn;
      - execution-context gate: only affects a turn that truly needs missing
        context, with a safe snapshot or a new explicit branch;
      - ownership/source fence: affects only the proven ambiguous stream.
- [ ] Define the live turn state machine as
      `running -> final | error | cancelled`.
- [ ] Suppress stale status after a terminal result and coalesce intermediate
      status updates instead of creating notification floods.
- [ ] Choose and document the Teams presentation for late history: preferred
      separate history thread/section, otherwise an explicit history marker and
      original timestamp.
- [ ] Keep internal source paths, checkpoints, lease details, stack traces,
      secrets, and unredacted tool data out of user-facing Teams messages.

## Phase 2: live-turn admission and queue repair

- [x] Remove any admission dependency on `ImportCheckpoint.Status`,
      `SourceRewriteBlocked`, `tail_too_large`, ordinary history backlog, or
      history-notice outbox rows.
- [x] Preserve the existing same-chat running-turn serialization; only an
      actually running durable turn may make a new turn wait normally.
- [x] For an unresolved execution anchor, distinguish an actually active or
      unconfirmed same-thread execution from a stale legacy/recovery marker.
- [x] For a stale marker with no current running turn, start a safely fenced
      new attempt/branch or use the last verified context boundary. Do not reuse
      the ambiguous old execution as if it were confirmed.
- [x] Persist the branch/attempt/context boundary before dispatching the new
      turn and make old workers unable to publish into it.
- [x] If a safe new context cannot be established, keep only that turn in an
      explicit recoverable state with one actionable notice; never create an
      indefinitely repeating queue.
- [x] Ensure queued-turn draining applies the same narrow policy and cannot
      starve other sessions.
- [x] Ensure a paused history stream cannot produce `queued-wait` notices for
      every new message.

## Phase 3: bounded and resumable history scanning

- [x] Replace the all-or-nothing `tail_too_large` automatic result with bounded
      streaming progress.
- [x] Read only a snapshot range in each pass and persist the exact complete
      record boundary consumed.
- [x] For a partial final line, retain its start offset and proof, do not move
      the cursor beyond it, and retry only after growth/backoff.
- [x] For a complete oversized record, use an explicit quarantine gap; never
      truncate it silently.
- [x] Do not cross a malformed or unrepresentable record without a durable gap
      decision and an ordering policy.
- [ ] Bound bytes, records, time, memory, spool size, and retry frequency. The
      focused implementation bounds bytes/records and suppresses unchanged
      retries; explicit time/spool quotas remain follow-up work.
- [x] Detect truncate, rotate, rename, same-path replacement, and in-place
      rewrite before committing or publishing the scanned suffix.
- [x] Keep source identity, generation, prefix anchor, read-range proof, and
      checkpoint CAS in the automatic path.
- [x] Ensure a large unchanged tail does not cause a hot loop or repeated full
      parsing.

## Phase 4: durable ledger, outbox, and ownership fencing

- [ ] Use stable logical event IDs, source-observation IDs, and target-specific
      delivery keys; do not dedupe user messages by text alone.
- [ ] Keep ingest cursor, delivery frontier, and recovery-gap tasks separate.
- [ ] Commit ledger, delivery intent, and checkpoint atomically or through a
      documented equivalent WAL/recovery protocol.
- [ ] Track delivery lifecycle as `pending`, `in_flight`, `sent`, `retryable`,
      `uncertain`, `quarantined`, or `manual_recovery`.
- [ ] Advance a delivery frontier only across a continuous, gap-free prefix.
- [ ] Add persistent owner epoch/fencing checks to checkpoint, ledger, outbox,
      and terminal transitions.
- [ ] Make parent/child branches single-winner and prevent retired branches
      from sending new visible messages.
- [ ] Handle Graph accepted-but-response-lost, timeout, 429, 5xx, token
      expiry, and parent-ID-loss without blind duplicate sends.
- [ ] Reconcile a remote uncertain send where possible; otherwise stop only the
      affected delivery stream and expose an operator recovery path.

## Phase 5: legacy checkpoint migration and recovery commands

- [ ] Inventory rc8 through current checkpoint shapes, including rc16/rc17
      `blocked`, source-rewritten, incomplete, and unresolved-execution rows.
- [ ] Preserve the legacy row and create a versioned migration record/mapping.
- [ ] Provide dry-run classification and counts before changing state.
- [ ] Make migration idempotent, crash-resumable, CAS/lease protected, and
      auditable.
- [ ] Auto-resume only when source identity/generation/record boundary and
      delivery history are sufficiently proven.
- [ ] Quarantine rows with missing proof, ambiguous ownership, uncertain remote
      delivery, or no usable delivery ledger.
- [ ] Prevent old workers or old schema writers from overwriting migrated state.
- [x] Keep `helper publish-history` and `helper publish-history full` explicit,
      idempotent, previewable recovery operations; never require them for every
      new live message. The new oversized-record marker is cleared by the
      existing explicit recovery paths.
- [ ] Ensure rollback cannot restore an old cursor and replay already-sent
      Teams messages.

## Phase 6: tests and performance gates

- [x] Regression: the s508/s510/s511/s512 failure class accepts and runs a new
      message while a
      history checkpoint is paused or oversized and no durable turn is running.
- [x] Regression: repeated polls/restarts do not resend the long history-gate
      warning or create repeated user-visible recovery notices.
- [x] Regression: valid large tails eventually drain without losing final,
      status, tool, or user records.
- [x] Parser tests: partial line, no-newline tail, malformed JSON, invalid UTF-8,
      oversized complete record, file rotation, truncate, rewrite, and append
      race.
- [x] Crash-window test: a live branch is durably recorded at callback binding,
      before completion, so a restart cannot create a second fresh branch.
- [x] Concurrency tests: JSON/SQLite branch isolation, stale queued-turn
      retargeting, durable registry restart, and scanner race coverage.
- [ ] Full crash tests for ledger/outbox/Graph request, remote acceptance,
      response receipt, and sent-state commit remain follow-up work.
- [ ] Full lease/migration/parent-child race matrix remains follow-up work.
- [ ] Graph tests: uncertain create/update, retry-after, 429, 5xx, timeout,
      token expiry, and missing parent message ID.
- [ ] Migration tests: dry-run, repeat, interruption, rollback, mixed-version
      writer rejection, and old delivery-key compatibility.
- [x] UX tests: no automatic history spam, live messages are not queued solely
      by the paused history lane, and the giant-record final is not published
      past the quarantine boundary.
- [x] Performance tests: normal linked-session/SQLite idle profiles and large
      tail bounded-read smoke passed; the unchanged oversized marker avoids a
      repeated scan. A queued-turn benchmark fixture still has a TempDir
      cleanup failure (`directory not empty`) and needs fixture-level cleanup.

## Phase 7: rollout and final acceptance

- [x] Run the complete focused and repository test suites from this worktree.
- [x] Review the diff for unrelated production changes and hot-path locks.
- [x] Run a mock-sink replay of the failure classes without sending Teams
      messages.
- [ ] Enable a legacy-data migration in dry-run first and inspect
      classifications; no migration tool is included in this focused patch.
- [ ] Publish a prerelease only after the focused tests and performance gates
      pass.
- [ ] Canary the new state machine and monitor live latency, queue age, history
      lag, uncertain sends, quarantine size, duplicate suppression, and notice
      counts.
- [ ] Keep a recoverable state backup and a rollback path that does not rewind
      delivery cursors.
- [x] Focused-patch acceptance: history may be locally fenced, but no new live
      conversation is blocked by that fence; no message is silently skipped;
      no model/tool work is re-executed because delivery was uncertain; and the
      normal live path has no measured regression in the available smoke
      profiles. The global remote uncertain-send and migration items above are
      intentionally not claimed as complete.
