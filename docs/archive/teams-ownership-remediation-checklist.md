# Teams ownership, recovery, history, and liveness remediation

This checklist is the execution record for the remediation work in the dedicated
workspace `teams-ownership-remediation-20260824`, based on the latest
`origin/main`.  Each item must have an implementation and a regression test (or
an explicit documented reason why a test is not applicable) before completion.

Final scope decision for this iteration: cover user-reachable poll/history
combinations with deterministic protocol-level stress tests and make only
small fixes required by those tests. The existing durable migration,
execution, and attachment contracts were audited and retained; a new
universal protocol would be over-design for these failures. Items below
marked deferred are real boundaries that require process/release/E2E tests,
not claims that the unit suite proves them.

## 0. Baseline and invariants

- [x] Record the base commit, worktree path, and clean-status proof: base and
      `origin/main` are `1120e9f28ca0abfa98325d1799bc6c894f9de378` in branch
      `work/teams-ownership-remediation-20260824`.
- [x] Map the existing Teams, store, history, migration, CLI, and CI tests to
      the failure scenarios found by the ownership stress review.
- [x] Define the safety invariants: no automatic replay after an uncertain
      external side effect; no consumed-but-unpublished transcript record; no
      single-chat failure blocking unrelated chats; no cursor advance past an
      active inbound claim; no silent deletion of a deferred frontier.
- [ ] Validate stale callback fencing and old-writer rejection across actual
      processes; Docker now covers a real writer lock, graceful exit, and
      SIGKILL takeover, while delayed callbacks and old-binary boundaries remain
      deferred to E2E.
- [x] Run the focused baseline tests and preserve their result in the handoff;
      the Teams/store focused baseline passed before implementation.

## 1. Durable identity and state contracts

- [x] Separate logical `intent_id`, execution/post `attempt_id`, observation
      identity, and provider IDs in the durable model.
- [x] Add explicit execution states and transitions, including the distinct
      `execution_uncertain` fence and operator-resolvable outcomes.
- [x] Add explicit outbox states: armed, post-started, sent/accepted, and
      uncertain; recovery must resume only an armed attempt and must never
      automatically POST an uncertain attempt.
- [x] Make execution result and final outbox intent one durable commit (or an
      equivalent atomic journal) and rebuild missing intents without rerunning
      Codex.
- [x] Add lane ownership so execution admission, execution finalization,
      history, artifacts, and control/recovery do not share one global block.
- [x] Preserve the positive safety behavior that a TUI-owned Codex writer does
      not cause Teams to silently retry the same request.
- [x] Add unit and crash-boundary tests for every transition and invariant;
      existing CAS/crash tests plus the new stress suite cover these contracts.

## 2. Root epoch, leases, and stale callbacks

- [x] Preserve the existing root epoch/lineage, control-lease generation,
      inbound claim token, and terminal-callback CAS contracts.
- [ ] Add owner tokens/row revisions to every asynchronous poll-schedule
      aggregate and reject a delayed old-owner poll result after takeover. The
      current patch protects inbound claims and terminal callbacks, but does
      not claim a complete poll-state lease-generation CAS protocol.
- [x] Make stale inbound claims, terminal callbacks, reclaimed control leases,
      deleted rows, and same-owner ABA cleanup harmless no-ops in the covered
      store paths.
- [x] Validate protocol compatibility before schema/WAL initialization or any
      state write, and reject tokenless/unknown legacy mutations safely.
- [ ] Run actual two-process takeover tests where the old worker loses its
      lease, its executor is still unwinding, and a delayed poll/state callback
      races the new owner. The deterministic local CAS tests are not a
      substitute for this process boundary.

## 3. Migration and upgrade safety

- [x] Add a supervisor migration barrier covering coordinator, history, outbox,
      ledger, Codex child, and every runtime database handle.
- [x] Implement a durable migration phase journal with fail-closed states and
      explicit adoption/reconciliation acknowledgement.
- [x] Import JSON state through a validated temporary SQLite database; for
      SQLite input, close runtime handles and use the SQLite backup API without
      copying live WAL/SHM files.
- [x] Make the manifest/object digest and pointer publication crash-safe; never
      select state by mtime and never delete legacy data before adoption and
      reconciliation.
- [x] Keep the current RC16/legacy fixture migrations and upgrade-only schema
      checks passing; schema 8 deliberately refuses writes from an older
      helper that cannot preserve the new safety fields.
- [ ] Run an actual old/new release-binary matrix (including RC16 and the
      current prerelease) against one pointer and SQLite/WAL directory. The
      fixture tests do not prove that two installed binaries can safely run at
      the same time.
- [x] Exercise deterministic failure injection at migration phases, rollback,
      re-adoption with a rotated root epoch, and external-side-effect
      non-rollback.
- [ ] Exercise real helper-process SIGKILL/restart with live WAL/SHM and
      pointer files. The Docker boundary now covers SIGKILL takeover and a real
      filesystem-full SQLite retry, but not a full helper restart with live
      pointer/WAL/SHM files.

## 4. Canonical transcript scanning and history recovery

- [x] Route watch, tiered, linked, full, canceled, recovery, and backfill paths
      through one bounded scanner/result contract.
- [x] Correctly handle complete JSON at EOF without a newline, partial records,
      truncation/rewrite, source generation changes, and malformed middle gaps.
- [x] Persist source identity/generation, safe physical offset, continuity/tail
      state, and CAS revision; reset parser/dedupe state on a new generation.
- [x] Separate source generation from root epoch and use producer proof where
      available; legacy sources receive a one-time quarantine/rescan path.
- [x] Treat the record limit as a streaming per-record quarantine limit and the
      tail limit as a bounded page budget.  Oversized records must advance a
      quarantine cursor and retain a recovery pointer rather than blocking all
      later status/final records.
- [x] Commit physical progress and history publish intent atomically; a scan
      with zero publishable records must still persist safe progress.
- [x] Isolate one source/chat error from unrelated sources/chats.  History
      uncertainty must never become an execution-admission block.
- [x] Add regressions for s512 pending continuation livelock, s514 oversized
      image record, s519 large non-text tool record, base64 image inflation,
      valid EOF, rewrite/truncate, and consumed-but-unpublished crash windows.

## 5. Graph polling and scheduler liveness

- [x] Replace serial/unbounded Graph work with a bounded I/O executor and
      reserved control/poll capacity; enforce per-chat single flight and queue
      limits/coalescing.
- [x] Bound the full Graph operation (auth, refresh, connect, TLS, headers,
      body, retry, and backoff), persist retry timing, and always release
      permits on timeout/cancellation.
- [x] Apply per-chat quantum/page budgets and continuation cursors so one busy
      chat cannot starve other chats or its own later pages.
- [x] Make nextLink/continuation failures structured retryable or unrecoverable
      gaps; retain gap evidence and allow the readable head to continue.
- [x] Keep parked chats visible, probe them periodically or on a new-message
      wake, and atomically re-admit them after a successful head probe.
- [x] Limit restart/drain escalation to control, tenant-wide, or all-eligible
      chat failures; do not let one chat or outbox stall stop polling globally.
- [x] Add deterministic blackhole, delayed Graph, large backlog, expired page,
      rate-limit, multi-day outage, parked-chat wake, stale parked continuation
      with a new message, repeated nextLink, active-claim cursor fencing,
      action-limited continuation, control-reply stall, worker-wave fairness,
      and mixed TUI/Teams tests.
- [x] Assert definite progress or deferral within the configured deadline, not
      merely eventual completion.

## 6. Attachment and local recovery paths

- [x] Ensure every Teams/Drive/artifact POST path creates a durable intent and
      attempt before the first external side effect.
- [x] Reconcile unknown Drive/Teams results with GET and artifact identity;
      never re-upload or re-POST solely because a local response was lost.
- [x] Keep armed/uncertain staging data until reconciliation; dropping local
      state never implicitly deletes a remote object.
- [x] Provide offline, CAS-protected, audited observe/resolve/retry/drop
      controls and keep them usable while a chat is blocked.
- [x] Add crash oracles for upload, POST, local commit, and provider-accepted
      but response-lost boundaries.

## 7. Regression suite, performance, and CI

- [x] Port the relevant ownership-stress scenarios into tracked clean-worktree
      tests with an explicit strict CI gate; retain the TUI-active no-retry
      positive test. Local runs remain diagnostic unless the strict environment
      variable is set so existing flaky/optional pressure diagnostics do not
      destabilize ordinary package tests.
- [x] Add a unified test manifest and make empty selectors fail rather than
      silently passing.
- [x] Cover the existing deterministic migration, history, outbox, attachment,
      Graph-liveness, inbound-ledger, and upgrade/recovery scenarios in the
      repository suites.
- [ ] Add the actual old/new binary, real Codex app-server / TUI, and full
      Bridge-level disk-ENOSPC integration suites. The release workflow already
      runs the disposable old-release upgrade matrix; this package's CI now
      adds Docker OS-level SIGKILL and real filesystem-full store boundaries,
      but those do not prove two binaries can concurrently use one live
      pointer/WAL/SHM directory or complete an end-to-end Bridge poll under
      ENOSPC.
- [ ] Run production-scale 100k-message and p95 Graph/body/SQLite benchmarks.
      Static inspection and the bounded deterministic tests show no new
      unbounded scan or hot-path global lock, but they are not a performance
      measurement at that scale.
- [x] Run the repository's CI-equivalent checks and targeted race/stress tests
      with `CXP_RUNTIME_DISABLE=1` where required.
- [x] Re-run every exposed failure scenario and record the evidence that the
      fix addresses the failure rather than merely changing its label.

## 8. Final review and handoff

- [x] Inspect the complete diff for scope contamination and accidental changes
      outside this worktree.
- [x] Verify the original worktree remains unchanged.
- [x] Reconcile all pending migration/ledger/state invariants and document any
      residual risk or intentionally deferred item.
- [x] Summarize tests, performance results, workspace/branch, and follow-up
      release steps.  Do not restart or modify the running Teams helper here.

## 9. Remaining test gaps (explicit follow-up, not hidden assumptions)

- [ ] Poll-state lease-generation CAS and cancellation of an executor after
      its service lease is lost; current inbound/terminal CAS coverage does not
      prove this aggregate-level behavior.
- [ ] Old RC16/current-release binaries sharing the same pointer, SQLite,
      WAL/SHM, and registry files, including an old writer arriving after the
      new helper has upgraded the store.
- [ ] Full Bridge-level disk-full/ENOSPC and restart with live WAL/SHM. Docker
      now proves a real process SIGKILL can be followed by safe takeover and
      that a real tmpfs-full deferred-frontier write remains retryable; it does
      not exercise a complete Graph poll cycle while the filesystem is full.
- [ ] A real Codex app-server plus TUI transcript append/listener race, rather
      than the deterministic fake-writer and transcript-file models.
- [ ] Permanent stale Graph continuation recovery/rebase policy for three or
      more simultaneous frontiers, including an A→B→A token cycle and a
      Graph-visible backlog beyond the provider's retrievable message window.
- [ ] Production-scale latency and memory measurements for large backlogs,
      large tool/image records, and concurrent Graph blackholes.

## Execution evidence

- `CXP_RUNTIME_DISABLE=1 go test ./internal/teams ./internal/teams/store -count=1`
  passed after the final changes (`teams 266.293s`, `store 27.533s`).
- `CXP_RUNTIME_DISABLE=1 go test -race ./internal/teams ./internal/teams/store -count=1`
  passed (`teams 420.549s`, `store 132.284s`).
- `CXP_RUNTIME_DISABLE=1 CODEX_HELPER_TEAMS_OWNERSHIP_STRESS_STRICT=1 go test ./internal/teams -run '^TestTeamsOwnershipStress' -count=1 -v`
  passed in 8.579s; all 38 top-level ownership-stress tests passed.
- `bash scripts/ci/teams_ownership_stress_docker_smoke.sh` passed in 4.36s;
  the same 38 strict ownership-stress tests passed inside a scratch Docker
  image with no network, read-only root, and bounded `/tmp`.
- `bash scripts/ci/teams_runtime_takeover_process_smoke.sh` passed in 14.4s;
  Docker covered graceful real-writer exit, real-writer SIGKILL takeover, and
  a real 32 MiB tmpfs filesystem-full SQLite deferred-frontier retry/reopen.
- `python3 -m unittest scripts/tests/test_ci_targeted_shards.py` passed all 10
  tests, including strict-gate and selector-freshness checks.
- `git diff --check` passed.
- A repository-wide `go test ./... -count=1` run passed every package in scope,
  including `internal/teams`, `internal/teams/store`, and
  `internal/helperruntime`. The `internal/helperruntime` launcher test must not
  inherit `CXP_RUNTIME_DISABLE=1`, because that marker intentionally makes the
  launcher return `handled=false`; source-binary and Teams tests used the marker
  only where required.
- The persistence/performance audit completed with the existing history-watch
  and SQLite profile benchmarks. The unchanged-tail benchmark performed no
  disk writes in either worktree; JSON and SQLite allocations stayed within
  measurement noise and no new unbounded scan or write amplification was
  observed. Production-scale 100k-message and p95 measurements remain an
  explicit follow-up gap below.
- The checklist intentionally separates deterministic protocol coverage from
  release-CI/E2E boundaries; the remaining gaps above are explicit follow-up
  work, not silently treated as passed.
