# Teams transcript recovery state-machine fix checklist

Workspace: test/teams-recovery-state-machine-20260826

Base: 6315796b3998932259f21b49725215aa32ca8b04

Scope: fix the linked-transcript pending-root race and the safe-prefix/quarantine suffix leak. Keep history-only ambiguity separate from live Teams execution, preserve existing TUI active-writer behavior, and do not touch the running helper.

## 0. Baseline and safety

- [x] Work only in the isolated workspace.
- [x] Preserve the pre-existing test-only changes:
  - internal/teams/ownership_boundary_linux_test.go
  - scripts/ci/teams_runtime_takeover_process_smoke.sh
  - internal/teams/recovery_cross_system_combinations_test.go
  - internal/teams/transcript_recovery_state_machine_test.go
- [x] Confirm the three diagnostic tests are red before production changes:
  - pending-root release across polls;
  - pending-root release across SQLite/reopen;
  - safe-prefix delivery followed by history quarantine.
- [x] Confirm unrelated focused tests are green enough to establish the expected baseline; the liveness case passes while the three recovery diagnostics fail.
- [x] Do not restart, reload, replace, or background any Teams helper.

## 1. State-machine contract

- [x] Keep the physical transcript cursor separate from the semantic history frontier.
- [x] Treat history ambiguity as history-only; it must not create UnresolvedExecution, a user-visible recovery notice, or a live Teams admission block.
- [x] Allow safe records strictly before a frontier to be delivered exactly once.
- [x] Prevent every record at or after a non-context ownership frontier from reaching dedupe, delivery ledger, outbox, or Graph.
- [x] Keep explicit publish-history and publish-history full as the recovery path; automatic filtering does not affect them.
- [x] Preserve TUI-vs-Teams active-writer rejection, source-rewrite/legacy/context-gap/parent/generation fences, and existing outbox ownership rules.

## 2. Non-crossing automatic cursor

- [x] Enforce the frontier bound in both bridge flow and checkpoint persistence/CAS.
- [x] For a pending-root marker-only poll, allow progress only through the complete task_started marker end.
- [x] For any other non-context quarantine, advance the physical cursor only to the last complete scan newline while retaining the exact bounded semantic range; never deliver the suffix.
- [x] Never persist scanner EOF/Consumed as a semantic frontier when it lies after the active frontier.
- [x] Hold the witness and every suffix record out of dedupe, delivery ledger, outbox, and Graph until release succeeds.
- [x] Ensure repeat polls are silent and do not advance the cursor across the semantic suffix; pending-root state remains at the marker end.
- [x] Never automatically rewind a legacy/crossed cursor; keep it quiet and route repair through explicit bounded recovery.

## 3. Durable pending-root proof

- [x] Reuse HistoryPendingRange; do not add a new SQL table or a second state machine.
- [x] Add only required additive checkpoint fields for source path, known zero offset, marker identity, and marker owner.
- [x] Persist the actual raw marker record ID, one-based line, exact start/end offsets, source path, source generation, and bounded raw-range fingerprint.
- [x] Require a pending-root checkpoint cursor exactly at the marker end.
- [x] Restore scanner pending-root state only after validating path, generation, bytes, newline boundary, record identity, line, offsets, and owner.
- [x] Make missing/invalid proof loadable but present-and-unusable, silent, and ineligible for automatic delivery.
- [x] Ensure marker, prompt, and final visible in one snapshot first install durable marker state before any publication decision.

## 4. Release witness

- [x] Add a transient typed release-witness result; never treat the witness itself as a deliverable record.
- [x] Require a complete newline-bounded raw canonical user record strictly after the marker.
- [x] Reject internal/context envelopes, partial JSON, source replacement, generation/path mismatch, child/different-ID/ambiguous evidence, and prompt-before-marker.
- [x] Bind the marker turn ID to the existing durable InboundEvent with source teams and its Turn CodexThreadID/CodexTurnID relation.
- [x] Support real response-item prompts without a prompt turn ID only through that marker-owner plus durable Teams-turn proof.
- [x] Reject same-thread TUI or unbound prompts even when text/hash matches.
- [x] Clear only the matching pending-root lineage; never clear unrelated quarantine, runtime execution ownership, source-rewrite/legacy, context-gap, parent, or generation fences.
- [x] Do not use parser-inherited IDs, text, hash, or thread-only matching as release proof.

## 5. Semantic-only clear CAS

- [x] Use the existing parent-fenced checkpoint update with a complete expected-snapshot comparison; explicit masks were not added because the release callback constructs a semantic-only next snapshot and preserves every unrelated field.
- [x] Support semantic-only updates without cursor movement and with a valid cursor at offset zero.
- [x] Compare the complete expected checkpoint, including partial-tail, source, generation, anchor, fence, quarantine, pending-range, terminal, final provenance, and new recovery fields.
- [x] Distinguish applied, already-same, and conflict results; stale no-op does not count as release.
- [x] Revalidate source path/generation/prefix/range proof immediately before the CAS.
- [x] Clear only matching history-only semantic fields; clear terminal/final provenance only when explicitly proven to belong to the replaced lineage.
- [x] Preserve unrelated terminal/final state and all runtime/source/context/parent fences.
- [x] On conflict or ENOSPC, create no witness/suffix ledger, outbox, Graph call, or cursor mutation.
- [x] After successful clear, discard the scan and rescan from the unchanged cursor with fresh source proof.
- [x] Do not redesign ordinary outbox/ledger transactions.
- [x] Verify the crash-equivalent boundaries covered by atomic CAS/reopen, conflict, duplicate-delivery, and ENOSPC tests: before clear records remain held; after clear records are recoverable exactly once.

## 6. Unified frontier and automatic-path audit

- [x] Add one O(1) typed frontier helper with explicit present/usable/crossed state, offset, owner, kind, generation, and path.
- [x] Treat offset zero as valid and distinguish it from absent.
- [x] Use the helper in linked reads, filtering, final attribution, active-turn status/backfill, recent-completed tail, automatic import/resume, and completion recovery/error fallback.
- [x] Preflight frontiers before setting automatic import status or queuing import-title/status output.
- [x] Audit HistoryWatch overlap: linked transcript release remains in the linked path, invalid-proof watcher state is silent and automatic-inert, and unlinked watcher state retains its existing behavior. No second linkage protocol or new atomic lease was added because that would be unrelated overdesign; existing store/session guards and the linked-path tests cover the duplicate-publication boundary.

## 7. Legacy and explicit recovery

- [x] Define usable, invalid/untrusted, crossed, and absent proof dispositions.
- [x] Make invalid optional semantic proof loadable in JSON and SQLite without aborting store startup or multi-session loads.
- [x] Preserve hard errors for malformed row identity, foreign session IDs, and unrelated corruption.
- [x] Keep invalid/untrusted checkpoints automatic-inert and live-session-safe.
- [x] Make explicit history recovery replay a crossed range from its stored inclusive frontier/marker boundary after fresh source proof.
- [x] Keep automatic polling from rewinding or guessing an old boundary.
- [x] Test JSON and SQLite behavior identically across reopen and migration.

## 8. Required regression and stress tests

- [x] Turn the three diagnostic tests green:
  - [x] marker-only poll, unchanged poll, prompt, final;
  - [x] same flow across JSON-to-SQLite migration and reopen;
  - [x] safe prefix followed by quarantine suffix, including repeat poll.
- [x] Assert exact marker range identity, proof, cursor position, and release clearing.
- [x] Assert no suffix delivery record, ledger row, outbox row, or Graph call.
- [x] Add same-snapshot marker/prompt/final coverage.
- [x] Add partial prompt before and after newline completion through the scanner and recovery-path regressions.
- [x] Add prompt-before-marker, child-before/after-prompt, internal/TUI-like/unbound same-text, mixed-ID unrelated quarantine, offset-zero, and source replacement cases.
- [x] Add deterministic stale-CAS barriers; require fresh rescan and zero held-suffix side effects.
- [x] Cover crash-equivalent before/after-clear and ENOSPC semantic-clear behavior through atomic persistence, reopen, conflict, and disk-full tests; no unsafe process-crash hook was needed.
- [x] Reopen with an empty registry through the real restore path; prove live Teams execution remains usable.
- [x] Add completion-recovery/error fallback tests proving no final is selected behind a frontier.
- [x] Audit HistoryWatch linked/unlinked behavior against the existing backend coverage; no new linkage state machine was introduced.
- [x] Add crossed legacy checkpoint explicit-recovery tests.
- [x] Retain and rerun S462, tail/oversized/invisible, Graph 429/unknown, TUI/Teams, drain/restart, disk-full, concurrency, race, and Docker coverage.

## 9. CI and performance

- [x] Track all new regression test files in the workspace.
- [x] Add anchored focused selectors for recovery, persistence, negative witnesses, and performance.
- [x] Add a go test -list assertion so a zero-match selector fails.
- [x] Run focused tests normally and under race.
- [x] Run package/full tests and the bounded Docker smoke.
- [x] Verify unchanged trusted-EOF and unchanged pending-frontier polls perform zero checkpoint/revision/outbox/Graph writes through the existing no-op assertions and stress cases.
- [x] Verify bounded proof reads and O(1) frontier lookup.
- [x] Measure allocations/write counts rather than relying only on wall-clock thresholds.
- [x] Apply an Amdahl-law review to the recovery job: the dominant avoidable serial fraction was repeated package compile/link and process startup, not the test assertions themselves.
- [x] Compile each manifest package once per mode and retain one isolated process/watchdog per test entry; this removes repeated build work without grouping tests or weakening failure isolation.
- [x] Cache package source parsing during selector validation and combine package selectors into one `go test -list` call per package; retain exact-name validation for every manifest entry.
- [x] Bound manifest process-tree cancellation and cleanup on Unix, Windows, and unsupported targets; test the descendant-kill path without changing production listener timing or race-detector settings.
- [x] Run the already-isolated manifest processes through a CPU-bounded worker pool (maximum four workers); keep one binary, watchdog, JSON validation, and process tree per entry, and serialize only console writes.
- [x] Isolate the SQLite/Graph stress fixtures that are intentionally timing-sensitive from unrelated full-package tests, and add finite test-only scheduling margins/cleanup for hosted Windows and race runners without changing assertions or production timeouts.
- [x] Replace the timeout-only ledger-fairness budget increase with one-transaction outbox fixture seeding, restore the 15-second manifest budget, and retain the same bounded page, no-poison-POST, and healthy-tail assertions.
- [x] Seed the sender-only predecessor fixtures in one transaction before SQLite migration, restore the original 10-second manifest budget, and retain the short retry-gate and no-duplicate-POST assertions.
- [x] Isolate the non-SQLite CXP performance external-scenario fixture after Ubuntu full race evidence showed the same listener-takeover timing interference in its three scenario subtests; keep all scenario assertions and exact-once shard coverage unchanged.
- [x] Start ownership-stress scenario deadlines only after file/SQLite/session setup and Graph fixtures are ready; keep setup outside the liveness budget so a slow hosted runner cannot report that the scenario never reached its barrier.
- [x] Remove the global 90-second ownership-stress minimum; retain the original short Unix operation budgets and the previously established finite Windows margin after separating setup from scenario execution.
- [x] Keep the live TUI catch-up fixture just over the eight-record production cycle boundary, rather than using dozens of paced sends; preserve the concurrent append, later rescan, exact-once, and checkpoint-progress assertions.
- [x] Isolate the real continuous ambiguous-outbox recovery test from unrelated package startup/maintenance contention while preserving its no-POST-before-continuation assertion.
- [x] Audit the historical 90–180 second listener watchdogs against the actual fixtures; remove the generic busy-runner budget from short JSON/SQLite scenarios and retain long budgets only for the six-row reopen and 64-page durable-frontier cases with observed slow persistence.
- [x] Isolate the formerly timing-sensitive listener scenarios before tightening their liveness waits; keep the production phase/worker budgets where the test covers production scheduling, and use finite 20-second assertion stages for the small stateful, fairness, malformed-state, and task/prompt fixtures.
- [x] Align each recovery manifest watchdog with the sum of its intentional bounded stages; do not leave an external 15–45 second watchdog shorter than an internal 60–90 second assertion, and keep the exact-once, checkpoint, cursor, and no-manual-gate oracles unchanged.
- [x] Keep the one-generation owner-loss harness separate from `Bridge.Listen`'s automatic next-generation startup recovery, and verify the handoff under `GOMAXPROCS=1` so scheduler ordering cannot rewrite the turn before the stale-worker assertion.
- [x] Do not introduce full transcript scans/hashes, new ownership leases, new outbox systems, or Cartesian test duplication.

## 10. Final acceptance

- [x] All focused diagnostic tests pass.
- [x] All relevant package, full, race, and Docker tests pass.
- [x] No CI selector matches zero tests.
- [x] No hot-path write, allocation, or unbounded-read regression is observed.
- [x] No user-visible history gate is emitted for the normal pending-root race.
- [x] No live Teams request is blocked by history-only quarantine or invalid proof.
- [x] No hidden suffix is delivered or queued.
- [x] Review the final diff for unrelated changes and preserve the original worktree.
- [x] Record remaining limitations: pre-existing crossed legacy/source-untrusted state requires explicit recovery; automatic code never guesses or rewinds it.

## Implementation notes and evidence

- The physical file cursor may move past opaque or malformed bytes only to a complete newline, while the semantic frontier remains bounded and suppresses every suffix record. A pending-root marker is stricter: its cursor remains at the marker end until a source-bound release witness is proven.
- A normal `task_started` followed by a delayed user prompt is released only after the prompt is complete and the durable Teams inbound turn proves ownership. The same-snapshot case and the cross-poll/reopen case are both regression tested.
- Invalid optional proof is retained as an automatic-inert, history-only condition. It does not emit the old recovery notice or block a live Teams turn; explicit bounded history recovery remains the deliberate repair path.
- Release is a parent-fenced semantic-only checkpoint CAS. The latest ledger-write path preserves `HistoryRootReleased`; this is covered by the mixed-ID cross-poll regression that originally exposed the re-blocking bug.
- Verification completed in this workspace:
  - `go test ./... -count=1`
  - full `internal/teams` race run and focused recovery/store race runs
  - `go vet ./internal/teams ./internal/teams/store`
  - `scripts/ci/teams_runtime_takeover_process_smoke.sh`
  - `scripts/ci/teams_ownership_stress_docker_smoke.sh`
  - focused benchmark runs for warm no-change scans and checkpoint reads
- The benchmark comparison showed no meaningful hot-path regression: warm no-change allocations remained unchanged, the linked read stayed within measurement noise, and the tiered scan was slightly faster in the repeated run. `actionlint`/`yamllint` were unavailable locally; the CI selector was validated with `go test -list` and matched 19 recovery tests.
- The Windows source-identity path now relies only on the mandatory `GetFileInformationByHandle` result. It uses the handle's creation and last-write times, avoiding an optional `FileBasicInfo` query that can fail under the Windows race-test environment and incorrectly erase source proof. `state_file_stamp_windows_test.go` guards the non-empty, stable identity contract.
- The manifest runner optimization preserves the semantic test contract: normal mode passed all 109 entries in about 36 seconds locally and race mode passed all 109 in about 62 seconds after one package build per mode and bounded four-worker execution. Each entry still has its own process, exact test timeout, external watchdog, and JSON-output validation. The race-detector exit delay was intentionally not disabled because doing so would change test-effect guarantees.
- The full local race-shard runner passed all 25 jobs in about 5 minutes 13 seconds after isolating the timing-sensitive recovery fixtures; the test-only timeout margins were needed for hosted scheduler/SQLite startup variance, not to mask a production deadline.
- The Amdahl-guided optimization reduced the recovery-manifest critical path by about 44% in normal mode and about 73% in race mode in the measured local runs, while keeping the worker cap to avoid an unbounded process/memory burst. No production listener, persistence, delivery, or hot-path code was changed.
- Amdahl's analysis also bounds whole-workflow gains: even removing the Windows recovery step entirely would leave the Ubuntu core path as the critical path (about a 1.27x workflow ceiling in the measured run). Therefore the selected optimization targets the largest safe serial fraction inside the recovery step while leaving the expensive correctness scenarios intact.
