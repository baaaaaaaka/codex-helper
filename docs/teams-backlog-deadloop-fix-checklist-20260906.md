# Teams backlog dead-loop fix and realistic Docker validation

Date: 2026-09-06

Scope: remove the listener dead-loop in which optional linked-transcript and
history maintenance consume the same phase/SQLite/owner-heartbeat budget as a
durable Teams backlog. Keep the fix narrow, restart-safe, owner-fenced, and
compatible with both the legacy JSON store and the SQLite store. Docker must
replay a read-only copy of the current durable data with fake Graph and Codex
endpoints; it must never use the Teams token, contact Teams, write the source
database, or send a real message.

## Safety boundaries

- [x] Do not merge per-message inbound claim/complete operations.
- [x] Do not advance cursor/seen before durable handling.
- [x] Do not retry an unknown Graph POST result automatically.
- [x] Preserve owner, lease, attempt, process-incarnation, frontier, and CAS fencing.
- [x] Keep one durable frontier per chat; never run head and continuation as independent writers.
- [x] Keep Graph requests outside SQLite transactions.
- [x] Keep ACK, marker, and final as separate semantic outbox rows.
- [x] Do not defer mandatory transcript recovery/source-proof/delete work while deferring only optional discovery and unchanged-tail scans.

## Production changes

- [x] Add a bounded `TeamsOperationalBacklog` store probe. SQLite answers from
  indexed turn/inbound/poll projections and only inspect the small poll JSON
  fallback when the materialized frontier hint is empty; JSON keeps a selected
  compatibility snapshot. The probe must not call the full SQLite state loader.
- [x] Add durable optional-maintenance deferral fields to `ServiceControl`.
  Persist the next probe time and a bounded reason, but do not change pause,
  drain, or control semantics. Add owner-fenced set/clear operations that
  validate the current control lease in the same durable mutation.
- [x] Gate the main-loop optional maintenance after poll and the first queued-turn
  quantum. When queued/persisted/deferred Teams work or an operational poll
  frontier exists, set a short durable probe deadline and skip expensive normal
  linked/history discovery. When the probe finds no backlog, clear the deferral
  and wake the normal maintenance timers.
- [x] Preserve mandatory linked-transcript work during backlog mode: source-proof
  revalidation, source rewrite/recovery fences, pending history/context gaps,
  unresolved execution/completion recovery, and importing/failed checkpoint
  recovery remain eligible. Ordinary unchanged-tail scans and project discovery
  are deferred.
- [x] Preserve mandatory history-watch work during backlog mode: already dirty
  paths, resumable partial/recovery paths, source rewrite/delete probes, and
  their durable CAS updates remain eligible. New project discovery, baseline
  reconciliation, and unchanged paths wait for the wake probe.
- [x] Make the deferred path observable through existing phase/error counters and
  concise diagnostics without adding a hot-loop full-state read or unbounded
  writes.
- [x] Keep concurrent async turns safe: do not use the singleton
  `ServiceOwner.ActiveSessionID/ActiveTurnID` diagnostic slot as an exclusive
  thread-binding lock. Continue to require the immutable owner instance,
  control-lease generation, and running-turn machine/lease fences.

## Unit/store tests

- [x] Probe reports queued/running-related durable work, persisted/deferred
  inbound, and pending/continuation/gap frontiers correctly for JSON and SQLite.
- [x] Probe stays bounded on a large SQLite fixture with corrupt unrelated cold
  rows and does not invoke the full SQLite loader.
- [x] Deferral set/clear is JSON/SQLite equivalent, survives close/reopen, is a
  no-op when unchanged, and does not rewrite control fields unrelated to it.
- [x] A stale machine or stale lease generation cannot set or clear the durable
  deferral; a current owner can do both.
- [x] Poll frontier/pending-page/gap recovery remains visible while deferral is
  active; no cursor/frontier/revision is changed by the probe.

## Listener regression tests

- [x] The existing JSON and SQLite vertical listener tests reproduce the old
  failure with one blocked running turn plus one queued turn and a real history
  tail; after the fix no optional history/linked worker starts while the queue
  remains durable.
- [x] The same listener test releases the executor, drains the queued turn, and
  proves the durable deferral clears and normal history maintenance wakes.
- [x] Add a mandatory linked recovery fixture proving backlog mode still runs a
  source-proof/recovery job.
- [x] Add a mandatory history fixture proving a dirty/deleted/resumable path is
  still processed while ordinary discovery is deferred.
- [x] Add JSON/SQLite thread-binding coverage for concurrent turns under one
  service owner, while retaining the owner-takeover rejection test.
- [x] Run JSON and SQLite variants under `-race`; keep fake Graph/executor only.

## Docker experiment

- [x] Snapshot the current pointer database with SQLite `.backup`, copy current
  Codex session files read-only, and never mount the source writable.
- [x] Use `--network none`, a local fake Graph server, and a local executor; do
  not require or copy a Teams auth token.
- [x] Replay real durable queued inbound rows with original chat/message/body
  data, preserve the real multi-chat distribution/history corpus, and assert
  message accounting rather than a smoke-test fixed count.
- [x] Add a backlog-mode assertion that optional maintenance does not enter while
  durable replay work remains; the JSON/SQLite vertical gate additionally proves
  the durable deferral clears and normal maintenance wakes after drain.
- [x] Exercise restart/reopen of the disposable SQLite copy during replay.
- [x] Exercise fake Graph 429/503/backoff and an unknown POST result; assert no
  unsafe replay or cursor/frontier advance.
- [x] Assert owner generation remains stable, no failed/running/interrupted turn
  is left at the end, all executor runs have durable completion, and no unknown
  Graph route is used.
- [x] Report completed inbound messages/s and executor starts/s separately, with
  phase durations, Graph status/error counts, durable completion counts, and
  source-copy integrity diagnostics.

## Validation and handoff

- [x] Run focused store/listener tests, package tests, race tests, and
  `go test ./... -count=1`.
- [x] Run `git diff --check` and inspect all staged/unstaged/untracked files.
- [x] Build/run the real-data Docker experiment after the focused tests pass.
- [x] If Docker exposes a new code defect, add a regression first, make the
  smallest safe fix, rerun focused tests, then repeat the Docker gate.
- [x] Record exact commands/results and residual uncertainty here before handoff.

## Execution log

- 2026-09-06: Baseline listener regression is red on the current source: both
  JSON and SQLite variants enter `history-watch` while a second durable Teams
  turn remains queued. The Docker replay harness currently reaches only about
  `0.178 inbound/s` and loses the owner generation while history/linked phases
  run; this is the reproduction baseline, not an acceptance result.
- 2026-09-06: Implemented the bounded SQLite/JSON backlog probe, owner-fenced durable optional-maintenance deferral, backlog gate, mandatory linked/history recovery subsets, fair recovery quantums, and the 30-second bounded owner-heartbeat busy retry window. Terminal inbound rows linked to completed/failed/interrupted turns are excluded from the operational backlog so completed work cannot keep the gate open forever.
- 2026-09-06: Added focused store tests for JSON/SQLite backlog semantics, terminal inbound handling, corrupt unrelated cold SQLite rows (1,024 rows), bounded-loader behavior, durable close/reopen, and stale owner fencing. Added JSON/SQLite listener tests that hold one executor turn, keep a second turn queued, prove ordinary history is skipped, prove mandatory linked/history recovery still runs, release the executor, and assert deferral clear plus normal maintenance wake.
- 2026-09-06: Focused commands passed: `go test ./internal/teams/store -run 'Test(TeamsOperationalBacklogAcrossBackends|OptionalMaintenanceDeferralIsDurableAndOwnerFenced|TeamsOperationalBacklogIgnoresQueuedInboundForTerminalTurn)$' -count=1 -timeout=120s`; JSON/SQLite listener gate passed; the corresponding store and listener `-race` commands passed.
- 2026-09-06: Full commands passed: `go test ./internal/teams -count=1 -timeout=25m` (`226.814s`) and `go test ./... -count=1 -timeout=30m` (Teams `245.701s`, store `38.107s`, all packages passed). `git diff --check` passed.
- 2026-09-06: The first final Docker run exposed a stop-boundary defect in the experiment itself: canceling `Listen` after executor return but before the asynchronous durable completion closed one turn as `interrupted` (`48` starts, `47` completed). Added a test-only admission stop/wait boundary, preserving the hard accounting assertion; no production lease/frontier behavior was weakened.
- 2026-09-06: The next exact-window Docker diagnostic exposed a real production defect: with concurrent async workers, a valid same-owner thread bind could fail because another worker had overwritten the singleton owner diagnostic fields. That path became `codex_thread_conflict`/`interrupted`. Removed only that false singleton check; owner identity, lease generation, claimed-turn machine/lease, running status, and thread CAS checks remain enforced. Added the dual-backend regression before repeating the Docker gate.
- 2026-09-06: The first optimized Docker metrics query exposed a test-harness-only problem: a JSON aggregate join over the large copied SQLite database selected a nested-loop plan and could consume a full CPU while holding read pressure. Replaced it with an indexed inbound-ID map plus one bounded turns scan; no production code or durable state semantics were changed. The stop coordinator was also tightened to wait for async admission/completion before canceling the listener, so teardown cannot manufacture an interrupted turn.
- 2026-09-06: Final Docker command: `CXP_TEAMS_DOCKER_REAL_DATA_DURATION=30s scripts/ci/teams_real_data_docker_experiment.sh /home/baka/.local/state/codex-helper/teams/scopes/scope_e0afa741e6fc2975 /home/baka/.codex`. It used a read-only `.backup` snapshot, `--network none`, fake Graph/local executor, no Teams token, and two listener windows separated by SQLite close/reopen. Snapshot: `117652` inbound rows, `7864` queued rows, `7457` replay payloads across `357` chats. Result: `44/44` completed, `failed=0`, `queued=0`, `running=0`, `interrupted=0`, `executor_runs=44`, owner generations `[5659 5660]`, owner changes `0`, poll/history/linked errors and deadlines `0`, fake Graph `429=1`, `503=1`, unknown POST `1` with repeats `0`, ordinary history/linked work while backlog `0/0`. The two one-minute measurement windows completed `29` inbound messages, for `0.483 msg/s` and `0.483 executor/s`; full wall rate including startup/reopen was `0.323 msg/s`.
- 2026-09-06: Post-Docker focused validation passed, including the new JSON/SQLite owner-thread binding race coverage and the owner-fenced schedule batch coverage. The first post-change `go test ./...` run found only two schedule-owner fixture failures caused by a fixed lease timestamp; changed that test-only seed to `time.Now().UTC()` with a one-hour lease, then the focused tests passed.
- 2026-09-06: Final validation passed: `go test ./internal/teams -count=1 -timeout=25m` (`218.737s`) and `go test ./... -count=1 -timeout=30m` (`276.312s` for Teams, `56.311s` for store, all packages passed), followed by `gofmt`, `bash -n scripts/ci/teams_real_data_docker_experiment.sh`, and `git diff --check`.
- 2026-09-06: Residual uncertainty: the Docker Graph and executor are deterministic local stand-ins, so this proves durable lifecycle/fencing/backoff paths but not Microsoft Graph's exact production latency or a real token response. The source scope is live and the helper may write it concurrently; the script reports a source-hash warning when that happens, while the container only reads its disposable snapshot and never writes the source database or sends a message. The run processed 44 of the 7,457 replayable rows, so it is a realistic stress/lifecycle gate, not a complete-drain benchmark; the exact-window throughput is an observed local-executor result, not a production Graph SLA.
