# Teams fork process lifetime validation

## Behavior

`Agent.Fork` uses a dedicated, short-lived app-server through `Pool.WithTemporary`. It returns a child only after the creator process has exited. The parent conversation keeps its existing process and can continue working while the child starts in another process. Each concurrent fork gets a separate temporary server.

The existing pool records temporary process identities for crash cleanup. Cleanup runs after success, RPC failure, caller cancellation, or panic. An unconfirmed exit returns an error and retains the pool entry and process record. No new database schema, conversation state, polling loop, automatic retry, or thread ownership transfer is introduced. The existing history snapshot, child binding, Teams import, and announcement ordering remain unchanged.

The native fork explicitly persists the child, omits response history, and defers inherited goal continuation. A missing child ID is an error. An unconfirmed operation no longer claims that nothing was changed.

## Validation

Base revision: upstream `origin/main` at `91542ffed726ce7ef8440b4ce47a734c9b34305a`. Branch: `fix/fork-process-lifetime`. Local tests exercised this change before submission; no installed helper was changed or restarted. Before submission, `git fetch origin main --tags` and `git merge --ff-only origin/main` confirmed that the upstream base had no newer changes.

- Before changing the production path, an offline native Codex 0.167.1 probe confirmed that another process can fork a parent with a running model request. A third process is rejected while the fork creator remains alive and can load the child immediately after creator exit. Both stdio and authenticated WebSocket transports passed.
- The production-path regression test was compiled once with the upstream `Agent.Fork` implementation. Both transports reproduced `already has an active writer`. The same test passed with the fixed implementation, proving that the regression detects the original defect.
- Fixed native tests also check the exact completed-turn cutoff, successful child execution in a separate process, and continued parent execution until explicitly released. They use the real Codex executable and a local synthetic Responses server, with no external account or Teams credentials.
- Codex 0.131.0 also passed the production-path native fork test over stdio (0.28 s). Its npm binary layout differs from 0.167.1; the first compatibility attempts failed only while locating that binary, before any fork request. The corrected run discovers the executable from the installed package.
- Unit cases cover successful cleanup, operation error, cancelled context, panic, concurrent operations, startup failure, retained ownership after unconfirmed shutdown, fork request parameters, and missing child ID.

The Docker run used Go 1.25.11 and Codex 0.167.1. `go test ./internal/core/... ./internal/channel/... ./internal/e2e/fakecodex -count=1 -timeout=15m` passed, including the environment snapshot assertion that conflicted with the host Teams environment. Engine tests took 145.871 s; Codex client tests took 18.821 s; store tests took 42.241 s.

On Linux/WSL with Go 1.26.3, `go test -race ./internal/core/codex ./internal/core/codexagent ./internal/core/engine ./internal/e2e/fakecodex -skip '^TestStartRemovesRuntimeMarkers$' -count=1 -timeout=15m` passed. Engine took 227.753 s, client 45.847 s, agent 6.313 s, and fake Codex 1.011 s. The single skipped existing environment assertion passed in the clean container's complete non-race package run.

`go vet ./internal/core/codex ./internal/core/codexagent ./internal/core/engine`, actionlint v1.7.11, `gofmt`, and `git diff --check` passed. Windows amd64 and macOS arm64 test binaries for both affected client/agent packages cross-compiled with CGO disabled.

The first E2E compilation exceeded the harness container's existing 512-PID limit before scenarios ran. The retry sets `GOFLAGS=-p=1` and `GOMAXPROCS=2`, preserving the container limit and the scenario script's own parallelism. All three `scripts/ci/run_e2e_shard.sh` shards then passed: 185 scenario verdicts and 1,567 checks across 15 scenario packages. Shard 0 passed 51 scenarios / 496 checks; shard 1 passed 50 / 542; shard 2 passed 84 / 529. This includes fork commands and history publication, shared-home ownership, crash/restart recovery, entry/migration, turns, chats, notifications, and delegation. The production-timing entry package took 251.495 s, including its large startup catch-up check. Full logs are retained outside the source worktree at `/home/baka/.local/state/codex-helper/scratch/fork-process-lifetime/e2e-logs.json`. Counts are parsed from the `E2E-VERDICT` records in those logs; the scratch runner's console count used the wrong marker and printed zero without affecting test execution or exit status.

## CI coverage

The pool and request tests run in the existing cross-platform unit suite and core race jobs. Existing black-box command scenarios cover fork while running, history publication, and crash recovery. The nightly `codex-runtime-contract` job now invokes both native fork test packages for the `latest` Codex matrix entry on Linux, macOS, and Windows. The older-version matrix entry also runs the production-path fork test over stdio; its existing title fallback remains covered. `release.yml` invokes this nightly workflow before release.

## Performance and persistence

The new work occurs only for an explicit fork command. Each operation adds one cold app-server startup and a bounded shutdown, with one process-record insert and removal through the existing incremental SQLite store. Cleanup changes only the operation's own key; it does not scan or rewrite the pool or session history. There is no new idle polling or persistent format. `excludeTurns` avoids returning a potentially large inherited history in the fork RPC; the existing one-time snapshot/import remains responsible for Teams history.

## Environment and limits

Tests use a private Git worktree and the bundled Docker real-process harness. The WSL default Docker socket is absent; the available Linux Docker Desktop socket denies this user, and passwordless sudo is unavailable. The existing Windows Docker CLI supplies access to the same Docker Desktop daemon. No socket permissions, daemon settings, container capabilities, or running helper processes were changed. A Go toolchain is copied from the existing build image into the disposable container, under an executable home directory. Runtime containers retain their existing capability restrictions.

An initial host-side unit invocation failed an existing environment snapshot assertion because the Teams-launched test child received the host proxy rather than the fixture proxy. This is separate from the fork defect. The clean container suite includes that assertion; host race execution excludes only `TestStartRemovesRuntimeMarkers` and records the exclusion explicitly.

Windows amd64 and macOS arm64 test packages cross-compile. Native execution on those operating systems and real Microsoft Graph delivery are not part of the local run. The initial local validation did not invoke GitHub CI or publish a release. Submission and prerelease publication use the normal main CI and full Release workflow, including native-platform gates. The development host helper is not deployed or restarted by this change.

## Prerelease follow-up

Commit `db2dd464e7d3a806372a286b05d963855336bcd5` includes the production fix.
Its [main CI](https://github.com/baaaaaaaka/codex-helper/actions/runs/43831875879)
passed all 22 jobs. The first RC attempt, `v0.1.23-rc.40`, was blocked before
packaging by the full-repository race sweep's partition 3:
`TestSwitchCoalescesAndUsesPendingInput` timed out after the model switch was
paused. The [failed Release run](https://github.com/baaaaaaaka/codex-helper/actions/runs/43831960776)
records the failure; its tag is retained rather than moved or reused.

That existing test deliberately holds `InterruptExecution` behind a channel
while processing additional messages, but inherited the fast fixture's 5 s
interrupt deadline. The busy runner exhausted that deadline before stop
processing finished. This path uses an in-process fake and never calls fork.
The follow-up changes only this ordering test's `CancelWait` to one minute;
its condition assertions and controlled gates are unchanged. The individual
condition waits still bound a broken test, and rig cleanup cancels the held
operation. Production timeouts are unchanged. The separate
`TestSwitchUnconfirmedStopPausesWithoutKillOrContinuation` still uses 100 ms
and checks both rejected and unconfirmed interrupts without killing or resuming
the execution. The next candidate uses a new commit and RC tag.

The correction passed the focused race command on Linux/WSL Go 1.26.3:
`go test -race ./internal/core/engine -run '^Test(SwitchCoalescesAndUsesPendingInput|SwitchUnconfirmedStop|BusyModelSwitch)' -count=10 -parallel=16 -timeout=10m -v`
(127.694 s). This exercises the gated coalescing assertions, successful automatic
continuation, and both unconfirmed-stop cases on every iteration. Before the
follow-up commit, another fetch and fast-forward merge check confirmed current
upstream main `db2dd464e7d3a806372a286b05d963855336bcd5` was already included.

## Send-receipt ordering follow-up

The same RC.40 sweep later reported a second existing fixture failure in
partition 1: `TestBreakerHoldsPromptsAndUnpauseDoesNotRunThem` did not observe a
pause. The fake channel exposes a sent answer before the sender commits its
outbox receipt. `classify` intentionally treats a copied hash as self while any
matching outbox row is not `sent`; only a confirmed send copied under a different
message ID is an echo for the breaker. The test copied the visible answer
without waiting for that state, so the copy could be ignored permanently.

The correction waits for the work conversation's `PendingOutbox` to be empty
before posting the three marker-less copies. This API includes queued, sending,
and unknown rows. The test's pause, hold, unpause, fresh-answer, and no-replay
assertions remain unchanged. No production sender, classification, breaker,
retry, timeout, schema, or scheduling behavior changes. The test now observes
the durable precondition instead of relying on the scheduler's speed.

RC.41 was intentionally cancelled before packaging when this additional evidence
arrived; its tag and commit `1a949daa` remain. Cancellation propagated to the
workflow's aggregate jobs; those aggregate failures are not new test failures.
The next candidate includes both fixture corrections and uses a fresh RC tag.

The corrected breaker fixture passed on Linux/WSL Go 1.26.3:
`go test -race ./internal/core/engine -run '^TestBreakerHoldsPromptsAndUnpauseDoesNotRunThem$' -count=10 -parallel=16 -timeout=10m -v`
(88.645 s). Before this follow-up commit, another fetch and fast-forward merge
check confirmed current upstream main `1a949daaa46c50d3de1f9ac5cff8a1fef342de85`
was already included. There are no new production-path performance or disk
writes: the added receipt reads run only inside this test fixture.


## Crash-recovery fixture polling follow-up

RC.42's [main CI](https://github.com/baaaaaaaka/codex-helper/actions/runs/43835662510)
passed all 22 jobs. Its [Release run](https://github.com/baaaaaaaka/codex-helper/actions/runs/43836719561)
found one additional E2E fixture limit: `TestForkHistorySurvivesACrash`
passed every behavioral assertion and all other invariants, but used 5.229834 s
CPU against the unchanged 5 s budget. The 80-record parent import and fork
recovery made 1,798 Graph requests, including 338 chat-list and 1,272 message-list
reads. The fixture's 500 ms polling repeatedly reads growing history faster
than the one-message-per-second write pace. All six native Codex contract jobs
passed on Linux, macOS, and Windows, for latest and 0.131.0.

The correction changes only this fixture's `Interval` to one second, matching
its existing history write pace. It preserves 40 turns (80 records), the
condition-gated crash after four answers, every ordering/deduplication/recovery
assertion, the 5 s CPU budget, rate and no-loop caps, and production timing.
No production code changes. This reduces unnecessary reads instead of raising
a resource limit or shrinking the history workload. RC.42's tag remains fixed;
the correction uses a new commit and RC.43.

Validation uses the bundled ephemeral Docker harness and Go 1.25.11, with
`E2E_SUT=cxp go test ./internal/e2e/scenarios/commands -run '^TestForkHistorySurvivesACrash$' -count=3 -v -timeout=15m`.
The first sandbox invocation could not reach Docker Desktop. The same harness
command succeeded with precise escalation for that interface; this is a sandbox
boundary, not a test or daemon defect. No daemon settings, privileges inside the
container, or host helper processes were changed.

All three repetitions passed in 528.000 s. They used 1.967391 s, 1.986209 s,
and 1.956585 s helper CPU, with 1,052, 1,052, and 1,051 Graph requests. All
behavioral checks and every invariant passed, including the unchanged 5 s CPU
limit. The earlier full local shard's same 500 ms fixture recorded 1,798
requests and 2.635446 s CPU; these are separate runs, not a controlled benchmark.
The request reduction follows from the slower test polling, without reducing
the history records checked. Formatting and `git diff --check` also pass.
