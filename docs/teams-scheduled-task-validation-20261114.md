# Teams native scheduled-task adaptation — validation

## Integration candidate

The original worktree below was integrated with upstream main at
`5670e3e7da5b6f2127f167f4c957c06a3bef00dc` using a three-way patch. The
original worktree remains intact. The scheduled regression tests and affected
package race suite were rerun against this integrated candidate. Linux vet was
also rerun against the affected core and channel packages. The same host proxy
environment exception applies to `TestStartRemovesRuntimeMarkers`; the race
suite excludes that pre-existing test and CI runs its normal selection.

Native platform and real-process coverage must be checked on the candidate's
GitHub CI. The host still has no Docker daemon, so no local helper lifecycle
test was substituted. Live Teams delivery and real native Codex scheduling
remain separate, unverified integration paths.

Candidate CI run 43604855415 passed its platform and E2E jobs. Main CI run
43605859080 then failed `TestR09_R10_MirrorTUIRollout`: its golden expected
the control-chat summary for the down-time session, but the snapshot did not
contain that asynchronously delivered message. All scenario checks passed.
The test now waits for that session's summary before comparing the unchanged
golden, so an absent summary still fails rather than being accepted. The
restart scenario package compiled locally with `-run '^$'`; actual lifecycle
execution remains assigned to isolated GitHub CI.

The `rc.31` release run 43608729413 failed the Windows 2022
`TestRecoveryRestartRetainsRejectedBackoff`: its two-second wall-clock
backoff could expire while the engine was stopping and restarting. The test
now freezes the injected clock before persisting the backoff, waits for the
persisted deadline to be restored, checks that the retry has not started,
then advances past the deadline and wakes the scheduler. The changed test
passed ten Linux race runs. In a separate disposable worktree, replacing the
persisted-deadline guard with `false` made the test fail while waiting for the
restored backoff, and the fake agent showed the unintended second call. The
runtime guard and its behavior are unchanged. Native Windows CI must rerun
the test; the `rc.31` release run was cancelled and the tag is not reused.

The `rc.32` release run 43612194227 failed the `CATCHUP-CRASH` golden
because a kill left a send unconfirmed and the existing delivery-safety
notice appeared in the control chat. The scenario's exactly-once, ordering,
and no-mention checks passed. The scenario now accepts that optional notice
only when its complete text names one of the two test sessions, has no
mention, has a unique message reference, and remains bounded by the two
simultaneous sends across each of two kills. Other messages still use the
unchanged golden. The optional-message rendering test and a notice-pattern
test passed locally; actual crash and recovery execution must pass in isolated
GitHub CI. The `rc.32` release run was cancelled and its tag is not reused.

## Scope and user behavior

This change observes native Codex evidence. Codex continues to own execution
and scheduling. CXP does not add a scheduler, a task registry, a database
migration, a background process, or schedule polling.

All new CXP labels and notices are English. User prompts, saved instructions,
and Codex answers keep their original language.

| Evidence and situation | User-visible behavior |
| --- | --- |
| No native scheduling or wait evidence | Existing messages, completion footer, silence hints, and notifications remain in use. |
| Native `sleep` starts in a helper-owned execution | One status gives the maximum duration of that wait, says that input can end it early, and makes no saved-schedule claim. |
| The same execution enters more waits | No further wait-start messages. No repeated `Still working` or stuck notices during a valid wait. |
| A wait ends, or its bounded deadline passes, followed by silence | At most one attention notice for the execution, after the existing silence threshold. Pending progress takes priority. The notice does not stop or retry execution. |
| A helper-owned execution finishes after a native wait | Answer label: `Codex answer after an in-session wait`. Owner notice: `In-session wait result`. |
| A mirrored native scheduled turn | Instructions, status, and result have distinct `Scheduled task` labels. A validated heartbeat supplies its automation ID and exact instruction body. The internal envelope is not displayed. |
| Human input joins a scheduled turn, or a heartbeat joins an ordinary turn | Later output says `with additional input`. Human text is not rewritten as scheduled instructions. |
| A scheduled result is delivered | The owner gets the existing Workflow card or control-chat mention, labelled `Scheduled task result`. The hint says that a result does not confirm the schedule has ended. |
| Workflow delivery has an unknown outcome | Existing one-attempt card handling and durable mention fallback apply. There is no second card attempt. |
| A later ordinary turn, another conversation, or replay after restart | Scheduling evidence stays within its native turn. Existing output keys, checkpoints, and notification rows provide replay deduplication. |

An in-session wait is **not** proof of a saved periodic task. The receipt says:

> Codex entered an in-session wait (up to 10m0s for this wait). New input can
> end a wait early. Further waits in this run will be quiet. This does not
> confirm a saved schedule.

`helper cancel running` stops the current execution through the existing
cancel path. It does not remove an external saved schedule.

The change does not claim schedule creation, deletion, pause, listing, next-run
calculation, restart survival, or an exact future execution time. Those need a
verified native management interface. It also keeps the existing division of
output ownership: the runner owns helper finals, and the mirror owns external
native finals. Schedule attribution for a heartbeat steered into a running
helper execution is not guaranteed on that runner's final. The mirror can
label records it receives; this change does not delay ordinary finals to wait
for optional metadata.

## Evidence, compatibility, and resource bounds

- Wait detection accepts only typed app-server `item/started` and
  `item/completed` sleep items for the confirmed native turn, with an item ID
  and an integer duration from 1 ms through 12 hours. Text and shell `sleep`
  commands do not enable the new policy.
- Items before the start response use one bounded buffer. A completed early
  wait cannot be restored as active. Queued observers exclude earlier work.
- Wait state is two fields on the current execution. Matching completion clears
  its deadline; its last ID remains to reject immediate replay. A removed,
  cancelled, or superseded execution cannot accept new wait state.
- Rollout attribution requires a matching, nonempty turn ID and a recognized
  native automation trigger. Heartbeat instructions also require the native
  `user.heartbeat` marker, one input-text item, and a bounded canonical envelope.
- Optional metadata is parsed separately. Malformed or unknown metadata cannot
  reject an otherwise ordinary record. Ordinary records keep the existing
  projection, including its boilerplate filtering.
- Prompt text used for delivery identity remains the original record text.
  Envelope removal occurs only during rendering. Output snapshots copy the
  small schedule value, so later input cannot relabel earlier output.
- There are no extra history walks, file rewrites, or per-token database writes.
  Wait receipt and attention message use two existing, stable per-turn outbox
  keys. Scheduled finals use existing notification rows and delivery barriers.
  Repeated sleep calls do not create more wait receipt rows.

Implementation base: helper `57d0dd02`, branch
`investigate/codex-scheduled-tasks-20261114`. Native source inspected:
`openai/codex` `336b19ab26`, including the sleep handler, generated app-server
schemas, turn attribution, and host-marked heartbeat parsing. This is source
inspection, not an executed contract against every installed Codex version.

Final Go source/test snapshot: 15 changed files, SHA-256
`e4c50fd735f942583a41774988210e6ba977a8ce1738f186d02630f4f5429488`.
The digest covers sorted relative paths, NUL separators, and file contents;
documentation is excluded.

## Validation

Host: Linux amd64, Go 1.25.11. The isolated worktree, temporary files, and Go
cache are under `/home/baka/.local/state/codex-schedule-review.es2EoAwL`.
The installed Teams helper was not changed or restarted.

Commands use these environment values:

```sh
GOCACHE=/home/baka/.local/state/codex-schedule-review.es2EoAwL/go-cache
TMPDIR=/home/baka/.local/state/codex-schedule-review.es2EoAwL/test-tmp
```

Executed entry points:

```sh
go test ./internal/core/... ./internal/channel/... -count=1 -timeout=10m
go test -race -p=1 -parallel=8 ./internal/core/... ./internal/channel/... \
  -skip '^TestStartRemovesRuntimeMarkers$' -count=1 -timeout=15m -json
go test -race -p=1 -parallel=8 ./internal/core/codex ./internal/core/codexagent \
  ./internal/core/transcript ./internal/core/engine \
  -skip '^TestStartRemovesRuntimeMarkers$' -count=1 -timeout=15m -json
go test -race ./internal/core/engine -run '^TestScheduled' \
  -count=1 -parallel=8 -timeout=3m -json
go vet ./internal/core/codex ./internal/core/codexagent \
  ./internal/core/transcript ./internal/core/engine ./internal/channel/...
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go vet ./internal/core/codex \
  ./internal/core/codexagent ./internal/core/transcript ./internal/core/engine ./internal/channel/...
GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go vet ./internal/core/codex \
  ./internal/core/codexagent ./internal/core/transcript ./internal/core/engine ./internal/channel/...
git diff --check
```

The initial full core/channel run passed all packages except one existing
environment test, `TestStartRemovesRuntimeMarkers`. Its eight failing subcases
expect a fixture proxy value, but each child receives the host's fixed proxy
value. The same test fails on the unchanged base revision and in a narrow
escalated rerun. This is a host execution-environment limitation, not a
scheduling assertion. The race run excludes that one top-level test explicitly;
repository CI selection remains unchanged.

The final affected-package race run passed all four packages: 415 top-level
tests, or 558 named tests when subcases are included. All 14 new top-level
`TestScheduled*` tests ran, with 47 named tests including subcases; none of
these new tests was skipped. The final text-only change makes the attention
notice say only that a result has not been reported, without treating the
deadline as proof of native completion. All six scheduled engine tests were
rerun under race instrumentation after that wording change and passed.

The broader core/channel race sweep passed every other package, including
store and migration tests. That sweep initially failed the heartbeat fixture
described below; the corrected engine passed its full race rerun. The initial
full non-race sweep had only the existing environment-test failure described
above. Native-runtime contract tests that require `E2E_REAL_CODEX` remain
environment-gated and were not executed.

Linux amd64, Windows amd64, and macOS arm64 vet passed for all changed core
packages and channel adapters. The final Linux CLI build passed; the binary
was not started. Formatting and diff checks passed. No dependency or existing
golden-file change was needed.

New regression coverage checks malformed evidence, extra content, source
isolation, ordinary Chinese text, distinct status/result labels, mixed input,
stable identity and checkpoint reconstruction. In-process engine tests use
real SQLite with fake channel and agent adapters. They cover wait suppression,
completion, expiry, replay, concurrent ordinary conversations, later ordinary
turns, Workflow success/fallback, and restart deduplication. Wire-order tests
explicitly put sleep events before start acknowledgements, including queue
dispatch; they do not depend on repeated runs to create that order.

Tests found and corrected a hidden heartbeat caused by the ordinary
boilerplate filter, a control-notification path that lost the scheduled source,
repeated wait receipt risk, and buffered inputs acquiring later instructions.
A separate disposable source copy deliberately restores the last defect:
`TestScheduledBufferedPromptsKeepTheirOwnInstructions` fails with the first
prompt taking the second task's ID and body. The candidate passes that test
under race instrumentation. No existing behavior expectation or golden file
was weakened to make the new tests pass. A test-fixture error also labelled
the initial environment record as a heartbeat; the fixture now changes only
the intended user input, and the single-instruction assertion remains intact.

## Performance audit

Compared the unchanged base worktree with the candidate, three samples each:

```sh
go test ./internal/core/transcript -run '^$' \
  -bench 'Benchmark(TrackerProgressHistory|ParseLargeToolOutput)$' -benchmem -count=3
```

| Workload | Base median | Candidate median | Allocations |
| --- | --- | --- | --- |
| 1,000 ordinary commentary records with prompt and final | 2.947 ms, 1,881,105 B | 3.029 ms, 1,878,818 B | 29,908 for both |
| Parse a 4 MiB ignored tool-output record | 494.1 ns, 59 B | 483.6 ns, 59 B | 4 for both |

The first workload adds about 0.082 ms per 1,000 records in this microbenchmark.
There is no allocation-count increase, whole-file rescan, or tool-output decode.
The comparison is local and does not establish production latency or throughput.
An earlier noisy baseline was repeated after build and vet jobs finished; the
table uses the three idle baseline samples and three final candidate samples.
The initial implementation used larger common record/output structures; the
audit led to separate optional parsing and smaller output metadata.

## CI reachability and unverified paths

- `CI / Unit tests (ubuntu-latest)` invokes
  `scripts/ci/run_linux_full_go_test_collect.sh`; its package list includes all
  changed core packages. Only E2E scenarios and a named unrelated migration
  process test are excluded there. All `TestScheduled*` tests are selected.
- CI Windows/macOS unit shards include the same packages. Core/channel race
  jobs invoke `scripts/ci/race_core_packages.sh`; engine tests are in its
  `engine` group and codex/transcript/codexagent tests are in `store`.
- `Release / Teams core, store migration, and channel adapter regressions`
  invokes `go test ./internal/core/... ./internal/channel/... -count=1` on its
  native-platform matrix. No extra workflow or selective test-name gate is needed.
- Docker has no reachable daemon socket at `/var/run/docker.sock` on this host.
  Per the development skill, no source-built helper lifecycle test was run on
  the host as a substitute. Real helper E2E, installed native Codex inference,
  and live Teams delivery remain unverified. Cross-platform vet is not native
  Windows/macOS execution. GitHub CI has not been triggered for this unsubmitted
  worktree.

## Write-priority test follow-up

PR CI run 43613467156 failed two existing macOS pacing tests: a progress
worker reached the fake channel before a previously admitted final, and five
catch-up chat creations were observed after the `new` command was received.
The sender admits jobs in priority order but runs up to eight channel workers
concurrently; observed channel call order is not the same as admission order.
The interval from receiving a command also includes command processing before
the foreground writer begins waiting for a token.

The final-versus-progress test now uses one sender so the channel order checks
the actual admission order. The chat-creation test keeps concurrent senders
and treats the configured sender count as a responsiveness budget, rather than
claiming that the observed count proves exact priority. A new controlled test
pauses a registered foreground writer, makes a token available, and verifies
that the background sender cannot take it and the foreground writer does.
Other tests continue to exercise concurrent sender pacing.

On Linux, the controlled test passed 20 times with the race detector; the two
end-to-end pacing tests passed 15 times. In a separate disposable worktree,
removing the foreground-waiter check made the controlled test fail, and
reversing the final/progress classes made the single-sender test fail. Removing
the foreground-waiter check did not make the chat-creation integration test
fail in one trial; the controlled test, not that timing-dependent trial, is
the evidence for the exact next-token guarantee. Native macOS behavior requires
the PR CI run.
