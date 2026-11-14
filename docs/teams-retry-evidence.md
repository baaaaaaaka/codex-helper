# Teams automatic recovery

The owner requested the equivalent of a manual retry after OOM or a similar
interruption, with a warning to Codex about the failure. Accepted work resumes
in its existing thread with a recovery instruction. The original task prompt
and attachments are not replayed after acceptance.

## Decisions

| Evidence | Action |
| --- | --- |
| Codex reports `willRetry`, or an executor can still run | Observe; do not start another execution |
| A completed original turn is found | Save and deliver its result |
| A definite temporary capacity rejection before acceptance | Retry under the existing bounded backoff policy |
| A terminal accepted capacity/exhausted-connection failure | Continue in the same thread with failure context |
| Managed executor stopped; original accepted turn remains `inProgress` | Queue an inspection-and-continuation instruction in the same thread |
| Turn lookup fails, turn is absent, status is unknown, or another writer exists | Do not infer permission to execute again |
| User cancellation, changed thread, host takeover, pending newer request, or unread startup chat | Stop or suppress interruption continuation |

For process interruption, the agent must attest that the prior managed executor
stopped and no native client or conversation executor is attached. Restart
recovery requires retained process-registry evidence and successful startup
cleanup. Missing process evidence is not proof. A live process failure can
provide its own observed-exit evidence, including on the first fresh start.
An explicitly interrupted native turn does not by itself authorize continuation:
that state can also represent cancellation from another client.

The prompt tells Codex to inspect completed changes, outputs, checkpoints and
local/remote jobs before continuing. It requires verification before repeating
uncertain external writes, and asks the user when the result cannot be checked.
For memory, OOM or disk pressure, it requires checking resources and changing
the workload or concurrency before repeating the affected work. Diagnostics are
quoted as data. A killed process alone is not reported as a confirmed OOM.
After a helper restart the original root cause may remain unknown; the prompt
says so explicitly. It retains the previous response language.

This is user-authorized model-mediated continuation. It is not a proof that
arbitrary shell commands or remote writes can be repeated exactly once. Remote
jobs and operation-specific idempotency still need their own verification.

## Durable state and limits

Schema version 10 adds `turn_recovery`, with one indexed checkpoint per request.
It records attempt, thread/turn identity, observation time/source, acceptance,
retry owner, decision, diagnostic/error code, due time and deadline. This is the
latest checkpoint, not an append-only audit of every probe or tool effect.
Missing turn, failed lookup and still-active turn remain different observations.

An interruption permits at most three scheduled continuations, with exponential
backoff and an absolute 30-minute scheduling budget from the first continuation.
This does not kill an ongoing long task at the deadline. The existing transient
capacity/connection window remains separate; live progress can renew that window.
Budget, recovery prompt, due time and requeue are committed together under the
store generation fence. Restart does not reset them. Native retry events use the
existing progress rate limit and never schedule a second execution.

Cancellation is committed with the command response before native interrupt.
Its sticky flag survives later probe checkpoints and process restart. Dispatch
checks cancellation, the interruption deadline, the thread binding and the
saved due time. Reconciliation reads the chat before continuing so cancellation
received during downtime can take effect. A failed initial chat read does not
authorize automatic continuation. A newer pending request suppresses automatic
interruption continuation at scheduling time.

Completion closes live retry ownership in the same transaction as result/outbox
creation. Existing Teams outbox and unknown-delivery handling remain unchanged;
this change does not resubmit a completed task to repair message delivery.
Model-switch handoffs and the delegation inbox keep their existing continuation
contracts and do not opt into process-interruption recovery.

## Validation

Workspace: `/home/baka/.local/state/cxp-retry-work/workspace`, branch
`teams-retry-evidence`; fetched upstream `main` base `4fecb03b`. The earlier test results below were
run at base `57d0dd02`; after fast-forwarding, the recovery and Teams read
selection was run again as described below. The development
checkout and installed Teams helper were not changed. Linux amd64, Go 1.26.3;
all test scratch files use `/home/baka/.local/state/cxp-retry-work` via `TMPDIR`.
The candidate was not installed and the host service was not restarted during validation.

`TestRecovery*` covers Codex retry ownership, saved backoff across restart,
completed/failed/active/absent native turns, unknown acceptance, active writers,
interruption warning and original thread/key preservation, repeated crashes,
budget persistence, changed thread, cancellation during lookup, cancellation
committed before native interrupt, and sticky state across reopen. A native fake
app-server test verifies that unknown start failure kills the old executor and
retains process-exit evidence. Unknown protocol status is not mapped to failure.

The startup recovery script requires the new test names and runs them in the
existing Linux/native unit jobs. The race workflow includes the engine, store
and native agent packages. The capacity E2E expectation now requires the recovery
warning, retains same-thread/client/language checks, rejects original prompt
replay and checks that the synthetic prompt is not mirrored as user input.
Model-switch continuation expectations remain unchanged.

After fast-forward, Docker Desktop's WSL CLI link was missing but the Windows
CLI reached the Linux Docker backend. The bundled harness ran with an isolated
empty JSON configuration: the installed configuration uses SQLite and the
current harness requires a JSON file. Its smoke test passed. The first actual
CI E2E shard found outdated golden prompts and an old schema-version assertion;
those were updated, with an explicit check for the new `turn_recovery` table.
Focused real-process tests for all three capacity scenarios and the old-database
upgrade passed in the container. The final isolated run passed `scripts/ci/run_e2e_shard.sh 2` (entry,
turns, chats, notify and delegation), and both modes of
`scripts/ci/test_teams_startup_recovery.sh` including three runs of each
critical startup E2E. Go 1.26.3 was copied into the disposable container; Go's
temporary directory was placed on its executable overlay rather than `/tmp`.
The configured JSON, Codex home and SSH directory were disposable test inputs.

Storage checks use a 10,000-request fixture: recovery reads use the integer
primary key and fifty reads change zero rows. Fifty checkpoint writes leave one
row. Failed transactions roll back and reopen preserves the last committed
checkpoint. No JSON history scan or growing-document rewrite was added. A local `BenchmarkRecoveryAdmission` over 10,001 turns compared the existing
runnable lookup with and without the added checkpoint point-read. Three samples
were 80.1–80.4 microseconds / 14,412–14,414 bytes / 356 allocations without it,
and 91.8–92.5 microseconds / 16,500 bytes / 411 allocations with it. This is
approximately 12 microseconds and 2 KiB extra per request admission, not a
per-poll history scan. Startup
probes are bounded; the dispatch read and completion update are point queries.

Passed locally after fast-forwarding to `4fecb03b`:

- `go test -race ./internal/core/engine ./internal/core/store
  ./internal/core/codexagent -run
  'TestRecovery|TestCapacity|TestReconnect|TestUnknownStartError|TestTeamsRead'
  -count=1 -timeout=4m` (26.288 s / 5.317 s / 1.061 s).
- `go test ./internal/channel/teams -run 'Test.*Read' -count=1 -timeout=4m`.
- `go vet` of engine, store, codex and codexagent, a `CGO_ENABLED=0` source
  build, and compile-only `go test -c` of the turns E2E package.

Earlier local results at base `57d0dd02`:

- Complete package suites: `go test ./internal/core/engine
  ./internal/core/codexagent -count=1 -timeout=8m` (122.671 s / 0.349 s),
  `go test ./internal/core/store -count=1` (53.980 s in the combined run),
  and `go test ./internal/core/codex -count=1 -timeout=3m` (7.664 s).
- After the final separation of interruption and capacity deadlines:
  `go test -race ./internal/core/engine ./internal/core/store
  ./internal/core/codexagent -run
  'TestRecovery|TestCapacity|TestReconnect|TestUnknownStartError'
  -count=1 -timeout=4m` (22.880 s / 5.392 s / 1.085 s).
  The repeated-crash test also rejects replacing the shorter transient window
  with the 30-minute interruption budget.
- Additional controlled cancellation/ownership race selection:
  `-run 'TestRecovery|TestOwnershipLossStopsEngine|TestRetryingPosts|TestCancel|TestSwitch.*Cancel|TestUnknownStartError'`
  (35.062 s / 4.753 s / 2.086 s).
- `go vet ./internal/core/engine ./internal/core/store ./internal/core/codex
  ./internal/core/codexagent`.
- `CGO_ENABLED=0 go build -o ../cxp-candidate ./cmd/codex-proxy`;
  the candidate was not executed or installed.
- `go test -c -o ../turns.test ./internal/e2e/scenarios/turns`;
  compile only, no E2E execution.
- Required CI-name discovery: three store, nine engine and three native agent
  recovery tests exist and match the owning script's selectors.
- `git diff --check` and `bash -n scripts/ci/test_teams_startup_recovery.sh`.

The first full Codex package run failed its existing environment fixture because
inherited lowercase `https_proxy` conflicted with the uppercase value set by the
fixture. That fixture normalizes both spellings when inspecting the child.
The full package passed with only inherited lowercase proxy aliases removed:
`env -u https_proxy -u http_proxy -u all_proxy`. No production or fixture code
was changed to hide this environment dependency. These tests use local fake
app-servers rather than a live model or Teams service. The full engine suite
preceded the final small budget-separation change; the final race selection
above rechecked every affected recovery/capacity/reconnect path.


## Unverified integration paths

The first release candidate's nightly race run exposed an existing unlocked
conversation-kind read during task startup and a 30-second timeout in the
high-concurrency write-pacing test. Task startup now snapshots the conversation
kind and label under the engine mutex. The pacing test allows 90 seconds for
catch-up while retaining its independent rate assertions. Both affected tests
passed three consecutive local race runs. The first candidate's main CI passed,
but its release workflow failed the nightly race partition; that tag is not
reused.

The WSL Docker CLI symlink and socket are absent. The Windows Docker CLI can
access the Linux Docker backend with the required interop permission. The
bundled harness uses a disposable container with a fresh test configuration.
Live Teams delivery, actual OOM injection, remote-board recovery, native
Windows/macOS execution and hosted CI remain unverified. No host helper process
was launched or altered to bypass isolation.
