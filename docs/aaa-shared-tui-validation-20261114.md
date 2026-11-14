# Shared TUI AAA regression validation — 2026-11-14

## Behavior and cause

Base: upstream `origin/main` at `57d0dd02`, fetched on 2026-11-14.
Workspace: `fix/shared-tui-aaa` in `/home/baka/.local/state/cxp-aaa-worktree`.

The shared-backend launch returned before constructing the approval broker.
The helper defers approvals to attached native TUI clients, so the persisted
AAA preference was ineffective on this path.

With AAA enabled, the TUI now connects through the existing approval broker,
whose new WebSocket transport attaches to the existing shared app-server.
No second app-server is started. Manual shared attachment remains direct.
Broker shutdown closes its client connection, not the shared backend.
The existing approval handler and supported request types remain unchanged;
the default delay changes from 500 ms to 100 ms. Ordinary questions still
reach the official TUI. The helper process and installed runtime were not changed.

## Executed checks

All Go commands used the validated scratch directory
`/home/baka/.local/state/cxp-aaa-inspect.1yGGaF` for `TMPDIR` and `GOTMPDIR`,
and its `go-cache` subdirectory for `GOCACHE`.

Passed on Linux amd64, with `-count=1`:

```sh
go test -race -count=1 -timeout=5m ./internal/codexrunner -run '^Test(SharedRemoteBroker|RemoteAppServer|RemoteBroker|AutomaticApproval|DefaultApprovalDelay)'
go test -race -count=1 -timeout=5m ./internal/cli -run '^Test(SharedTUIAAA|RunCodex(SessionAttaches|NewSession(UsesShared|IgnoresDifferent|RefusesLegacy)|SessionRefusesMissing|ForkAttaches))'
```

Coverage includes AAA on/off, command/file/permissions/MCP approval requests,
request IDs, the initialization exchange, ordinary questions, authenticated
loopback attachment, rejection of invalid endpoints, disconnect/reconnect,
streaming beyond the old line limit, and cancellation. CLI fixtures cover new,
resume and fork launches, no second writer, lease release, incompatible homes,
legacy New chat handoff, and missing live-helper endpoints.

Regression sensitivity: temporarily disabling the new shared AAA broker made
all three `TestSharedTUIAAALaunch` cases (new/resume/fork) fail with
`AAA bypassed the approval broker`. The implementation was restored and the
final race checks passed. The existing fixed-delay test now explicitly requires
100 ms; this is an intentional contract change requested by the user.

Also passed:

```sh
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build ./internal/codexrunner ./internal/cli
CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build ./internal/codexrunner ./internal/cli
CGO_ENABLED=0 go test -c -o <scratch>/native-aaa.test ./internal/e2e/realcodex
<scratch>/native-aaa.test -test.list '^TestRealCodexAAASharedTUI$'
bash -n scripts/ci/run_shared_codex_contracts.sh
git diff --check
```

The new native acceptance test was listed exactly. CI YAML parsed with PyYAML.
Windows and macOS results are cross-compilation, not native execution.

## CI reachability and remaining validation

The existing `race-core-exec` engine matrix entry now explicitly runs both
focused race commands above. The required `shared-codex-contract` job runs
`scripts/ci/run_shared_codex_contracts.sh` on Codex 0.166.0 and 0.167.1.
That script now also selects `TestRealCodexAAASharedTUI` and requires an actual
PASS line, so an absent or skipped native acceptance test fails the gate.

The native acceptance reuses the existing two-window product scenario with
AAA enabled. It requires an approval-producing turn to complete without
sending an approval key, verifies no approval prompt reaches the TUI, checks
that all app-servers belong to the helper, and closes/resumes a native window.
The manual scenario remains separate and still requires explicit approval.

Initial local real-process validation was blocked because Docker Desktop
was stopped, leaving `/usr/bin/docker` with a missing target. A follow-up on
2026-11-14 started Docker Desktop 29.9.3. Its default WSL integration is disabled,
so the Linux default socket remains unavailable. Windows Docker API access and
an offline Alpine container smoke both passed. Sandbox WSL interop initially
failed with `UtilBindVsockAnyPort`; the narrow approved Windows CLI calls worked.
The guest proxy socket was also denied outside the sandbox (host permissions),
so its permissions were not changed.

The test-only adapter at
`/home/baka/.local/state/cxp-docker-test.o4VkBZ/bin/docker` uses the Windows
Docker CLI and replaces the contract harness's two read-only bind inputs with
copies into fresh offline containers. It copies only compiled tests, CXP and
native Codex, not host credentials, state, sockets or source directories. The
containers are removed on exit. No Docker global integration setting changed.

Executed the repository's `scripts/ci/run_shared_codex_contracts.sh` against
native Codex 0.167.1 using this adapter. Protocol probes, production-client
contracts and production-agent thread-binding contracts passed three rounds.
Gateway recovery and manual New chat acceptance passed. The new AAA acceptance
initially failed: its test wrote `CODEX_HELPER_CONFIG`, but that environment
variable does not select CXP's settings store. The selector was using default
settings, with AAA off. Updated every helper/selector/resume launch in this
scenario to pass `--config` explicitly; no runtime fix was changed.

Reran the affected native TUI gate in a new offline container using
`/home/baka/.local/state/cxp-docker-test.o4VkBZ/run-native-tui.sh`:

- `TestRealCodexGatewayRecoveryTUI`: PASS (2.00 s).
- `TestRealCodexNewChatTUI`: PASS (3.88 s).
- `TestRealCodexAAASharedTUI`: PASS (3.06 s), with no approval key sent.

Logs: `/home/baka/.local/state/cxp-docker-test.o4VkBZ/contracts.log` and
`/home/baka/.local/state/cxp-docker-test.o4VkBZ/native-tui.log`. The initial full
invocation exited nonzero for the test-fixture error; the corrected TUI-only
rerun exited zero. Earlier passing, unaffected contracts were not repeated.
No live helper was restarted, and no source-built helper was run on the host.
The isolated Linux `scripts/ci/run_linux_full_go_test_collect.sh` entry point
passed both the main and migration suites. Its initial fixture needed an
external vendor directory so the formatting check did not inspect third-party
files, and a container-local passwd entry for UID 1000. The first E2E shard
exposed a pre-existing golden assumption: control history can fit without
truncation when temporary paths are shorter. Its mask now normalizes both
valid histories while separate checks still verify the full command response.
The selected 100 ms CLI approval test also passed with race instrumentation.
After the mask fix, `scripts/ci/run_e2e_shard.sh` passed for shards 0, 1 and 2
in fresh isolated Docker execution with `-count=1`; all 15 scenario packages
ran. The unchanged full Linux suite was not repeated after this test-only edit.
The first remote CI run exposed an outdated fake output in
`test_shared_codex_contract_gate.py`: it omitted the new mandatory AAA test.
The fixture now includes it and explicitly checks skipped or missing AAA
results. Its focused test passed, as did the full Python script suite in an
isolated Linux container (149 tests, 9 skipped, including unavailable
PowerShell-dependent cases). Remote CI provides the PowerShell environment.
The first remote E2E shard 1 also exposed a restart-test race: Graph made a
message visible before its send result was committed, so stopping immediately
could produce a legitimate unconfirmed-delivery notice. The scenario now
waits for the existing outbox-status check before stopping. The affected shard
passed again in the isolated Linux container after that test-only change.
The next release run exposed the same restart boundary for a Workflow card:
the fake endpoint accepted it before the helper committed the delivery result.
The webhook scenario now waits for that notification to leave the read-only
pending-notification view before stopping; its full E2E shard passed locally.
The `v0.1.23-rc.19` main CI passed the real native Codex 0.166.0 and
0.167.1 shared-contract jobs; the replacement release's full remote CI
remains to be verified.

The `v0.1.23-rc.20` main CI passed. Its Linux Codex 0.131.0 release
contract observed the approved command but timed out after five seconds
waiting for Codex's asynchronous command analytics; the same version passed
locally, and the macOS and Windows 0.131.0 matrix entries passed remotely.
The contract still requires both user approval and the completed-command
event, but now allows 20 seconds and reads the received events from a
mutex-protected snapshot.

## Memory and disk audit

The added adapter connects once per AAA TUI attachment. It retains one 32 KiB
read buffer and streams frames into the existing bounded broker classifier.
It adds no disk writes, persisted settings, history scans, polling, or retries.

```sh
go test ./internal/codexrunner -run '^$' -bench '^BenchmarkRemoteAppServerReadRecord$' -benchtime=10x -benchmem
```

On this host (AMD Ryzen 9 7950X), the 64 KiB frame workload measured
21,711 ns/op, 237 B/op and 6 allocs/op. The 32 MiB workload measured
5,547,828 ns/op, 285 B/op and 6 allocs/op. These are adapter throughput and
bounded-allocation measurements, not a before/after native-TUI latency claim.
Tests cannot guarantee that no future regression will occur; the new unit,
race, and mandatory native acceptance gates specifically guard this defect.
