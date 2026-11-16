# E2E safety checks and coverage boundaries

## Behavior and scope

This change extends the confirmed-runtime rollback fix in this workspace.
It changes test infrastructure, fixtures, CI selection and documentation; it
adds no new production update or retry behavior. The installed Teams helper is
not changed or restarted.

- `CODEX-UPGRADE` covers the explicit-path refusal/manual-retry branch.
  `CODEX-AUTO-UPGRADE` and `CODEX-AUTO-UPGRADE-FAIL` own the automatic-upgrade
  guarantee. `CODEX-MANUAL-UPGRADE` covers a successful `codex update now`.
- These new scenarios use production CLI wiring, source detection, the embedded
  POSIX installer, real npm, version probes, persistent restart handling and
  real app-server child processes. Only the npm registry and Codex model are
  local fixtures. They check installed bytes and executable/process identities,
  not only success text. Upgrading never automatically retries failed work.
- The download-failure case must reach the package request, leave the previous
  executable byte-for-byte intact, avoid a restart, and still answer new work.
- A running, successful scenario must settle before the global idle check.
  Intentionally active scenarios need an explicit `idle-quiet` waiver with a
  reason. Already-stopped helpers and failed scenarios retain a distinct
  `not checked` result; a normal settle timeout is a failure.
- The behavior catalog checks declarations and links. Its summary now separates
  implementation status from E2E links and does not call either branch coverage.
  The default business adapter still models service restarts and disables
  runtime dispatch; native bootstrap/service gates remain separate.

## SSH contract correction

The orphaned `TestInitProfileHostKeyRetryIntegration` described an obsolete
interactive host-key retry, which production no longer supports. Its replacement
`TestInitProfileHostKeyFirstUseIntegration` selects a real SSH config host,
connects through production tunnel code, verifies that the first key is saved,
and verifies that a second connection leaves the host file unchanged.

The Linux nightly SSH job enables `SSH_TEST_ENABLED=1 SSH_HOSTKEY_INIT_TEST=1`
and selects the exact new test. Explicit opt-in with an incomplete fixture now
fails rather than skips. The job also requires an actual JSON test-pass event: missing
selection and runtime skips cannot count as success. A workflow-execution unit
test injects pass, skip, missing-selection and failure outputs into that step.

## Validation record

Source: upstream baseline `db2dd464e7d3a806372a286b05d963855336bcd5`, branch
`fix/confirmed-runtime-no-downgrade`, including the preceding uncommitted runtime
fix and this test-infrastructure extension. The preceding rollback regression
and native installer results are in `confirmed-runtime-validation-20261116.md`.

Real processes run inside the cxp-development skill's ephemeral Docker harness,
with synthetic config and empty Codex/SSH fixture directories. The external
fixture uses a local registry; no real inference or host helper state is used.

Completed checks:

- Harness, fake Codex and behavior catalog unit tests, including required idle
  failure, explicit waiver and stopped-helper distinctions; focused `go vet`.
- Three managed Codex upgrade E2Es with real Node 22/npm and executable packages:
  automatic success, manual success and package-download failure/recovery.
- Linux first-use SSH integration against a real OpenSSH server, extracted into
  the disposable container's user directory. First and repeat connections pass.
- Negative SSH fixture check: explicit opt-in with missing fixture exits FAIL.
- Workflow guard rejects skip, missing selection and failure, and accepts PASS.

The container setup first encountered an unsupported Python tar API and a host
proxy address unreachable from the container. Test-fixture iterations exposed
an unset disposable-account login shell and npm's refusal to overwrite a loose
non-npm executable. The fixtures now use the public shell override and an actual
npm package layout. These setup failures are not classified as product defects;
upgrade assertions require a real package download to avoid false positives.

Final checks from the same source worktree:

- All three owning CI entry points, `bash scripts/ci/run_e2e_shard.sh 0`, `1`
  and `2`, pass in the isolated Linux container: 188 scenario verdicts, all
  passing, across 15 packages (186 distinct scenario IDs; two IDs run more than
  once). No new idle waivers were needed. Idle observation ran for 170 verdicts;
  18 explicitly report no helper running at teardown. The two opt-in personal
  real-store/shadow-data tests skip because their datasets are not supplied;
  they are not counted as executed scenario verdicts.
- `go test -tags cxp_boot_e2e ./internal/helperruntime -run
  '^TestBootE2ERealCLIErrorNeverChangesConfirmedVersion$' -count=1 -timeout=3m -v`
  passes, including unchanged boot state, no replay, subsequent version and
  Teams database reads, and unavailable-executable errors.
- `go test ./internal/helperruntime ./cmd/codex-proxy -count=1 -timeout=3m`
  passes in that container.
- The real OpenSSH run also passes with `go test -json`; the exact named test
  emits an `Action=pass` event despite interactive prompts without line endings.
- All 26 `test_ci_targeted_shards.py` tests pass. The updated structured SSH
  pass/skip/missing/fail guard was rerun after its final edit and passes.
- CI helper script tests: seven pass; two PowerShell tests are explicitly
  skipped because PowerShell is unavailable on this Linux host. All Bash CI
  scripts parse. `git diff --check` passes.
- `go vet` passes for the changed harness, adapter, lifecycle and CLI packages.

Logs are retained beside the workspace under `../tools/`:
`e2e-all-shards-validation.log`, `e2e-upgrade-validation-3.log`,
`ssh-hostkey-final-validation.log`, `final-rollback-validation.log`,
`ci-shard-validation.log` and `ci-helper-validation.log`.

These are local Linux results through the CI scripts, not GitHub Actions run
results. New POSIX installer scenarios have not been executed natively on macOS
or Windows; Windows explicitly skips them and Linux CI owns them. The separate
native bootstrap gates and their earlier cross-compilation limits are unchanged.
No commit, push, release, installation or live-service restart is part of this
validation.

## Upstream synchronization before submission

Fetched and fast-forwarded to upstream main
`85518355523386464dd0b6f5f0620484ce9c8fb1`, preserving this change set. Its three
new commits change existing test fixtures and release notes only; there are no
overlapping files or additional production changes. Rechecked upstream before
submission and confirmed the same main revision.

Post-merge checks through a fresh isolated Docker harness all pass:

- `go test ./internal/core/engine -run
  '^Test(BusyModelSwitchAutomaticallyContinues|BreakerHoldsPromptsAndUnpauseDoesNotRunThem)$'
  -count=1 -timeout=3m` (8.178 s).
- `E2E_SUT=cxp go test ./internal/e2e/scenarios/commands -run
  '^TestForkHistorySurvivesACrash$' -count=1 -timeout=4m -v` (177.656 s),
  including every original behavior and resource invariant.
- `go test ./internal/helperruntime ./cmd/codex-proxy -count=1 -timeout=3m`.
- `actionlint .github/workflows/ci.yml .github/workflows/nightly.yml` and
  `git diff --check` pass on the host.

The process-test log is `../tools/post-merge-validation.log`. The fresh
container was destroyed after successful completion. GitHub native platform
validation remains a required subsequent gate before merging and tagging.

The first PR CI run found that the new bootstrap/store regressions used
`teams service status`, whose exit code also depends on a native service being
installed. GitHub's Linux/macOS runners correctly read the database marker but
returned missing-service errors. The final tests use the public read-only
`teams status` command for the same current-store marker and path assertions.
This removes the unrelated service-installation assumption without accepting
nonzero command results or dropping database/dispatch checks. The original
local results above describe the pre-submission test command; subsequent
native PR runs validate this correction before merge.

After that correction, `bash scripts/ci/test_tui_update_safety.sh all` passes
again in a fresh isolated Linux container, including the real-CLI current-store
checks, full native bootstrap suite, production PTY checks and pinned historical
reinstall test. Log: `../tools/read-only-status-validation.log`.

## Native CI and RC follow-up

PR #179 CI run `43855329544` passes all 22 jobs on commit `4fe7a986`, after
the store-status fixture correction. Downloaded native logs confirm actual
PASS events for both the real CLI and pinned-bootstrap reinstall regressions
on Linux, macOS 15, Windows 2022 and Windows 2025. All three E2E shards pass;
each new managed npm upgrade scenario emits an actual PASS event.

After the exact validated commit was fast-forwarded to main, CI run
`43857173836` found an existing macOS timing assumption in
`TestReadThrottleResumesWithActivityScan`. The test enabled a 429 after creating
the control chat, before its first read necessarily finished. Production
`scanLoop` deliberately yields to the first control read to avoid startup
deadlock, covered separately by
`TestControlFirstReadAfterAThrottleWithTheScanWaiting`. The recovery test must
first observe `controlReadLocked`, then inject its post-startup throttle.
The fix adds that bounded state wait; scan-first and read-count assertions,
production behavior and timeouts remain unchanged.

RC.44 Release run `43857227338` was cancelled before packaging/publication;
its tag is retained. A fresh commit and RC.45 carry this test correction.
Both startup/recovery tests pass ten race-enabled iterations with Go 1.25.11,
`-parallel=16 -count=10 -timeout=5m`, on the local CPU-only unit-test host.
No source-built helper lifecycle process is launched by those unit tests.
Log: `../tools/read-recovery-unit-race.log`. The first attempted Docker race
command did not execute tests because that image defaults to CGO disabled;
it is not counted as a passing gate.
