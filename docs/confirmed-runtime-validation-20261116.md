# Confirmed-runtime dispatch validation

## Change and boundaries

Branch: `fix/confirmed-runtime-no-downgrade`, based on upstream main
`db2dd464e7d3a806372a286b05d963855336bcd5`.

Once a boot record selects a confirmed runtime, an invocation executes that
runtime once and returns its result. Profile/configuration/argument failures and
crashes cannot change `good`, run `previous`, or run the bootstrap's embedded
CLI. Missing/invalid executables produce `ErrBootstrapState`, which the real
main entry treats as fatal rather than falling through to its embedded CLI.
Candidate claim, confirmation, cancellation and pre-handoff recovery remain
separate and unchanged. Explicit recovery is not made data-compatible by this
change. No boot/database format or protocol changes are introduced.

The old confirmed-failure E2E expected permanent downgrade; that expectation was
removed because an invocation failure does not establish version corruption or
backward data compatibility. A fixture remains useful for process fault tests,
but the new regression also uses a production CLI behind an installed entry.

## Evidence and entry points

Linux process tests run only through the cxp-development skill's
`docker-real-process-test.sh exec bash .validation/run.sh`, using a synthetic
configuration and empty Codex/SSH fixture directories. No credentials, host
Teams state, service ownership, or inference are needed. The validation driver
is retained outside the source tree with the workspace's test tools.

The driver reverses only the production `boot.go`/`boot_launch.go` patch, builds
the unfixed CLI, and runs the new production-CLI `version-profile` case. It then
reapplies that exact patch and runs the normal safety entry point. The unfixed
case fails with `ordinary CLI error rewrote boot state`, alongside the expected
`profile "version" not found` message. Thus failure is tied to the incident's
persistent side effect, not a build error or an unrelated CLI error.

With the fix, `TestBootE2ERealCLIErrorNeverChangesConfirmedVersion` passes for:

- Positional `version`, an explicit missing profile, unknown flags and malformed
  configuration. The boot snapshot is byte-for-byte unchanged; profile errors
  appear once and no retained-fixture runtime executes.
- A subsequent version query and real `teams service status` read of a current
  Teams store. The status must include a metadata marker stored before the
  failed command; checking only an exit code or database path is insufficient.
- Missing and invalid confirmed executable files, with and without `previous`.
  The production main reports a startup error and does not execute its embedded
  CLI. Boot state stays unchanged.

Executed checks:

- `go test ./internal/helperruntime ./cmd/codex-proxy -count=1`.
- `go test -race ./internal/helperruntime ./internal/update -run '^Test(SafeBoot|TUIAuto)' -count=1`.
- `go vet ./internal/helperruntime ./internal/candidateupdate`.
- `bash scripts/ci/test_tui_update_safety.sh all` in the isolated container:
  focused unit gates, all native `TestBootE2E`/`TestTUIAutoNative` tests, production
  POSIX PTY checks, and the historical upgrade/installer gate.
- Windows/amd64 and macOS/arm64 E2E test binaries cross-compile with
  `CGO_ENABLED=0 go test -tags cxp_boot_e2e ./internal/helperruntime -c`.
- `bash -n scripts/ci/test_tui_update_safety.sh` and `git diff --check`.

All listed Linux behavior gates pass. An additional full-package run after the
safety script initially failed its process-identity assertion because `TMPDIR`
was under `/workspace/codex-helper`: that test prohibits `codex` anywhere in the
observed executable path. Both full package test binaries were then rebuilt
from the same worktree and passed in a second isolated harness invocation with
`TMPDIR=$HOME/cxp-unit-tmp`. No production/test assertion was weakened. The
safety-script log and the successful package-run log are retained separately as
`tools/validation.log` and `tools/unit-validation.log` beside this workspace.
Earlier harness setup failures concerned Docker socket access, an absent
conventional config path, container proxy/Python assumptions and a noexec
`/tmp`. Synthetic configuration and an executable workspace scratch directory
resolved those environment constraints; they were not product failures.

The owning CI job is `tui-update-safety` in `.github/workflows/ci.yml`, which
runs the safety script on Linux, macOS and Windows. The script explicitly checks
selection of the new real-CLI regression before execution. Its historical gate
selects `TestCandidateLegacyNativeReinstallFixesCommandRollback`, which replaces
a pinned rc.104 native bootstrap using the explicit install sequence. The
retained rc.104 runtime understands Teams schema 6; the installed current
runtime must still read the current store marker after the erroneous command.

## I/O and startup review

Confirmed launches retain the same bounded boot-record read and executable
metadata/path checks. They no longer create a readiness listener, token, timer,
supervisor or rollback state write. No business database or history scan is
added to dispatch. Unit and real-CLI tests check unchanged boot bytes after
ordinary failures, including OS execution failures. Candidate writes remain
limited to their existing claim/confirmation path.

The Linux PTY gate observed the confirmed TUI first frame in 14 ms. This is one
functional timing observation, not a before/after benchmark or a cross-platform
performance claim.

## Deployment and limits

A runtime-only upgrade preserves the physical bootstrap, so deploying this fix
requires the platform installer to replace that entry. The historical reinstall
regression covers this supported route; this workspace does not replace the
installed helper or modify its live boot record.

Native Windows/macOS execution and remote GitHub CI have not run in this local
workspace. Cross-compilation does not replace those gates. No commit, push,
release or installed-service update is part of this local fix.
