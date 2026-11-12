# Teams agent CLI runtime validation

## Behavior and scope

The Teams helper supplies its owning stable installation entry in
`CODEX_HELPER_CLI_PATH`, with the containing directory in `CODEX_HELPER_CLI_DIR`.
An agent command enters that installation's current confirmed runtime, even
when the stable dispatcher is older or the helper still runs an earlier runtime.
Standalone helpers retain the existing stable executable resolution.

The core app-server launcher removes the existing ten runtime and boot markers
after environment overlays and the execution-identity callback. Both stdio and
shared WebSocket launch use this boundary. Other environment entries, including
Teams child protection and proxy settings, keep their existing behavior. A nil
configured environment inherits the sanitized parent; an explicit empty
environment stays empty (apart from OS-required values added by Go).

No PATH directory is added or removed. No foreign installation is selected and
no installed executable or shell profile is changed. A missing managed entry is
kept as the explicit failing path, rather than replaced with a version-directory
binary. The bundled skill tells agents to report that path and error.

## Regression evidence

Base revision: `ce75760d69b420d1dfcae05f495a9429972a1bd0`.
Candidate branch: `fix/teams-runtime-child-env-20261112`.
The original implementation failed the new tests: internal markers reached the
actual child process and the advertised CLI path pointed into `versions/`.

The core test re-executes its own inert fixture and records the actual process
environment. It covers both transports, inherited values, overlay values,
configuration callbacks, nil and empty environments, mixed-case markers,
Unicode paths, spaces and retained application environment. It does not launch
a real helper. Native Windows runs use the same tests and fixture.

The black-box scenario builds two production CLIs with different versions. It
installs v9.9.0 as the stable entry, publishes v9.9.1 as the confirmed runtime,
and starts the real Teams entry against fake Graph and fake Codex. The fake
agent records its environment and invokes the supplied entry for version,
Teams read help and a prohibited service restart. The scenario also exercises
the real Bash login-PATH probe. Its Docker account has no login shell, so the
fixture configures `/bin/bash` through the public CLI first.

Assertions check the selected version and command capability, all ten markers,
stable entry/directory, preserved self-management protection, no PATH fallback,
unchanged boot database/publication seal and no nested `.cxp-runtime` under
immutable versions. The help assertion uses the actual `--before` and `--replies`
flags; there is no `--history` flag. A second turn after external rollback to
v9.9.0 checks that the existing agent follows the newly confirmed runtime while
its helper remains running. Existing child-guard E2E tests also cover
stop, pause, restart and upgrade refusal, followed by another answered prompt.

## Test entry points

All commands use Go 1.25.11 with the test-result cache disabled. Real helper
processes run only through the cxp-development ephemeral Docker harness.

```text
go test ./internal/core/codex ./internal/cli -count=1 \
  -run '^Test(StartRemovesRuntimeMarkers|TeamsCodexChildEnv.*|TeamsAppServerReceivesUserPathInsteadOfServiceOrManagedRuntime|TeamsStandardRuntimeRefreshesManagedNodePATHAfterExecutorCreation|ApplyTeamsUserPathRuntime.*|ResolveTeamsCodexUserPath.*)$' -v
go test ./internal/core/codex ./internal/cli ./internal/helperruntime ./internal/userpath ./internal/skills -count=1
go test ./internal/e2e/scenarios/entry -count=1 -run '^TestTeamsAgentUsesStableCLIEntry$' -v
go test ./internal/e2e/scenarios/turns -count=1 -run '^TestCodexChildCannotManageHelper$' -v
go test ./internal/e2e ./internal/skills -count=1
go test ./internal/core/codex -run '^$' -bench '^BenchmarkAppServerRuntimeEnvironment$' -benchmem -count=1
git diff --check
```

The normal CI unit suites execute the package-local regressions on Linux,
macOS and Windows. CI's Linux entry E2E shard executes the new managed-runtime
scenario. The Release workflow also explicitly selects the core environment
and CLI entry tests in Teams runtime preflight on Ubuntu, macOS 15, Windows
2022 and Windows 2025. Cross-compilation alone is not native execution.

Local results: all five affected package suites passed in Docker, as did the
initial managed-entry scenario and existing child-guard scenario. The second
activation turn is also selected by the required CI entry shard. Both new test
groups passed as native Windows executables. Skill tests, the E2E boundary
check, focused vet, workflow lint and whitespace checks passed.

The development sandbox rewrites `HTTPS_PROXY` at process creation. Its local
environment test therefore reports a proxy-value mismatch even after the
runtime-marker assertions pass. The unchanged assertion passes in the isolated
Docker environment and on native Windows. WSL-to-Windows test startup also
requires sandbox permission for its interop socket. These are environment
restrictions, not product failures; neither assertion nor protection is weakened.

## Performance and writes

The added startup work is one environment filter per new app-server. It copies
a bounded list and compares each key against ten names; it does not run once
per message, turn or shell command. It adds no file reads, writes, polling,
persistent cache, boot protocol change or retention requirement. Managed entry
resolution uses the already pinned runtime context, without PATH search.

On the test host, the filter with 64 application entries plus ten markers took
about 2.25 microseconds, 1280 bytes and one allocation per operation. With 512
application entries plus ten markers it took about 14.6 microseconds, 9472 bytes
and one allocation. These are isolated workload measurements, not process
startup latency guarantees. The real entry scenario verifies no boot record
change or nested installation from the agent's read-only CLI commands.

## Rollout limits

The rc.3 Release CI reported a transcript mismatch in the existing
`TestModelSwitchWhileBusy`. Its functional checks passed, but its simulated
crash could interrupt the queued turn's separate control-chat notification.
The test now waits for that notification before the crash. This makes the
required event ordering explicit while retaining the full golden transcript,
the crash, model-persistence assertions and all invariants. A new rc.4 tag is
used for the corrected candidate; the failed rc.3 tag is not moved.

An already running app-server keeps its original environment. This source
change takes effect when the updated helper starts a new app-server through
the normal control-chat lifecycle. No running host helper was restarted,
replaced or updated during development. The immutable runtime and boot protocol,
service registration, user PATH and business-state schema remain unchanged.

The release notes and linked GitHub runs provide final publication and complete
platform gate results. The change does not repair unrelated old PATH installs
or retrospectively change the environment of an existing agent turn.
