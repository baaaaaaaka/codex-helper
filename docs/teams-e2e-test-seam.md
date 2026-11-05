# The Teams helper test seam (`CXP_TEST_ONLY_SEAM`)

The black-box E2E suite (`internal/e2e`) runs the release `cxp` binary.
Everything the suite does to cxp is something a user or the OS can do —
flags, environment variables a user sets, files, signals, the CLI — except
one thing: cxp has to talk to a fake Microsoft Graph instead of the real
one, on a test's clock. That is the test seam. It is the only place where
cxp behaves differently for a test, and it lives in one file,
`internal/cli/teams_test_seam.go`.

## The switch

One environment variable, `CXP_TEST_ONLY_SEAM`, holding `key=value` pairs
separated by `;`:

| key | value | effect |
| --- | --- | --- |
| `endpoint` | `http://<loopback>:<port>` (required) | every HTTP request of cxp's Teams HTTP client (Graph, `login.microsoftonline.com`, SharePoint upload URLs, the Workflow webhook) goes to this URL instead; method, path, query, headers and body are unchanged, the original scheme and host travel in `X-Cxp-Test-Original-Scheme` / `X-Cxp-Test-Original-Host` |
| `instance` | any tag | sent as `X-Cxp-Test-Instance` on every such request, so the fake can tell helper instances apart |
| `timing` | `production` (default) or `fast:<duration>` | `fast:250ms` shortens the poll and mirror cadences to multiples of 250 ms (`engine.FastTiming`) and scales the account read rate with it |
| `timing.<Knob>` | a positive duration | shortens one user-visible wait: `SilenceHint`, `SilenceStuck`, `StallLimit`, `DuplicateFor`, `Heartbeat`, `CapacityRetry`, `CapacityBackoff`, `RefusalRecheck`, `Takeover` (the stale bound of the shared-home lease; its other intervals scale with it) |
| `hostname` | a name | the host identity used for the store owner, per-host files and the default machine label, so two helpers in one container act as two hosts sharing one home |

## Why it cannot be enabled by accident

- The variable's name says what it is; nothing else enables the seam (no
  build tag, no config file, no flag).
- cxp refuses to run when it is set but invalid: the endpoint must be a
  plain `http://` URL on a loopback address with a port and no path, and
  unknown keys or knobs are rejected instead of ignored. An empty value is
  an error, not "off".
- It can only send cxp's Microsoft traffic to the same machine; it cannot
  redirect a real helper to another server.
- When it is set, `cxp teams run` prints one line saying so.

## What it must never change

What cxp selects, sends, renders or decides. The seam only changes where
the requests go, how long the waits are, and which host name cxp believes
it runs on. A rewrite of the helper keeps the same seam (the same variable,
keys and knob names, mapped to its own settings) and runs the same suite.

## Former test hooks and where they went

| hook (before) | now |
| --- | --- |
| `go build -tags e2e` (`internal/cli/teams_newcore_e2e.go`), importing the E2E packages into cxp | deleted; the suite runs the release build |
| `E2E_SUT_GRAPH_URL`, `E2E_SUT_INSTANCE` + the sandbox transport with its `E2E-BLOCKED` stderr protocol | seam `endpoint`, `instance`; the fake records any other host as blocked (`offline` invariant) |
| `E2E_SUT_INTERVAL` | seam `timing=fast:<interval>` |
| `E2E_SUT_PRODUCTION_TIMING` | seam `timing=production` |
| `E2E_SUT_TIMING` (any `engine.Timing` field by reflection) | seam `timing.<Knob>`, a fixed list of user-visible waits |
| `E2E_SUT_HOSTNAME` | seam `hostname` |
| `E2E_SUT_REFUSAL_RECHECK` | seam `timing.RefusalRecheck` |
| `E2E_SUT_WORKFLOW_WEBHOOK_URL` + the `Prepare` store hook | the real CLI, `cxp teams workflow enable --webhook-url-file` |
| `E2E_SUT_MACHINE_REGISTRY` | the real flag `--machine-registry` |
| `E2E_SUT_MANAGED_CHILD` | the real (hidden) flag `--managed-service-child` |
| skipping the Codex launch, the self-updater and the Codex upgrader under test | removed; `--auto-update=false` and the fake Codex make them inert as for a user |
| `teamsNewCoreTestOverrides` (HTTP client, timing, host, prepare) | removed; the seam is read in the production path |
| `E2E_SUT` (choose the SUT) | only `cxp` (or unset) is accepted; the in-process `newcore-sut` is deleted |

Environment the suite sets that is **not** a hook: the token caches,
client ids and directories (`HOME`, `XDG_*`, `CODEX_HOME`,
`CODEX_HELPER_*`) are the helper's normal configuration;
`CXP_RUNTIME_DISABLE=1` is a production switch (no runtime prompt
injection); `HTTPS_PROXY=http://127.0.0.1:9` makes any request outside the
seam fail fast. The fake Codex reads `E2E_FAKECODEX_*`, which cxp passes
through like any environment of the Codex it starts.

Harness-only switches (they never reach cxp): `E2E_SUT`,
`E2E_UPDATE_GOLDEN`, `E2E_GOLDEN_OUT`, `E2E_REPORT_DIR`, `E2E_SUT_STACKS`
(SIGQUIT the helper for a stack dump while debugging a hang), and the
opt-in acceptance runs' `E2E_REAL_STORE*` / `E2E_SHADOW_*`.
