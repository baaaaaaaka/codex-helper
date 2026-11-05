// Package e2e is the root of the black-box end-to-end suite of the Teams
// helper. It contains test code only.
//
// The system under test is the release `cxp` binary (./cmd/codex-proxy,
// built without build tags) started as a user starts it (`cxp teams run`),
// and nothing else. The suite reaches it only through:
//
//   - Microsoft Graph and login over HTTP: fakegraph, a strict in-memory
//     Graph (chats, messages, hosted contents, drive uploads, meetings),
//     login.microsoftonline.com and Teams Workflow webhooks, with fault
//     injection and a full request log;
//   - Codex: fakecodex, a fake `codex app-server` binary cxp launches, and
//     rollout files under CODEX_HOME (rollout);
//   - the cxp command line (Scenario.RunCLI / RunCLIAs);
//   - files a user or the OS sees (the store location, the kill-switch
//     file, the conversion report), signals (SIGTERM, SIGKILL, SIGSTOP);
//   - the one documented test seam, CXP_TEST_ONLY_SEAM
//     (docs/teams-e2e-test-seam.md): Microsoft endpoints, instance tag,
//     timing profile, host name.
//
// TestBlackBoxBoundary enforces the boundary on imports: E2E code may
// import only the harness, the fakes, rollout, the standard library and
// third-party packages.
//
// Packages:
//
//   - harness: isolated environment, scenario lifecycle, waiters, transcript
//     goldens (golden.go, normalize.go) and global invariants checked at
//     every teardown (invariants.go);
//   - sut: builds cxp and the fake Codex and starts cxp for the harness;
//   - steps: shared black-box scenario steps;
//   - scenarios/*: the scenario packages, each with testdata/<id>.golden.
//
// Run the scenarios only in the offline Docker runner (no network,
// throwaway HOME), e.g. `go test -count=1 ./internal/e2e/...`. Each scenario
// prints one `E2E-VERDICT {json}` line with its structured result.
// E2E_UPDATE_GOLDEN=1 regenerates the goldens; E2E_GOLDEN_OUT=<dir> writes
// the actual transcripts to <dir> (for a read-only source tree).
package e2e
